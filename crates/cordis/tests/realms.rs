//! Realms and interception: `isolate` gives a key another binding for the fibers under a
//! scope, and `intercept` changes how they use the binding they reach.

use std::sync::{Arc, Mutex};

use cordis::{App, Catalog, Component, Cx, LoadError, Loader, Merge, Realm, Result, Status, component, key};
use serde::{Deserialize, Serialize};
use serde_json::json;

// Tools: a named registry, so a consumer can tell which provider it reached.

pub trait Tools: Send + Sync {
    fn owner(&self) -> String;
}
key!(dyn Tools = "tools");

#[derive(Serialize, Deserialize)]
struct Named {
    name: String,
}

#[component(config = Named, provides(dyn Tools))]
struct Registry(String);

impl Tools for Registry {
    fn owner(&self) -> String {
        self.0.clone()
    }
}

impl Component for Registry {
    async fn activate(_: Cx<Self>, config: Named) -> Result<Self> {
        Ok(Self(config.name))
    }
}

static SEEN: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

fn seen(agent: &str) -> Vec<String> {
    SEEN.lock().unwrap().iter().filter(|(name, _)| name == agent).map(|(_, owner)| owner.clone()).collect()
}

/// Records which registry it activated against.
#[component(config = Named, deps(tools: dyn Tools))]
struct Agent {
    name: String,
}

impl Component for Agent {
    async fn activate(cx: Cx<Self>, config: Named) -> Result<Self> {
        SEEN.lock().unwrap().push((config.name.clone(), cx.deps().tools.owner()));
        Ok(Self { name: config.name })
    }

    async fn deactivate(self) {
        SEEN.lock().unwrap().push((self.name, "down".into()));
    }
}

/// Starts its own registry and agent in a fresh realm for `dyn Tools`, and a second agent
/// outside it.
#[component(config = Named)]
struct Reviewer;

impl Component for Reviewer {
    async fn activate(cx: Cx<Self>, config: Named) -> Result<Self> {
        let sub = cx.isolate::<dyn Tools>();
        sub.plugin::<Registry>(Named { name: format!("{}-registry", config.name) }).await?;
        sub.plugin::<Agent>(Named { name: format!("{}-inner", config.name) }).await?;
        cx.plugin::<Agent>(Named { name: format!("{}-outer", config.name) }).await?;
        Ok(Reviewer)
    }
}

fn named(name: &str) -> serde_json::Value {
    json!({ "name": name })
}

#[tokio::test]
async fn an_isolated_key_resolves_to_the_provider_inside_the_realm() {
    let app = App::new();
    app.insert::<Registry>(named("root")).await;
    let reviewer = app.insert::<Reviewer>(named("iso")).await;
    app.settled().await;

    // Two providers of one key run at once, one per realm.
    assert_eq!(seen("iso-inner"), ["iso-registry"]);
    assert_eq!(seen("iso-outer"), ["root"]);
    assert_eq!(app.get::<dyn Tools>().await.unwrap().owner(), "root");

    app.remove(reviewer);
    app.settled().await;
    assert_eq!(seen("iso-inner"), ["iso-registry", "down"]);
    assert_eq!(app.get::<dyn Tools>().await.unwrap().owner(), "root");
}

#[tokio::test]
async fn a_consumer_in_a_realm_without_a_provider_waits() {
    let app = App::new();
    app.insert::<Registry>(named("shared")).await;
    let alone = app.isolate::<dyn Tools>().plugin::<Agent>(named("alone")).await.unwrap();
    app.settled().await;
    assert_eq!(alone.status().await, Some(Status::Inactive));
    assert!(seen("alone").is_empty());
}

#[tokio::test]
async fn a_named_realm_is_shared_by_every_scope_that_names_it() {
    let app = App::new();
    let tenant = Realm::named("tenant-a");
    let registry = app.isolate_in::<dyn Tools>(&tenant).plugin::<Registry>(named("tenant-a")).await.unwrap();
    app.isolate_in::<dyn Tools>(&Realm::named("tenant-a")).plugin::<Agent>(named("tenant-agent")).await.unwrap();
    app.isolate_in::<dyn Tools>(&Realm::named("tenant-b")).plugin::<Agent>(named("other-agent")).await.unwrap();
    app.settled().await;

    assert_eq!(seen("tenant-agent"), ["tenant-a"]);
    assert!(seen("other-agent").is_empty());
    assert_eq!(app.isolate_in::<dyn Tools>(&tenant).get::<dyn Tools>().await.unwrap().owner(), "tenant-a");
    assert!(app.get::<dyn Tools>().await.is_none());

    // Withdrawing the realm's provider unloads only the consumers inside it.
    registry.dispose();
    app.settled().await;
    assert_eq!(seen("tenant-agent"), ["tenant-a", "down"]);
}

/// Instantiates an agent through its own `Cx`, so the agent inherits this fiber's context.
#[component(config = Named)]
struct Spawner;

impl Component for Spawner {
    async fn activate(cx: Cx<Self>, config: Named) -> Result<Self> {
        cx.plugin::<Agent>(config).await?;
        Ok(Spawner)
    }
}

#[tokio::test]
async fn descendants_inherit_the_realms_of_their_scope() {
    let app = App::new();
    app.insert::<Registry>(named("root")).await;
    let realm = Realm::new();
    let sub = app.isolate_in::<dyn Tools>(&realm);
    sub.plugin::<Registry>(named("private")).await.unwrap();
    sub.plugin::<Spawner>(named("grandchild")).await.unwrap();
    // `Realm::shared` undoes the inherited isolation.
    sub.isolate_in::<dyn Tools>(&Realm::shared()).plugin::<Spawner>(named("rejoined")).await.unwrap();
    app.settled().await;

    assert_eq!(seen("grandchild"), ["private"]);
    assert_eq!(seen("rejoined"), ["root"]);
}

// Interception: a file system whose metadata sets a path prefix and read-only mode.

pub trait Fs: Send + Sync {
    fn describe(&self) -> String;
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FsMeta {
    prefix: Option<String>,
    read_only: Option<bool>,
}

impl Merge for FsMeta {
    fn merge(&mut self, right: Self) {
        self.prefix.merge(right.prefix);
        self.read_only.merge(right.read_only);
    }
}

static BINDS: Mutex<Vec<FsMeta>> = Mutex::new(Vec::new());

struct Bound(FsMeta);

impl Fs for Bound {
    fn describe(&self) -> String {
        format!(
            "{}:{}",
            self.0.prefix.as_deref().unwrap_or("/"),
            if self.0.read_only == Some(true) { "ro" } else { "rw" }
        )
    }
}

key!(dyn Fs = "fs", meta = FsMeta, bind = |_, meta| {
    BINDS.lock().unwrap().push(meta.clone());
    Arc::new(Bound(meta.clone()))
});

#[component(provides(dyn Fs))]
struct LocalFs;

impl Fs for LocalFs {
    fn describe(&self) -> String {
        "raw".into()
    }
}

impl Component for LocalFs {
    async fn activate(_: Cx<Self>, _: ()) -> Result<Self> {
        Ok(LocalFs)
    }
}

static VIEWS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

fn view(reader: &str) -> String {
    VIEWS.lock().unwrap().iter().rev().find(|(name, _)| name == reader).unwrap().1.clone()
}

/// Declares `prefix: /work` for its file system.
#[component(config = Named, deps(fs: dyn Fs = FsMeta { prefix: Some("/work".into()), read_only: None }))]
struct Reader;

impl Component for Reader {
    async fn activate(cx: Cx<Self>, config: Named) -> Result<Self> {
        VIEWS.lock().unwrap().push((config.name, cx.deps().fs.describe()));
        Ok(Reader)
    }
}

#[component(config = Named, deps(fs: dyn Fs))]
struct PlainReader;

impl Component for PlainReader {
    async fn activate(cx: Cx<Self>, config: Named) -> Result<Self> {
        VIEWS.lock().unwrap().push((config.name, cx.deps().fs.describe()));
        Ok(PlainReader)
    }
}

/// Runs a reader under read-only interception.
#[component(config = Named)]
struct Sandbox;

impl Component for Sandbox {
    async fn activate(cx: Cx<Self>, config: Named) -> Result<Self> {
        cx.intercept::<dyn Fs>(FsMeta { read_only: Some(true), ..FsMeta::default() }).plugin::<Reader>(config).await?;
        Ok(Sandbox)
    }
}

#[tokio::test]
async fn context_metadata_merges_over_declared_metadata() {
    let app = App::new();
    app.insert::<LocalFs>(json!(null)).await;
    app.insert::<PlainReader>(named("plain")).await;
    app.insert::<Reader>(named("declared")).await;
    app.insert::<Sandbox>(named("sandboxed")).await;
    // The context's prefix takes priority over the declared one, and later intercepts over
    // earlier ones.
    let tmp = FsMeta { prefix: Some("/tmp".into()), read_only: Some(true) };
    app.intercept::<dyn Fs>(tmp)
        .intercept::<dyn Fs>(FsMeta { read_only: Some(false), ..FsMeta::default() })
        .plugin::<Reader>(named("overridden"))
        .await
        .unwrap();
    app.settled().await;

    // Without metadata the consumer receives the provider as is.
    assert_eq!(view("plain"), "raw");
    assert_eq!(view("declared"), "/work:rw");
    assert_eq!(view("sandboxed"), "/work:ro");
    assert_eq!(view("overridden"), "/tmp:rw");
    assert!(!BINDS.lock().unwrap().contains(&FsMeta::default()));
}

#[tokio::test]
async fn a_child_inherits_its_parents_interception() {
    let app = App::new();
    app.insert::<LocalFs>(json!(null)).await;
    app.intercept::<dyn Fs>(FsMeta { prefix: Some("/jail".into()), read_only: None })
        .plugin::<Sandbox>(named("nested"))
        .await
        .unwrap();
    app.settled().await;
    assert_eq!(view("nested"), "/jail:ro");
}

// Configuration: rows name realms by key.

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.register::<Registry>("registry").register::<Agent>("agent");
    catalog
}

#[tokio::test]
async fn the_loader_places_rows_in_named_realms() {
    let app = App::new();
    let mut loader = Loader::new(app.clone(), catalog());
    let tree = r#"
plugins:
  - { id: shared, use: registry, config: { name: loader-shared } }
  - { id: sandbox, use: registry, config: { name: loader-sandbox }, isolate: { tools: sandbox } }
  - { id: a, use: agent, config: { name: loader-a } }
  - { id: b, use: agent, config: { name: loader-b }, isolate: { tools: sandbox } }
"#;
    loader.apply_yaml(tree).await.unwrap();
    app.settled().await;
    assert_eq!(seen("loader-a"), ["loader-shared"]);
    assert_eq!(seen("loader-b"), ["loader-sandbox"]);

    // Moving a row to another realm rebuilds its fiber there.
    let b = loader.fiber("b").unwrap();
    loader
        .apply_yaml(
            &tree.replace("config: { name: loader-b }, isolate: { tools: sandbox }", "config: { name: loader-b }"),
        )
        .await
        .unwrap();
    app.settled().await;
    assert_eq!(app.status(b).await, None);
    assert_eq!(seen("loader-b"), ["loader-sandbox", "down", "loader-shared"]);
}

#[tokio::test]
async fn the_loader_checks_providers_within_each_realm() {
    let mut loader = Loader::new(App::new(), catalog());
    let missing = "plugins:
  - { id: shared, use: registry, config: { name: x } }
  - { id: b, use: agent, config: { name: y }, isolate: { tools: sandbox } }
";
    match loader.apply_yaml(missing).await {
        Err(LoadError::MissingProvider { key: "tools", realm: Some(realm), .. }) => assert_eq!(realm, "sandbox"),
        other => panic!("expected a missing provider, got {other:?}"),
    }
    let unknown = "plugins:\n  - { id: a, use: agent, config: { name: z }, isolate: { nope: x } }\n";
    assert!(matches!(loader.apply_yaml(unknown).await, Err(LoadError::UnknownKey { .. })));
}
