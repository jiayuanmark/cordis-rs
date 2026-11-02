//! Realms and interception: `isolate` gives a key another binding for the fibers under a
//! scope, and `intercept` changes how they use the binding they reach.

use std::sync::{Arc, Mutex};

use cordis::{App, Catalog, Component, Cx, LoadError, LoadWarning, Loader, Realm, Result, Status, component, key};
use serde::{Deserialize, Serialize};
use serde_json::json;

// Tools: a named registry, so a consumer can tell which provider it reached.

pub trait Tools: Send + Sync {
    fn owner(&self) -> String;
}
key!(dyn Tools = "tools");

#[derive(Clone, Serialize, Deserialize)]
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
struct Agent;

impl Component for Agent {
    async fn activate(cx: Cx<Self>, config: Named) -> Result<Self> {
        let name = config.name;
        SEEN.lock().unwrap().push((name.clone(), cx.deps().tools.owner()));
        cx.on_dispose(move || async move { SEEN.lock().unwrap().push((name, "down".into())) });
        Ok(Agent)
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

fn named(name: &str) -> Named {
    Named { name: name.into() }
}

fn named_json(name: &str) -> serde_json::Value {
    json!({ "name": name })
}

#[tokio::test]
async fn an_isolated_key_resolves_to_the_provider_inside_the_realm() {
    let app = App::new();
    app.insert::<Registry>(named_json("root")).await;
    let reviewer = app.insert::<Reviewer>(named_json("iso")).await;
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
    app.insert::<Registry>(named_json("shared")).await;
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
    assert_eq!(app.get_in::<dyn Tools>(&tenant).await.unwrap().owner(), "tenant-a");
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
    app.insert::<Registry>(named_json("root")).await;
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

// Interception: consumers under a scope receive a remapped view of the shared file system.

/// An ordinary service trait: nothing in it exists for interception.
pub trait Fs: Send + Sync {
    fn describe(&self) -> String;
}
key!(dyn Fs = "fs");

#[component(provides(dyn Fs))]
struct LocalFs;

impl Fs for LocalFs {
    fn describe(&self) -> String {
        "/".into()
    }
}

impl Component for LocalFs {
    async fn activate(_: Cx<Self>, _: ()) -> Result<Self> {
        Ok(LocalFs)
    }
}

// Wrappers written by whoever intercepts, not by the provider. Each delegates to the file
// system it wraps.

/// A view that sees only paths under a prefix.
struct Under(&'static str, Arc<dyn Fs>);

impl Fs for Under {
    fn describe(&self) -> String {
        format!("{}{}", self.0, self.1.describe())
    }
}

/// A wrapper that marks the view read-only.
struct ReadOnly(Arc<dyn Fs>);

impl Fs for ReadOnly {
    fn describe(&self) -> String {
        format!("{} (ro)", self.0.describe())
    }
}

static VIEWS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

fn view(reader: &str) -> String {
    VIEWS.lock().unwrap().iter().rev().find(|(name, _)| name == reader).unwrap().1.clone()
}

#[component(config = Named, deps(fs: dyn Fs))]
struct Reader;

impl Component for Reader {
    async fn activate(cx: Cx<Self>, config: Named) -> Result<Self> {
        VIEWS.lock().unwrap().push((config.name, cx.deps().fs.describe()));
        Ok(Reader)
    }
}

/// Runs a reader under a read-only interception of its own.
#[component(config = Named)]
struct Sandbox;

impl Component for Sandbox {
    async fn activate(cx: Cx<Self>, config: Named) -> Result<Self> {
        cx.intercept::<dyn Fs>(|fs| Arc::new(ReadOnly(fs))).plugin::<Reader>(config).await?;
        Ok(Sandbox)
    }
}

#[tokio::test]
async fn interception_changes_only_the_view_of_consumers_under_the_scope() {
    let app = App::new();
    app.insert::<LocalFs>(json!(null)).await;
    app.insert::<Reader>(named_json("plain")).await;
    app.intercept::<dyn Fs>(|fs| Arc::new(Under("/work", fs))).plugin::<Reader>(named("work")).await.unwrap();
    app.insert::<Sandbox>(named_json("sandboxed")).await;
    app.settled().await;

    assert_eq!(view("plain"), "/");
    assert_eq!(view("work"), "/work/");
    assert_eq!(view("sandboxed"), "/ (ro)");
    // The shared provider itself is untouched.
    assert_eq!(app.get::<dyn Fs>().await.unwrap().describe(), "/");
}

#[tokio::test]
async fn nested_interceptions_apply_outermost_first() {
    let app = App::new();
    app.insert::<LocalFs>(json!(null)).await;
    // The app scope narrows to /jail, then the sandbox inside marks it read-only.
    app.intercept::<dyn Fs>(|fs| Arc::new(Under("/jail", fs))).plugin::<Sandbox>(named("nested")).await.unwrap();
    app.intercept::<dyn Fs>(|fs| Arc::new(Under("/a", fs)))
        .intercept::<dyn Fs>(|fs| Arc::new(Under("/b", fs)))
        .plugin::<Reader>(named("chained"))
        .await
        .unwrap();
    app.settled().await;
    assert_eq!(view("nested"), "/jail/ (ro)");
    assert_eq!(view("chained"), "/b/a/");
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
    let warnings = loader.apply_yaml(missing).await.unwrap();
    let realm = Some("sandbox".to_owned());
    assert_eq!(warnings, [LoadWarning::MissingProvider { row: "b".into(), key: "tools", realm }]);
    let unknown = "plugins:\n  - { id: a, use: agent, config: { name: z }, isolate: { nope: x } }\n";
    assert!(matches!(loader.apply_yaml(unknown).await, Err(LoadError::UnknownKey { .. })));
}
