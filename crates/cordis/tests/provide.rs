//! Direct in-memory value provision (`App::provide`, `Scope::provide`, `Cx::provide`).

use std::sync::{Arc, Mutex};

use cordis::{App, Catalog, Component, Cx, Guard, Key, Loader, Provide, Result, Status, component, key};
use serde::Deserialize;

type Log = Arc<Mutex<Vec<String>>>;

fn push(log: &Log, entry: impl Into<String>) {
    log.lock().unwrap().push(entry.into());
}

fn entries(log: &Log) -> Vec<String> {
    log.lock().unwrap().clone()
}

pub trait Tools: Send + Sync {
    fn label(&self) -> String;
}
key!(dyn Tools = "tools");

struct SimpleTools {
    label: String,
    drop_log: Option<Log>,
}

impl SimpleTools {
    fn new(label: impl Into<String>) -> Self {
        Self { label: label.into(), drop_log: None }
    }

    fn tracked(label: impl Into<String>, log: Log) -> Self {
        Self { label: label.into(), drop_log: Some(log) }
    }
}

impl Tools for SimpleTools {
    fn label(&self) -> String {
        self.label.clone()
    }
}

impl Drop for SimpleTools {
    fn drop(&mut self) {
        if let Some(log) = &self.drop_log {
            push(log, format!("-tools({})", self.label));
        }
    }
}

/// Simulates a foreign concrete type (such as `sqlx::PgPool`) that cannot implement `Component`.
struct ForeignPool {
    max_conns: u32,
}

struct DbPool;
impl Key for DbPool {
    type Value = ForeignPool;
    const NAME: &'static str = "db.pool";
}

#[component(config = Log, deps(tools: dyn Tools, pool: DbPool))]
struct Worker;

impl Component for Worker {
    async fn activate(cx: Cx<Self>, log: Log) -> Result<Self> {
        let label = cx.deps().tools.label();
        let conns = cx.deps().pool.max_conns;
        push(&log, format!("+worker({label},{conns})"));
        let on_drop_cx = cx.clone();
        cx.on_dispose(Guard::new(move || {
            let label = on_drop_cx.deps().tools.label();
            let conns = on_drop_cx.deps().pool.max_conns;
            push(&log, format!("-worker({label},{conns})"));
        }));
        Ok(Worker)
    }
}

#[component(config = Log, deps(tools: dyn Tools))]
struct ToolsConsumer;

impl Component for ToolsConsumer {
    async fn activate(cx: Cx<Self>, log: Log) -> Result<Self> {
        let label = cx.deps().tools.label();
        push(&log, format!("+consumer({label})"));
        let on_drop_cx = cx.clone();
        cx.on_dispose(Guard::new(move || {
            let label = on_drop_cx.deps().tools.label();
            push(&log, format!("-consumer({label})"));
        }));
        Ok(ToolsConsumer)
    }
}

#[derive(Deserialize)]
struct YamlConsumerConfig {
    name: String,
}

#[component(config = YamlConsumerConfig, deps(tools: dyn Tools, pool: DbPool))]
struct YamlConsumer;

impl Component for YamlConsumer {
    async fn activate(cx: Cx<Self>, config: YamlConsumerConfig) -> Result<Self> {
        assert_eq!(cx.deps().tools.label(), "host");
        assert_eq!(cx.deps().pool.max_conns, 8);
        assert_eq!(config.name, "from-yaml");
        Ok(YamlConsumer)
    }
}

#[tokio::test]
async fn app_provide_publishes_trait_objects_foreign_types_and_feeds_yaml_rows() {
    let app = App::new();
    let log: Log = Arc::default();

    let worker = app.plugin::<Worker>(log.clone()).await.unwrap();
    app.settled().await;
    assert_eq!(worker.status().await, Some(Status::Inactive));

    // Unsized coercion (`Arc<SimpleTools>` -> `Arc<dyn Tools>`) and a concrete foreign type.
    let tools = app.provide::<dyn Tools>(Arc::new(SimpleTools::new("host"))).await.unwrap();
    let pool = app.provide::<DbPool>(Arc::new(ForeignPool { max_conns: 8 })).await.unwrap();
    app.settled().await;

    assert_eq!(tools.status().await, Some(Status::Active));
    assert_eq!(pool.status().await, Some(Status::Active));
    assert_eq!(worker.status().await, Some(Status::Active));
    assert_eq!(entries(&log), vec!["+worker(host,8)"]);
    assert_eq!(app.get::<dyn Tools>().await.unwrap().label(), "host");
    assert_eq!(app.get::<DbPool>().await.unwrap().max_conns, 8);

    // YAML-loaded rows consume host-provided values seamlessly.
    let mut catalog = Catalog::new();
    catalog.register::<YamlConsumer>("yaml-consumer");
    let mut loader = Loader::new(app.clone(), catalog);
    let warnings = loader
        .apply_yaml(
            r#"
plugins:
  - { id: c, use: yaml-consumer, config: { name: from-yaml } }
"#,
        )
        .await
        .unwrap();
    // Loader validates only rows within the YAML tree, so external providers surface as warnings.
    assert_eq!(warnings.len(), 2);
    app.settled().await;
    assert_eq!(app.status(loader.fiber("c").unwrap()).await, Some(Status::Active));

    app.shutdown().await;
    assert_eq!(entries(&log), vec!["+worker(host,8)", "-worker(host,8)"]);
}

#[tokio::test]
async fn update_reloads_consumers_and_dispose_tears_down_consumers_before_dropping_value() {
    let app = App::new();
    let log: Log = Arc::default();

    let provider = app.provide::<dyn Tools>(Arc::new(SimpleTools::tracked("v1", log.clone()))).await.unwrap();
    let _consumer = app.plugin::<ToolsConsumer>(log.clone()).await.unwrap();
    app.settled().await;
    assert_eq!(entries(&log), vec!["+consumer(v1)"]);

    // Updating the provided value unloads consumers against v1, drops v1, then reloads against v2.
    provider.update(Arc::new(SimpleTools::tracked("v2", log.clone())));
    app.settled().await;
    assert_eq!(entries(&log), vec!["+consumer(v1)", "-consumer(v1)", "-tools(v1)", "+consumer(v2)"]);
    assert_eq!(app.get::<dyn Tools>().await.unwrap().label(), "v2");

    // Disposing tears down the consumer while v2 is still alive, then drops v2.
    provider.dispose();
    app.settled().await;
    assert_eq!(provider.status().await, None);
    assert!(app.get::<dyn Tools>().await.is_none());
    assert_eq!(
        entries(&log),
        vec!["+consumer(v1)", "-consumer(v1)", "-tools(v1)", "+consumer(v2)", "-consumer(v2)", "-tools(v2)"]
    );

    app.shutdown().await;
}

#[component(config = Log)]
struct ParentProvider;

impl Component for ParentProvider {
    async fn activate(cx: Cx<Self>, log: Log) -> Result<Self> {
        // Providing a `dyn Trait` key inside `Component::activate` must be `Send` across `.await`.
        let child = cx.provide::<dyn Tools>(Arc::new(SimpleTools::tracked("child", log))).await?;
        assert!(child.status().await.is_some());
        Ok(Self)
    }
}

#[tokio::test]
async fn cx_provide_retires_with_parent_and_rejects_late_calls() {
    let app = App::new();
    let log: Log = Arc::default();

    // Also prove `App::provide::<dyn Trait>` is `Send` inside `tokio::spawn`.
    let spawned_app = app.clone();
    let spawned_fiber = tokio::spawn(async move {
        let f = spawned_app.provide::<DbPool>(Arc::new(ForeignPool { max_conns: 4 })).await?;
        Ok::<_, cordis::Error>(f)
    })
    .await
    .unwrap()
    .unwrap();
    app.settled().await;
    assert_eq!(app.get::<DbPool>().await.unwrap().max_conns, 4);
    spawned_fiber.dispose();

    let parent = app.plugin::<ParentProvider>(log.clone()).await.unwrap();
    let consumer = app.plugin::<ToolsConsumer>(log.clone()).await.unwrap();
    app.settled().await;

    assert_eq!(consumer.status().await, Some(Status::Active));
    assert_eq!(entries(&log), vec!["+consumer(child)"]);

    // Grab the parent's `Cx` by mounting a second parent that exposes its `Cx`, or test late calls
    // after parent disposal via a dedicated component.
    #[component(config = Arc<Mutex<Option<Cx<Self>>>>)]
    struct KeepCx;
    impl Component for KeepCx {
        async fn activate(cx: Cx<Self>, slot: Arc<Mutex<Option<Cx<Self>>>>) -> Result<Self> {
            *slot.lock().unwrap() = Some(cx);
            Ok(Self)
        }
    }

    let kept_slot: Arc<Mutex<Option<Cx<KeepCx>>>> = Arc::default();
    let keeper = app.plugin::<KeepCx>(kept_slot.clone()).await.unwrap();
    app.settled().await;
    let stale_cx = kept_slot.lock().unwrap().take().unwrap();
    keeper.dispose();
    app.settled().await;
    assert!(stale_cx.provide::<dyn Tools>(Arc::new(SimpleTools::new("late"))).await.is_err());

    // Disposing the parent retires its `Provide<dyn Tools>` child and deactivates the consumer.
    parent.dispose();
    app.settled().await;
    assert_eq!(consumer.status().await, Some(Status::Inactive));
    assert!(app.get::<dyn Tools>().await.is_none());
    assert_eq!(entries(&log), vec!["+consumer(child)", "-consumer(child)", "-tools(child)"]);

    app.shutdown().await;
}

struct PrefixedTools {
    prefix: &'static str,
    inner: Arc<dyn Tools>,
}

impl Tools for PrefixedTools {
    fn label(&self) -> String {
        format!("{}:{}", self.prefix, self.inner.label())
    }
}

#[tokio::test]
async fn provide_respects_realms_interception_and_exclusivity() {
    let app = App::new();
    let shared_log: Log = Arc::default();
    let iso_log: Log = Arc::default();
    let intercepted_log: Log = Arc::default();

    let first = app.provide::<dyn Tools>(Arc::new(SimpleTools::new("shared-1"))).await.unwrap();
    let second = app.provide::<dyn Tools>(Arc::new(SimpleTools::new("shared-2"))).await.unwrap();
    let _shared_consumer = app.plugin::<ToolsConsumer>(shared_log.clone()).await.unwrap();

    // Isolated scope provides its own value without colliding with the shared realm.
    let sub = app.isolate::<dyn Tools>();
    let _iso_provider = sub.provide::<dyn Tools>(Arc::new(SimpleTools::new("isolated"))).await.unwrap();
    let _iso_consumer = sub.plugin::<ToolsConsumer>(iso_log.clone()).await.unwrap();

    // Interception wraps a `Provide<dyn Tools>` value for consumers under the scope.
    let _intercepted = app
        .intercept::<dyn Tools>(|inner| Arc::new(PrefixedTools { prefix: "ro", inner }))
        .plugin::<ToolsConsumer>(intercepted_log.clone())
        .await
        .unwrap();

    app.settled().await;

    assert_eq!(first.status().await, Some(Status::Active));
    assert_eq!(second.status().await, Some(Status::Inactive));
    assert_eq!(entries(&shared_log), vec!["+consumer(shared-1)"]);
    assert_eq!(entries(&iso_log), vec!["+consumer(isolated)"]);
    assert_eq!(entries(&intercepted_log), vec!["+consumer(ro:shared-1)"]);

    // Removing the first shared provider lets the waiting second provider take over, without
    // touching the isolated realm.
    first.dispose();
    app.settled().await;
    assert_eq!(second.status().await, Some(Status::Active));
    assert_eq!(entries(&shared_log), vec!["+consumer(shared-1)", "-consumer(shared-1)", "+consumer(shared-2)"]);
    assert_eq!(
        entries(&intercepted_log),
        vec!["+consumer(ro:shared-1)", "-consumer(ro:shared-1)", "+consumer(ro:shared-2)"]
    );
    assert_eq!(entries(&iso_log), vec!["+consumer(isolated)"]);

    app.shutdown().await;
}

#[tokio::test]
async fn provide_works_with_when_and_optional() {
    let app = App::new();
    let log: Log = Arc::default();

    let when_log = log.clone();
    let _watcher = app
        .when::<dyn Tools>(move |cx, tools| {
            let log = when_log.clone();
            async move {
                let label = tools.label();
                push(&log, format!("+when({label})"));
                cx.on_dispose(Guard::new(move || push(&log, format!("-when({label})"))));
                Ok(())
            }
        })
        .await
        .unwrap();
    let opt = app.optional::<dyn Tools>().await.unwrap();
    app.settled().await;
    assert!(opt.get().is_none());

    let provider: cordis::Fiber<Provide<dyn Tools>> =
        app.provide::<dyn Tools>(Arc::new(SimpleTools::new("a"))).await.unwrap();
    app.settled().await;
    assert_eq!(opt.get().unwrap().label(), "a");
    assert_eq!(entries(&log), vec!["+when(a)"]);

    provider.update(Arc::new(SimpleTools::new("b")));
    app.settled().await;
    assert_eq!(opt.get().unwrap().label(), "b");
    assert_eq!(entries(&log), vec!["+when(a)", "-when(a)", "+when(b)"]);

    provider.dispose();
    app.settled().await;
    assert!(opt.get().is_none());
    assert_eq!(entries(&log), vec!["+when(a)", "-when(a)", "+when(b)", "-when(b)"]);

    app.shutdown().await;
}
