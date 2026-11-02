//! Optional dependencies: live handle (`deps(k?: Key)` / `Optional<K>`) and reactive child fiber
//! (`Cx::when`, `Scope::when`, `App::when`).

use std::sync::{Arc, Mutex};

use cordis::{
    App, Catalog, Component, Cx, Guard, KeyId, Loader, Manifest, Optional, Realm, Result, Status, component, key,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

// Journal shared by the components below. Tests run in parallel, so each entry carries the
// test's tag.

static JOURNAL: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

fn record(tag: &str, entry: impl Into<String>) {
    JOURNAL.lock().unwrap().push((tag.to_owned(), entry.into()));
}

fn journal(tag: &str) -> Vec<String> {
    JOURNAL.lock().unwrap().iter().filter(|(t, _)| t == tag).map(|(_, e)| e.clone()).collect()
}

pub trait Metrics: Send + Sync {
    fn name(&self) -> String;
}
key!(dyn Metrics = "metrics");

#[derive(Clone, Serialize, Deserialize)]
struct MetricsConfig {
    #[serde(default)]
    tag: String,
    name: String,
}

#[component(config = MetricsConfig, provides(dyn Metrics))]
struct Recorder {
    name: String,
}

impl Metrics for Recorder {
    fn name(&self) -> String {
        self.name.clone()
    }
}

impl Component for Recorder {
    async fn activate(cx: Cx<Self>, config: MetricsConfig) -> Result<Self> {
        let MetricsConfig { tag, name } = config;
        record(&tag, format!("metrics:{name} up"));
        let down = name.clone();
        cx.on_dispose(move || async move { record(&tag, format!("metrics:{down} down")) });
        Ok(Self { name })
    }
}

pub trait Probe: Send + Sync {
    fn current_metrics(&self) -> Option<String>;
    fn optional_handle(&self) -> Optional<dyn Metrics>;
}
key!(dyn Probe = "probe");

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct ServiceConfig {
    tag: String,
}

#[component(config = ServiceConfig, deps(metrics?: dyn Metrics), provides(dyn Probe))]
struct Service {
    metrics: Optional<dyn Metrics>,
}

impl Probe for Service {
    fn current_metrics(&self) -> Option<String> {
        self.metrics.get().map(|m| m.name())
    }

    fn optional_handle(&self) -> Optional<dyn Metrics> {
        self.metrics.clone()
    }
}

impl Component for Service {
    async fn activate(cx: Cx<Self>, config: ServiceConfig) -> Result<Self> {
        record(&config.tag, "service up");
        Ok(Self { metrics: cx.deps().metrics.clone() })
    }
}

#[tokio::test]
async fn optional_dep_activates_without_provider_and_tracks_live_table_without_reloading() {
    assert!(Service::deps().is_empty());
    assert_eq!(Service::optional(), [KeyId::of::<dyn Metrics>()]);

    let app = App::new();
    let service = app.insert::<Service>(json!({ "tag": "live" })).await;
    app.settled().await;

    // Activates even though `dyn Metrics` has no provider.
    assert_eq!(app.status(service).await, Some(Status::Active));
    let probe = app.get::<dyn Probe>().await.unwrap();
    assert_eq!(journal("live"), ["service up"]);
    assert_eq!(probe.current_metrics(), None);

    // Provider arrives: `Optional::get` sees it immediately, without reloading `Service`.
    let metrics = app.insert::<Recorder>(json!({ "tag": "live", "name": "v1" })).await;
    app.settled().await;
    assert_eq!(journal("live"), ["service up", "metrics:v1 up"]);
    assert_eq!(probe.current_metrics().as_deref(), Some("v1"));

    // Provider reloads: `Optional::get` sees the new provider, still without reloading `Service`.
    app.update(metrics, json!({ "tag": "live", "name": "v2" }));
    app.settled().await;
    assert_eq!(journal("live"), ["service up", "metrics:v1 up", "metrics:v1 down", "metrics:v2 up"]);
    assert_eq!(probe.current_metrics().as_deref(), Some("v2"));

    // Provider leaves: `Optional::get` returns `None`, `Service` stays `Active`.
    app.remove(metrics);
    app.settled().await;
    assert_eq!(app.status(service).await, Some(Status::Active));
    assert_eq!(journal("live"), ["service up", "metrics:v1 up", "metrics:v1 down", "metrics:v2 up", "metrics:v2 down"]);
    assert_eq!(probe.current_metrics(), None);

    // A kept `Optional<K>` returns `None` once the app is dropped and the orchestrator stops.
    let _metrics2 = app.insert::<Recorder>(json!({ "tag": "live", "name": "v3" })).await;
    app.settled().await;
    let handle = probe.optional_handle();
    assert_eq!(handle.get().unwrap().name(), "v3");
    drop(probe);
    drop(app);
    tokio::task::yield_now().await;
    assert!(handle.get().is_none());
}

struct PrefixedMetrics(&'static str, Arc<dyn Metrics>);

impl Metrics for PrefixedMetrics {
    fn name(&self) -> String {
        format!("{}{}", self.0, self.1.name())
    }
}

#[tokio::test]
async fn optional_dep_respects_realms_and_interception() {
    let app = App::new();
    let realm = Realm::named("isolated");

    // Shared provider has name "shared"; isolated realm has no provider yet.
    app.insert::<Recorder>(json!({ "tag": "realm", "name": "shared" })).await;
    app.isolate_in::<dyn Metrics>(&realm)
        .intercept::<dyn Metrics>(|m| Arc::new(PrefixedMetrics("iso:", m)))
        .plugin::<Service>(ServiceConfig { tag: "realm".into() })
        .await
        .unwrap();
    app.settled().await;

    let probe = app.get::<dyn Probe>().await.unwrap();
    assert_eq!(probe.current_metrics(), None);

    // Inserting a provider in the isolated realm makes it visible with interception applied.
    let iso_metrics = app
        .isolate_in::<dyn Metrics>(&realm)
        .plugin::<Recorder>(MetricsConfig { tag: "realm".into(), name: "local".into() })
        .await
        .unwrap();
    app.settled().await;
    assert_eq!(probe.current_metrics().as_deref(), Some("iso:local"));

    iso_metrics.dispose();
    app.settled().await;
    assert_eq!(probe.current_metrics(), None);
}

#[component(deps(probe?: dyn Probe), provides(dyn Metrics))]
struct MutualMetrics;

impl Metrics for MutualMetrics {
    fn name(&self) -> String {
        "mutual".into()
    }
}

impl Component for MutualMetrics {
    async fn activate(_: Cx<Self>, _: ()) -> Result<Self> {
        Ok(MutualMetrics)
    }
}

#[tokio::test]
async fn loader_accepts_optional_deps_without_missing_or_cycle_warnings_and_isolates_them() {
    let app = App::new();
    let mut catalog = Catalog::new();
    catalog.register::<Service>("service").register::<MutualMetrics>("mutual");
    let mut loader = Loader::new(app.clone(), catalog);

    // `service` has an optional dep on `metrics` and `isolate: { metrics: tenant }`, while no
    // `metrics` provider is enabled. No warning or UnknownKey error is produced.
    let warnings = loader
        .apply_yaml("plugins:\n  - { id: svc, use: service, config: {}, isolate: { metrics: tenant } }\n")
        .await
        .unwrap();
    assert!(warnings.is_empty());
    app.settled().await;
    assert_eq!(app.status(loader.fiber("svc").unwrap()).await, Some(Status::Active));

    // Mutual optional dependencies (`service` -> `metrics?`, `mutual` -> `probe?`) do not form a
    // dependency cycle.
    let warnings = loader
        .apply_yaml("plugins:\n  - { id: svc, use: service, config: {} }\n  - { id: mut, use: mutual }\n")
        .await
        .unwrap();
    assert!(warnings.is_empty());
    app.settled().await;
    assert_eq!(app.get::<dyn Probe>().await.unwrap().current_metrics().as_deref(), Some("mutual"));
}

#[component(config = ServiceConfig)]
struct HookedService;

impl Component for HookedService {
    async fn activate(cx: Cx<Self>, config: ServiceConfig) -> Result<Self> {
        let tag = config.tag;
        record(&tag, "service up");
        let down = tag.clone();
        cx.on_dispose(move || async move { record(&down, "service down") });

        cx.when::<dyn Metrics>(move |cx, metrics| {
            let tag = tag.clone();
            async move {
                let name = metrics.name();
                record(&tag, format!("when:{name} up"));
                cx.on_dispose(Guard::new(move || record(&tag, format!("when:{name} down"))));
                Ok(())
            }
        })
        .await?;

        Ok(HookedService)
    }
}

#[tokio::test]
async fn when_runs_for_each_provider_lifetime_and_tears_down_before_provider() {
    let app = App::new();
    let service = app.insert::<HookedService>(json!({ "tag": "hook" })).await;
    app.settled().await;

    // Parent is active even without `dyn Metrics`.
    assert_eq!(app.status(service).await, Some(Status::Active));
    assert_eq!(journal("hook"), ["service up"]);

    // `dyn Metrics` arrives: the `when` child activates.
    let metrics = app.insert::<Recorder>(json!({ "tag": "hook", "name": "m1" })).await;
    app.settled().await;
    assert_eq!(journal("hook"), ["service up", "metrics:m1 up", "when:m1 up"]);

    // `dyn Metrics` reloads: `when:m1 down` runs BEFORE `metrics:m1 down`, and `service` never
    // reloads.
    app.update(metrics, json!({ "tag": "hook", "name": "m2" }));
    app.settled().await;
    assert_eq!(
        journal("hook"),
        ["service up", "metrics:m1 up", "when:m1 up", "when:m1 down", "metrics:m1 down", "metrics:m2 up", "when:m2 up"]
    );

    // `dyn Metrics` is removed: `when:m2 down` runs before `metrics:m2 down`; `service` stays up.
    app.remove(metrics);
    app.settled().await;
    assert_eq!(app.status(service).await, Some(Status::Active));
    assert_eq!(
        journal("hook"),
        [
            "service up",
            "metrics:m1 up",
            "when:m1 up",
            "when:m1 down",
            "metrics:m1 down",
            "metrics:m2 up",
            "when:m2 up",
            "when:m2 down",
            "metrics:m2 down",
        ]
    );

    // `dyn Metrics` comes back, then the parent `service` is removed: `when:m3` is retired with
    // the parent while `metrics:m3` stays active.
    let metrics3 = app.insert::<Recorder>(json!({ "tag": "hook", "name": "m3" })).await;
    app.settled().await;
    let before = journal("hook").len();

    app.remove(service);
    app.settled().await;
    assert_eq!(app.status(metrics3).await, Some(Status::Active));
    let mut tail = journal("hook")[before..].to_vec();
    tail.sort();
    assert_eq!(tail, ["service down", "when:m3 down"]);
}

#[tokio::test]
async fn when_works_on_app_and_scope_with_isolation_and_interception() {
    let app = App::new();

    // Root `app.when` and `Scope::when` with interception.
    let hook = app
        .intercept::<dyn Metrics>(|m| Arc::new(PrefixedMetrics("root:", m)))
        .when::<dyn Metrics>(|cx, metrics| async move {
            let name = metrics.name();
            record("scope_when", format!("{name} up"));
            // `cx.deps()` returns the same `Arc<dyn Metrics>` passed as the second argument.
            assert_eq!(cx.deps().name(), name);
            cx.on_dispose(move || async move {
                record("scope_when", format!("{name} down"));
            });
            Ok(())
        })
        .await
        .unwrap();

    app.settled().await;
    assert_eq!(hook.status().await, Some(Status::Inactive));

    let metrics = app.insert::<Recorder>(json!({ "tag": "scope_when_provider", "name": "live" })).await;
    app.settled().await;
    assert_eq!(hook.status().await, Some(Status::Active));
    assert_eq!(journal("scope_when"), ["root:live up"]);

    // Disposing the returned `Fiber` handle retires the hook early while the provider stays active.
    hook.dispose();
    app.settled().await;
    assert_eq!(hook.status().await, None);
    assert_eq!(app.status(metrics).await, Some(Status::Active));
    assert_eq!(journal("scope_when"), ["root:live up", "root:live down"]);
}
