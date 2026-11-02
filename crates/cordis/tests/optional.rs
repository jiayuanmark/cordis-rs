//! Optional dependencies: `deps(name?: Key)` fields and reactive child fibers (`Cx::when`,
//! `Scope::when`, `App::when`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cordis::{
    App, Catalog, Component, Cx, Guard, KeyId, LoadWarning, Loader, Manifest, Realm, Result, Status, component, key,
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

#[derive(Serialize, Deserialize)]
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
}
key!(dyn Probe = "probe");

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct ServiceConfig {
    tag: String,
}

#[component(config = ServiceConfig, deps(metrics?: dyn Metrics), provides(dyn Probe))]
struct Service {
    cx: Cx<Self>,
}

impl Probe for Service {
    fn current_metrics(&self) -> Option<String> {
        self.cx.deps().metrics.as_ref().map(|metrics| metrics.name())
    }
}

impl Component for Service {
    async fn activate(cx: Cx<Self>, config: ServiceConfig) -> Result<Self> {
        let ServiceConfig { tag } = config;
        let seen = cx.deps().metrics.as_ref().map_or("none".to_owned(), |metrics| metrics.name());
        record(&tag, format!("service({seen}) up"));
        cx.on_dispose(move || async move { record(&tag, format!("service({seen}) down")) });
        Ok(Self { cx })
    }
}

#[tokio::test]
async fn an_optional_dep_does_not_hold_back_activation_and_reloads_with_its_provider() {
    assert!(Service::deps().is_empty());
    assert_eq!(Service::optional(), [KeyId::of::<dyn Metrics>()]);

    let app = App::new();
    let service = app.insert::<Service>(json!({ "tag": "live" })).await;
    app.settled().await;

    // Activates even though `dyn Metrics` has no provider.
    assert_eq!(app.status(service).await, Some(Status::Active));
    assert_eq!(journal("live"), ["service(none) up"]);
    assert_eq!(app.get::<dyn Probe>().await.unwrap().current_metrics(), None);

    // The provider arrives: `Service` stops when the provider starts (the two run side by
    // side), then starts again once the provider is active, and sees it.
    let metrics = app.insert::<Recorder>(json!({ "tag": "live", "name": "v1" })).await;
    app.settled().await;
    let mut arrival = journal("live")[1..3].to_vec();
    arrival.sort();
    assert_eq!(arrival, ["metrics:v1 up", "service(none) down"]);
    assert_eq!(journal("live")[3..], ["service(v1) up"]);
    assert_eq!(app.get::<dyn Probe>().await.unwrap().current_metrics().as_deref(), Some("v1"));

    // The provider reloads: `Service` is undone before the old provider, as with a required
    // dependency, and waits for the new one instead of running without it in between.
    app.update(metrics, json!({ "tag": "live", "name": "v2" }));
    app.settled().await;
    assert_eq!(journal("live")[4..], ["service(v1) down", "metrics:v1 down", "metrics:v2 up", "service(v2) up"]);

    // The provider leaves: `Service` is undone first, then runs again without it.
    app.remove(metrics);
    app.settled().await;
    assert_eq!(app.status(service).await, Some(Status::Active));
    assert_eq!(journal("live")[8..], ["service(v2) down", "metrics:v2 down", "service(none) up"]);
    assert_eq!(app.get::<dyn Probe>().await.unwrap().current_metrics(), None);
}

struct PrefixedMetrics(&'static str, Arc<dyn Metrics>);

impl Metrics for PrefixedMetrics {
    fn name(&self) -> String {
        format!("{}{}", self.0, self.1.name())
    }
}

#[tokio::test]
async fn an_optional_dep_resolves_in_its_realm_and_is_intercepted_once_per_activation() {
    let app = App::new();
    let realm = Realm::named("isolated");
    let remaps = Arc::new(AtomicUsize::new(0));

    // The shared realm has a provider; the isolated realm has none yet.
    app.insert::<Recorder>(json!({ "tag": "realm", "name": "shared" })).await;
    let counted = remaps.clone();
    app.isolate_in::<dyn Metrics>(&realm)
        .intercept::<dyn Metrics>(move |m| {
            counted.fetch_add(1, Ordering::SeqCst);
            Arc::new(PrefixedMetrics("iso:", m))
        })
        .plugin::<Service>(ServiceConfig { tag: "realm".into() })
        .await
        .unwrap();
    app.settled().await;
    assert_eq!(app.get::<dyn Probe>().await.unwrap().current_metrics(), None);

    // A provider in the isolated realm is seen with the interception applied.
    let local = app
        .isolate_in::<dyn Metrics>(&realm)
        .plugin::<Recorder>(MetricsConfig { tag: "realm".into(), name: "local".into() })
        .await
        .unwrap();
    app.settled().await;
    let probe = app.get::<dyn Probe>().await.unwrap();
    for _ in 0..3 {
        assert_eq!(probe.current_metrics().as_deref(), Some("iso:local"));
    }
    assert_eq!(remaps.load(Ordering::SeqCst), 1);

    local.dispose();
    app.settled().await;
    assert_eq!(app.get::<dyn Probe>().await.unwrap().current_metrics(), None);
}

#[component(deps(metrics?: dyn Metrics), provides(dyn Metrics))]
struct Fallback;

impl Metrics for Fallback {
    fn name(&self) -> String {
        "fallback".into()
    }
}

impl Component for Fallback {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        // The fiber's own provision is not its optional dependency, so it settles.
        assert!(cx.deps().metrics.is_none());
        record("own", "fallback up");
        Ok(Fallback)
    }
}

#[tokio::test]
async fn an_optional_dep_on_a_key_the_fiber_provides_does_not_reload_it() {
    let app = App::new();
    app.insert::<Fallback>(json!(null)).await;
    app.settled().await;
    assert_eq!(journal("own"), ["fallback up"]);
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
async fn the_loader_never_reports_an_optional_dep_missing_but_reports_its_cycles() {
    let app = App::new();
    let mut catalog = Catalog::new();
    catalog.register::<Service>("service").register::<MutualMetrics>("mutual");
    let mut loader = Loader::new(app.clone(), catalog);

    // `service` optionally uses `metrics` and isolates it, while no row provides `metrics`.
    let warnings = loader
        .apply_yaml("plugins:\n  - { id: svc, use: service, config: {}, isolate: { metrics: tenant } }\n")
        .await
        .unwrap();
    assert!(warnings.is_empty());
    app.settled().await;
    assert_eq!(app.status(loader.fiber("svc").unwrap()).await, Some(Status::Active));

    // `service` -> `metrics?` and `mutual` -> `probe?` would reload each other forever, so the
    // loader reports the cycle. (The app is dropped without settling.)
    let mut catalog = Catalog::new();
    catalog.register::<Service>("service").register::<MutualMetrics>("mutual");
    let warnings = Loader::new(App::new(), catalog)
        .apply_yaml("plugins:\n  - { id: svc, use: service, config: {} }\n  - { id: mut, use: mutual }\n")
        .await
        .unwrap();
    assert!(matches!(warnings.as_slice(), [LoadWarning::Cycle(rows)] if rows.len() == 3), "{warnings:?}");
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
