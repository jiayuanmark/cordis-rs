//! Inline reactive hooks (`Cx::when`, `Scope::when`, `App::when`) and live optional handles
//! (`Cx::optional`, `Scope::optional`, `App::optional`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cordis::{App, Component, Cx, Guard, Optional, Realm, Result, Status, component, key};
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

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct ServiceConfig {
    tag: String,
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

struct PrefixedMetrics(&'static str, Arc<dyn Metrics>);

impl Metrics for PrefixedMetrics {
    fn name(&self) -> String {
        format!("{}{}", self.0, self.1.name())
    }
}

#[tokio::test]
async fn when_works_on_app_and_scope_with_isolation_and_interception() {
    let app = App::new();
    let realm = Realm::named("when-isolated");

    // Root `app.when` watches the shared realm; `Scope::when` watches an isolated realm with
    // interception.
    let shared_hook = app
        .when::<dyn Metrics>(|cx, metrics| async move {
            let name = metrics.name();
            record("app_when", format!("{name} up"));
            assert_eq!(cx.deps().name(), name);
            cx.on_dispose(move || async move { record("app_when", format!("{name} down")) });
            Ok(())
        })
        .await
        .unwrap();

    let iso_hook = app
        .isolate_in::<dyn Metrics>(&realm)
        .intercept::<dyn Metrics>(|m| Arc::new(PrefixedMetrics("iso:", m)))
        .when::<dyn Metrics>(|cx, metrics| async move {
            let name = metrics.name();
            record("scope_when", format!("{name} up"));
            assert_eq!(cx.deps().name(), name);
            cx.on_dispose(move || async move { record("scope_when", format!("{name} down")) });
            Ok(())
        })
        .await
        .unwrap();

    app.settled().await;
    assert_eq!(shared_hook.status().await, Some(Status::Inactive));
    assert_eq!(iso_hook.status().await, Some(Status::Inactive));

    let shared_metrics = app.insert::<Recorder>(json!({ "tag": "shared_provider", "name": "live" })).await;
    app.settled().await;
    assert_eq!(shared_hook.status().await, Some(Status::Active));
    assert_eq!(iso_hook.status().await, Some(Status::Inactive));
    assert_eq!(journal("app_when"), ["live up"]);
    assert!(journal("scope_when").is_empty());

    let iso_metrics = app
        .isolate_in::<dyn Metrics>(&realm)
        .plugin::<Recorder>(MetricsConfig { tag: "iso_provider".into(), name: "local".into() })
        .await
        .unwrap();
    app.settled().await;
    assert_eq!(iso_hook.status().await, Some(Status::Active));
    assert_eq!(journal("scope_when"), ["iso:local up"]);

    // Disposing the returned `Fiber` handle retires the hook early while the provider stays active.
    shared_hook.dispose();
    iso_hook.dispose();
    app.settled().await;
    assert_eq!(shared_hook.status().await, None);
    assert_eq!(iso_hook.status().await, None);
    assert_eq!(app.status(shared_metrics).await, Some(Status::Active));
    assert_eq!(iso_metrics.status().await, Some(Status::Active));
    assert_eq!(journal("app_when"), ["live up", "live down"]);
    assert_eq!(journal("scope_when"), ["iso:local up", "iso:local down"]);
}

pub trait Probe: Send + Sync {
    fn current_metrics(&self) -> Option<String>;
    fn handle(&self) -> Optional<dyn Metrics>;
}
key!(dyn Probe = "probe");

#[component(config = ServiceConfig, provides(dyn Probe))]
struct OptionalService {
    metrics: Optional<dyn Metrics>,
}

impl Probe for OptionalService {
    fn current_metrics(&self) -> Option<String> {
        self.metrics.get().map(|metrics| metrics.name())
    }

    fn handle(&self) -> Optional<dyn Metrics> {
        self.metrics.clone()
    }
}

impl Component for OptionalService {
    async fn activate(cx: Cx<Self>, config: ServiceConfig) -> Result<Self> {
        record(&config.tag, "optional-service up");
        let metrics = cx.optional::<dyn Metrics>().await?;
        Ok(Self { metrics })
    }
}

/// A `Metrics` provider that inspects an `Optional<dyn Metrics>` handle during its own teardown
/// to verify that the handle was already cleared before the provider began unloading.
#[derive(Clone)]
struct ObservedMetricsConfig {
    name: String,
    observer: Optional<dyn Metrics>,
    saw_cleared: Arc<Mutex<Vec<bool>>>,
}

#[component(config = ObservedMetricsConfig, provides(dyn Metrics))]
struct ObservedMetrics(String);

impl Metrics for ObservedMetrics {
    fn name(&self) -> String {
        self.0.clone()
    }
}

impl Component for ObservedMetrics {
    async fn activate(cx: Cx<Self>, config: ObservedMetricsConfig) -> Result<Self> {
        let ObservedMetricsConfig { name, observer, saw_cleared } = config;
        cx.on_dispose(move || async move {
            saw_cleared.lock().unwrap().push(observer.get().is_none());
        });
        Ok(Self(name))
    }
}

#[tokio::test]
async fn optional_tracks_provider_without_restarting_owner_and_clears_before_provider_teardown() {
    let app = App::new();
    let service = app.insert::<OptionalService>(json!({ "tag": "opt" })).await;
    app.settled().await;

    assert_eq!(app.status(service).await, Some(Status::Active));
    let probe = app.get::<dyn Probe>().await.unwrap();
    let handle = probe.handle();
    assert_eq!(journal("opt"), ["optional-service up"]);
    assert_eq!(probe.current_metrics(), None);

    // Provider arrives: handle sees it without restarting `OptionalService`.
    let saw_cleared = Arc::new(Mutex::new(Vec::new()));
    let metrics = app
        .plugin::<ObservedMetrics>(ObservedMetricsConfig {
            name: "v1".into(),
            observer: handle.clone(),
            saw_cleared: saw_cleared.clone(),
        })
        .await
        .unwrap();
    app.settled().await;
    assert_eq!(journal("opt"), ["optional-service up"]);
    assert_eq!(probe.current_metrics().as_deref(), Some("v1"));

    // Provider reloads: `handle` is cleared BEFORE `v1`'s teardown runs, then sees `v2`.
    metrics.update(ObservedMetricsConfig {
        name: "v2".into(),
        observer: handle.clone(),
        saw_cleared: saw_cleared.clone(),
    });
    app.settled().await;
    assert_eq!(journal("opt"), ["optional-service up"]);
    assert_eq!(*saw_cleared.lock().unwrap(), [true]);
    assert_eq!(probe.current_metrics().as_deref(), Some("v2"));

    // Provider leaves: `handle` clears before `v2`'s teardown; `OptionalService` stays active.
    metrics.dispose();
    app.settled().await;
    assert_eq!(app.status(service).await, Some(Status::Active));
    assert_eq!(*saw_cleared.lock().unwrap(), [true, true]);
    assert_eq!(probe.current_metrics(), None);

    // Provider returns, then parent unloads: even a kept clone of `Optional` clears to `None`
    // while the provider stays active.
    let metrics3 = app.insert::<Recorder>(json!({ "tag": "opt_p3", "name": "v3" })).await;
    app.settled().await;
    assert_eq!(handle.get().unwrap().name(), "v3");

    app.remove(service);
    app.settled().await;
    assert_eq!(app.status(metrics3).await, Some(Status::Active));
    assert!(handle.get().is_none());
}

#[tokio::test]
async fn optional_respects_realms_intercepts_once_per_activation_and_supports_dispose() {
    let app = App::new();
    let realm = Realm::named("opt-isolated");
    let remaps = Arc::new(AtomicUsize::new(0));

    let shared_opt = app.optional::<dyn Metrics>().await.unwrap();
    let counted = remaps.clone();
    let iso_opt = app
        .isolate_in::<dyn Metrics>(&realm)
        .intercept::<dyn Metrics>(move |m| {
            counted.fetch_add(1, Ordering::SeqCst);
            Arc::new(PrefixedMetrics("iso:", m))
        })
        .optional::<dyn Metrics>()
        .await
        .unwrap();

    // Shared provider is visible only to `shared_opt`.
    let shared_metrics = app.insert::<Recorder>(json!({ "tag": "opt_shared", "name": "shared" })).await;
    app.settled().await;
    assert_eq!(shared_opt.get().unwrap().name(), "shared");
    assert!(iso_opt.get().is_none());

    // Isolated provider is visible to `iso_opt`, with interception applied once (not per read).
    let iso_metrics = app
        .isolate_in::<dyn Metrics>(&realm)
        .plugin::<Recorder>(MetricsConfig { tag: "opt_iso".into(), name: "local".into() })
        .await
        .unwrap();
    app.settled().await;
    for _ in 0..3 {
        assert_eq!(iso_opt.get().unwrap().name(), "iso:local");
    }
    assert_eq!(remaps.load(Ordering::SeqCst), 1);

    // `Optional::dispose` retires the backing fiber and clears the handle while providers stay up.
    shared_opt.dispose();
    iso_opt.dispose();
    app.settled().await;
    assert!(shared_opt.get().is_none());
    assert!(iso_opt.get().is_none());
    assert_eq!(app.status(shared_metrics).await, Some(Status::Active));
    assert_eq!(iso_metrics.status().await, Some(Status::Active));
}
