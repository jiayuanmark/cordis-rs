//! Event bus behavior: dispatch modes, waterfall flow, and listener lifetimes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cordis::{App, Component, Cx, Events, Flow, Result, Status, component, event};
use serde::Deserialize;
use serde_json::json;

event!(Appended = "test/appended", Emit(String));
event!(Classify = "test/classify", Bail(u32) -> &'static str);
event!(Flushed = "test/flushed", Parallel(String));
event!(Stopping = "test/stopping", Serial(u32) -> String);
event!(Execute = "test/execute", Waterfall(String) -> String);

fn log() -> Arc<Mutex<Vec<String>>> {
    Arc::default()
}

fn push(log: &Arc<Mutex<Vec<String>>>, entry: impl Into<String>) {
    log.lock().unwrap().push(entry.into());
}

fn entries(log: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    log.lock().unwrap().clone()
}

#[tokio::test]
async fn emit_calls_listeners_in_registration_order_until_their_guards_drop() {
    let events = Events::default();
    let seen = log();
    let (a, b) = (seen.clone(), seen.clone());
    let first = events.on::<Appended>().listen(move |text| push(&a, format!("first {text}")));
    let _second = events.on::<Appended>().listen(move |text| push(&b, format!("second {text}")));

    events.emit::<Appended>("x".into());
    drop(first);
    events.emit::<Appended>("y".into());
    assert_eq!(entries(&seen), ["first x", "second x", "second y"]);
}

#[tokio::test]
async fn bail_and_serial_stop_at_the_first_some() {
    let events = Events::default();
    let _small = events.on::<Classify>().listen(|n| (*n < 10).then_some("small"));
    let _any = events.on::<Classify>().listen(|_| Some("any"));
    assert_eq!(events.bail::<Classify>(3), Some("small"));
    assert_eq!(events.bail::<Classify>(30), Some("any"));

    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let _budget = events
        .on::<Stopping>()
        .listen(|steps: Arc<u32>| async move { (*steps >= 10).then(|| format!("stopped after {steps} steps")) });
    let _never = events.on::<Stopping>().listen(move |_| {
        let counted = counted.clone();
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            None
        }
    });
    assert_eq!(events.serial::<Stopping>(12).await, Some("stopped after 12 steps".into()));
    assert_eq!(events.serial::<Stopping>(2).await, None);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parallel_awaits_every_listener() {
    let events = Events::default();
    let seen = log();
    for name in ["a", "b", "c"] {
        let seen = seen.clone();
        let guard = events.on::<Flushed>().listen(move |text| {
            let seen = seen.clone();
            async move {
                tokio::task::yield_now().await;
                push(&seen, format!("{name} {text}"));
            }
        });
        std::mem::forget(guard);
    }
    events.parallel::<Flushed>("done".into()).await;
    let mut seen = entries(&seen);
    seen.sort();
    assert_eq!(seen, ["a done", "b done", "c done"]);
}

#[tokio::test]
async fn waterfall_listeners_wrap_the_default_and_may_short_circuit() {
    let events = Events::default();
    let _deny = events.on::<Execute>().listen(|command, next| async move {
        if command.starts_with("rm ") {
            return Flow::short_circuit(format!("denied: {command}"));
        }
        next.run(command).await
    });
    let _normalize = events.on::<Execute>().listen(|command, next| async move {
        next.run(command.trim().to_owned()).await.map(|output| output.to_uppercase())
    });

    let ls = events.waterfall::<Execute>("  ls  ".into(), |command| async move { format!("ran {command}") });
    assert_eq!(ls.await, "RAN LS");
    let rm = events.waterfall::<Execute>("rm -rf /".into(), |command| async move { format!("ran {command}") });
    assert_eq!(rm.await, "denied: rm -rf /");
}

// A component whose listeners live exactly as long as its fiber.

#[derive(Deserialize)]
struct Label {
    label: String,
}

static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn seen_by(label: &str) -> Vec<String> {
    SEEN.lock().unwrap().iter().filter(|entry| entry.starts_with(label)).cloned().collect()
}

struct Recorder;

component!(Recorder { config: Label });

impl Component for Recorder {
    async fn activate(cx: Cx<Self>, config: Label) -> Result<Self> {
        let label = config.label;
        cx.on::<Appended>().listen(move |text| SEEN.lock().unwrap().push(format!("{label} {text}")));
        Ok(Recorder)
    }
}

#[tokio::test]
async fn a_fiber_s_listeners_are_removed_when_the_fiber_unloads() {
    let app = App::new();
    let recorder = app.insert::<Recorder>(json!({ "label": "unload" })).await;
    app.settled().await;
    app.events().emit::<Appended>("while active".into());

    app.remove(recorder);
    app.settled().await;
    app.events().emit::<Appended>("after removal".into());
    assert_eq!(seen_by("unload"), ["unload while active"]);
}

#[derive(Deserialize)]
struct StallConfig {
    label: String,
    fail: bool,
}

/// Registers a listener, then either fails or never finishes activating.
struct Stall;

component!(Stall { config: StallConfig });

impl Component for Stall {
    async fn activate(cx: Cx<Self>, config: StallConfig) -> Result<Self> {
        let label = config.label.clone();
        cx.on::<Appended>().listen(move |text| SEEN.lock().unwrap().push(format!("{label} {text}")));
        if config.fail {
            return Err("stalled".into());
        }
        tokio::time::sleep(Duration::from_secs(3600)).await;
        unreachable!("the test cancels this activation");
    }
}

#[tokio::test]
async fn listeners_of_a_failed_or_cancelled_activation_are_removed() {
    let app = App::new();
    let failed = app.insert::<Stall>(json!({ "label": "failed", "fail": true })).await;
    app.settled().await;
    assert_eq!(app.status(failed).await, Some(Status::Failed("stalled".into())));
    app.events().emit::<Appended>("event".into());
    assert!(seen_by("failed").is_empty());

    let stalled = app.insert::<Stall>(json!({ "label": "cancelled", "fail": false })).await;
    loop {
        app.events().emit::<Appended>("probe".into());
        if !seen_by("cancelled").is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }
    app.remove(stalled);
    app.settled().await;
    let before = seen_by("cancelled").len();
    app.events().emit::<Appended>("after cancel".into());
    assert_eq!(seen_by("cancelled").len(), before);
}
