//! The undo list of a fiber run: what `Cx::on_dispose` hands over, in what order it is undone,
//! and when.

use std::collections::HashMap;
use std::future::{Future, Ready};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cordis::{App, Component, Cx, Guard, Result, Status, component, event, key};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Notify;

// Journal shared by the components below. Tests run in parallel, so each entry carries the
// test's tag.

static JOURNAL: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

fn record(tag: &str, entry: impl Into<String>) {
    JOURNAL.lock().unwrap().push((tag.to_owned(), entry.into()));
}

fn journal(tag: &str) -> Vec<String> {
    JOURNAL.lock().unwrap().iter().filter(|(t, _)| t == tag).map(|(_, e)| e.clone()).collect()
}

/// Records `entry` when dropped.
struct Recorder {
    tag: String,
    entry: &'static str,
}

impl Drop for Recorder {
    fn drop(&mut self) {
        record(&self.tag, self.entry);
    }
}

/// Polls until `done` holds; the orchestrator's work is asynchronous.
async fn until(mut done: impl FnMut() -> bool) {
    for _ in 0..1000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("condition not reached");
}

#[derive(Clone, Deserialize)]
struct Tag {
    tag: String,
}

fn tag(tag: &str) -> serde_json::Value {
    json!({ "tag": tag })
}

pub trait Store: Send + Sync {
    fn name(&self) -> String;
}
key!(dyn Store = "store");

#[component(config = Tag, provides(dyn Store))]
struct MemoryStore;

impl Store for MemoryStore {
    fn name(&self) -> String {
        "memory".into()
    }
}

impl Component for MemoryStore {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        cx.on_dispose(move || async move { record(&config.tag, "store down") });
        Ok(MemoryStore)
    }
}

/// Three async undos that each yield halfway through.
#[component(config = Tag)]
struct Stack;

impl Component for Stack {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        for step in 1..=3 {
            let tag = config.tag.clone();
            cx.on_dispose(move || async move {
                record(&tag, format!("start {step}"));
                tokio::task::yield_now().await;
                record(&tag, format!("end {step}"));
            });
        }
        Ok(Stack)
    }
}

#[tokio::test]
async fn undos_run_newest_first_one_at_a_time() {
    let app = App::new();
    let stack = app.insert::<Stack>(tag("stack")).await;
    app.settled().await;

    app.remove(stack);
    app.settled().await;
    assert_eq!(journal("stack"), ["start 3", "end 3", "start 2", "end 2", "start 1", "end 1"]);
}

/// Runs a background task, flushing after it has stopped.
#[component(config = Tag)]
struct Ticker;

impl Component for Ticker {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        let tag = config.tag.clone();
        cx.on_dispose(move || async move { record(&tag, "flush") });
        let owned = Recorder { tag: config.tag, entry: "task stopped" };
        cx.on_dispose(tokio::spawn(async move {
            let _owned = owned;
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }));
        Ok(Ticker)
    }
}

#[tokio::test]
async fn a_task_has_stopped_before_older_undos_run() {
    let app = App::new();
    let ticker = app.insert::<Ticker>(tag("ticker")).await;
    app.settled().await;
    assert_eq!(journal("ticker"), Vec::<String>::new());

    app.remove(ticker);
    app.settled().await;
    assert_eq!(journal("ticker"), ["task stopped", "flush"]);
}

/// Consumes `dyn Store`, and still reads it while undoing.
#[component(config = Tag, deps(store: dyn Store))]
struct Reader;

impl Component for Reader {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        let store = cx.deps().store.clone();
        cx.on_dispose(move || async move { record(&config.tag, format!("reader down, store {}", store.name())) });
        Ok(Reader)
    }
}

#[tokio::test]
async fn a_held_service_is_still_undone_on_time_and_consumers_go_first() {
    let app = App::new();
    let store = app.insert::<MemoryStore>(tag("held")).await;
    app.insert::<Reader>(tag("held")).await;
    app.settled().await;
    let held = app.get::<dyn Store>().await.expect("the store is active");

    app.remove(store);
    app.settled().await;
    assert_eq!(app.status(store).await, None);
    assert_eq!(journal("held"), ["reader down, store memory", "store down"]);
    // The host's copy is now just a value; its effects were undone without it.
    assert_eq!(held.name(), "memory");
}

event!(Ping = "dispose/ping", Emit(String));

/// Emits `Ping` from an undo older and an undo newer than its own listener.
#[component(config = Tag)]
struct Echo;

impl Component for Echo {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        let events = cx.events().clone();
        cx.on_dispose(move || async move { events.emit::<Ping>("older".into()) });
        let tag = config.tag;
        cx.on::<Ping>().listen(move |text| record(&tag, format!("heard {text}")));
        let events = cx.events().clone();
        cx.on_dispose(move || async move { events.emit::<Ping>("newer".into()) });
        Ok(Echo)
    }
}

#[tokio::test]
async fn a_listener_is_removed_in_its_place_in_the_list() {
    let app = App::new();
    let echo = app.insert::<Echo>(tag("echo")).await;
    app.settled().await;

    app.remove(echo);
    app.settled().await;
    assert_eq!(journal("echo"), ["heard newer"]);
}

#[component(config = Tag)]
struct Leaf;

impl Component for Leaf {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        cx.on_dispose(move || async move { record(&config.tag, "leaf down") });
        Ok(Leaf)
    }
}

/// Instantiates a `Leaf` between two undos.
#[component(config = Tag)]
struct Branch;

impl Component for Branch {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        let tag = config.tag.clone();
        cx.on_dispose(move || async move { record(&tag, "older undo") });
        let leaf = cx.plugin::<Leaf>(Tag { tag: config.tag.clone() }).await?;
        let tag = config.tag;
        cx.on_dispose(move || async move { record(&tag, "newer undo") });
        drop(leaf);
        Ok(Branch)
    }
}

#[tokio::test]
async fn a_child_is_retired_in_its_place_in_the_list() {
    let app = App::new();
    let branch = app.insert::<Branch>(tag("branch")).await;
    app.settled().await;

    app.remove(branch);
    app.settled().await;
    let entries = journal("branch");
    let at = |entry: &str| entries.iter().position(|e| e == entry).unwrap_or_else(|| panic!("{entry} missing"));
    assert_eq!(entries.len(), 3);
    assert_eq!(at("newer undo"), 0);
    // The parent doesn't wait for a retired child, so only the newer undo is ordered before it.
    assert!(at("newer undo") < at("leaf down"));
}

#[derive(Serialize, Deserialize)]
struct EndConfig {
    tag: String,
    /// `fail`, or `hang` until cancelled.
    then: String,
}

/// Adds an async undo, then fails or hangs.
#[component(config = EndConfig, deps(store: dyn Store))]
struct Unfinished;

impl Component for Unfinished {
    async fn activate(cx: Cx<Self>, config: EndConfig) -> Result<Self> {
        let tag = config.tag.clone();
        cx.on_dispose(move || async move {
            tokio::task::yield_now().await;
            record(&tag, "undone");
        });
        record(&config.tag, "started");
        match config.then.as_str() {
            "fail" => Err("unfinished".into()),
            _ => {
                let _bomb = Bomb;
                cx.cancelled().await;
                Err("cancelled".into())
            }
        }
    }
}

/// Panics when dropped, as a local of an activation that returns because it was cancelled.
struct Bomb;

impl Drop for Bomb {
    fn drop(&mut self) {
        panic!("bomb dropped");
    }
}

#[tokio::test]
async fn a_failed_activation_runs_its_async_undos() {
    let app = App::new();
    app.insert::<MemoryStore>(tag("failed")).await;
    let unfinished = app.insert::<Unfinished>(json!({ "tag": "failed", "then": "fail" })).await;
    app.settled().await;
    assert_eq!(app.status(unfinished).await, Some(Status::Failed("unfinished".into())));
    assert_eq!(journal("failed"), ["started", "undone"]);
}

#[tokio::test]
async fn a_cancelled_activation_runs_its_async_undos_even_when_a_local_panics_on_drop() {
    let app = App::new();
    let store = app.insert::<MemoryStore>(tag("cancelled")).await;
    let unfinished = app.insert::<Unfinished>(json!({ "tag": "cancelled", "then": "hang" })).await;
    until(|| journal("cancelled") == ["started"]).await;

    app.remove(store);
    tokio::time::timeout(Duration::from_secs(5), app.settled()).await.expect("the app settles");
    assert_eq!(app.status(unfinished).await, Some(Status::Inactive));
    let mut entries = journal("cancelled");
    entries.sort();
    assert_eq!(entries, ["started", "store down", "undone"]);
}

/// Panics in two of its undos.
#[component(config = Tag)]
struct Faulty;

impl Component for Faulty {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        let tag = config.tag.clone();
        cx.on_dispose(move || async move { record(&tag, "oldest") });
        cx.on_dispose(|| async { panic!("async undo panicked") });
        cx.on_dispose(|| -> Ready<()> { panic!("undo panicked before returning its future") });
        let tag = config.tag;
        cx.on_dispose(move || async move { record(&tag, "newest") });
        Ok(Faulty)
    }
}

#[tokio::test]
async fn a_panicking_undo_does_not_stop_the_rest() {
    let app = App::new();
    let faulty = app.insert::<Faulty>(tag("faulty")).await;
    app.settled().await;

    app.remove(faulty);
    app.settled().await;
    assert_eq!(app.status(faulty).await, None);
    assert_eq!(journal("faulty"), ["newest", "oldest"]);
}

static CONTEXTS: Mutex<Option<HashMap<String, Cx<Keeper>>>> = Mutex::new(None);

/// Keeps its `Cx` where the test can reach it after the fiber is gone.
#[component(config = Tag)]
struct Keeper;

impl Component for Keeper {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        CONTEXTS.lock().unwrap().get_or_insert_default().insert(config.tag, cx);
        Ok(Keeper)
    }
}

#[tokio::test]
async fn an_undo_added_after_disposal_runs_at_once() {
    let app = App::new();
    let keeper = app.insert::<Keeper>(tag("late")).await;
    app.settled().await;
    let cx = CONTEXTS.lock().unwrap().as_ref().unwrap()["late"].clone();
    assert!(cx.is_live());

    app.remove(keeper);
    app.settled().await;
    assert!(!cx.is_live());
    tokio::time::timeout(Duration::from_secs(5), cx.cancelled()).await.expect("cancelled once the fiber leaves");
    cx.on_dispose(Guard::new(|| record("late", "guard")));
    assert_eq!(journal("late"), ["guard"]);

    let owned = Recorder { tag: "late".into(), entry: "task stopped" };
    cx.on_dispose(tokio::spawn(async move {
        let _owned = owned;
        std::future::pending::<()>().await;
    }));
    until(|| journal("late") == ["guard", "task stopped"]).await;
}

/// Consumes `dyn Store`, and keeps its `Cx` in a listener, the cycle a dropped app must break.
#[component(config = Tag, deps(store: dyn Store))]
struct Clinger;

impl Component for Clinger {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        let tag = config.tag.clone();
        let kept = cx.clone();
        cx.on::<Ping>().listen(move |text| record(&tag, format!("heard {text}, live: {}", kept.is_live())));
        let tag = config.tag;
        cx.on_dispose(Guard::new(move || record(&tag, "clinger down")));
        Ok(Clinger)
    }
}

#[tokio::test]
async fn dropping_the_app_undoes_consumers_first_even_when_a_listener_keeps_its_cx() {
    let app = App::new();
    app.insert::<MemoryStore>(tag("dropped")).await;
    app.insert::<Clinger>(tag("dropped")).await;
    app.settled().await;
    let events = app.events().clone();
    events.emit::<Ping>("before".into());

    drop(app);
    until(|| journal("dropped").len() == 3).await;
    events.emit::<Ping>("after".into());
    assert_eq!(journal("dropped"), ["heard before, live: true", "clinger down", "store down"]);
}

/// Fails, and tries to instantiate a child from its own undo.
#[component(config = Tag)]
struct LateParent;

impl Component for LateParent {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        let kept = cx.clone();
        cx.on_dispose(move || async move {
            let tag = config.tag.clone();
            let refused = kept.plugin::<Leaf>(Tag { tag: config.tag }).await.is_err();
            record(&tag, format!("plugin refused: {refused}"));
        });
        Err("late parent failed".into())
    }
}

#[tokio::test]
async fn an_activation_that_ended_cannot_instantiate_children_while_it_is_undone() {
    let app = App::new();
    app.insert::<LateParent>(tag("late-parent")).await;
    app.settled().await;
    assert_eq!(journal("late-parent"), ["plugin refused: true"]);
}

pub trait Registry: Send + Sync {
    /// Registers `name` at once, then finishes setting it up; the guard comes back at the end.
    fn add(self: Arc<Self>, name: String) -> Pin<Box<dyn Future<Output = Guard> + Send>>;
}
key!(dyn Registry = "registry");

static SETUP: Notify = Notify::const_new();

#[component(config = Tag, provides(dyn Registry))]
struct SlowRegistry(String);

impl Registry for SlowRegistry {
    fn add(self: Arc<Self>, name: String) -> Pin<Box<dyn Future<Output = Guard> + Send>> {
        record(&self.0, format!("registered {name}"));
        Box::pin(async move {
            SETUP.notified().await;
            Guard::new(move || record(&self.0, format!("unregistered {name}")))
        })
    }
}

impl Component for SlowRegistry {
    async fn activate(_: Cx<Self>, config: Tag) -> Result<Self> {
        Ok(SlowRegistry(config.tag))
    }
}

#[component(deps(registry: dyn Registry))]
struct SearchTool;

impl Component for SearchTool {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        let registration = cx.deps().registry.clone().add("search".into()).await;
        cx.on_dispose(registration);
        Ok(SearchTool)
    }
}

#[tokio::test]
async fn an_activation_cancelled_mid_step_still_undoes_that_step() {
    let app = App::new();
    let registry = app.insert::<SlowRegistry>(tag("mid-step")).await;
    app.insert::<SearchTool>(json!(null)).await;
    until(|| journal("mid-step") == ["registered search"]).await;

    // The registry leaves while the tool's activation is inside `add`, between the effect and
    // the guard that undoes it.
    app.remove(registry);
    tokio::task::yield_now().await;
    SETUP.notify_one();
    app.settled().await;
    assert_eq!(journal("mid-step"), ["registered search", "unregistered search"]);
}

/// Adds an undo, then waits until its run is cancelled.
#[component(config = Tag)]
struct Waiter;

impl Component for Waiter {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        let tag = config.tag;
        let undone = tag.clone();
        cx.on_dispose(Guard::new(move || record(&undone, "undone")));
        record(&tag, "started");
        cx.cancelled().await;
        Err("cancelled".into())
    }
}

#[tokio::test]
async fn dropping_the_app_cancels_a_run_still_activating() {
    let app = App::new();
    app.insert::<Waiter>(tag("dropped mid-activate")).await;
    until(|| journal("dropped mid-activate") == ["started"]).await;

    drop(app);
    until(|| journal("dropped mid-activate") == ["started", "undone"]).await;
}

static QUITTERS: Mutex<Vec<Cx<Quitter>>> = Mutex::new(Vec::new());

/// Keeps its `Cx` where the test can reach it, then fails.
#[component]
struct Quitter;

impl Component for Quitter {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        QUITTERS.lock().unwrap().push(cx);
        Err("quit".into())
    }
}

#[tokio::test]
async fn a_failed_run_is_cancelled() {
    let app = App::new();
    let quitter = app.insert::<Quitter>(json!(null)).await;
    app.settled().await;
    assert_eq!(app.status(quitter).await, Some(Status::Failed("quit".into())));

    let cx = QUITTERS.lock().unwrap().pop().expect("the run kept its context");
    assert!(!cx.is_live());
    tokio::time::timeout(Duration::from_secs(5), cx.cancelled()).await.expect("cancelled once the run fails");
}
