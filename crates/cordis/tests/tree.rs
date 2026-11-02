//! Child fibers: instantiated through `Cx::plugin`, retired with the activation that made them.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use cordis::{App, Component, Cx, FiberId, Result, Status, component, key};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{Notify, mpsc};

// Journal shared by the components below. Tests run in parallel, so each entry carries the
// test's tag.

static JOURNAL: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

fn record(tag: &str, entry: impl Into<String>) {
    JOURNAL.lock().unwrap().push((tag.to_owned(), entry.into()));
}

fn journal(tag: &str) -> Vec<String> {
    JOURNAL.lock().unwrap().iter().filter(|(t, _)| t == tag).map(|(_, e)| e.clone()).collect()
}

pub trait Prompt: Send + Sync {
    fn text(&self) -> String;
}
key!(dyn Prompt = "prompt");

#[derive(Deserialize)]
struct PromptConfig {
    text: String,
}

#[component(config = PromptConfig, provides(dyn Prompt))]
struct StaticPrompt(String);

impl Prompt for StaticPrompt {
    fn text(&self) -> String {
        self.0.clone()
    }
}

impl Component for StaticPrompt {
    async fn activate(_: Cx<Self>, config: PromptConfig) -> Result<Self> {
        Ok(Self(config.text))
    }
}

#[derive(Clone)]
struct ChildConfig {
    tag: String,
    name: String,
    /// Makes its undo wait for `GATE`.
    slow: bool,
}

#[component(config = ChildConfig)]
struct Child;

static GATE: Notify = Notify::const_new();

impl Component for Child {
    async fn activate(cx: Cx<Self>, config: ChildConfig) -> Result<Self> {
        record(&config.tag, format!("{} up", config.name));
        let ChildConfig { tag, name, slow } = config;
        cx.on_dispose(move || async move {
            if slow {
                GATE.notified().await;
            }
            record(&tag, format!("{name} down"));
        });
        Ok(Child)
    }
}

#[derive(Deserialize)]
struct ParentConfig {
    tag: String,
    /// What `activate` does after instantiating its first child: `ok`, `fail`, or `hang` until
    /// cancelled.
    #[serde(default)]
    then: String,
    #[serde(default)]
    slow_child: bool,
}

pub trait Marker: Send + Sync {}
key!(dyn Marker = "marker");

/// Instantiates a child named after the prompt text, then follows `then`. Keeps its `Cx` in
/// `CONTEXTS` so tests can instantiate more children from outside `activate`.
#[component(config = ParentConfig, deps(prompt: dyn Prompt), provides(dyn Marker))]
struct Parent;

impl Marker for Parent {}

/// Consumes the parent's `Marker`; its undo waits for `WATCH_GATE`, which holds the parent in
/// `Leaving`.
#[component(deps(marker: dyn Marker))]
struct Watcher;

static WATCH_GATE: Notify = Notify::const_new();

impl Component for Watcher {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        cx.on_dispose(|| WATCH_GATE.notified());
        Ok(Watcher)
    }
}

static CONTEXTS: Mutex<Option<HashMap<String, Cx<Parent>>>> = Mutex::new(None);

fn context(tag: &str) -> Cx<Parent> {
    CONTEXTS.lock().unwrap().as_ref().unwrap()[tag].clone()
}

impl Component for Parent {
    async fn activate(cx: Cx<Self>, config: ParentConfig) -> Result<Self> {
        let tag = config.tag;
        CONTEXTS.lock().unwrap().get_or_insert_default().insert(tag.clone(), cx.clone());
        let down = tag.clone();
        cx.on_dispose(move || async move { record(&down, "parent down") });
        let name = cx.deps().prompt.text();
        cx.plugin::<Child>(ChildConfig { tag: tag.clone(), name, slow: config.slow_child }).await?;
        match config.then.as_str() {
            "fail" => Err("parent failed".into()),
            "hang" => {
                cx.cancelled().await;
                Err("cancelled".into())
            }
            _ => Ok(Parent),
        }
    }
}

async fn start(tag: &str, parent: serde_json::Value) -> (App, FiberId, FiberId) {
    let app = App::new();
    let prompt = app.insert::<StaticPrompt>(json!({ "text": "first" })).await;
    let mut config = parent;
    config["tag"] = json!(tag);
    let parent = app.insert::<Parent>(config).await;
    while !journal(tag).iter().any(|entry| entry == "first up") {
        tokio::task::yield_now().await;
    }
    (app, prompt, parent)
}

/// The child's journal for an activation that failed or was cancelled, after checking that the
/// parent's own undo ran too. Retiring the child doesn't wait for it, so the parent's entry can
/// land anywhere among the child's.
fn undone(tag: &str) -> Vec<String> {
    let mut entries = journal(tag);
    let parent = entries.iter().position(|entry| entry == "parent down").expect("the parent's undo ran");
    entries.remove(parent);
    entries
}

/// Polls until `done` holds; the orchestrator's work is asynchronous.
async fn until(mut done: impl AsyncFnMut() -> bool) {
    for _ in 0..1000 {
        if done().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("condition not reached");
}

#[tokio::test]
async fn a_child_is_retired_when_its_parent_unloads() {
    let (app, _, parent) = start("unload", json!({})).await;
    let extra = context("unload")
        .plugin::<Child>(ChildConfig { tag: "unload".into(), name: "extra".into(), slow: false })
        .await
        .unwrap();
    app.settled().await;
    assert_eq!(app.parent(extra.id()).await, Some(parent));
    assert_eq!(app.parent(parent).await, None);
    assert_eq!(extra.status().await, Some(Status::Active));

    app.remove(parent);
    app.settled().await;
    assert_eq!(app.status(parent).await, None);
    assert_eq!(extra.status().await, None);
    let mut entries = journal("unload");
    entries.sort();
    assert_eq!(entries, ["extra down", "extra up", "first down", "first up", "parent down"]);
}

#[tokio::test]
async fn a_parent_unloads_without_waiting_for_its_children() {
    let (app, _, parent) = start("eager", json!({ "slow_child": true })).await;
    app.settled().await;

    app.remove(parent);
    until(async || app.status(parent).await.is_none()).await;
    assert_eq!(journal("eager"), ["first up", "parent down"]);

    GATE.notify_one();
    app.settled().await;
    assert_eq!(journal("eager"), ["first up", "parent down", "first down"]);
}

#[tokio::test]
async fn a_cancelled_activation_retires_its_children() {
    let (app, prompt, parent) = start("cancel", json!({ "then": "hang" })).await;
    assert_eq!(app.status(parent).await, Some(Status::Loading));

    app.remove(prompt);
    app.settled().await;
    assert_eq!(app.status(parent).await, Some(Status::Inactive));
    assert_eq!(undone("cancel"), ["first up", "first down"]);
}

#[tokio::test]
async fn a_failed_activation_retires_its_children() {
    let (app, _, parent) = start("fail", json!({ "then": "fail" })).await;
    app.settled().await;
    assert_eq!(app.status(parent).await, Some(Status::Failed("parent failed".into())));
    assert_eq!(undone("fail"), ["first up", "first down"]);
}

#[tokio::test]
async fn a_reloaded_parent_replaces_its_children() {
    let (app, prompt, parent) = start("reload", json!({})).await;
    app.settled().await;

    app.update(prompt, json!({ "text": "second" }));
    app.settled().await;
    assert_eq!(app.status(parent).await, Some(Status::Active));
    // The old activation and its child unload concurrently; the new child comes after both.
    let entries = journal("reload");
    let mut old = entries[1..3].to_vec();
    old.sort();
    assert_eq!(old, ["first down", "parent down"]);
    assert_eq!(entries[3..], ["second up"]);
}

#[tokio::test]
async fn a_context_kept_past_its_activation_cannot_instantiate_children() {
    let (app, _, parent) = start("late", json!({})).await;
    app.settled().await;
    let cx = context("late");

    app.remove(parent);
    app.settled().await;
    let late = cx.plugin::<Child>(ChildConfig { tag: "late".into(), name: "late".into(), slow: false }).await;
    assert!(late.is_err());
    app.settled().await;
    assert_eq!(journal("late"), ["first up", "parent down", "first down"]);
}

#[tokio::test]
async fn a_child_handle_disposes_the_child_early() {
    let (app, _, parent) = start("dispose", json!({})).await;
    let child = context("dispose")
        .plugin::<Child>(ChildConfig { tag: "dispose".into(), name: "agent".into(), slow: false })
        .await
        .unwrap();
    app.settled().await;
    assert_eq!(child.status().await, Some(Status::Active));

    child.dispose();
    app.settled().await;
    assert_eq!(child.status().await, None);
    assert_eq!(app.status(parent).await, Some(Status::Active));
    assert_eq!(journal("dispose"), ["first up", "agent up", "agent down"]);
}

#[tokio::test]
async fn a_leaving_fiber_cannot_instantiate_children() {
    let (app, _, parent) = start("leaving", json!({})).await;
    app.insert::<Watcher>(json!(null)).await;
    app.settled().await;

    // The watcher's teardown holds the parent in `Leaving`.
    app.remove(parent);
    until(async || app.status(parent).await == Some(Status::Leaving)).await;
    let late = context("leaving")
        .plugin::<Child>(ChildConfig { tag: "leaving".into(), name: "late".into(), slow: false })
        .await;
    assert!(late.is_err());

    WATCH_GATE.notify_one();
    app.settled().await;
    assert_eq!(journal("leaving"), ["first up", "parent down", "first down"]);
}

/// Non-serializable configuration carrying an in-memory channel sender.
#[derive(Clone)]
struct ChannelConfig {
    label: &'static str,
    tx: mpsc::UnboundedSender<String>,
}

#[component(config = ChannelConfig, deps(prompt: dyn Prompt))]
struct ChannelChild;

impl Component for ChannelChild {
    async fn activate(cx: Cx<Self>, config: ChannelConfig) -> Result<Self> {
        let _ = config.tx.send(format!("{}:{}", config.label, cx.deps().prompt.text()));
        Ok(ChannelChild)
    }
}

#[tokio::test]
async fn a_child_config_need_not_be_serializable_and_survives_reloads_and_updates() {
    let (app, prompt, _parent) = start("non_serde", json!({})).await;
    app.settled().await;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let child =
        context("non_serde").plugin::<ChannelChild>(ChannelConfig { label: "v1", tx: tx.clone() }).await.unwrap();
    app.settled().await;
    assert_eq!(rx.try_recv().unwrap(), "v1:first");

    // Updating the child's dependency reloads the child with its cloned in-memory config.
    app.update(prompt, json!({ "text": "second" }));
    // Wait: `parent` depends on `prompt`, so reloading `prompt` reloads `parent` and retires `child`.
    // Test `app.plugin` / `child.update` directly for a fiber that survives `prompt` reload:
    app.settled().await;
    assert_eq!(child.status().await, None);

    let root_child = app.plugin::<ChannelChild>(ChannelConfig { label: "r1", tx: tx.clone() }).await.unwrap();
    app.settled().await;
    assert_eq!(rx.try_recv().unwrap(), "r1:second");

    // Reloading `prompt` reloads `root_child` by cloning `ChannelConfig` in memory.
    app.update(prompt, json!({ "text": "third" }));
    app.settled().await;
    assert_eq!(rx.try_recv().unwrap(), "r1:third");

    // Updating `root_child` through its `Fiber` handle replaces the in-memory config.
    root_child.update(ChannelConfig { label: "r2", tx });
    app.settled().await;
    assert_eq!(rx.try_recv().unwrap(), "r2:third");
}
