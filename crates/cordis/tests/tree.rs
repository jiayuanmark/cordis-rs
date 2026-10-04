//! Child fibers: instantiated through `Cx::plugin`, retired with the activation that made them.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use cordis::{App, Component, Cx, FiberId, Result, Status, component, key};
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

#[derive(Serialize, Deserialize)]
struct ChildConfig {
    tag: String,
    name: String,
    /// Makes `deactivate` wait for `GATE`.
    slow: bool,
}

#[component(config = ChildConfig)]
struct Child {
    tag: String,
    name: String,
    slow: bool,
}

static GATE: Notify = Notify::const_new();

impl Component for Child {
    async fn activate(_: Cx<Self>, config: ChildConfig) -> Result<Self> {
        record(&config.tag, format!("{} up", config.name));
        Ok(Self { tag: config.tag, name: config.name, slow: config.slow })
    }

    async fn deactivate(self) {
        if self.slow {
            GATE.notified().await;
        }
        record(&self.tag, format!("{} down", self.name));
    }
}

#[derive(Deserialize)]
struct ParentConfig {
    tag: String,
    /// What `activate` does after instantiating its first child: `ok`, `fail`, or `hang`.
    #[serde(default)]
    then: String,
    #[serde(default)]
    slow_child: bool,
}

/// Instantiates a child named after the prompt text, then follows `then`. Keeps its `Cx` in
/// `CONTEXTS` so tests can instantiate more children from outside `activate`.
#[component(config = ParentConfig, deps(prompt: dyn Prompt))]
struct Parent {
    tag: String,
}

static CONTEXTS: Mutex<Option<HashMap<String, Cx<Parent>>>> = Mutex::new(None);

fn context(tag: &str) -> Cx<Parent> {
    CONTEXTS.lock().unwrap().as_ref().unwrap()[tag].clone()
}

impl Component for Parent {
    async fn activate(cx: Cx<Self>, config: ParentConfig) -> Result<Self> {
        let tag = config.tag;
        CONTEXTS.lock().unwrap().get_or_insert_default().insert(tag.clone(), cx.clone());
        let name = cx.deps().prompt.text();
        cx.plugin::<Child>(ChildConfig { tag: tag.clone(), name, slow: config.slow_child }).await?;
        match config.then.as_str() {
            "fail" => Err("parent failed".into()),
            "hang" => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                unreachable!("the test cancels this activation");
            }
            _ => Ok(Self { tag }),
        }
    }

    async fn deactivate(self) {
        record(&self.tag, "parent down");
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

/// Polls until `done` holds; the reactor's work is asynchronous.
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
    assert_eq!(app.parent(extra).await, Some(parent));
    assert_eq!(app.parent(parent).await, None);
    assert_eq!(app.status(extra).await, Some(Status::Active));

    app.remove(parent);
    app.settled().await;
    assert_eq!(app.status(parent).await, None);
    assert_eq!(app.status(extra).await, None);
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
    assert_eq!(journal("cancel"), ["first up", "first down"]);
}

#[tokio::test]
async fn a_failed_activation_retires_its_children() {
    let (app, _, parent) = start("fail", json!({ "then": "fail" })).await;
    app.settled().await;
    assert_eq!(app.status(parent).await, Some(Status::Failed("parent failed".into())));
    assert_eq!(journal("fail"), ["first up", "first down"]);
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
