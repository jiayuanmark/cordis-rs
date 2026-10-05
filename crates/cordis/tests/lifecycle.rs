//! Reactive lifecycle behavior: cascades, provider replacement, failure, cancellation, loading.

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use cordis::{App, Catalog, Component, Cx, FiberId, Guard, LoadError, Loader, Result, Status, component, key};
use serde::Deserialize;
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

// Services.

pub trait Prompt: Send + Sync {
    fn text(&self) -> String;
}
key!(dyn Prompt = "prompt");

pub trait Tools: Send + Sync {
    fn register(&self, name: &str) -> Guard;
    fn names(&self) -> Vec<String>;
}
key!(dyn Tools = "tools");

// Components.

#[derive(Deserialize)]
struct Tag {
    tag: String,
}

#[derive(Deserialize)]
struct PromptConfig {
    tag: String,
    text: String,
}

#[component(config = PromptConfig, provides(dyn Prompt))]
struct StaticPrompt {
    tag: String,
    text: String,
}

impl Prompt for StaticPrompt {
    fn text(&self) -> String {
        self.text.clone()
    }
}

impl Component for StaticPrompt {
    async fn activate(_: Cx<Self>, config: PromptConfig) -> Result<Self> {
        record(&config.tag, format!("activate prompt {}", config.text));
        Ok(Self { tag: config.tag, text: config.text })
    }
}

impl Drop for StaticPrompt {
    fn drop(&mut self) {
        record(&self.tag, format!("drop prompt {}", self.text));
    }
}

#[component(config = Tag, deps(prompt: dyn Prompt), provides(dyn Tools))]
struct ToolRegistry {
    tag: String,
    _prompt: Arc<dyn Prompt>,
    names: Arc<Mutex<Vec<String>>>,
}

impl Tools for ToolRegistry {
    fn register(&self, name: &str) -> Guard {
        self.names.lock().unwrap().push(name.to_owned());
        let names: Weak<Mutex<Vec<String>>> = Arc::downgrade(&self.names);
        let name = name.to_owned();
        Guard::new(move || {
            if let Some(names) = names.upgrade() {
                names.lock().unwrap().retain(|entry| *entry != name);
            }
        })
    }

    fn names(&self) -> Vec<String> {
        self.names.lock().unwrap().clone()
    }
}

impl Component for ToolRegistry {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        let prompt = cx.deps().prompt.clone();
        record(&config.tag, format!("activate tools with {}", prompt.text()));
        Ok(Self { tag: config.tag, _prompt: prompt, names: Arc::default() })
    }
}

impl Drop for ToolRegistry {
    fn drop(&mut self) {
        record(&self.tag, "drop tools");
    }
}

#[component(config = Tag, deps(tools: dyn Tools))]
struct GrepTool {
    tag: String,
    _registration: Guard,
}

impl Component for GrepTool {
    async fn activate(cx: Cx<Self>, config: Tag) -> Result<Self> {
        record(&config.tag, "activate grep");
        Ok(Self { tag: config.tag, _registration: cx.deps().tools.register("grep") })
    }
}

impl Drop for GrepTool {
    fn drop(&mut self) {
        record(&self.tag, "drop grep");
    }
}

async fn status(app: &App, ids: &[FiberId]) -> Vec<Option<Status>> {
    let mut statuses = Vec::new();
    for id in ids {
        statuses.push(app.status(*id).await);
    }
    statuses
}

#[tokio::test]
async fn activation_cascades_to_dependents_and_teardown_runs_consumers_first() {
    let app = App::new();
    let tag = json!({ "tag": "cascade" });
    let grep = app.insert::<GrepTool>(tag.clone()).await;
    let tools = app.insert::<ToolRegistry>(tag).await;
    app.settled().await;
    assert_eq!(status(&app, &[tools, grep]).await, [Some(Status::Inactive), Some(Status::Inactive)]);

    let prompt = app.insert::<StaticPrompt>(json!({ "tag": "cascade", "text": "v1" })).await;
    app.settled().await;
    assert_eq!(status(&app, &[prompt, tools, grep]).await, [const { Some(Status::Active) }; 3]);
    assert_eq!(app.get::<dyn Tools>().await.unwrap().names(), ["grep"]);

    app.remove(prompt);
    app.settled().await;
    assert_eq!(status(&app, &[prompt, tools, grep]).await, [None, Some(Status::Inactive), Some(Status::Inactive)]);
    assert!(app.get::<dyn Tools>().await.is_none());
    assert_eq!(
        journal("cascade"),
        ["activate prompt v1", "activate tools with v1", "activate grep", "drop grep", "drop tools", "drop prompt v1",]
    );
}

#[tokio::test]
async fn a_waiting_provider_takes_over_and_dependents_reload_against_it() {
    let app = App::new();
    let first = app.insert::<StaticPrompt>(json!({ "tag": "failover", "text": "a" })).await;
    let tools = app.insert::<ToolRegistry>(json!({ "tag": "failover" })).await;
    app.settled().await;
    let second = app.insert::<StaticPrompt>(json!({ "tag": "failover", "text": "b" })).await;
    app.settled().await;
    assert_eq!(app.status(second).await, Some(Status::Inactive));

    app.remove(first);
    app.settled().await;
    assert_eq!(status(&app, &[second, tools]).await, [const { Some(Status::Active) }; 2]);
    assert_eq!(app.get::<dyn Prompt>().await.unwrap().text(), "b");

    // The replacement starts only once the old provider has fully stopped, which in turn waits
    // for the old consumer's teardown.
    let entries = journal("failover");
    let at = |entry: &str| entries.iter().position(|e| e == entry).unwrap_or_else(|| panic!("missing {entry}"));
    assert_eq!(entries.len(), 6);
    assert!(at("activate tools with a") < at("drop tools"));
    assert!(at("drop tools") < at("drop prompt a"));
    assert!(at("drop prompt a") < at("activate prompt b"));
    assert!(at("activate prompt b") < at("activate tools with b"));
}

#[tokio::test]
async fn a_configuration_update_reloads_the_fiber_and_its_dependents() {
    let app = App::new();
    let prompt = app.insert::<StaticPrompt>(json!({ "tag": "update", "text": "v1" })).await;
    app.insert::<ToolRegistry>(json!({ "tag": "update" })).await;
    app.settled().await;

    app.update(prompt, json!({ "tag": "update", "text": "v2" }));
    app.settled().await;
    assert_eq!(
        journal("update"),
        [
            "activate prompt v1",
            "activate tools with v1",
            "drop tools",
            "drop prompt v1",
            "activate prompt v2",
            "activate tools with v2"
        ]
    );
}

#[derive(Deserialize)]
struct FlakyConfig {
    fail: bool,
}

#[component(config = FlakyConfig)]
struct Flaky;

impl Component for Flaky {
    async fn activate(_: Cx<Self>, config: FlakyConfig) -> Result<Self> {
        if config.fail { Err("configured to fail".into()) } else { Ok(Flaky) }
    }
}

#[tokio::test]
async fn a_failed_activation_is_isolated_and_retried_after_a_configuration_change() {
    let app = App::new();
    let flaky = app.insert::<Flaky>(json!({ "fail": true })).await;
    let prompt = app.insert::<StaticPrompt>(json!({ "tag": "failure", "text": "ok" })).await;
    app.settled().await;
    assert_eq!(app.status(flaky).await, Some(Status::Failed("configured to fail".into())));
    assert_eq!(app.status(prompt).await, Some(Status::Active));

    app.update(flaky, json!({ "fail": false }));
    app.settled().await;
    assert_eq!(app.status(flaky).await, Some(Status::Active));
}

#[component(config = Tag, deps(prompt: dyn Prompt))]
struct Slow;

impl Component for Slow {
    async fn activate(_: Cx<Self>, config: Tag) -> Result<Self> {
        let tag = config.tag.clone();
        let _partial = Guard::new(move || record(&tag, "revert partial activation"));
        record(&config.tag, "activation started");
        tokio::time::sleep(Duration::from_secs(3600)).await;
        unreachable!("the test cancels this activation");
    }
}

#[tokio::test]
async fn losing_a_dependency_cancels_an_activation_and_reverts_its_partial_effects() {
    let app = App::new();
    let prompt = app.insert::<StaticPrompt>(json!({ "tag": "cancel", "text": "x" })).await;
    let slow = app.insert::<Slow>(json!({ "tag": "cancel" })).await;
    while !journal("cancel").iter().any(|entry| entry == "activation started") {
        tokio::task::yield_now().await;
    }
    assert_eq!(app.status(slow).await, Some(Status::Loading));

    app.remove(prompt);
    app.settled().await;
    assert_eq!(app.status(slow).await, Some(Status::Inactive));
    assert_eq!(
        journal("cancel"),
        ["activate prompt x", "activation started", "revert partial activation", "drop prompt x"]
    );
}

// A provider whose start and stop take a while, so a competitor has a window to overlap with it.

#[derive(Deserialize)]
struct ServerConfig {
    tag: String,
    name: String,
    #[serde(default)]
    fail: bool,
}

#[component(config = ServerConfig, provides(dyn Prompt))]
struct Server {
    tag: String,
    name: String,
}

impl Prompt for Server {
    fn text(&self) -> String {
        self.name.clone()
    }
}

impl Component for Server {
    async fn activate(_: Cx<Self>, config: ServerConfig) -> Result<Self> {
        record(&config.tag, format!("start {}", config.name));
        tokio::time::sleep(Duration::from_millis(20)).await;
        if config.fail {
            record(&config.tag, format!("fail {}", config.name));
            return Err("configured to fail".into());
        }
        Ok(Self { tag: config.tag, name: config.name })
    }

    async fn deactivate(self) {
        tokio::time::sleep(Duration::from_millis(20)).await;
        record(&self.tag, format!("stop {}", self.name));
    }
}

#[tokio::test]
async fn a_competing_provider_waits_while_the_first_is_still_starting() {
    let app = App::new();
    let first = app.insert::<Server>(json!({ "tag": "starting", "name": "a" })).await;
    let second = app.insert::<Server>(json!({ "tag": "starting", "name": "b" })).await;
    app.settled().await;
    assert_eq!(status(&app, &[first, second]).await, [Some(Status::Active), Some(Status::Inactive)]);
    assert_eq!(journal("starting"), ["start a"]);
}

#[tokio::test]
async fn a_waiting_provider_starts_only_after_the_first_has_fully_stopped() {
    let app = App::new();
    let first = app.insert::<Server>(json!({ "tag": "stopping", "name": "a" })).await;
    app.settled().await;
    let second = app.insert::<Server>(json!({ "tag": "stopping", "name": "b" })).await;
    app.remove(first);
    app.settled().await;
    assert_eq!(app.status(second).await, Some(Status::Active));
    assert_eq!(journal("stopping"), ["start a", "stop a", "start b"]);
}

#[tokio::test]
async fn a_configuration_update_keeps_the_key_from_a_waiting_provider() {
    let app = App::new();
    let first = app.insert::<Server>(json!({ "tag": "reload", "name": "a1" })).await;
    app.settled().await;
    let second = app.insert::<Server>(json!({ "tag": "reload", "name": "b" })).await;
    app.update(first, json!({ "tag": "reload", "name": "a2" }));
    app.settled().await;
    assert_eq!(status(&app, &[first, second]).await, [Some(Status::Active), Some(Status::Inactive)]);
    assert_eq!(app.get::<dyn Prompt>().await.unwrap().text(), "a2");
    assert_eq!(journal("reload"), ["start a1", "stop a1", "start a2"]);
}

#[tokio::test]
async fn a_failed_provider_frees_the_key_for_a_waiting_one() {
    let app = App::new();
    let first = app.insert::<Server>(json!({ "tag": "failed", "name": "a", "fail": true })).await;
    let second = app.insert::<Server>(json!({ "tag": "failed", "name": "b" })).await;
    app.settled().await;
    assert_eq!(
        status(&app, &[first, second]).await,
        [Some(Status::Failed("configured to fail".into())), Some(Status::Active)]
    );
    assert_eq!(journal("failed"), ["start a", "fail a", "start b"]);
}

// A prompt derived from tools closes a cycle with ToolRegistry.
#[component(deps(tools: dyn Tools), provides(dyn Prompt))]
struct ToolsPrompt;

impl Prompt for ToolsPrompt {
    fn text(&self) -> String {
        String::new()
    }
}

impl Component for ToolsPrompt {
    async fn activate(_: Cx<Self>, _: ()) -> Result<Self> {
        Ok(ToolsPrompt)
    }
}

fn catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog
        .register::<StaticPrompt>("static-prompt")
        .register::<ToolsPrompt>("tools-prompt")
        .register::<ToolRegistry>("tool-registry")
        .register::<GrepTool>("grep-tool");
    catalog
}

#[tokio::test]
async fn the_loader_mounts_a_tree_and_reconciles_changes() {
    let app = App::new();
    let mut loader = Loader::new(app.clone(), catalog());
    let tree = r#"
plugins:
  - id: prompt
    use: static-prompt
    config: { tag: loader, text: v1 }
  - id: tools
    use: tool-registry
    config: { tag: loader }
  - id: grep
    use: grep-tool
    config: { tag: loader }
"#;
    loader.apply_yaml(tree).await.unwrap();
    app.settled().await;
    let grep = loader.fiber("grep").unwrap();
    assert_eq!(app.status(grep).await, Some(Status::Active));

    let without_grep = tree.replace("  - id: grep\n    use: grep-tool\n    config: { tag: loader }\n", "");
    loader.apply_yaml(&without_grep).await.unwrap();
    app.settled().await;
    assert_eq!(app.status(grep).await, None);
    assert!(app.get::<dyn Tools>().await.unwrap().names().is_empty());

    loader.apply_yaml(&without_grep.replace("text: v1", "text: v2")).await.unwrap();
    app.settled().await;
    assert_eq!(app.get::<dyn Prompt>().await.unwrap().text(), "v2");
}

#[tokio::test]
async fn the_loader_rejects_invalid_trees_before_mounting_anything() {
    let app = App::new();
    let mut loader = Loader::new(app, catalog());

    let missing = "plugins:\n  - { id: grep, use: grep-tool, config: { tag: t } }\n";
    assert!(matches!(loader.apply_yaml(missing).await, Err(LoadError::MissingProvider { key: "tools", .. })));

    let unknown = "plugins:\n  - { id: x, use: nope }\n";
    assert!(matches!(loader.apply_yaml(unknown).await, Err(LoadError::UnknownComponent { .. })));

    let invalid = "plugins:\n  - { id: p, use: static-prompt, config: { tag: t } }\n";
    assert!(matches!(loader.apply_yaml(invalid).await, Err(LoadError::InvalidConfig { .. })));

    let cycle =
        "plugins:\n  - { id: tools, use: tool-registry, config: { tag: t } }\n  - { id: prompt, use: tools-prompt }\n";
    match loader.apply_yaml(cycle).await {
        Err(LoadError::Cycle(rows)) => assert_eq!(rows, ["tools", "prompt", "tools"]),
        other => panic!("expected a cycle, got {other:?}"),
    }
    assert!(loader.fiber("tools").is_none());
}
