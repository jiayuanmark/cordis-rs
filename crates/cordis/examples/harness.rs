//! A miniature agent harness: an agent runs turns through a tool runtime, which reads a
//! workspace and records to a session log. Independent components extend tool execution,
//! session recording, and turn control through typed events, and configuration changes take
//! effect without a restart, restarting exactly the components whose dependencies changed.
//!
//! Run with `cargo run --example harness`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cordis::{App, Catalog, Component, Cx, Events, Flow, Loader, Result, component, event, key};
use futures::future::BoxFuture;
use serde::Deserialize;

// ---- Vocabulary owned by the tools and session crates ----------------------------------

#[derive(Clone, Debug)]
pub struct ToolCall {
    pub tool: String,
    pub input: String,
}

#[derive(Clone, Debug)]
pub enum ToolResult {
    Ok(String),
    Failed(String),
    Denied(String),
}

#[derive(Debug)]
pub enum SessionEvent {
    ToolCall(ToolCall),
    ToolResult(ToolResult),
}

pub struct TurnState {
    pub steps: u32,
}

event!(
    /// Wraps every tool execution; listeners may rewrite the call, deny it, or edit the result.
    pub ToolsExecute = "tools/execute", Waterfall(ToolCall) -> ToolResult
);
event!(
    /// A durable session fact was appended.
    pub SessionAppended = "session/event", Emit(Arc<SessionEvent>)
);
event!(
    /// Asked after each step; the first listener that returns a reason stops the turn.
    pub TurnStopping = "agent/turn-stopping", Serial(TurnState) -> String
);

/// The files tools may read.
pub trait Workspace: Send + Sync {
    fn read(&self, path: &str) -> Option<String>;
}
key!(dyn Workspace = "workspace");

/// The durable record of a session.
pub trait Session: Send + Sync {
    fn append(&self, event: SessionEvent);
    fn count(&self) -> usize;
}
key!(dyn Session = "session");

/// Runs tool calls, with every listener of [`ToolsExecute`] around each one.
pub trait Tools: Send + Sync {
    fn run(&self, call: ToolCall) -> BoxFuture<'static, ToolResult>;
}
key!(dyn Tools = "tools");

/// Drives a turn of tool calls until a listener of [`TurnStopping`] stops it.
pub trait Agent: Send + Sync {
    fn turn(&self, calls: Vec<ToolCall>) -> BoxFuture<'static, ()>;
}
key!(dyn Agent = "agent");

// ---- Services the runtime builds on ------------------------------------------------------

#[derive(Deserialize)]
struct WorkspaceConfig {
    files: HashMap<String, String>,
}

/// A workspace held in memory, with its files taken from configuration.
#[component(config = WorkspaceConfig, provides(dyn Workspace))]
struct MemoryWorkspace {
    files: HashMap<String, String>,
}

impl Workspace for MemoryWorkspace {
    fn read(&self, path: &str) -> Option<String> {
        self.files.get(path).cloned()
    }
}

impl Component for MemoryWorkspace {
    async fn activate(_: Cx<Self>, config: WorkspaceConfig) -> Result<Self> {
        Ok(Self { files: config.files })
    }
}

/// Keeps session events in memory and announces each one on the event bus.
#[component(provides(dyn Session))]
struct SessionLog {
    events: Events,
    log: Mutex<Vec<Arc<SessionEvent>>>,
}

impl Session for SessionLog {
    fn append(&self, event: SessionEvent) {
        let event = Arc::new(event);
        self.log.lock().unwrap().push(event.clone());
        self.events.emit::<SessionAppended>(event);
    }

    fn count(&self) -> usize {
        self.log.lock().unwrap().len()
    }
}

impl Component for SessionLog {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        Ok(Self { events: cx.events().clone(), log: Mutex::default() })
    }
}

// ---- The tool runtime: owns execution and dispatches the events around it --------------

#[component(deps(workspace: dyn Workspace, session: dyn Session), provides(dyn Tools))]
struct ToolRuntime {
    cx: Cx<Self>,
}

impl Tools for ToolRuntime {
    fn run(&self, call: ToolCall) -> BoxFuture<'static, ToolResult> {
        let cx = self.cx.clone();
        Box::pin(async move {
            let deps = cx.deps();
            deps.session.append(SessionEvent::ToolCall(call.clone()));
            let workspace = deps.workspace.clone();
            let result = cx
                .events()
                .waterfall::<ToolsExecute>(call, |call| async move {
                    match call.tool.as_str() {
                        "read" => match workspace.read(&call.input) {
                            Some(text) => ToolResult::Ok(text),
                            None => ToolResult::Failed(format!("no such file `{}`", call.input)),
                        },
                        tool => ToolResult::Ok(format!("{tool} output for `{}`", call.input)),
                    }
                })
                .await;
            deps.session.append(SessionEvent::ToolResult(result.clone()));
            result
        })
    }
}

impl Component for ToolRuntime {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        Ok(Self { cx })
    }
}

// ---- The agent: runs a turn through whatever tools are active ----------------------------

#[component(deps(tools: dyn Tools, session: dyn Session), provides(dyn Agent))]
struct ScriptedAgent {
    cx: Cx<Self>,
}

impl Agent for ScriptedAgent {
    fn turn(&self, calls: Vec<ToolCall>) -> BoxFuture<'static, ()> {
        let cx = self.cx.clone();
        Box::pin(async move {
            let deps = cx.deps();
            for (steps, call) in (1..).zip(calls) {
                let result = deps.tools.run(call).await;
                println!("step {steps}: {result:?}");
                if let Some(reason) = cx.events().serial::<TurnStopping>(TurnState { steps }).await {
                    println!("turn stopped: {reason}");
                    break;
                }
            }
            println!("session holds {} events", deps.session.count());
        })
    }
}

impl Component for ScriptedAgent {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        Ok(Self { cx })
    }
}

// ---- Independent extensions: none of them names the runtime or each other --------------

#[derive(Deserialize)]
struct ApprovalConfig {
    deny: Vec<String>,
}

/// Denies shell commands with configured prefixes before they run.
#[component(config = ApprovalConfig)]
struct ApprovalPolicy;

impl Component for ApprovalPolicy {
    async fn activate(cx: Cx<Self>, config: ApprovalConfig) -> Result<Self> {
        let deny = Arc::new(config.deny);
        cx.on::<ToolsExecute>().listen(move |call, next| {
            let deny = deny.clone();
            async move {
                if call.tool == "bash" && deny.iter().any(|prefix| call.input.starts_with(prefix.as_str())) {
                    return Flow::short_circuit(ToolResult::Denied(format!("`{}` needs approval", call.input)));
                }
                next.run(call).await
            }
        });
        Ok(Self)
    }
}

#[derive(Deserialize)]
struct LimitConfig {
    max_chars: usize,
}

/// Truncates tool output after the tool ran.
#[component(config = LimitConfig)]
struct OutputLimit;

impl Component for OutputLimit {
    async fn activate(cx: Cx<Self>, config: LimitConfig) -> Result<Self> {
        let max = config.max_chars;
        cx.on::<ToolsExecute>().listen(move |call, next| async move {
            next.run(call).await.map(|result| match result {
                ToolResult::Ok(text) if text.len() > max => ToolResult::Ok(format!("{}…", &text[..max])),
                other => other,
            })
        });
        Ok(Self)
    }
}

/// Prints every session event, as a transcript view would.
#[component]
struct Transcript;

impl Component for Transcript {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        cx.on::<SessionAppended>().listen(|event| println!("  session: {event:?}"));
        Ok(Self)
    }
}

#[derive(Deserialize)]
struct BudgetConfig {
    max_steps: u32,
}

/// Stops a turn after a fixed number of steps.
#[component(config = BudgetConfig)]
struct StepBudget;

impl Component for StepBudget {
    async fn activate(cx: Cx<Self>, config: BudgetConfig) -> Result<Self> {
        let max = config.max_steps;
        cx.on::<TurnStopping>()
            .listen(move |state| async move { (state.steps >= max).then(|| format!("step budget of {max} reached")) });
        Ok(Self)
    }
}

// ---- Running it --------------------------------------------------------------------------

const CONFIG: &str = r#"
plugins:
  - { id: agent, use: scripted-agent }
  - { id: tools, use: tool-runtime }
  - { id: session, use: session-log }
  - { id: workspace, use: memory-workspace, config: { files: { src/main.rs: "fn main() {}" } } }
  - { id: approval, use: approval-policy, config: { deny: ["rm "] } }
  - { id: limit, use: output-limit, config: { max_chars: 24 } }
  - { id: transcript, use: transcript }
  - { id: budget, use: step-budget, config: { max_steps: 3 } }
"#;

async fn turn(app: &App) {
    let calls = [("bash", "ls -la"), ("bash", "rm -rf build"), ("read", "src/main.rs"), ("read", "README.md")];
    let calls = calls.iter().map(|(tool, input)| ToolCall { tool: tool.to_string(), input: input.to_string() });
    let agent = app.get::<dyn Agent>().await.expect("the agent and everything it needs are active");
    agent.turn(calls.collect()).await;
}

#[tokio::main]
async fn main() {
    let app = App::new();
    let mut catalog = Catalog::new();
    catalog
        .register::<ScriptedAgent>("scripted-agent")
        .register::<ToolRuntime>("tool-runtime")
        .register::<SessionLog>("session-log")
        .register::<MemoryWorkspace>("memory-workspace")
        .register::<ApprovalPolicy>("approval-policy")
        .register::<OutputLimit>("output-limit")
        .register::<Transcript>("transcript")
        .register::<StepBudget>("step-budget");
    let mut loader = Loader::new(app.clone(), catalog);

    // Rows are listed consumers first: the orchestrator activates each one once its
    // dependencies are active, whatever the order.
    println!("== full profile");
    loader.apply_yaml(CONFIG).await.unwrap();
    app.settled().await;
    turn(&app).await;

    // The workspace's configuration changes, so it restarts. The tool runtime depends on it
    // and the agent on the runtime, so both restart too. The session log does not, and keeps
    // its events.
    println!("\n== transcript and approval removed, budget raised, README added; no restart");
    let patched = CONFIG
        .lines()
        .filter(|line| !line.contains("id: transcript") && !line.contains("id: approval"))
        .collect::<Vec<_>>()
        .join("\n")
        .replace("max_steps: 3", "max_steps: 10")
        .replace(
            r#"src/main.rs: "fn main() {}""#,
            r#"src/main.rs: "fn main() {}", README.md: "A miniature agent harness built on cordis""#,
        );
    loader.apply_yaml(&patched).await.unwrap();
    app.settled().await;
    turn(&app).await;

    app.shutdown().await;
}
