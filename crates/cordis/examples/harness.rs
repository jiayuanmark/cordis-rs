//! A miniature agent harness: independent components extend tool execution, session
//! recording, and turn control through typed events, and configuration changes take effect
//! without a restart.
//!
//! Run with `cargo run --example harness`.

use std::sync::Arc;

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

pub trait Tools: Send + Sync {
    fn run(&self, call: ToolCall) -> BoxFuture<'static, ToolResult>;
}
key!(dyn Tools = "tools");

// ---- The tool runtime: owns execution and dispatches the events around it --------------

struct ToolRuntime {
    events: Events,
}

impl Tools for ToolRuntime {
    fn run(&self, call: ToolCall) -> BoxFuture<'static, ToolResult> {
        let events = self.events.clone();
        Box::pin(async move {
            events.emit::<SessionAppended>(Arc::new(SessionEvent::ToolCall(call.clone())));
            let result = events
                .waterfall::<ToolsExecute>(call, |call| async move {
                    ToolResult::Ok(format!("{} output for `{}`", call.tool, call.input))
                })
                .await;
            events.emit::<SessionAppended>(Arc::new(SessionEvent::ToolResult(result.clone())));
            result
        })
    }
}

component!(ToolRuntime { provides: [dyn Tools] });

impl Component for ToolRuntime {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        Ok(Self { events: cx.events().clone() })
    }
}

// ---- Independent extensions: none of them names the runtime or each other --------------

#[derive(Deserialize)]
struct ApprovalConfig {
    deny: Vec<String>,
}

/// Denies shell commands with configured prefixes before they run.
struct ApprovalPolicy;

component!(ApprovalPolicy { config: ApprovalConfig });

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
struct OutputLimit;

component!(OutputLimit { config: LimitConfig });

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
struct Transcript;

component!(Transcript {});

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
struct StepBudget;

component!(StepBudget { config: BudgetConfig });

impl Component for StepBudget {
    async fn activate(cx: Cx<Self>, config: BudgetConfig) -> Result<Self> {
        let max = config.max_steps;
        cx.on::<TurnStopping>()
            .listen(move |state| async move { (state.steps >= max).then(|| format!("step budget of {max} reached")) });
        Ok(Self)
    }
}

// ---- A scripted agent turn ---------------------------------------------------------------

async fn turn(app: &App, calls: &[(&str, &str)]) {
    let tools = app.get::<dyn Tools>().await.expect("the tool runtime is active");
    for (steps, (tool, input)) in (1..).zip(calls) {
        let result = tools.run(ToolCall { tool: tool.to_string(), input: input.to_string() }).await;
        println!("step {steps}: {result:?}");
        if let Some(reason) = app.events().serial::<TurnStopping>(TurnState { steps }).await {
            println!("turn stopped: {reason}");
            return;
        }
    }
}

const CONFIG: &str = r#"
plugins:
  - { id: tools, use: tool-runtime }
  - { id: approval, use: approval-policy, config: { deny: ["rm "] } }
  - { id: limit, use: output-limit, config: { max_chars: 24 } }
  - { id: transcript, use: transcript }
  - { id: budget, use: step-budget, config: { max_steps: 3 } }
"#;

#[tokio::main]
async fn main() {
    let app = App::new();
    let mut catalog = Catalog::new();
    catalog
        .register::<ToolRuntime>("tool-runtime")
        .register::<ApprovalPolicy>("approval-policy")
        .register::<OutputLimit>("output-limit")
        .register::<Transcript>("transcript")
        .register::<StepBudget>("step-budget");
    let mut loader = Loader::new(app.clone(), catalog);
    let calls = [("bash", "ls -la"), ("bash", "rm -rf build"), ("read", "src/main.rs"), ("bash", "echo unreachable")];

    println!("== full profile");
    loader.apply_yaml(CONFIG).await.unwrap();
    app.settled().await;
    turn(&app, &calls).await;

    println!("\n== transcript and approval removed, budget raised; no restart");
    let patched = CONFIG
        .lines()
        .filter(|line| !line.contains("id: transcript") && !line.contains("id: approval"))
        .collect::<Vec<_>>()
        .join("\n")
        .replace("max_steps: 3", "max_steps: 10");
    loader.apply_yaml(&patched).await.unwrap();
    app.settled().await;
    turn(&app, &calls).await;

    app.shutdown().await;
}
