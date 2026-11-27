//! Tracing span propagation: lifecycle spans (`cordis.activate`, `cordis.deactivate`),
//! `follows_from` links, `Scope::in_span`, and event middleware spans (`cordis.event`,
//! `cordis.listener`).

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cordis::{App, Catalog, Component, Cx, Guard, Loader, Result, component, event};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Instrument, Level, Metadata, Span, Subscriber};
use tracing_core::span::Current;

#[derive(Clone, Debug)]
struct SpanRecord {
    id: u64,
    metadata: &'static Metadata<'static>,
    name: &'static str,
    target: &'static str,
    parent: Option<u64>,
    fields: BTreeMap<String, String>,
    follows_from: Vec<u64>,
    ref_count: usize,
    closed: bool,
}

#[derive(Clone, Debug)]
struct EventRecord {
    message: String,
    span_stack: Vec<u64>,
}

struct RecorderState {
    spans: HashMap<u64, SpanRecord>,
    events: Vec<EventRecord>,
    stack: Vec<u64>,
}

#[derive(Clone)]
struct Recorder {
    max_level: Level,
    next_id: Arc<AtomicU64>,
    state: Arc<Mutex<RecorderState>>,
}

impl Recorder {
    fn new(max_level: Level) -> Self {
        Self {
            max_level,
            next_id: Arc::new(AtomicU64::new(1)),
            state: Arc::new(Mutex::new(RecorderState { spans: HashMap::new(), events: Vec::new(), stack: Vec::new() })),
        }
    }

    fn spans(&self) -> Vec<SpanRecord> {
        let mut spans: Vec<_> = self.state.lock().unwrap().spans.values().cloned().collect();
        spans.sort_by_key(|span| span.id);
        spans
    }

    fn span(&self, id: u64) -> SpanRecord {
        self.state.lock().unwrap().spans.get(&id).cloned().expect("recorded span")
    }

    fn events(&self) -> Vec<EventRecord> {
        self.state.lock().unwrap().events.clone()
    }
}

#[derive(Default)]
struct FieldMap(BTreeMap<String, String>);

impl Visit for FieldMap {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
}

impl Subscriber for Recorder {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() <= self.max_level
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut fields = FieldMap::default();
        attrs.record(&mut fields);
        let mut state = self.state.lock().unwrap();
        let parent = if attrs.is_root() {
            None
        } else if let Some(parent) = attrs.parent() {
            Some(parent.into_u64())
        } else {
            state.stack.last().copied()
        };
        state.spans.insert(
            id,
            SpanRecord {
                id,
                metadata: attrs.metadata(),
                name: attrs.metadata().name(),
                target: attrs.metadata().target(),
                parent,
                fields: fields.0,
                follows_from: Vec::new(),
                ref_count: 1,
                closed: false,
            },
        );
        Id::from_u64(id)
    }

    fn current_span(&self) -> Current {
        let state = self.state.lock().unwrap();
        match state.stack.last().and_then(|id| state.spans.get(id)) {
            Some(record) => Current::new(Id::from_u64(record.id), record.metadata),
            None => Current::none(),
        }
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        let mut fields = FieldMap::default();
        values.record(&mut fields);
        if let Some(record) = self.state.lock().unwrap().spans.get_mut(&span.into_u64()) {
            record.fields.extend(fields.0);
        }
    }

    fn record_follows_from(&self, span: &Id, follows: &Id) {
        if let Some(record) = self.state.lock().unwrap().spans.get_mut(&span.into_u64()) {
            record.follows_from.push(follows.into_u64());
        }
    }

    fn event(&self, event: &Event<'_>) {
        let mut fields = FieldMap::default();
        event.record(&mut fields);
        let message = fields.0.remove("message").unwrap_or_default();
        let mut state = self.state.lock().unwrap();
        let span_stack = state.stack.clone();
        state.events.push(EventRecord { message, span_stack });
    }

    fn enter(&self, span: &Id) {
        self.state.lock().unwrap().stack.push(span.into_u64());
    }

    fn exit(&self, span: &Id) {
        let mut state = self.state.lock().unwrap();
        if let Some(pos) = state.stack.iter().rposition(|&id| id == span.into_u64()) {
            state.stack.remove(pos);
        }
    }

    fn clone_span(&self, id: &Id) -> Id {
        if let Some(record) = self.state.lock().unwrap().spans.get_mut(&id.into_u64()) {
            record.ref_count += 1;
        }
        id.clone()
    }

    fn try_close(&self, id: Id) -> bool {
        if let Some(record) = self.state.lock().unwrap().spans.get_mut(&id.into_u64()) {
            record.ref_count = record.ref_count.saturating_sub(1);
            if record.ref_count == 0 {
                record.closed = true;
                return true;
            }
        }
        false
    }
}

#[component]
struct TrackedService;

impl Component for TrackedService {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        tracing::info!("svc-activating");
        cx.on_dispose(Guard::new(|| tracing::info!("svc-disposing")));
        Ok(Self)
    }
}

#[tokio::test]
async fn lifecycle_spans_carry_identity_and_follows_from_without_pinning_trigger_span() {
    let recorder = Recorder::new(Level::INFO);
    let _guard = tracing::subscriber::set_default(recorder.clone());

    let app = App::new();
    let mut catalog = Catalog::new();
    catalog.register::<TrackedService>("svc");
    let mut loader = Loader::new(app.clone(), catalog);

    let boot_id = {
        let boot = tracing::info_span!("boot");
        let id = boot.id().unwrap().into_u64();
        loader.apply_yaml("plugins:\n  - { id: my-svc, use: svc }\n").instrument(boot).await.unwrap();
        id
    };
    app.settled().await;

    // The short-lived `boot` span closed cleanly even though `my-svc` is still active.
    assert!(recorder.span(boot_id).closed, "trigger span must not be pinned open by a long-lived fiber");

    let activate =
        recorder.spans().into_iter().find(|s| s.name == "cordis.activate").expect("cordis.activate span recorded");
    assert_eq!(activate.target, "cordis::lifecycle");
    assert_eq!(activate.parent, None);
    assert_eq!(activate.follows_from, vec![boot_id]);
    assert_eq!(activate.fields.get("row").map(String::as_str), Some("my-svc"));
    assert!(activate.fields.get("component").unwrap().contains("TrackedService"));
    assert!(activate.closed);

    let act_event = recorder.events().into_iter().find(|e| e.message == "svc-activating").unwrap();
    assert_eq!(act_event.span_stack, vec![activate.id]);

    let remove_id = {
        let remove = tracing::info_span!("remove-tree");
        let id = remove.id().unwrap().into_u64();
        loader.apply_yaml("plugins: []\n").instrument(remove).await.unwrap();
        id
    };
    app.settled().await;

    assert!(recorder.span(remove_id).closed);
    let deactivate =
        recorder.spans().into_iter().find(|s| s.name == "cordis.deactivate").expect("cordis.deactivate span recorded");
    assert_eq!(deactivate.target, "cordis::lifecycle");
    assert_eq!(deactivate.parent, None);
    assert_eq!(deactivate.follows_from, vec![remove_id]);
    assert_eq!(deactivate.fields.get("row").map(String::as_str), Some("my-svc"));
    assert!(deactivate.fields.get("component").unwrap().contains("TrackedService"));
    assert!(deactivate.closed);

    let disp_event = recorder.events().into_iter().find(|e| e.message == "svc-disposing").unwrap();
    assert_eq!(disp_event.span_stack, vec![deactivate.id]);

    app.shutdown().await;
}

#[component]
struct ChildWorker;

impl Component for ChildWorker {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        cx.on_dispose(Guard::new(|| tracing::info!("child-disposing")));
        Ok(Self)
    }
}

#[component]
struct DetachedWorker;

impl Component for DetachedWorker {
    async fn activate(_: Cx<Self>, _: ()) -> Result<Self> {
        Ok(Self)
    }
}

#[component]
struct SubAgentParent;

impl Component for SubAgentParent {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        cx.plugin::<ChildWorker>(()).await?;
        cx.in_span(Span::none()).plugin::<DetachedWorker>(()).await?;
        Ok(Self)
    }
}

#[tokio::test]
async fn in_span_parents_subtree_and_span_none_detaches() {
    let recorder = Recorder::new(Level::INFO);
    let _guard = tracing::subscriber::set_default(recorder.clone());

    let app = App::new();
    let turn = tracing::info_span!("turn", session = "s1");
    let turn_id = turn.id().unwrap().into_u64();

    let fiber = async { app.in_span(Span::current()).plugin::<SubAgentParent>(()).await.unwrap() }
        .instrument(turn.clone())
        .await;
    app.settled().await;

    let spans = recorder.spans();
    let parent_act =
        spans.iter().find(|s| s.name == "cordis.activate" && s.fields["component"].contains("SubAgentParent")).unwrap();
    let child_act =
        spans.iter().find(|s| s.name == "cordis.activate" && s.fields["component"].contains("ChildWorker")).unwrap();
    let detached_act =
        spans.iter().find(|s| s.name == "cordis.activate" && s.fields["component"].contains("DetachedWorker")).unwrap();

    assert_eq!(parent_act.parent, Some(turn_id));
    assert!(parent_act.follows_from.is_empty(), "cause already equals parent, so no redundant follows_from");
    assert_eq!(child_act.parent, Some(turn_id));
    assert_eq!(detached_act.parent, None);

    // Retiring the subagent subtree runs its deactivation spans under `turn` and releases `turn`.
    fiber.dispose();
    app.settled().await;
    drop(turn);
    assert!(recorder.span(turn_id).closed, "turn span closes once instrumented fibers are discarded");

    let child_deact = recorder
        .spans()
        .into_iter()
        .find(|s| s.name == "cordis.deactivate" && s.fields["component"].contains("ChildWorker"))
        .unwrap();
    assert_eq!(child_deact.parent, Some(turn_id));

    app.shutdown().await;
}

event!(TestWaterfall = "test/waterfall", Waterfall(String) -> String);
event!(TestEmit = "test/emit", Emit(String));
event!(TestBail = "test/bail", Bail(u32) -> u32);
event!(TestSerial = "test/serial", Serial(u32) -> u32);
event!(TestParallel = "test/parallel", Parallel(u32));

#[component]
struct AuthMiddleware;

impl Component for AuthMiddleware {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        cx.on::<TestWaterfall>().listen(|arg, next| async move {
            tracing::info!("auth-pre");
            next.run(arg).await
        });
        Ok(Self)
    }
}

#[component]
struct LimitMiddleware;

impl Component for LimitMiddleware {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        cx.on::<TestWaterfall>().listen(|arg, next| async move {
            tracing::info!("limit-pre");
            next.run(arg).await
        });
        cx.on::<TestEmit>().listen(|_| tracing::info!("emit-listener"));
        cx.on::<TestBail>().listen(|n| Some(*n + 1));
        cx.on::<TestSerial>().listen(|n| async move { Some(*n + 2) });
        cx.on::<TestParallel>().listen(|_| async move { tracing::info!("parallel-listener") });
        Ok(Self)
    }
}

#[tokio::test]
async fn events_propagate_emitter_span_and_record_debug_middleware_spans() {
    let recorder = Recorder::new(Level::DEBUG);
    let _guard = tracing::subscriber::set_default(recorder.clone());

    let app = App::new();
    let _auth = app.plugin::<AuthMiddleware>(()).await.unwrap();
    app.settled().await;
    let _limit = app.plugin::<LimitMiddleware>(()).await.unwrap();
    app.settled().await;
    let _detached = app.events().on::<TestEmit>().listen(|_| tracing::info!("detached-emit"));

    let tool_run = tracing::info_span!("tool.run", tool = "bash");
    let tool_id = tool_run.id().unwrap().into_u64();

    async {
        let out = app
            .events()
            .waterfall::<TestWaterfall>("ls".into(), |arg| async move {
                tracing::info!("default-handler");
                arg
            })
            .await;
        assert_eq!(out, "ls");
        app.events().emit::<TestEmit>("done".into());
        assert_eq!(app.events().bail::<TestBail>(1), Some(2));
        assert_eq!(app.events().serial::<TestSerial>(1).await, Some(3));
        app.events().parallel::<TestParallel>(1).await;
    }
    .instrument(tool_run)
    .await;

    let spans = recorder.spans();
    let wf_event = spans
        .iter()
        .find(|s| s.name == "cordis.event" && s.fields.get("mode").map(String::as_str) == Some("waterfall"))
        .unwrap();
    assert_eq!(wf_event.target, "cordis::events");
    assert_eq!(wf_event.parent, Some(tool_id));
    assert_eq!(wf_event.fields.get("event").map(String::as_str), Some("test/waterfall"));

    let auth_span = spans
        .iter()
        .find(|s| {
            s.name == "cordis.listener" && s.fields.get("component").is_some_and(|c| c.contains("AuthMiddleware"))
        })
        .unwrap();
    let limit_span = spans
        .iter()
        .find(|s| {
            s.name == "cordis.listener"
                && s.fields.get("event").map(String::as_str) == Some("test/waterfall")
                && s.fields.get("component").is_some_and(|c| c.contains("LimitMiddleware"))
        })
        .unwrap();
    assert_eq!(auth_span.parent, Some(wf_event.id));
    assert_eq!(limit_span.parent, Some(auth_span.id));

    let default_ev = recorder.events().into_iter().find(|e| e.message == "default-handler").unwrap();
    assert_eq!(default_ev.span_stack.last(), Some(&limit_span.id));

    // All 5 modes created `cordis.event` spans parented to `tool.run`.
    for mode in ["emit", "bail", "serial", "parallel", "waterfall"] {
        assert!(
            spans.iter().any(|s| s.name == "cordis.event"
                && s.parent == Some(tool_id)
                && s.fields.get("mode").map(String::as_str) == Some(mode)),
            "missing cordis.event span for mode {mode}"
        );
    }

    // Detached host listener has no `component` field.
    let detached_span = spans
        .iter()
        .find(|s| {
            s.name == "cordis.listener"
                && s.fields.get("event").map(String::as_str) == Some("test/emit")
                && !s.fields.contains_key("component")
        })
        .expect("detached listener span");
    let detached_ev = recorder.events().into_iter().find(|e| e.message == "detached-emit").unwrap();
    assert_eq!(detached_ev.span_stack.last(), Some(&detached_span.id));

    app.shutdown().await;
}

#[tokio::test]
async fn info_only_subscriber_preserves_emitter_span_through_events() {
    let recorder = Recorder::new(Level::INFO);
    let _guard = tracing::subscriber::set_default(recorder.clone());

    let app = App::new();
    let _limit = app.plugin::<LimitMiddleware>(()).await.unwrap();
    app.settled().await;

    let tool_run = tracing::info_span!("tool.run", tool = "read");
    let tool_id = tool_run.id().unwrap().into_u64();

    async {
        app.events()
            .waterfall::<TestWaterfall>("README.md".into(), |arg| async move {
                tracing::info!("default-info");
                arg
            })
            .await;
        app.events().emit::<TestEmit>("done".into());
    }
    .instrument(tool_run)
    .await;

    // No `cordis.event` / `cordis.listener` debug spans are created at INFO level.
    assert!(!recorder.spans().iter().any(|s| s.target == "cordis::events"));

    // Yet the emitter's `tool.run` span flows straight through to middleware, default handler, and emit listeners.
    for msg in ["limit-pre", "default-info", "emit-listener"] {
        let ev = recorder.events().into_iter().find(|e| e.message == msg).unwrap();
        assert_eq!(ev.span_stack, vec![tool_id], "expected `{msg}` to run under tool.run");
    }

    app.shutdown().await;
}
