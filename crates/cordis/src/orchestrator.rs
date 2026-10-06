//! The orchestrator: one task that owns every fiber and applies lifecycle transitions.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::FutureExt;
use serde_json::Value;
use slotmap::{SecondaryMap, SlotMap, new_key_type};
use tokio::sync::mpsc::{UnboundedSender, WeakUnboundedSender};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::dispose::{Undo, UndoHandle, panic_message};
use crate::erased::{Factory, Instance};
use crate::realm::{Env, Slot};
use crate::{Component, Dispose, Erased, Events, Key, KeyId, Realm, Scope};

new_key_type! {
    /// Identity of a fiber: one configured instance of a component.
    pub struct FiberId;
}

/// Lifecycle state of a fiber.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    /// Not running; waiting for its dependencies or for an exclusively provided key to free up.
    Inactive,
    /// `activate` is running.
    Loading,
    /// Running; its provisions are visible to consumers.
    Active,
    /// Its provisions are withdrawn; waiting for its consumers to finish their teardown.
    Leaving,
    /// Its undo list is running.
    Deactivating,
    /// `activate` returned an error or panicked. Retried when the fiber's dependencies or
    /// configuration change.
    Failed(String),
}

/// Handle to a running orchestrator.
///
/// Clones share one orchestrator. The orchestrator stops when every handle is dropped; call
/// [`shutdown`](App::shutdown) first to run every component's teardown in dependency order and
/// wait for it. Without it, each run's undo list is still undone, consumers first, but only the
/// synchronous part of each undo runs in order; the rest runs on the runtime and nothing waits
/// for it.
#[derive(Clone)]
pub struct App {
    commands: mpsc::UnboundedSender<Command>,
    events: Events,
}

impl App {
    /// Starts an orchestrator on the current Tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    pub fn new() -> Self {
        let (commands, command_rx) = mpsc::unbounded_channel();
        let (orchestrator_events, event_rx) = mpsc::unbounded_channel();
        let events = Events::default();
        let orchestrator = Orchestrator::new(commands.downgrade(), orchestrator_events, events.clone());
        tokio::spawn(orchestrator.run(command_rx, event_rx));
        Self { commands, events }
    }

    /// Returns the event bus shared by every component of this app.
    pub fn events(&self) -> &Events {
        &self.events
    }

    /// Inserts a fiber of component `C` with `config`.
    ///
    /// # Returns
    ///
    /// The new fiber's id. The fiber activates once its dependencies are available.
    pub async fn insert<C: Component>(&self, config: Value) -> FiberId {
        self.insert_factory(Arc::new(Factory::of::<C>()), config, Env::default()).await
    }

    pub(crate) async fn insert_factory(&self, factory: Arc<Factory>, config: Value, env: Env) -> FiberId {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Insert { factory, config, env, parent: None, reply });
        answer.await.expect("the orchestrator replies to every command").expect("a root fiber is always inserted")
    }

    /// Returns a scope where `K` resolves in a fresh realm, for inserting root fibers that
    /// share a private provider of `K`. See [`Scope::isolate`].
    pub fn isolate<K: Key + ?Sized>(&self) -> Scope {
        self.scope().isolate::<K>()
    }

    /// Returns a scope where `K` resolves in `realm`. See [`Scope::isolate_in`].
    pub fn isolate_in<K: Key + ?Sized>(&self, realm: &Realm) -> Scope {
        self.scope().isolate_in::<K>(realm)
    }

    /// Returns a scope whose root fibers receive `remap(provider)` for `K`. See
    /// [`Scope::intercept`].
    pub fn intercept<K: Key + ?Sized>(
        &self,
        remap: impl Fn(Arc<K::Value>) -> Arc<K::Value> + Send + Sync + 'static,
    ) -> Scope {
        self.scope().intercept::<K>(remap)
    }

    fn scope(&self) -> Scope {
        Scope::new(Env::default(), Spawner::App(self.commands.downgrade()))
    }

    /// Replaces a fiber's configuration; an active fiber reloads, and its consumers with it.
    pub fn update(&self, id: FiberId, config: Value) {
        self.send(Command::Update { id, config });
    }

    /// Retires a fiber. It deactivates after its consumers, then is discarded.
    pub fn remove(&self, id: FiberId) {
        self.send(Command::Remove { id });
    }

    /// Returns a fiber's state, or `None` once the fiber has been discarded.
    pub async fn status(&self, id: FiberId) -> Option<Status> {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Status { id, reply });
        answer.await.expect("the orchestrator replies to every command")
    }

    /// Returns the fiber a fiber was instantiated under, or `None` for a root fiber and once
    /// the fiber has been discarded.
    pub async fn parent(&self, id: FiberId) -> Option<FiberId> {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Parent { id, reply });
        answer.await.expect("the orchestrator replies to every command")
    }

    /// Returns the current active provider of `K` in the shared realm, if any.
    pub async fn get<K: Key + ?Sized>(&self) -> Option<Arc<K::Value>> {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Get { slot: Env::default().slot(KeyId::of::<K>()), reply });
        let value = answer.await.expect("the orchestrator replies to every command");
        value.map(|value| value.get::<K>())
    }

    /// Waits until no activation or deactivation is in flight and every fiber has settled.
    pub async fn settled(&self) {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Settled { reply });
        answer.await.expect("the orchestrator replies to every command");
    }

    /// Retires every fiber and waits until all of them have been torn down.
    pub async fn shutdown(&self) {
        self.send(Command::RemoveAll);
        self.settled().await;
    }

    fn send(&self, command: Command) {
        self.commands.send(command).expect("the orchestrator runs while a handle exists");
    }
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) enum Command {
    Insert {
        factory: Arc<Factory>,
        config: Value,
        env: Env,
        parent: Option<Owner>,
        reply: oneshot::Sender<Option<FiberId>>,
    },
    Update {
        id: FiberId,
        config: Value,
    },
    Remove {
        id: FiberId,
    },
    RemoveAll,
    Status {
        id: FiberId,
        reply: oneshot::Sender<Option<Status>>,
    },
    Parent {
        id: FiberId,
        reply: oneshot::Sender<Option<FiberId>>,
    },
    Get {
        slot: Slot,
        reply: oneshot::Sender<Option<Erased>>,
    },
    Settled {
        reply: oneshot::Sender<()>,
    },
}

/// One activation of a fiber: it owns the children it instantiates until it unloads, and it is
/// what a consumer's view records as its provider, so a provider that restarts counts as new.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Owner {
    pub(crate) id: FiberId,
    token: u64,
}

/// A fiber's handle to the orchestrator, carried by its `Cx`.
///
/// The sender is weak, so a `Cx` a component keeps does not keep the orchestrator running.
#[derive(Clone)]
pub(crate) struct Control {
    commands: WeakUnboundedSender<Command>,
    owner: Owner,
    /// The context the fiber was inserted under, which its children inherit.
    env: Env,
    /// The undo list of this activation.
    undo: UndoHandle,
}

impl Control {
    pub(crate) fn id(&self) -> FiberId {
        self.owner.id
    }

    pub(crate) fn env(&self) -> &Env {
        &self.env
    }

    pub(crate) fn undo(&self) -> &UndoHandle {
        &self.undo
    }

    /// Returns the fiber's own context, from which its children derive theirs.
    pub(crate) fn scope(&self) -> Scope {
        Scope::new(self.env.clone(), Spawner::Fiber(self.clone()))
    }
}

/// Where a [`Scope`] inserts fibers: as root fibers of the app, or as children of one
/// activation, which the orchestrator retires when that activation ends.
#[derive(Clone)]
pub(crate) enum Spawner {
    App(WeakUnboundedSender<Command>),
    Fiber(Control),
}

/// Why a [`Scope`] inserted no fiber.
#[derive(Debug, thiserror::Error)]
enum Refused {
    #[error("the fiber's activation has ended, so it cannot instantiate children")]
    Ended,
    #[error("the app has stopped")]
    Stopped,
}

impl Spawner {
    pub(crate) fn commands(&self) -> Option<UnboundedSender<Command>> {
        match self {
            Spawner::App(commands) => commands.upgrade(),
            Spawner::Fiber(control) => control.commands.upgrade(),
        }
    }

    pub(crate) async fn insert(&self, factory: Arc<Factory>, config: Value, env: Env) -> crate::Result<Fiber> {
        let commands = self.commands().ok_or(Refused::Stopped)?;
        let parent = match self {
            Spawner::App(_) => None,
            Spawner::Fiber(control) => Some(control.owner),
        };
        let (reply, answer) = oneshot::channel();
        commands.send(Command::Insert { factory, config, env, parent, reply }).map_err(|_| Refused::Stopped)?;
        let id = answer.await.map_err(|_| Refused::Stopped)?.ok_or(Refused::Ended)?;
        let fiber = Fiber { id, commands: commands.downgrade(), disposed: Arc::default() };
        if let Spawner::Fiber(control) = self {
            control.undo.push(Retire(fiber.clone()));
        }
        Ok(fiber)
    }
}

/// A view of a child fiber, returned by [`Cx::plugin`](crate::Cx::plugin).
///
/// The handle does not own the fiber: dropping it changes nothing, and the parent's activation
/// retires the child when it ends. Use [`dispose`](Fiber::dispose) to retire it sooner, for
/// example when an agent finishes its task. Clones refer to the same fiber, and the handle does
/// not keep the app running.
#[derive(Clone, Debug)]
pub struct Fiber {
    id: FiberId,
    commands: WeakUnboundedSender<Command>,
    /// Set by `dispose` on any clone.
    disposed: Arc<AtomicBool>,
}

impl Fiber {
    /// Returns the fiber's id, for use with [`App`].
    pub fn id(&self) -> FiberId {
        self.id
    }

    /// Returns the fiber's state, or `None` once it has been discarded or the app has stopped.
    pub async fn status(&self) -> Option<Status> {
        let (reply, answer) = oneshot::channel();
        self.send(Command::Status { id: self.id, reply });
        answer.await.ok().flatten()
    }

    /// Replaces the fiber's configuration; an active fiber reloads, and its consumers with it.
    ///
    /// # Errors
    ///
    /// Returns an error when `config` fails to serialize.
    pub fn update(&self, config: impl serde::Serialize) -> crate::Result<()> {
        let config = serde_json::to_value(config)?;
        self.send(Command::Update { id: self.id, config });
        Ok(())
    }

    /// Retires the fiber now. It deactivates after its consumers, then is discarded.
    pub fn dispose(&self) {
        self.disposed.store(true, Ordering::Relaxed);
        self.send(Command::Remove { id: self.id });
    }

    fn send(&self, command: Command) {
        if let Some(commands) = self.commands.upgrade() {
            let _ = commands.send(command);
        }
    }
}

/// Retiring a child: an inverse of the parent run that instantiated it (Def 52), so it takes its
/// place in that run's undo list. The parent does not wait for the child to leave. Once the
/// child has been disposed through its handle, the entry is finished and the list can drop it,
/// so a parent that runs a child per request doesn't accumulate them.
struct Retire(Fiber);

impl Dispose for Retire {
    fn dispose(self) -> impl Future<Output = ()> + Send + 'static {
        self.0.dispose();
        std::future::ready(())
    }

    fn is_finished(&self) -> bool {
        self.0.disposed.load(Ordering::Relaxed)
    }
}

enum Event {
    /// An activation task ended. `outcome` is `None` when the task was cancelled.
    Loaded {
        id: FiberId,
        token: u64,
        outcome: Option<Result<Box<dyn Instance>, String>>,
    },
    Deactivated {
        id: FiberId,
    },
}

/// The provider of each dependency, in declaration order: the provider fiber and the activation
/// of it that published the key. A provider that is replaced, reloaded, or restarted after a
/// failure changes the view, so its consumers reload or retry. (The paper reinserts a revised
/// fiber as a new one, §4.4; a `FiberId` survives a configuration change, so the activation tells
/// them apart.)
#[derive(Clone, Debug, PartialEq, Eq)]
struct View(Vec<(Slot, Owner)>);

impl View {
    fn providers(&self) -> impl Iterator<Item = FiberId> + '_ {
        self.0.iter().map(|(_, provider)| provider.id)
    }
}

/// The view and configuration epoch a fiber activated against.
struct Committed {
    view: View,
    epoch: u64,
}

enum State {
    Inactive,
    Loading { token: u64, cancel: CancellationToken, undo: UndoHandle },
    Active { instance: Box<dyn Instance> },
    Leaving { instance: Box<dyn Instance> },
    Deactivating,
    Failed { view: View, epoch: u64, error: String },
}

impl State {
    fn status(&self) -> Status {
        match self {
            State::Inactive => Status::Inactive,
            State::Loading { .. } => Status::Loading,
            State::Active { .. } => Status::Active,
            State::Leaving { .. } => Status::Leaving,
            State::Deactivating => Status::Deactivating,
            State::Failed { error, .. } => Status::Failed(error.clone()),
        }
    }
}

struct Entry {
    factory: Arc<Factory>,
    config: Value,
    /// The context the fiber was inserted under.
    env: Env,
    /// Where each dependency key resolves under `env`, in declaration order.
    deps: Vec<Slot>,
    /// Where each provided key is published under `env`.
    provides: Vec<Slot>,
    /// The activation this fiber was instantiated under, or `None` for a root fiber.
    parent: Option<Owner>,
    /// The activation that owns children created through this fiber's `Cx`: set when an
    /// activation begins, cleared when it ends and its children are retired.
    owner: Option<u64>,
    epoch: u64,
    retired: bool,
    committed: Option<Committed>,
    state: State,
}

struct Orchestrator {
    commands: WeakUnboundedSender<Command>,
    fibers: SlotMap<FiberId, Entry>,
    /// Active provisions, by key and realm, with the activation that published each. Only
    /// `Active` fibers appear here.
    table: HashMap<Slot, (Owner, Erased)>,
    /// The fiber that holds each provided slot, from the start of its activation until it has
    /// fully stopped. A slot has at most one holder, so two providers never run at once.
    holders: HashMap<Slot, FiberId>,
    /// Fibers to settle when a slot's provider changes: dependents and competing providers.
    watchers: HashMap<Slot, Vec<FiberId>>,
    /// Number of committed views that name each fiber.
    relied: SecondaryMap<FiberId, u32>,
    dirty: VecDeque<FiberId>,
    in_flight: usize,
    next_token: u64,
    events: mpsc::UnboundedSender<Event>,
    bus: Events,
    waiters: Vec<oneshot::Sender<()>>,
}

impl Orchestrator {
    fn new(commands: WeakUnboundedSender<Command>, events: mpsc::UnboundedSender<Event>, bus: Events) -> Self {
        Self {
            commands,
            fibers: SlotMap::with_key(),
            table: HashMap::new(),
            holders: HashMap::new(),
            watchers: HashMap::new(),
            relied: SecondaryMap::new(),
            dirty: VecDeque::new(),
            in_flight: 0,
            next_token: 0,
            events,
            bus,
            waiters: Vec::new(),
        }
    }

    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>, mut events: mpsc::UnboundedReceiver<Event>) {
        loop {
            tokio::select! {
                command = commands.recv() => match command {
                    Some(command) => self.command(command),
                    None => return self.drop_consumers_first(),
                },
                Some(event) = events.recv() => self.event(event),
            }
            while let Some(id) = self.dirty.pop_front() {
                self.settle(id);
            }
            if self.in_flight == 0 {
                for waiter in self.waiters.drain(..) {
                    let _ = waiter.send(());
                }
            }
        }
    }

    fn command(&mut self, command: Command) {
        match command {
            Command::Insert { factory, config, env, parent, reply } => {
                // A child requested by an activation that has already ended is not inserted.
                if parent.is_some_and(|parent| !self.owns_children(parent)) {
                    let _ = reply.send(None);
                    return;
                }
                let deps: Vec<Slot> = factory.deps.iter().map(|key| env.slot(*key)).collect();
                let provides: Vec<Slot> = factory.provides.iter().map(|key| env.slot(*key)).collect();
                let watched: Vec<Slot> = deps.iter().chain(&provides).cloned().collect();
                let id = self.fibers.insert(Entry {
                    factory,
                    config,
                    env,
                    deps,
                    provides,
                    parent,
                    owner: None,
                    epoch: 0,
                    retired: false,
                    committed: None,
                    state: State::Inactive,
                });
                for key in watched {
                    self.watchers.entry(key).or_default().push(id);
                }
                self.relied.insert(id, 0);
                self.dirty.push_back(id);
                let _ = reply.send(Some(id));
            }
            Command::Update { id, config } => {
                if let Some(fiber) = self.fibers.get_mut(id) {
                    fiber.config = config;
                    fiber.epoch += 1;
                    self.dirty.push_back(id);
                }
            }
            Command::Remove { id } => {
                if let Some(fiber) = self.fibers.get_mut(id) {
                    fiber.retired = true;
                    self.dirty.push_back(id);
                }
            }
            Command::RemoveAll => {
                for (id, fiber) in &mut self.fibers {
                    fiber.retired = true;
                    self.dirty.push_back(id);
                }
            }
            Command::Status { id, reply } => {
                let _ = reply.send(self.fibers.get(id).map(|fiber| fiber.state.status()));
            }
            Command::Parent { id, reply } => {
                let _ = reply.send(self.fibers.get(id).and_then(|fiber| fiber.parent).map(|parent| parent.id));
            }
            Command::Get { slot, reply } => {
                let _ = reply.send(self.table.get(&slot).map(|(_, value)| value.clone()));
            }
            Command::Settled { reply } => self.waiters.push(reply),
        }
    }

    /// Whether `owner` may instantiate children: its activation is current and the fiber is
    /// still starting or running. A leaving fiber has stopped performing effects, as in the
    /// paper's `Unloading` state.
    fn owns_children(&self, owner: Owner) -> bool {
        self.fibers.get(owner.id).is_some_and(|fiber| {
            fiber.owner == Some(owner.token)
                && match &fiber.state {
                    // A failed or cancelled activation closes its list before it reports back.
                    State::Loading { undo, .. } => undo.is_open(),
                    State::Active { .. } => true,
                    _ => false,
                }
        })
    }

    /// Ends the fiber's ownership of its children and retires them (rule O-Retire, applied by
    /// the parent's accumulator). The parent does not wait for them to leave.
    fn retire_children(&mut self, id: FiberId) {
        let Some(token) = self.fibers[id].owner.take() else { return };
        let owner = Owner { id, token };
        for (child, fiber) in &mut self.fibers {
            if fiber.parent == Some(owner) && !fiber.retired {
                fiber.retired = true;
                self.dirty.push_back(child);
            }
        }
    }

    fn event(&mut self, event: Event) {
        self.in_flight -= 1;
        match event {
            Event::Loaded { id, token, outcome } => self.loaded(id, token, outcome),
            Event::Deactivated { id } => self.deactivated(id),
        }
    }

    /// Returns the providers the fiber should run against, or `None` when it should not run.
    fn target(&self, id: FiberId) -> Option<View> {
        let fiber = &self.fibers[id];
        if fiber.retired {
            return None;
        }
        // A key has one provider per realm, from the start of its activation until it has fully
        // stopped; a competing provider waits until the key is free.
        let contested = fiber.provides.iter().any(|slot| self.holders.get(slot).is_some_and(|holder| *holder != id));
        if contested {
            return None;
        }
        fiber
            .deps
            .iter()
            .map(|slot| self.table.get(slot).map(|(provider, _)| (slot.clone(), *provider)))
            .collect::<Option<Vec<_>>>()
            .map(View)
    }

    fn is_current(&self, id: FiberId, target: Option<&View>) -> bool {
        let fiber = &self.fibers[id];
        match (&fiber.committed, target) {
            (Some(committed), Some(target)) => committed.view == *target && committed.epoch == fiber.epoch,
            _ => false,
        }
    }

    fn settle(&mut self, id: FiberId) {
        if !self.fibers.contains_key(id) {
            return;
        }
        let target = self.target(id);
        let current = self.is_current(id, target.as_ref());
        let fiber = &self.fibers[id];
        let (retired, epoch) = (fiber.retired, fiber.epoch);
        match &fiber.state {
            // A stopped fiber keeps the keys it holds only if it starts again right away, so a
            // reload does not hand them to a waiting competitor.
            State::Inactive => match target {
                Some(view) => self.begin(id, view),
                None => self.stop(id, retired),
            },
            State::Failed { view, epoch: failed_epoch, .. } => match target {
                Some(target) if target != *view || *failed_epoch != epoch => self.begin(id, target),
                _ => self.stop(id, retired),
            },
            State::Loading { cancel, .. } => {
                if !current {
                    cancel.cancel();
                }
            }
            State::Active { .. } => {
                if !current {
                    self.leave(id);
                }
            }
            State::Leaving { .. } | State::Deactivating => {}
        }
    }

    fn begin(&mut self, id: FiberId, view: View) {
        for provider in view.providers() {
            self.relied[provider] += 1;
        }
        let deps = view.0.iter().map(|(slot, _)| (slot.key, self.table[slot].1.clone())).collect();
        let token = self.next_token;
        self.next_token += 1;
        let cancel = CancellationToken::new();

        for slot in self.fibers[id].provides.clone() {
            self.holders.insert(slot, id);
        }
        let fiber = &mut self.fibers[id];
        let undo = Undo::new();
        let control = Control {
            commands: self.commands.clone(),
            owner: Owner { id, token },
            env: fiber.env.clone(),
            undo: undo.handle(),
        };
        let mut task = (fiber.factory.activate)(deps, fiber.config.clone(), self.bus.clone(), control);
        fiber.committed = Some(Committed { view, epoch: fiber.epoch });
        fiber.owner = Some(token);
        fiber.state = State::Loading { token, cancel: cancel.clone(), undo: undo.handle() };

        self.in_flight += 1;
        let events = self.events.clone();
        tokio::spawn(async move {
            let outcome = tokio::select! {
                result = AssertUnwindSafe(&mut task).catch_unwind() => Some(match result {
                    Ok(Ok(instance)) => Ok(instance),
                    Ok(Err(error)) => Err(error.to_string()),
                    Err(panic) => Err(panic_message(&*panic)),
                }),
                () = cancel.cancelled() => None,
            };
            // Dropping a cancelled activation drops the locals it built so far, in reverse
            // order. A panic there must not keep this task from reporting back.
            if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(move || drop(task))) {
                tracing::error!(fiber = ?id, panic = panic_message(&*panic), "dropping a cancelled activation panicked");
            }
            // An activation that did not produce an instance is disposed of here; an instance
            // takes the undo list with it.
            if !matches!(outcome, Some(Ok(_))) {
                undo.run().await;
            }
            let _ = events.send(Event::Loaded { id, token, outcome });
        });
    }

    fn loaded(&mut self, id: FiberId, token: u64, outcome: Option<Result<Box<dyn Instance>, String>>) {
        let Some(fiber) = self.fibers.get(id) else { return };
        if !matches!(fiber.state, State::Loading { token: loading, .. } if loading == token) {
            return;
        }
        let target = self.target(id);
        let current = self.is_current(id, target.as_ref());
        match outcome {
            Some(Ok(instance)) if current => self.publish(Owner { id, token }, instance),
            Some(Ok(instance)) => {
                // The target moved while the last step ran; revert what it built.
                self.fibers[id].state = State::Leaving { instance };
                self.try_unload(id);
            }
            Some(Err(error)) => {
                tracing::error!(fiber = ?id, %error, "activation failed");
                self.retire_children(id);
                let fiber = &mut self.fibers[id];
                let committed = fiber.committed.take().expect("a loading fiber has a committed view");
                fiber.state = State::Failed { view: committed.view.clone(), epoch: committed.epoch, error };
                self.release(&committed.view);
                self.dirty.push_back(id);
            }
            None => {
                self.retire_children(id);
                let fiber = &mut self.fibers[id];
                let committed = fiber.committed.take().expect("a loading fiber has a committed view");
                fiber.state = State::Inactive;
                self.release(&committed.view);
                self.dirty.push_back(id);
            }
        }
    }

    fn publish(&mut self, run: Owner, instance: Box<dyn Instance>) {
        let fiber = &self.fibers[run.id];
        let provisions: Vec<(Slot, Erased)> =
            instance.provisions().into_iter().map(|(key, value)| (fiber.env.slot(key), value)).collect();
        for (slot, value) in provisions {
            self.table.insert(slot.clone(), (run, value));
            self.mark(&slot);
        }
        self.fibers[run.id].state = State::Active { instance };
    }

    fn leave(&mut self, id: FiberId) {
        let fiber = &mut self.fibers[id];
        let State::Active { instance } = std::mem::replace(&mut fiber.state, State::Inactive) else {
            unreachable!("only an active fiber leaves");
        };
        fiber.state = State::Leaving { instance };
        let provides = fiber.provides.clone();
        for slot in provides {
            if self.table.get(&slot).is_some_and(|(provider, _)| provider.id == id) {
                self.table.remove(&slot);
                self.mark(&slot);
            }
        }
        self.try_unload(id);
    }

    /// Runs the fiber's inverse once no committed view names it.
    fn try_unload(&mut self, id: FiberId) {
        if self.relied[id] > 0 || !matches!(self.fibers[id].state, State::Leaving { .. }) {
            return;
        }
        let State::Leaving { instance } = std::mem::replace(&mut self.fibers[id].state, State::Deactivating) else {
            unreachable!("checked above");
        };
        self.in_flight += 1;
        let events = self.events.clone();
        tokio::spawn(async move {
            if let Err(panic) = AssertUnwindSafe(instance.dispose()).catch_unwind().await {
                tracing::error!(fiber = ?id, panic = panic_message(&*panic), "disposal panicked");
            }
            let _ = events.send(Event::Deactivated { id });
        });
    }

    fn deactivated(&mut self, id: FiberId) {
        // The undo list retired the children; this catches any whose insertion raced with it.
        self.retire_children(id);
        let fiber = &mut self.fibers[id];
        fiber.state = State::Inactive;
        if let Some(committed) = fiber.committed.take() {
            self.release(&committed.view);
        }
        self.dirty.push_back(id);
    }

    /// Frees the keys a fiber that stays stopped holds, waking the providers waiting for them,
    /// and discards the fiber once it is retired.
    fn stop(&mut self, id: FiberId, retired: bool) {
        for slot in self.fibers[id].provides.clone() {
            if self.holders.get(&slot) == Some(&id) {
                self.holders.remove(&slot);
                self.mark(&slot);
            }
        }
        if retired {
            self.discard(id);
        }
    }

    /// Drops one reliance on each provider in `view`, unloading providers that were waiting.
    fn release(&mut self, view: &View) {
        for provider in view.providers() {
            self.relied[provider] -= 1;
            self.try_unload(provider);
        }
    }

    /// Every handle is gone, so nothing can wait for teardown any more, and each run's undo
    /// list is undone at once as its fiber drops. Drops the fibers consumers first, the order
    /// `shutdown` would undo them in, so a consumer's undos run while its providers' effects
    /// are still in place.
    fn drop_consumers_first(&mut self) {
        loop {
            let unrelied: Vec<FiberId> =
                self.fibers.keys().filter(|id| self.relied.get(*id).is_none_or(|count| *count == 0)).collect();
            // Committed views cannot form a cycle; if one ever did, the fibers left drop with
            // the orchestrator.
            if unrelied.is_empty() {
                return;
            }
            for id in unrelied {
                let fiber = self.fibers.remove(id).expect("listed above");
                for provider in fiber.committed.iter().flat_map(|committed| committed.view.providers()) {
                    if let Some(count) = self.relied.get_mut(provider) {
                        *count -= 1;
                    }
                }
            }
        }
    }

    fn discard(&mut self, id: FiberId) {
        let fiber = self.fibers.remove(id).expect("a discarded fiber exists");
        for slot in fiber.deps.iter().chain(&fiber.provides) {
            if let Some(watchers) = self.watchers.get_mut(slot) {
                watchers.retain(|watcher| *watcher != id);
                if watchers.is_empty() {
                    // Realms come and go with the fibers in them.
                    self.watchers.remove(slot);
                }
            }
        }
        self.relied.remove(id);
    }

    fn mark(&mut self, slot: &Slot) {
        if let Some(watchers) = self.watchers.get(slot) {
            self.dirty.extend(watchers.iter().copied());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_child_disposed_through_its_handle_is_a_finished_entry() {
        let (commands, _receiver) = mpsc::unbounded_channel();
        let child = Fiber { id: FiberId::default(), commands: commands.downgrade(), disposed: Arc::default() };
        let retire = Retire(child.clone());
        assert!(!retire.is_finished());
        child.dispose();
        assert!(retire.is_finished());
    }
}
