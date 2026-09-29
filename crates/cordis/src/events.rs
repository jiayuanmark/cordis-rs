//! Typed events: a bus every component can reach, with five dispatch modes.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use futures::future::join_all;

use crate::Guard;
use crate::erased::BoxFuture;
use crate::guard::Scope;

/// An event type: its name, dispatch mode, payload, and result.
///
/// Declare one with [`event!`](crate::event!).
pub trait Event: 'static {
    /// Name used by diagnostics.
    const NAME: &'static str;
    /// Dispatch mode; one of the types in [`mode`].
    type Mode: mode::Mode;
    /// Payload passed to listeners.
    type Args: Send + Sync + 'static;
    /// Result of a `Bail`, `Serial`, or `Waterfall` dispatch; `()` for the other modes.
    type Output: Send + 'static;
}

/// Dispatch modes. The mode of an event fixes its listener signature and dispatch method.
pub mod mode {
    use super::{BoxFuture, Flow, Next};
    use std::sync::Arc;

    /// A dispatch mode.
    pub trait Mode: 'static {
        #[doc(hidden)]
        type Listener<A: Send + Sync + 'static, O: Send + 'static>: ?Sized + Send + Sync;
    }

    /// Synchronous; listeners observe in registration order.
    pub struct Emit;
    /// Synchronous; listeners run in order until one returns `Some`.
    pub struct Bail;
    /// Awaited; all listeners run concurrently.
    pub struct Parallel;
    /// Awaited; listeners run in order until one returns `Some`.
    pub struct Serial;
    /// Awaited middleware around a default action.
    pub struct Waterfall;

    impl Mode for Emit {
        type Listener<A: Send + Sync + 'static, O: Send + 'static> = dyn Fn(&A) + Send + Sync;
    }
    impl Mode for Bail {
        type Listener<A: Send + Sync + 'static, O: Send + 'static> = dyn Fn(&A) -> Option<O> + Send + Sync;
    }
    impl Mode for Parallel {
        type Listener<A: Send + Sync + 'static, O: Send + 'static> = dyn Fn(Arc<A>) -> BoxFuture<()> + Send + Sync;
    }
    impl Mode for Serial {
        type Listener<A: Send + Sync + 'static, O: Send + 'static> =
            dyn Fn(Arc<A>) -> BoxFuture<Option<O>> + Send + Sync;
    }
    impl Mode for Waterfall {
        type Listener<A: Send + Sync + 'static, O: Send + 'static> =
            dyn Fn(A, Next<A, O>) -> BoxFuture<Flow<O>> + Send + Sync;
    }
}

use mode::{Bail, Emit, Mode, Parallel, Serial, Waterfall};

type ListenerOf<E> = <<E as Event>::Mode as Mode>::Listener<<E as Event>::Args, <E as Event>::Output>;
type Snapshot<E> = Arc<[(u64, Arc<ListenerOf<E>>)]>;
type Chain<A, O> = Arc<[(u64, Arc<<Waterfall as Mode>::Listener<A, O>>)]>;

/// The event bus. Clones share one bus.
#[derive(Clone, Default)]
pub struct Events {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    /// For each event type, a `Snapshot<E>` replaced on every registration and removal.
    lists: Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
    next_id: AtomicU64,
}

impl Inner {
    fn snapshot<E: Event>(&self) -> Snapshot<E> {
        let lists = self.lists.lock().unwrap();
        match lists.get(&TypeId::of::<E>()) {
            Some(list) => list.downcast_ref::<Snapshot<E>>().expect("lists are keyed by event type").clone(),
            None => Arc::from([]),
        }
    }

    fn replace<E: Event>(&self, edit: impl FnOnce(&mut Vec<(u64, Arc<ListenerOf<E>>)>)) {
        let mut lists = self.lists.lock().unwrap();
        let current = match lists.get(&TypeId::of::<E>()) {
            Some(list) => list.downcast_ref::<Snapshot<E>>().expect("lists are keyed by event type").to_vec(),
            None => Vec::new(),
        };
        let mut next = current;
        edit(&mut next);
        let snapshot: Snapshot<E> = next.into();
        lists.insert(TypeId::of::<E>(), Box::new(snapshot));
    }
}

impl Events {
    /// Returns a registrar for listeners of `E`.
    ///
    /// # Returns
    ///
    /// A registrar whose `listen` takes the listener signature of `E`'s mode and returns the
    /// registration's [`Guard`].
    pub fn on<E: Event>(&self) -> On<'_, E, E::Mode, Detached> {
        On { events: self, sink: Detached, _event: PhantomData }
    }

    fn register<E: Event>(&self, listener: Arc<ListenerOf<E>>) -> Guard {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner.replace::<E>(|list| list.push((id, listener)));
        let inner: Weak<Inner> = Arc::downgrade(&self.inner);
        Guard::new(move || {
            if let Some(inner) = inner.upgrade() {
                inner.replace::<E>(|list| list.retain(|(entry, _)| *entry != id));
            }
        })
    }

    /// Calls every listener of `E` in registration order.
    ///
    /// ```compile_fail
    /// use cordis::{Events, event};
    /// event!(Request = "request", Waterfall(u32) -> u32);
    /// Events::default().emit::<Request>(1); // `Request` is not an `Emit` event
    /// ```
    pub fn emit<E: Event<Mode = Emit>>(&self, args: E::Args) {
        for (_, listener) in self.inner.snapshot::<E>().iter() {
            listener(&args);
        }
    }

    /// Calls listeners of `E` in order until one returns `Some`.
    ///
    /// # Returns
    ///
    /// The first `Some` result, or `None` when every listener returned `None`.
    pub fn bail<E: Event<Mode = Bail>>(&self, args: E::Args) -> Option<E::Output> {
        self.inner.snapshot::<E>().iter().find_map(|(_, listener)| listener(&args))
    }

    /// Runs every listener of `E` concurrently and waits for all of them.
    pub async fn parallel<E: Event<Mode = Parallel>>(&self, args: E::Args) {
        let args = Arc::new(args);
        join_all(self.inner.snapshot::<E>().iter().map(|(_, listener)| listener(args.clone()))).await;
    }

    /// Runs listeners of `E` in order, awaiting each, until one returns `Some`.
    ///
    /// # Returns
    ///
    /// The first `Some` result, or `None` when every listener returned `None`.
    pub async fn serial<E: Event<Mode = Serial>>(&self, args: E::Args) -> Option<E::Output> {
        let args = Arc::new(args);
        for (_, listener) in self.inner.snapshot::<E>().iter() {
            if let Some(output) = listener(args.clone()).await {
                return Some(output);
            }
        }
        None
    }

    /// Runs the listeners of `E` as middleware around `default`.
    ///
    /// The first listener receives `args`; each listener either calls [`Next::run`] to pass
    /// possibly changed arguments down the chain or returns [`Flow::short_circuit`]. After the
    /// last listener, `default` produces the output.
    ///
    /// # Returns
    ///
    /// The output of the chain.
    pub async fn waterfall<E: Event<Mode = Waterfall>>(
        &self,
        args: E::Args,
        default: impl DefaultAction<E::Args, E::Output>,
    ) -> E::Output {
        let next =
            Next { chain: self.inner.snapshot::<E>(), index: 0, default: Box::new(|args| Box::pin(default(args))) };
        next.run(args).await.0
    }
}

/// The default action of a waterfall dispatch: an async closure from the event's payload to
/// its output. Implemented for every such closure.
pub trait DefaultAction<A, O>: FnOnce(A) -> Self::Future + Send + 'static {
    #[doc(hidden)]
    type Future: Future<Output = O> + Send + 'static;
}

impl<A, O, F, Fut> DefaultAction<A, O> for F
where
    F: FnOnce(A) -> Fut + Send + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    type Future = Fut;
}

/// The rest of a waterfall chain, passed to each waterfall listener.
pub struct Next<A: Send + Sync + 'static, O: Send + 'static> {
    chain: Chain<A, O>,
    index: usize,
    default: Box<dyn FnOnce(A) -> BoxFuture<O> + Send>,
}

impl<A: Send + Sync + 'static, O: Send + 'static> Next<A, O> {
    /// Passes `args` to the rest of the chain.
    ///
    /// # Returns
    ///
    /// A future of the downstream output, as a [`Flow`] the listener can return or map.
    pub fn run(self, args: A) -> BoxFuture<Flow<O>> {
        match self.chain.get(self.index).map(|(_, listener)| listener.clone()) {
            Some(listener) => listener(args, Next { chain: self.chain, index: self.index + 1, default: self.default }),
            None => {
                let output = (self.default)(args);
                Box::pin(async move { Flow(output.await) })
            }
        }
    }
}

/// The output of a waterfall listener.
///
/// Obtained only from [`Next::run`] or [`Flow::short_circuit`], so a listener cannot end the
/// chain without saying so.
#[must_use = "a waterfall listener returns its Flow"]
pub struct Flow<O>(O);

impl<O> Flow<O> {
    /// Ends the chain with `output`, skipping later listeners and the default action.
    pub fn short_circuit(output: O) -> Self {
        Self(output)
    }

    /// Transforms the output produced downstream.
    pub fn map(self, f: impl FnOnce(O) -> O) -> Self {
        Self(f(self.0))
    }

    /// Returns the output for inspection.
    pub fn output(&self) -> &O {
        &self.0
    }
}

/// Where a registrar puts its registration's [`Guard`].
pub trait Sink {
    /// What `listen` returns.
    type Out;
    #[doc(hidden)]
    fn accept(self, guard: Guard) -> Self::Out;
}

/// Returns the guard to the caller.
pub struct Detached;

impl Sink for Detached {
    type Out = Guard;
    fn accept(self, guard: Guard) -> Guard {
        guard
    }
}

/// Hands the guard to the registering fiber, which drops it when the fiber unloads.
pub struct Held<'a>(pub(crate) &'a Scope);

impl Sink for Held<'_> {
    type Out = ();
    fn accept(self, guard: Guard) {
        self.0.hold(guard);
    }
}

/// A listener registrar for event `E` with mode `M`, returned by `on::<E>()`.
pub struct On<'a, E, M, S> {
    pub(crate) events: &'a Events,
    pub(crate) sink: S,
    pub(crate) _event: PhantomData<fn() -> (E, M)>,
}

impl<E: Event<Mode = Emit>, S: Sink> On<'_, E, Emit, S> {
    /// Registers a listener called with each emitted payload.
    pub fn listen(self, listener: impl Fn(&E::Args) + Send + Sync + 'static) -> S::Out {
        let listener: Arc<ListenerOf<E>> = Arc::new(listener);
        self.sink.accept(self.events.register::<E>(listener))
    }
}

impl<E: Event<Mode = Bail>, S: Sink> On<'_, E, Bail, S> {
    /// Registers a listener; returning `Some` stops the dispatch with that result.
    pub fn listen(self, listener: impl Fn(&E::Args) -> Option<E::Output> + Send + Sync + 'static) -> S::Out {
        let listener: Arc<ListenerOf<E>> = Arc::new(listener);
        self.sink.accept(self.events.register::<E>(listener))
    }
}

impl<E: Event<Mode = Parallel>, S: Sink> On<'_, E, Parallel, S> {
    /// Registers a listener run concurrently with the others.
    pub fn listen<Fut>(self, listener: impl Fn(Arc<E::Args>) -> Fut + Send + Sync + 'static) -> S::Out
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let listener: Arc<ListenerOf<E>> = Arc::new(move |args| Box::pin(listener(args)) as BoxFuture<()>);
        self.sink.accept(self.events.register::<E>(listener))
    }
}

impl<E: Event<Mode = Serial>, S: Sink> On<'_, E, Serial, S> {
    /// Registers a listener; resolving to `Some` stops the dispatch with that result.
    pub fn listen<Fut>(self, listener: impl Fn(Arc<E::Args>) -> Fut + Send + Sync + 'static) -> S::Out
    where
        Fut: Future<Output = Option<E::Output>> + Send + 'static,
    {
        let listener: Arc<ListenerOf<E>> =
            Arc::new(move |args| Box::pin(listener(args)) as BoxFuture<Option<E::Output>>);
        self.sink.accept(self.events.register::<E>(listener))
    }
}

impl<E: Event<Mode = Waterfall>, S: Sink> On<'_, E, Waterfall, S> {
    /// Registers a middleware listener. It returns `next.run(args).await` to delegate, or
    /// [`Flow::short_circuit`] to end the chain.
    pub fn listen<Fut>(
        self,
        listener: impl Fn(E::Args, Next<E::Args, E::Output>) -> Fut + Send + Sync + 'static,
    ) -> S::Out
    where
        Fut: Future<Output = Flow<E::Output>> + Send + 'static,
    {
        let listener: Arc<ListenerOf<E>> =
            Arc::new(move |args, next| Box::pin(listener(args, next)) as BoxFuture<Flow<E::Output>>);
        self.sink.accept(self.events.register::<E>(listener))
    }
}

/// Declares an event type:
/// `event!(pub ToolsPreExecute = "tools/pre-execute", Waterfall(ToolCall) -> ToolResult);`.
///
/// The mode is one of `Emit`, `Bail`, `Parallel`, `Serial`, or `Waterfall`. The output
/// defaults to `()`.
#[macro_export]
macro_rules! event {
    ($(#[$meta:meta])* $vis:vis $name:ident = $label:literal, $mode:ident($args:ty) $(-> $output:ty)?) => {
        $(#[$meta])*
        $vis struct $name;

        impl $crate::Event for $name {
            const NAME: &'static str = $label;
            type Mode = $crate::mode::$mode;
            type Args = $args;
            type Output = $crate::__event_output!($($output)?);
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __event_output {
    () => {
        ()
    };
    ($output:ty) => {
        $output
    };
}
