//! Components and their declared dependencies and provisions.

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::events::{Held, On};
use crate::orchestrator::{Control, WeakTable};
use crate::realm::Env;
use crate::{Dispose, Erased, Event, Events, Fiber, FiberId, Key, KeyId, Realm, Resolve, Result, Scope};

/// A component's declarations: its configuration type, the keys it depends on, and the keys
/// it provides.
///
/// Implement it with [`#[component]`](crate::component), which also generates the
/// [`Deps`](Manifest::Deps) struct.
pub trait Manifest: Sized + 'static {
    /// Configuration deserialized from the component's configuration row.
    type Config: DeserializeOwned + Send + 'static;

    /// The resolved dependencies, with one typed field per declared key. `()` when the
    /// component declares none.
    type Deps: Send + Sync + 'static;

    /// Returns the declared dependency keys in declaration order.
    fn deps() -> Vec<KeyId>;

    /// Returns the declared optional dependency keys in declaration order.
    ///
    /// Optional dependencies do not gate activation or trigger reloads when their providers
    /// change; they are read on demand through [`Optional::get`].
    fn optional() -> Vec<KeyId> {
        Vec::new()
    }

    /// Returns the provided keys.
    fn provides() -> Vec<KeyId>;

    /// Builds [`Deps`](Manifest::Deps) from the provider value of each declared key.
    #[doc(hidden)]
    fn resolve(resolved: &Resolve<'_>) -> Self::Deps;

    /// Coerces the component to the value type of each provided key.
    #[doc(hidden)]
    fn publish(component: &Arc<Self>) -> Vec<(KeyId, Erased)>;
}

/// Handle to an optional dependency declared with `deps(name?: Key)` in
/// [`#[component]`](crate::component).
///
/// Unlike a required dependency, an optional dependency is not part of the fiber's view: the
/// fiber activates whether or not `K` has a provider, and does not reload when `K`'s provider
/// starts, reloads, or stops. Instead, [`get`](Optional::get) reads the active provider of `K` in
/// the fiber's realm at the moment of the call, with any [`intercept`](Cx::intercept) on `K`
/// applied.
///
/// Use `Optional<K>` when the component runs without `K` and checks for it when doing work (such
/// as logging or recording a metric). When the component should run a block of effects—such as
/// registering a tool or listener—for the lifetime of `K`'s provider, use [`Cx::when`] instead.
///
/// ```
/// use cordis::{Component, Cx, Result, component, key};
///
/// pub trait Metrics: Send + Sync {
///     fn increment(&self, counter: &str);
/// }
/// key!(dyn Metrics = "metrics");
///
/// #[component(deps(metrics?: dyn Metrics))]
/// struct Cache {
///     cx: Cx<Self>,
/// }
///
/// impl Cache {
///     fn read(&self, _key: &str) {
///         if let Some(metrics) = self.cx.deps().metrics.get() {
///             metrics.increment("cache.read");
///         }
///     }
/// }
///
/// impl Component for Cache {
///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
///         Ok(Cache { cx })
///     }
/// }
/// ```
pub struct Optional<K: Key + ?Sized> {
    env: Env,
    table: WeakTable,
    _key: PhantomData<fn() -> K>,
}

impl<K: Key + ?Sized> Clone for Optional<K> {
    fn clone(&self) -> Self {
        Self { env: self.env.clone(), table: self.table.clone(), _key: PhantomData }
    }
}

impl<K: Key + ?Sized> fmt::Debug for Optional<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Optional").field("key", &KeyId::of::<K>()).field("present", &self.get().is_some()).finish()
    }
}

impl<K: Key + ?Sized> Optional<K> {
    pub(crate) fn new(env: Env, table: WeakTable) -> Self {
        Self { env, table, _key: PhantomData }
    }

    /// Returns the active provider of `K` in this fiber's realm, with any interception of `K`
    /// applied, or `None` when no provider of `K` is currently active there (or the app has
    /// stopped).
    ///
    /// Does not cache the value: each call reads the current provider, and keeping an
    /// `Optional<K>` never keeps a provider or the app alive.
    pub fn get(&self) -> Option<Arc<K::Value>> {
        let id = KeyId::of::<K>();
        let value = self.table.get(&self.env.slot(id))?;
        Some(self.env.remap(id, value).get::<K>())
    }
}

/// The component type of a fiber instantiated by [`Cx::when`], [`Scope::when`], or
/// [`App::when`](crate::App::when).
///
/// A `when::<K>` closure receives a `Cx<When<K>>` as its first argument, giving it the same
/// context operations as any [`Component::activate`]: [`on_dispose`](Cx::on_dispose),
/// [`on`](Cx::on), [`plugin`](Cx::plugin), [`when`](Cx::when), [`isolate`](Cx::isolate),
/// [`intercept`](Cx::intercept), and [`cancelled`](Cx::cancelled). [`cx.deps()`](Cx::deps)
/// returns the `&Arc<K::Value>` also passed as the closure's second argument.
pub struct When<K: Key + ?Sized>(pub(crate) PhantomData<fn() -> K>);

impl<K: Key + ?Sized> Manifest for When<K> {
    type Config = ();
    type Deps = Arc<K::Value>;

    fn deps() -> Vec<KeyId> {
        vec![KeyId::of::<K>()]
    }

    fn provides() -> Vec<KeyId> {
        Vec::new()
    }

    fn resolve(resolved: &Resolve<'_>) -> Self::Deps {
        resolved.get::<K>()
    }

    fn publish(_: &Arc<Self>) -> Vec<(KeyId, Erased)> {
        Vec::new()
    }
}

impl<K: Key + ?Sized> Component for When<K> {
    async fn activate(_: Cx<Self>, (): ()) -> Result<Self> {
        Ok(Self(PhantomData))
    }
}

/// The body of a [`Cx::when`], [`Scope::when`], or [`App::when`](crate::App::when) fiber: an
/// async closure from the fiber's [`Cx`] and the provider of `K` to [`Result<()>`]. Implemented
/// for every such closure.
pub trait WhenAction<K: Key + ?Sized>: Fn(Cx<When<K>>, Arc<K::Value>) -> Self::Future + Send + Sync + 'static {
    #[doc(hidden)]
    type Future: Future<Output = Result<()>> + Send + 'static;
}

impl<K, F, Fut> WhenAction<K> for F
where
    K: Key + ?Sized,
    F: Fn(Cx<When<K>>, Arc<K::Value>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    type Future = Fut;
}

/// A component: the effects of activation, under the declarations of its [`Manifest`].
///
/// The value returned by [`activate`](Component::activate) is the service the component
/// provides. Its effects are undone by its fiber, not by dropping the value: `activate` hands
/// each inverse to [`Cx::on_dispose`], and the orchestrator runs them, newest first, when the
/// fiber unloads. That happens on time even if a consumer still holds the value.
pub trait Component: Manifest + Send + Sync {
    /// Builds the component from its resolved dependencies.
    ///
    /// Runs only while every declared dependency has an active provider. The orchestrator never
    /// stops it partway: if a dependency changes before it returns, [`Cx::cancelled`] resolves,
    /// and once it returns, the inverses handed to [`Cx::on_dispose`] are run. A long wait should
    /// therefore also wait on `cx.cancelled()` and return early; a cancelled activation that
    /// returns an error is not marked failed.
    fn activate(cx: Cx<Self>, config: Self::Config) -> impl Future<Output = Result<Self>> + Send;
}

/// The context of one fiber run of `C`: its resolved dependencies, the event bus, and the
/// run's undo list.
///
/// The orchestrator constructs it only when every declared key has an active provider. Cloning is
/// cheap, so a component can keep a clone as a field and reach its context from ordinary
/// methods, and from listeners, after `activate` returns:
///
/// ```
/// use cordis::{Component, Cx, Result, component, key};
///
/// pub trait Fs: Send + Sync {
///     fn read(&self, path: &str) -> String;
/// }
/// key!(dyn Fs = "fs");
///
/// #[component(deps(fs: dyn Fs))]
/// struct Reader {
///     cx: Cx<Self>,
/// }
///
/// impl Reader {
///     fn read(&self, path: &str) -> String {
///         self.cx.deps().fs.read(path)
///     }
/// }
///
/// impl Component for Reader {
///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
///         Ok(Reader { cx })
///     }
/// }
/// ```
///
/// Every clone adds to the same undo list, which the orchestrator runs when the fiber unloads
/// or its activation is cancelled or fails. After that, [`is_live`](Cx::is_live) returns
/// `false` and anything added is undone at once. A listener may capture a clone of the `Cx`
/// that registered it; the fiber still removes it, so the cycle cannot leak.
pub struct Cx<C: Component> {
    deps: Arc<C::Deps>,
    events: Events,
    control: Control,
}

impl<C: Component> Clone for Cx<C> {
    fn clone(&self) -> Self {
        Self { deps: self.deps.clone(), events: self.events.clone(), control: self.control.clone() }
    }
}

impl<C: Component> Cx<C> {
    pub(crate) fn new(deps: Arc<C::Deps>, events: Events, control: Control) -> Self {
        Self { deps, events, control }
    }

    /// Returns the id of the fiber this context belongs to.
    pub fn id(&self) -> FiberId {
        self.control.id()
    }

    /// Instantiates a child fiber of component `D` under this fiber, with `config`.
    ///
    /// The child is an effect of this fiber's current activation: it joins the run's undo list
    /// (see [`on_dispose`](Cx::on_dispose)), so the orchestrator retires it, in its place in the
    /// list, when that activation ends, whether the fiber unloads, its activation is cancelled,
    /// or it fails. Like a root fiber, the child activates once its own dependencies have providers
    /// and reloads when they change. The returned [`Fiber`] reads its status, updates its
    /// configuration, or disposes of it early. The parent does not wait for its children to
    /// finish unloading.
    ///
    /// ```
    /// use cordis::{Component, Cx, Result, component};
    ///
    /// #[component]
    /// struct Agent;
    ///
    /// impl Component for Agent {
    ///     async fn activate(_: Cx<Self>, _: ()) -> Result<Self> {
    ///         Ok(Agent)
    ///     }
    /// }
    ///
    /// #[component]
    /// struct Server {
    ///     cx: Cx<Self>,
    /// }
    ///
    /// impl Server {
    ///     async fn accept(&self) -> Result<()> {
    ///         let agent = self.cx.plugin::<Agent>(()).await?;
    ///         // ... and once the agent's work is done, before the server unloads:
    ///         agent.dispose();
    ///         Ok(())
    ///     }
    /// }
    ///
    /// impl Component for Server {
    ///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
    ///         Ok(Server { cx })
    ///     }
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when `config` fails to serialize, when this fiber is leaving or its
    /// activation has ended (see [`is_live`](Cx::is_live)), or when the app has stopped. In
    /// each case no child runs.
    pub async fn plugin<D: Component>(&self, config: impl Serialize) -> Result<Fiber> {
        self.control.scope().plugin::<D>(config).await
    }

    /// Instantiates a child fiber under this fiber that runs `run` whenever `K` has an active
    /// provider.
    ///
    /// This fiber does not depend on `K`, so it activates whether or not `K` is present and is
    /// unaffected when `K` changes. The child fiber depends on `K`: each time `K` becomes active,
    /// `run` is called with the child's [`Cx`] and the provider of `K`; when `K` unloads or
    /// reloads, or when this fiber's activation ends, the child's [`on_dispose`](Cx::on_dispose)
    /// list is undone before `K`'s provider tears down.
    ///
    /// This is the idiomatic way to attach optional effects—such as registering a tool, route, or
    /// metric collector—to a service that may not be loaded, or that may reload independently of
    /// this component:
    ///
    /// ```
    /// use cordis::{Component, Cx, Guard, Result, component, key};
    ///
    /// pub trait Tools: Send + Sync {
    ///     fn register(&self, name: &str) -> Guard;
    /// }
    /// key!(dyn Tools = "tools");
    ///
    /// #[component]
    /// struct Search;
    ///
    /// impl Component for Search {
    ///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
    ///         // `Search` is active even without `dyn Tools`. Whenever `dyn Tools` is active,
    ///         // the `"search"` tool is registered, and unregistered before `dyn Tools` unloads
    ///         // or when `Search` unloads.
    ///         cx.when::<dyn Tools>(|cx, tools| async move {
    ///             cx.on_dispose(tools.register("search"));
    ///             Ok(())
    ///         })
    ///         .await?;
    ///         Ok(Search)
    ///     }
    /// }
    /// ```
    ///
    /// To capture state from `self` across reloads of `K`, clone an `Arc` (or `cx.clone()`)
    /// outside the closure and clone it into the async block on each call. For reading an
    /// optional provider on demand without a child fiber, see [`Optional`].
    ///
    /// # Errors
    ///
    /// Returns an error when this fiber is leaving or its activation has ended (see
    /// [`is_live`](Cx::is_live)), or when the app has stopped. In each case no child runs.
    pub async fn when<K: Key + ?Sized>(&self, run: impl WhenAction<K>) -> Result<Fiber> {
        self.control.scope().when::<K>(run).await
    }

    /// Returns a scope where `K` resolves in a fresh realm, for children that share a private
    /// provider of `K`.
    ///
    /// Children instantiated through the scope, and their descendants, depend on and provide
    /// `K` in that realm: they see only a provider of `K` instantiated inside, and nothing
    /// outside sees it. Every other key resolves as it does for this fiber. This is the paper's
    /// `isolate` (Def 25), realized as a derived context.
    ///
    /// ```
    /// use cordis::{Component, Cx, Result, component, key};
    ///
    /// pub trait Tools: Send + Sync {}
    /// key!(dyn Tools = "tools");
    ///
    /// #[component(provides(dyn Tools))]
    /// struct Registry;
    /// impl Tools for Registry {}
    /// # impl Component for Registry {
    /// #     async fn activate(_: Cx<Self>, _: ()) -> Result<Self> { Ok(Registry) }
    /// # }
    ///
    /// #[component(deps(tools: dyn Tools))]
    /// struct Agent;
    /// # impl Component for Agent {
    /// #     async fn activate(_: Cx<Self>, _: ()) -> Result<Self> { Ok(Agent) }
    /// # }
    ///
    /// #[component]
    /// struct Reviewer;
    ///
    /// impl Component for Reviewer {
    ///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
    ///         // The agent sees this registry, not the app's.
    ///         let sub = cx.isolate::<dyn Tools>();
    ///         sub.plugin::<Registry>(()).await?;
    ///         sub.plugin::<Agent>(()).await?;
    ///         Ok(Reviewer)
    ///     }
    /// }
    /// ```
    pub fn isolate<K: Key + ?Sized>(&self) -> Scope {
        self.control.scope().isolate::<K>()
    }

    /// Returns a scope where `K` resolves in `realm`, which other scopes and fibers may name
    /// as well. See [`Scope::isolate_in`].
    pub fn isolate_in<K: Key + ?Sized>(&self, realm: &Realm) -> Scope {
        self.control.scope().isolate_in::<K>(realm)
    }

    /// Returns a scope whose children receive `remap(provider)` for `K`, after any interception
    /// this fiber's context already applies. See [`Scope::intercept`].
    pub fn intercept<K: Key + ?Sized>(
        &self,
        remap: impl Fn(Arc<K::Value>) -> Arc<K::Value> + Send + Sync + 'static,
    ) -> Scope {
        self.control.scope().intercept::<K>(remap)
    }

    /// Returns the resolved dependencies, one typed field per key in `deps(...)`.
    ///
    /// ```
    /// use cordis::{Component, Cx, Result, component, key};
    ///
    /// pub trait Fs: Send + Sync {
    ///     fn read(&self, path: &str) -> String;
    /// }
    /// key!(dyn Fs = "fs");
    ///
    /// #[component(deps(fs: dyn Fs))]
    /// struct Reader {
    ///     cx: Cx<Self>,
    /// }
    ///
    /// impl Reader {
    ///     fn read(&self, path: &str) -> String {
    ///         self.cx.deps().fs.read(path)
    ///     }
    /// }
    /// # impl Component for Reader {
    /// #     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
    /// #         Ok(Reader { cx })
    /// #     }
    /// # }
    /// ```
    ///
    /// Only declared dependencies have a field, so reading anything else does not compile:
    ///
    /// ```compile_fail
    /// use cordis::{Component, Cx, Result, component, key};
    ///
    /// pub trait Fs: Send + Sync {}
    /// key!(dyn Fs = "fs");
    ///
    /// #[component(deps(fs: dyn Fs))]
    /// struct Reader;
    ///
    /// impl Component for Reader {
    ///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
    ///         let _net = &cx.deps().net;
    ///         Ok(Reader)
    ///     }
    /// }
    /// ```
    pub fn deps(&self) -> &C::Deps {
        &self.deps
    }

    /// Hands an inverse to this fiber run, which undoes it when the fiber is disposed: when it
    /// unloads, or when its activation is cancelled or fails.
    ///
    /// `undo` is a [`Guard`](crate::Guard) (dropped), a Tokio `JoinHandle` (aborted, then awaited
    /// until the task has stopped), or an async closure (called and awaited); see [`Dispose`].
    /// The run undoes its entries newest first, each finishing before the next starts, after
    /// every consumer of this fiber has finished its own teardown, so dependencies are still
    /// available. Listeners registered with [`on`](Cx::on) and children instantiated with
    /// [`plugin`](Cx::plugin) join the same list in the order they are made.
    ///
    /// Add each inverse right after the effect it undoes; the order then comes out right:
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    ///
    /// use cordis::{Component, Cx, Guard, Result, component, key};
    ///
    /// pub trait Tools: Send + Sync {
    ///     fn register(&self, name: &str) -> Guard;
    /// }
    /// key!(dyn Tools = "tools");
    ///
    /// #[component(deps(tools: dyn Tools))]
    /// struct Indexer;
    ///
    /// impl Component for Indexer {
    ///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
    ///         cx.on_dispose(|| async { /* flush the index to disk */ });
    ///         cx.on_dispose(cx.deps().tools.register("search"));
    ///         cx.on_dispose(tokio::spawn(async {
    ///             loop {
    ///                 tokio::time::sleep(Duration::from_secs(60)).await; // reindex
    ///             }
    ///         }));
    ///         // Disposal stops the task, then unregisters the tool, then flushes.
    ///         Ok(Indexer)
    ///     }
    /// }
    /// ```
    ///
    /// Once the run has been disposed (see [`is_live`](Cx::is_live)), `undo` is undone at once.
    pub fn on_dispose(&self, undo: impl Dispose) {
        self.control.undo().push(undo);
    }

    /// Resolves once this run should stop: a dependency changed while `activate` was running,
    /// the fiber has started to unload, or the app was dropped. It also resolves once the
    /// activation has failed, so a `Cx` kept from a failed run sees it.
    ///
    /// `activate` is never dropped at an `.await`, because that could stop it between an effect
    /// and the `on_dispose` that undoes it. Instead, a wait that may take long races this:
    ///
    /// ```
    /// # use cordis::{Component, Cx, Result, component};
    /// # async fn connect() -> std::io::Result<()> { Ok(()) }
    /// #[component]
    /// struct Client;
    ///
    /// impl Component for Client {
    ///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
    ///         tokio::select! {
    ///             connected = connect() => connected?,
    ///             () = cx.cancelled() => return Err("cancelled".into()),
    ///         }
    ///         Ok(Client)
    ///     }
    /// }
    /// ```
    pub async fn cancelled(&self) {
        self.control.cancel().cancelled().await;
    }

    /// Returns whether this fiber run is still live, so that what is added through this
    /// context is undone with the run rather than at once.
    ///
    /// `true` while the fiber activates and while it is active; `false` once it unloads or its
    /// activation is cancelled or fails. A component that outlives its fiber, for example
    /// through an `Arc` a consumer kept, can use it to stop work that needs the fiber.
    pub fn is_live(&self) -> bool {
        self.control.undo().is_open()
    }

    /// Returns the event bus, for dispatching events.
    pub fn events(&self) -> &Events {
        &self.events
    }

    /// Returns a registrar for listeners of `E` owned by this fiber.
    ///
    /// # Returns
    ///
    /// A registrar whose `listen` takes the listener signature of `E`'s mode. The registration
    /// joins this run's undo list (see [`on_dispose`](Cx::on_dispose)), so the listener is
    /// removed when the fiber unloads or its activation is cancelled or fails. Registering after
    /// that removes the listener at once.
    pub fn on<E: Event>(&self) -> On<'_, E, E::Mode, Held<'_>> {
        On { events: &self.events, sink: Held(self.control.undo()), _event: PhantomData }
    }
}
