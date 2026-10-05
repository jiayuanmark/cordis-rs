//! Components and their declared dependencies and provisions.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::events::{Held, On};
use crate::guard::Registrations;
use crate::reactor::Control;
use crate::{Erased, Event, Events, Fiber, FiberId, Intercept, Key, KeyId, Realm, Resolve, Result, Scope};

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

    /// Returns the provided keys.
    fn provides() -> Vec<KeyId>;

    /// Builds [`Deps`](Manifest::Deps) from the provider value of each declared key.
    #[doc(hidden)]
    fn resolve(resolved: &Resolve<'_>) -> Self::Deps;

    /// Coerces the component to the value type of each provided key.
    #[doc(hidden)]
    fn publish(component: &Arc<Self>) -> Vec<(KeyId, Erased)>;
}

/// A component: the effects of activation, under the declarations of its [`Manifest`].
///
/// The value returned by [`activate`](Component::activate) is the component's accumulator.
/// When the component must stop, the reactor awaits [`deactivate`](Component::deactivate),
/// which drops the value and so reverts every effect it holds.
pub trait Component: Manifest + Send + Sync {
    /// Builds the component from its resolved dependencies.
    ///
    /// Runs only while every declared dependency has an active provider. If a dependency
    /// changes before this returns, the reactor drops the future at its next `.await`, which
    /// drops the locals built so far in reverse declaration order.
    fn activate(cx: Cx<Self>, config: Self::Config) -> impl Future<Output = Result<Self>> + Send;

    /// Asynchronous part of the inverse.
    ///
    /// Runs after every consumer of this component has finished its own teardown, so the
    /// component's own dependencies are still available. The default drops `self`.
    fn deactivate(self) -> impl Future<Output = ()> + Send {
        async move { drop(self) }
    }
}

/// The context of one fiber of `C`: its resolved dependencies, the event bus, and the
/// registrations the fiber owns.
///
/// The reactor constructs it only when every declared key has an active provider. Cloning is
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
/// Every clone registers listeners into the same fiber-owned set, which is reverted when the
/// fiber unloads or its activation is cancelled or fails. After that, [`is_live`](Cx::is_live)
/// returns `false` and a new registration is reverted immediately. A listener may capture a
/// clone of the `Cx` that registered it; the fiber still removes it, so the cycle cannot leak.
pub struct Cx<C: Component> {
    deps: Arc<C::Deps>,
    events: Events,
    registrations: Registrations,
    control: Control,
    _component: PhantomData<fn() -> C>,
}

impl<C: Component> Clone for Cx<C> {
    fn clone(&self) -> Self {
        Self {
            deps: self.deps.clone(),
            events: self.events.clone(),
            registrations: self.registrations.clone(),
            control: self.control.clone(),
            _component: PhantomData,
        }
    }
}

impl<C: Component> Cx<C> {
    pub(crate) fn new(deps: Arc<C::Deps>, events: Events, registrations: Registrations, control: Control) -> Self {
        Self { deps, events, registrations, control, _component: PhantomData }
    }

    /// Returns the id of the fiber this context belongs to.
    pub fn id(&self) -> FiberId {
        self.control.id()
    }

    /// Instantiates a child fiber of component `D` under this fiber, with `config`.
    ///
    /// The child is an effect of this fiber's current activation: the reactor retires it when
    /// that activation ends, whether the fiber unloads, its activation is cancelled, or it
    /// fails. Like a root fiber, the child activates once its own dependencies have providers
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

    /// Returns a scope whose children receive `K` with `meta` applied, merged onto the
    /// metadata this fiber's context carries for `K`. See [`Scope::intercept`].
    pub fn intercept<K: Intercept + ?Sized>(&self, meta: K::Meta) -> Scope {
        self.control.scope().intercept::<K>(meta)
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

    /// Returns whether the fiber still owns registrations made through this context.
    ///
    /// `true` while the fiber activates and while it is active; `false` once it unloads or its
    /// activation is cancelled or fails. A component that outlives its fiber, for example
    /// through an `Arc` a consumer kept, can use it to stop work that needs the fiber.
    pub fn is_live(&self) -> bool {
        self.registrations.is_open()
    }

    /// Returns the event bus, for dispatching events.
    pub fn events(&self) -> &Events {
        &self.events
    }

    /// Returns a registrar for listeners of `E` owned by this fiber.
    ///
    /// # Returns
    ///
    /// A registrar whose `listen` takes the listener signature of `E`'s mode. The fiber holds
    /// the registration and removes it when the fiber unloads or its activation is cancelled
    /// or fails. Registering after that removes the listener at once.
    pub fn on<E: Event>(&self) -> On<'_, E, E::Mode, Held<'_>> {
        On { events: &self.events, sink: Held(&self.registrations), _event: PhantomData }
    }
}
