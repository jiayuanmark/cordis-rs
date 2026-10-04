//! Components and their declared dependencies and provisions.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::erased::Factory;
use crate::events::{Held, On};
use crate::guard::Scope;
use crate::reactor::Control;
use crate::{Erased, Event, Events, Fiber, FiberId, Key, KeyId, Result};

/// A component's declarations: its configuration type, the keys it depends on, and the keys
/// it provides.
///
/// Implement it with [`#[component]`](crate::component), which also generates the
/// [`Deps`](Manifest::Deps) struct and implements [`Has`] for each declared dependency.
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
    fn resolve(resolved: &[(KeyId, Erased)]) -> Self::Deps;

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

/// The error of [`Cx::plugin`] on a context whose activation has ended.
#[derive(Debug, thiserror::Error)]
#[error("the fiber's activation has ended, so it cannot instantiate children")]
struct Ended;

/// Marks `K` as a declared dependency of the component and locates its field in
/// [`Manifest::Deps`]. [`#[component]`](crate::component) implements it for each key in
/// `deps`, which makes [`Cx::dep`] reject undeclared keys at compile time.
#[diagnostic::on_unimplemented(
    message = "`{K}` is not a declared dependency of `{Self}`",
    label = "undeclared dependency",
    note = "add `{K}` to `deps(...)` in the `#[component]` attribute of `{Self}`"
)]
pub trait Has<K: Key + ?Sized>: Manifest {
    /// Returns the field of `deps` that holds the provider of `K`.
    #[doc(hidden)]
    fn dep(deps: &Self::Deps) -> &Arc<K::Value>;
}

/// The context of one fiber of `C`: its resolved dependencies, the event bus, and the
/// registrations the fiber owns.
///
/// The reactor constructs it only when every declared key has an active provider. Cloning is
/// cheap, so a component can keep a clone as a field and reach its context from ordinary
/// methods, and from listeners, after `activate` returns:
///
/// ```
/// use std::sync::Arc;
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
    scope: Scope,
    control: Control,
    _component: PhantomData<fn() -> C>,
}

impl<C: Component> Clone for Cx<C> {
    fn clone(&self) -> Self {
        Self {
            deps: self.deps.clone(),
            events: self.events.clone(),
            scope: self.scope.clone(),
            control: self.control.clone(),
            _component: PhantomData,
        }
    }
}

impl<C: Component> Cx<C> {
    pub(crate) fn new(deps: Arc<C::Deps>, events: Events, scope: Scope, control: Control) -> Self {
        Self { deps, events, scope, control, _component: PhantomData }
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
        let config = serde_json::to_value(config)?;
        if !self.is_live() {
            return Err(Ended.into());
        }
        self.control.insert_child(Arc::new(Factory::of::<D>()), config).await.ok_or_else(|| Ended.into())
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
    pub fn deps(&self) -> &C::Deps {
        &self.deps
    }

    /// Returns whether the fiber still owns registrations made through this context.
    ///
    /// `true` while the fiber activates and while it is active; `false` once it unloads or its
    /// activation is cancelled or fails. A component that outlives its fiber, for example
    /// through an `Arc` a consumer kept, can use it to stop work that needs the fiber.
    pub fn is_live(&self) -> bool {
        self.scope.is_open()
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
        On { events: &self.events, sink: Held(&self.scope), _event: PhantomData }
    }

    /// Returns the active provider of `K`, which must be a declared dependency.
    ///
    /// The same value as the field of [`deps`](Cx::deps) declared for `K`, for code that
    /// names the key rather than the field.
    ///
    /// ```
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
    ///         let _fs = cx.dep::<dyn Fs>();
    ///         Ok(Reader)
    ///     }
    /// }
    /// ```
    ///
    /// Reading a key that is not declared does not compile:
    ///
    /// ```compile_fail
    /// use cordis::{Component, Cx, Result, component, key};
    ///
    /// pub trait Fs: Send + Sync {}
    /// key!(dyn Fs = "fs");
    /// pub trait Net: Send + Sync {}
    /// key!(dyn Net = "net");
    ///
    /// #[component(deps(fs: dyn Fs))]
    /// struct Reader;
    ///
    /// impl Component for Reader {
    ///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
    ///         let _net = cx.dep::<dyn Net>();
    ///         Ok(Reader)
    ///     }
    /// }
    /// ```
    pub fn dep<K: Key + ?Sized>(&self) -> Arc<K::Value>
    where
        C: Has<K>,
    {
        C::dep(&self.deps).clone()
    }
}
