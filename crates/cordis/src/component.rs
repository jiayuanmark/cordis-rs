//! Components and their declared dependencies and provisions.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::erased::Resolved;
use crate::events::{Held, On};
use crate::guard::Scope;
use crate::{Erased, Event, Events, Key, KeyId, Result};

/// A component's declarations: its configuration type, the keys it depends on, and the keys
/// it provides.
///
/// Implement it with [`#[component]`](crate::component), which also implements [`Has`] for
/// each declared dependency.
pub trait Manifest: Sized + 'static {
    /// Configuration deserialized from the component's configuration row.
    type Config: DeserializeOwned + Send + 'static;

    /// Returns the declared dependency keys in declaration order.
    fn deps() -> Vec<KeyId>;

    /// Returns the provided keys.
    fn provides() -> Vec<KeyId>;

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

/// Marks `K` as a declared dependency of the component. [`#[component]`](crate::component)
/// implements it for each key in `deps`, which makes [`Cx::dep`] reject undeclared keys at
/// compile time.
#[diagnostic::on_unimplemented(
    message = "`{K}` is not a declared dependency of `{Self}`",
    label = "undeclared dependency",
    note = "add `{K}` to `deps(...)` in the `#[component]` attribute of `{Self}`"
)]
pub trait Has<K: Key + ?Sized>: Manifest {}

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
/// #[component(deps(dyn Fs))]
/// struct Reader {
///     cx: Cx<Self>,
/// }
///
/// impl Reader {
///     fn read(&self, path: &str) -> String {
///         self.cx.dep::<dyn Fs>().read(path)
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
    deps: Arc<[(KeyId, Erased)]>,
    events: Events,
    scope: Scope,
    _component: PhantomData<fn() -> C>,
}

impl<C: Component> Clone for Cx<C> {
    fn clone(&self) -> Self {
        Self {
            deps: self.deps.clone(),
            events: self.events.clone(),
            scope: self.scope.clone(),
            _component: PhantomData,
        }
    }
}

impl<C: Component> Cx<C> {
    pub(crate) fn new(deps: Resolved, events: Events, scope: Scope) -> Self {
        Self { deps: deps.into(), events, scope, _component: PhantomData }
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
    /// ```
    /// use cordis::{Component, Cx, Result, component, key};
    ///
    /// pub trait Fs: Send + Sync {}
    /// key!(dyn Fs = "fs");
    ///
    /// #[component(deps(dyn Fs))]
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
    /// #[component(deps(dyn Fs))]
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
        let id = KeyId::of::<K>();
        let (_, value) = self
            .deps
            .iter()
            .find(|(key, _)| *key == id)
            .expect("the reactor resolves every declared key before activation");
        value.get::<K>()
    }
}
