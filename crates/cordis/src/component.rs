//! Components and their declared dependencies and provisions.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::erased::Resolved;
use crate::events::{Held, On};
use crate::guard::Scope;
use crate::{Erased, Event, Events, Key, KeyId, Result};

/// A component: declared dependencies, declared provisions, and the effects of activation.
///
/// The value returned by [`activate`](Component::activate) is the component's accumulator.
/// When the component must stop, the reactor awaits [`deactivate`](Component::deactivate),
/// which drops the value and so reverts every effect it holds.
pub trait Component: Sized + Send + Sync + 'static {
    /// Configuration deserialized from the component's configuration row.
    type Config: DeserializeOwned + Send + 'static;
    /// Declared dependencies; see [`deps!`](crate::deps!). Use `()` for none.
    type Deps: DepSet;
    /// Declared provisions; see [`provides!`](crate::provides!). Use `()` for none.
    type Provides: ProvSet<Self>;

    /// Builds the component from its resolved dependencies.
    ///
    /// Runs only while every key in `Deps` has an active provider. If a dependency changes
    /// before this returns, the reactor drops the future at its next `.await`, which drops the
    /// locals built so far in reverse declaration order.
    fn activate(cx: Cx<Self>, config: Self::Config) -> impl Future<Output = Result<Self>> + Send;

    /// Asynchronous part of the inverse.
    ///
    /// Runs after every consumer of this component has finished its own teardown, so the
    /// component's own dependencies are still available. The default drops `self`.
    fn deactivate(self) -> impl Future<Output = ()> + Send {
        async move { drop(self) }
    }
}

/// A set of dependency keys, declared with [`deps!`](crate::deps!).
pub trait DepSet: 'static {
    /// Returns the declared keys in declaration order.
    fn keys() -> Vec<KeyId>;
}

impl DepSet for () {
    fn keys() -> Vec<KeyId> {
        Vec::new()
    }
}

/// Marks `K` as a member of a dependency set. [`deps!`](crate::deps!) implements it for each
/// listed key, which makes [`Cx::dep`] reject undeclared keys at compile time.
#[diagnostic::on_unimplemented(
    message = "`{K}` is not a declared dependency of this component",
    label = "undeclared dependency",
    note = "add `{K}` to the component's `deps!` list"
)]
pub trait Has<K: Key + ?Sized>: DepSet {}

/// The keys component `C` provides, declared with [`provides!`](crate::provides!).
pub trait ProvSet<C>: 'static {
    /// Returns the provided keys.
    fn keys() -> Vec<KeyId>;

    /// Coerces the component to the value type of each provided key.
    #[doc(hidden)]
    fn publish(component: &Arc<C>) -> Vec<(KeyId, Erased)>;
}

impl<C> ProvSet<C> for () {
    fn keys() -> Vec<KeyId> {
        Vec::new()
    }

    fn publish(_: &Arc<C>) -> Vec<(KeyId, Erased)> {
        Vec::new()
    }
}

/// The context of one activation of `C`: its resolved dependencies and the event bus.
///
/// The reactor constructs it only when every declared key has an active provider.
pub struct Cx<C: Component> {
    deps: Resolved,
    events: Events,
    scope: Scope,
    _component: PhantomData<fn() -> C>,
}

impl<C: Component> Cx<C> {
    pub(crate) fn new(deps: Resolved, events: Events, scope: Scope) -> Self {
        Self { deps, events, scope, _component: PhantomData }
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
    /// or fails.
    pub fn on<E: Event>(&self) -> On<'_, E, E::Mode, Held<'_>> {
        On { events: &self.events, sink: Held(&self.scope), _event: PhantomData }
    }

    /// Returns the active provider of `K`, which must be a declared dependency.
    ///
    /// ```
    /// use cordis::{Component, Cx, Result, deps, key};
    ///
    /// pub trait Fs: Send + Sync {}
    /// key!(dyn Fs = "fs");
    /// deps!(ReaderDeps = [dyn Fs]);
    ///
    /// struct Reader;
    /// impl Component for Reader {
    ///     type Config = ();
    ///     type Deps = ReaderDeps;
    ///     type Provides = ();
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
    /// use cordis::{Component, Cx, Result, deps, key};
    ///
    /// pub trait Fs: Send + Sync {}
    /// key!(dyn Fs = "fs");
    /// pub trait Net: Send + Sync {}
    /// key!(dyn Net = "net");
    /// deps!(ReaderDeps = [dyn Fs]);
    ///
    /// struct Reader;
    /// impl Component for Reader {
    ///     type Config = ();
    ///     type Deps = ReaderDeps;
    ///     type Provides = ();
    ///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
    ///         let _net = cx.dep::<dyn Net>();
    ///         Ok(Reader)
    ///     }
    /// }
    /// ```
    pub fn dep<K: Key + ?Sized>(&self) -> Arc<K::Value>
    where
        C::Deps: Has<K>,
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

/// Declares a dependency set: `deps!(GrepDeps = [dyn Tools, dyn Fs]);`.
///
/// Defines the unit struct `GrepDeps`, implements [`DepSet`] for it, and implements [`Has`]
/// for each listed key. Listing a key twice is a conflicting-impl error.
#[macro_export]
macro_rules! deps {
    ($vis:vis $name:ident = [$($key:ty),* $(,)?]) => {
        $vis struct $name;

        impl $crate::DepSet for $name {
            fn keys() -> ::std::vec::Vec<$crate::KeyId> {
                ::std::vec![$($crate::KeyId::of::<$key>()),*]
            }
        }

        $(impl $crate::Has<$key> for $name {})*
    };
}

/// Declares the keys a component provides:
/// `provides!(RegistryProvides for ToolRegistry = [dyn Tools]);`.
///
/// Publishes the component's `Arc` as `Arc<K::Value>` for each listed key `K`. The coercion
/// is written once per key, so it fails to compile when the component does not implement the
/// key's value type:
///
/// ```compile_fail
/// use cordis::{key, provides};
///
/// pub trait Tools: Send + Sync {}
/// key!(dyn Tools = "tools");
///
/// struct NotTools;
/// provides!(NotToolsProvides for NotTools = [dyn Tools]);
/// ```
#[macro_export]
macro_rules! provides {
    ($vis:vis $name:ident for $component:ty = [$($key:ty),* $(,)?]) => {
        $vis struct $name;

        impl $crate::ProvSet<$component> for $name {
            fn keys() -> ::std::vec::Vec<$crate::KeyId> {
                ::std::vec![$($crate::KeyId::of::<$key>()),*]
            }

            fn publish(
                component: &::std::sync::Arc<$component>,
            ) -> ::std::vec::Vec<($crate::KeyId, $crate::Erased)> {
                ::std::vec![$({
                    let value: ::std::sync::Arc<<$key as $crate::Key>::Value> = component.clone();
                    ($crate::KeyId::of::<$key>(), $crate::Erased::new::<$key>(value))
                }),*]
            }
        }
    };
}
