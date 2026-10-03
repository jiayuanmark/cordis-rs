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
/// Implement it with [`component!`](crate::component!), which also implements [`Has`] for each
/// declared dependency. Implement it by hand only for what the macro cannot express, such as a
/// generic component.
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

/// Marks `K` as a declared dependency of the component. [`component!`](crate::component!)
/// implements it for each key in `deps`, which makes [`Cx::dep`] reject undeclared keys at
/// compile time.
#[diagnostic::on_unimplemented(
    message = "`{K}` is not a declared dependency of `{Self}`",
    label = "undeclared dependency",
    note = "add `{K}` to `deps` in the `component!` declaration of `{Self}`"
)]
pub trait Has<K: Key + ?Sized>: Manifest {}

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
    /// use cordis::{Component, Cx, Result, component, key};
    ///
    /// pub trait Fs: Send + Sync {}
    /// key!(dyn Fs = "fs");
    ///
    /// struct Reader;
    /// component!(Reader { deps: [dyn Fs] });
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
    /// struct Reader;
    /// component!(Reader { deps: [dyn Fs] });
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

/// Declares a component's configuration, dependencies, and provisions by implementing
/// [`Manifest`] and [`Has`].
///
/// ```
/// use std::sync::Arc;
///
/// use cordis::{Component, Cx, Result, component, key};
///
/// pub trait Prompt: Send + Sync {}
/// key!(dyn Prompt = "prompt");
/// pub trait Tools: Send + Sync {}
/// key!(dyn Tools = "tools");
///
/// #[derive(serde::Deserialize)]
/// struct RegistryConfig {
///     limit: usize,
/// }
///
/// struct ToolRegistry {
///     prompt: Arc<dyn Prompt>,
///     limit: usize,
/// }
/// component!(ToolRegistry {
///     config: RegistryConfig,
///     deps: [dyn Prompt],
///     provides: [dyn Tools],
/// });
///
/// impl Tools for ToolRegistry {}
///
/// impl Component for ToolRegistry {
///     async fn activate(cx: Cx<Self>, config: RegistryConfig) -> Result<Self> {
///         Ok(Self { prompt: cx.dep::<dyn Prompt>(), limit: config.limit })
///     }
/// }
/// ```
///
/// Every field is optional and they may appear in any order: `config` defaults to `()`, and
/// `deps` and `provides` default to empty. `component!(Plain {});` declares a component with
/// none of them.
///
/// - `deps` implements [`Has<K>`](Has) for each listed key, so [`Cx::dep`] compiles only for
///   listed keys. Listing a key twice is a conflicting-impl error.
/// - `provides` publishes the component's `Arc` as `Arc<K::Value>` for each listed key `K`. The
///   coercion is written once per key, so it fails to compile when the component does not
///   implement the key's value type:
///
/// ```compile_fail
/// use cordis::{component, key};
///
/// pub trait Tools: Send + Sync {}
/// key!(dyn Tools = "tools");
///
/// struct NotTools;
/// component!(NotTools { provides: [dyn Tools] });
/// ```
///
/// A field given twice is rejected:
///
/// ```compile_fail
/// struct Twice;
/// cordis::component!(Twice { config: (), config: () });
/// ```
#[macro_export]
macro_rules! component {
    ($component:ty { $($fields:tt)* }) => {
        $crate::component!(@parse $component; config []; deps []; provides []; $($fields)*);
    };

    (@parse $component:ty; config []; deps $deps:tt; provides $provides:tt;
        config: $config:ty $(, $($rest:tt)*)?) => {
        $crate::component!(@parse $component; config [$config]; deps $deps; provides $provides; $($($rest)*)?);
    };
    (@parse $component:ty; config $config:tt; deps []; provides $provides:tt;
        deps: [$($key:ty),* $(,)?] $(, $($rest:tt)*)?) => {
        $crate::component!(@parse $component; config $config; deps [[$($key),*]]; provides $provides; $($($rest)*)?);
    };
    (@parse $component:ty; config $config:tt; deps $deps:tt; provides [];
        provides: [$($key:ty),* $(,)?] $(, $($rest:tt)*)?) => {
        $crate::component!(@parse $component; config $config; deps $deps; provides [[$($key),*]]; $($($rest)*)?);
    };
    (@parse $component:ty; config $config:tt; deps $deps:tt; provides $provides:tt;
        $field:ident $($rest:tt)*) => {
        ::std::compile_error!(::std::concat!(
            "`", ::std::stringify!($field), "` is given twice or is not a `component!` field; ",
            "expected each of `config`, `deps`, and `provides` at most once",
        ));
    };

    (@parse $component:ty; config [$($config:ty)?]; deps [$([$($dep:ty),*])?]; provides [$([$($prov:ty),*])?];) => {
        impl $crate::Manifest for $component {
            type Config = $crate::component!(@config $($config)?);

            fn deps() -> ::std::vec::Vec<$crate::KeyId> {
                ::std::vec![$($($crate::KeyId::of::<$dep>()),*)?]
            }

            fn provides() -> ::std::vec::Vec<$crate::KeyId> {
                ::std::vec![$($($crate::KeyId::of::<$prov>()),*)?]
            }

            #[allow(unused_variables)]
            fn publish(
                component: &::std::sync::Arc<Self>,
            ) -> ::std::vec::Vec<($crate::KeyId, $crate::Erased)> {
                ::std::vec![$($({
                    let value: ::std::sync::Arc<<$prov as $crate::Key>::Value> = component.clone();
                    ($crate::KeyId::of::<$prov>(), $crate::Erased::new::<$prov>(value))
                }),*)?]
            }
        }

        $($(impl $crate::Has<$dep> for $component {})*)?
    };

    (@config) => { () };
    (@config $config:ty) => { $config };
}
