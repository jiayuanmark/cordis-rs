//! Components with revertible effects and reactive dependencies.
//!
//! A [`Component`] declares, with [`#[component]`](component), the [`Key`]s it depends on and the keys it
//! provides. An [`App`] runs a reactor that activates each configured component once all of
//! its dependencies have an active provider, and deactivates it, consumers first, when that
//! stops being true. A component's value is its accumulator: dropping it reverts every effect
//! it holds, such as the [`Guard`]s of its registrations.
//!
//! `design.md` at the repository root maps these types to the paper's model.

mod component;
mod erased;
mod events;
mod guard;
mod key;
mod loader;
mod reactor;

pub use component::{Component, Cx, Has, Manifest};
/// Declares a component's configuration, dependencies, and provisions by implementing
/// [`Manifest`] and [`Has`] for the type it is attached to, and generates a struct of its
/// resolved dependencies.
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
/// #[component(config = RegistryConfig, deps(prompt: dyn Prompt), provides(dyn Tools))]
/// struct ToolRegistry {
///     prompt: Arc<dyn Prompt>,
///     limit: usize,
/// }
///
/// impl Tools for ToolRegistry {}
///
/// impl Component for ToolRegistry {
///     async fn activate(cx: Cx<Self>, config: RegistryConfig) -> Result<Self> {
///         Ok(Self { prompt: cx.deps().prompt.clone(), limit: config.limit })
///     }
/// }
///
/// // Generated alongside the impls:
/// let _: fn(&ToolRegistryDeps) -> &Arc<dyn Prompt> = |deps| &deps.prompt;
/// ```
///
/// Every argument is optional and they may appear in any order: `config` defaults to `()`, and
/// `deps` and `provides` default to empty, so a bare `#[component]` declares none of them.
/// Generic types are supported; the generated impls carry the type's generics and `where`
/// clause.
///
/// - `deps(name: Key, ...)` generates `{Type}Deps`, a struct with one `pub name: Arc<Key::Value>`
///   field per entry and the visibility of the component type, and sets it as
///   [`Manifest::Deps`]. The reactor resolves it once before activation, and components read
///   it through [`Cx::deps`]. It also implements [`Has<K>`](Has) for each listed key, so
///   [`Cx::dep`] compiles only for listed keys. Listing a key twice is a conflicting-impl
///   error. Without `deps`, `Manifest::Deps` is `()` and no struct is generated. The struct
///   is not generic, so on a generic component the keys may not name its type parameters.
/// - `provides(...)` publishes the component's `Arc` as `Arc<K::Value>` for each listed key
///   `K`. The coercion is written once per key, so it fails to compile when the component
///   does not implement the key's value type:
///
/// ```compile_fail
/// use cordis::{component, key};
///
/// pub trait Tools: Send + Sync {}
/// key!(dyn Tools = "tools");
///
/// #[component(provides(dyn Tools))]
/// struct NotTools;
/// ```
///
/// A dependency without a field name is rejected:
///
/// ```compile_fail
/// use cordis::{component, key};
///
/// pub trait Fs: Send + Sync {}
/// key!(dyn Fs = "fs");
///
/// #[component(deps(dyn Fs))]
/// struct Unnamed;
/// ```
///
/// An argument given twice, or an unknown one, is rejected:
///
/// ```compile_fail
/// #[cordis::component(config = (), config = ())]
/// struct Twice;
/// ```
///
/// The generated code names the crate as `::cordis`, so depend on it under that name.
pub use cordis_macros::component;
#[doc(hidden)]
pub use erased::Erased;
pub use events::{DefaultAction, Detached, Event, Events, Flow, Held, Next, On, Sink, mode};
pub use guard::{Guard, Guards};
pub use key::{Key, KeyId};
pub use loader::{Catalog, Layer, LoadError, Loader, Row, Tree};
pub use reactor::{App, Fiber, FiberId, Status};

/// Error returned by component activation.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Result of component activation.
pub type Result<T, E = Error> = std::result::Result<T, E>;
