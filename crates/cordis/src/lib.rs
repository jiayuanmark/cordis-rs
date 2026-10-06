//! Components with revertible effects and reactive dependencies.
//!
//! A [`Component`] declares, with [`#[component]`](component), the [`Key`]s it depends on and the keys it
//! provides. An [`App`] runs an orchestrator that activates each configured component once all of
//! its dependencies have an active provider, and deactivates it, consumers first, when that
//! stops being true. Each fiber run keeps an undo list: a component hands every inverse, such as
//! the [`Guard`] of a registration, to [`Cx::on_dispose`], and the orchestrator runs the list,
//! newest first, when the fiber is disposed.
//!
//! A [`Scope`], derived from a fiber's [`Cx`] or from the [`App`], changes how keys resolve for
//! the fibers instantiated through it: [`Cx::isolate`] moves a key into another [`Realm`], so
//! a subtree gets its own provider, and [`Cx::intercept`] hands
//! its children a remapped view of a provider, such as a read-only wrapper.
//!
//! `design.md` at the repository root maps these types to the paper's model.

mod component;
mod dispose;
mod erased;
mod events;
mod guard;
mod key;
mod loader;
mod orchestrator;
mod realm;

pub use component::{Component, Cx, Manifest};
/// Declares a component's configuration, dependencies, and provisions by implementing
/// [`Manifest`] for the type it is attached to, and generates a struct of its
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
///   [`Manifest::Deps`]. The orchestrator resolves it once before activation, and components read
///   it through [`Cx::deps`]. Only listed keys have a field, so reading an undeclared key does
///   not compile, and listing a key twice is rejected. Without `deps`, `Manifest::Deps` is `()` and no struct is generated. The struct
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
/// A key listed twice is rejected:
///
/// ```compile_fail
/// use cordis::{component, key};
///
/// pub trait Fs: Send + Sync {}
/// key!(dyn Fs = "fs");
///
/// #[component(deps(a: dyn Fs, b: dyn Fs))]
/// struct Twice;
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
pub use dispose::Dispose;
#[doc(hidden)]
pub use erased::{Erased, Resolve};
pub use events::{DefaultAction, Detached, Event, Events, Flow, Held, Next, On, Sink, mode};
pub use guard::{Guard, Guards};
pub use key::{Key, KeyId};
pub use loader::{Catalog, Layer, LoadError, Loader, Row, Tree};
pub use orchestrator::{App, Fiber, FiberId, Status};
pub use realm::{Realm, Scope};

/// Error returned by component activation.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Result of component activation.
pub type Result<T, E = Error> = std::result::Result<T, E>;
