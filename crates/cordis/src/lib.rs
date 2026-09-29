//! Components with revertible effects and reactive dependencies.
//!
//! A [`Component`] declares the [`Key`]s it depends on and the keys it provides. An [`App`]
//! runs a reactor that activates each configured component once all of its dependencies have
//! an active provider, and deactivates it, consumers first, when that stops being true. A
//! component's value is its accumulator: dropping it reverts every effect it holds, such as
//! the [`Guard`]s of its registrations.
//!
//! `design.md` at the repository root maps these types to the paper's model.

mod component;
mod erased;
mod events;
mod guard;
mod key;
mod loader;
mod reactor;

pub use component::{Component, Cx, DepSet, Has, ProvSet};
#[doc(hidden)]
pub use erased::Erased;
pub use events::{DefaultAction, Detached, Event, Events, Flow, Held, Next, On, Sink, mode};
pub use guard::{Guard, Guards};
pub use key::{Key, KeyId};
pub use loader::{Catalog, Layer, LoadError, Loader, Row, Tree};
pub use reactor::{App, FiberId, Status};

/// Error returned by component activation.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Result of component activation.
pub type Result<T, E = Error> = std::result::Result<T, E>;
