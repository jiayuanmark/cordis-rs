//! Guards: revertible effects owned as values.

use std::fmt;

/// A revertible effect. Dropping the guard runs its inverse exactly once.
///
/// A registry method returns a guard, and the component hands it to its fiber with
/// [`Cx::on_dispose`](crate::Cx::on_dispose), so the registration is reverted when the fiber
/// unloads. An inverse that reaches back into a registry should capture a `Weak` reference, so
/// the guard never keeps its provider alive.
#[must_use = "dropping a Guard reverts its effect immediately"]
pub struct Guard(Option<Box<dyn FnOnce() + Send + Sync>>);

impl Guard {
    /// Returns a guard whose drop runs `revert`.
    pub fn new(revert: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self(Some(Box::new(revert)))
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(revert) = self.0.take() {
            revert();
        }
    }
}

impl fmt::Debug for Guard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Guard").finish_non_exhaustive()
    }
}
