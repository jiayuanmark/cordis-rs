//! Guards: revertible effects owned as values.

use std::fmt;
use std::sync::{Arc, Mutex};

/// A revertible effect. Dropping the guard runs its inverse exactly once.
///
/// A registry method returns a guard; the component stores it in its own value, so unloading
/// the component reverts the registration. An inverse that reaches back into a registry should
/// capture a `Weak` reference, so the guard never keeps its provider alive.
#[must_use = "dropping a Guard reverts its effect immediately"]
pub struct Guard(Option<Box<dyn FnOnce() + Send + Sync>>);

impl Guard {
    /// Returns a guard whose drop runs `revert`.
    pub fn new(revert: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self(Some(Box::new(revert)))
    }

    /// Returns a guard with nothing to revert.
    pub fn noop() -> Self {
        Self(None)
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
        f.debug_tuple("Guard").field(&self.0.is_some()).finish()
    }
}

/// Guards reverted in reverse order of insertion.
///
/// `Vec` drops its elements front to back; this type pops from the back, so later effects,
/// which may build on earlier ones, revert first.
#[derive(Debug, Default)]
pub struct Guards(Vec<Guard>);

impl Guards {
    /// Returns an empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a guard to revert before every guard already held.
    pub fn push(&mut self, guard: Guard) {
        self.0.push(guard);
    }
}

impl Drop for Guards {
    fn drop(&mut self) {
        while let Some(guard) = self.0.pop() {
            drop(guard);
        }
    }
}

/// Guards of the registrations a fiber made through its `Cx`. Clones share one set.
///
/// The activation task holds one clone, so a cancelled or failed activation reverts the
/// registrations made so far; a successful one moves the set into the fiber's instance.
#[derive(Clone, Default)]
pub(crate) struct Scope(Arc<Mutex<Guards>>);

impl Scope {
    pub(crate) fn hold(&self, guard: Guard) {
        self.0.lock().unwrap().push(guard);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn guards_revert_last_in_first_out() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut guards = Guards::new();
        for i in 0..3 {
            let log = log.clone();
            guards.push(Guard::new(move || log.lock().unwrap().push(i)));
        }
        drop(guards);
        assert_eq!(*log.lock().unwrap(), [2, 1, 0]);
    }
}
