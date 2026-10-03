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
/// The set is open while the fiber's activation runs and while the fiber is active; any `Cx`
/// clone, including one stored in the component, registers into it. [`ScopeOwner`] closes it
/// when the activation is cancelled or fails, or when the fiber unloads. Closing reverts every
/// held guard, and a guard handed to a closed scope reverts at once, so a registration can
/// never outlive its fiber, even when a listener captures a clone of the `Cx` that owns it.
#[derive(Clone)]
pub(crate) struct Scope(Arc<Mutex<Option<Guards>>>);

impl Scope {
    pub(crate) fn hold(&self, guard: Guard) {
        let mut guards = self.0.lock().unwrap();
        match guards.as_mut() {
            Some(guards) => guards.push(guard),
            None => {
                drop(guards);
                drop(guard);
            }
        }
    }

    pub(crate) fn is_open(&self) -> bool {
        self.0.lock().unwrap().is_some()
    }

    fn close(&self) {
        // Revert outside the lock, so an inverse may touch this scope.
        let guards = self.0.lock().unwrap().take();
        drop(guards);
    }
}

/// Owns a fiber's [`Scope`] and closes it on drop.
///
/// The activation task holds it, so a cancelled or failed activation reverts the registrations
/// made so far; a successful one moves it into the fiber's instance.
pub(crate) struct ScopeOwner(Scope);

impl ScopeOwner {
    pub(crate) fn new() -> Self {
        Self(Scope(Arc::new(Mutex::new(Some(Guards::new())))))
    }

    pub(crate) fn scope(&self) -> Scope {
        self.0.clone()
    }
}

impl Drop for ScopeOwner {
    fn drop(&mut self) {
        self.0.close();
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

    #[test]
    fn a_closed_scope_reverts_held_and_late_guards() {
        let reverted = Arc::new(Mutex::new(Vec::new()));
        let guard = |name: &'static str| {
            let reverted = reverted.clone();
            Guard::new(move || reverted.lock().unwrap().push(name))
        };
        let owner = ScopeOwner::new();
        let scope = owner.scope();
        scope.hold(guard("early"));
        assert!(scope.is_open());

        drop(owner);
        assert!(!scope.is_open());
        assert_eq!(*reverted.lock().unwrap(), ["early"]);
        scope.hold(guard("late"));
        assert_eq!(*reverted.lock().unwrap(), ["early", "late"]);
    }
}
