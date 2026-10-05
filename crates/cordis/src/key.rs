//! Keys: named slots with a declared value type.

use std::any::TypeId;
use std::fmt;

/// A named slot with a declared value type.
///
/// Consumers and providers meet at a key without naming each other. Consumers receive
/// `Arc<Self::Value>`. Implement it for a service trait object with [`key!`](crate::key!), or
/// by hand for a marker type that names a second slot of an existing interface.
pub trait Key: 'static {
    /// The type consumers receive behind an `Arc`.
    type Value: ?Sized + Send + Sync + 'static;
    /// Name used by configuration and diagnostics. Lookups use the key's `TypeId`.
    const NAME: &'static str;
}

/// Runtime identity of a [`Key`].
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyId {
    id: TypeId,
    name: &'static str,
}

impl KeyId {
    /// Returns the identity of `K`.
    pub fn of<K: Key + ?Sized>() -> Self {
        Self { id: TypeId::of::<K>(), name: K::NAME }
    }

    /// Returns the key's configuration name.
    pub fn name(&self) -> &'static str {
        self.name
    }
}

impl fmt::Debug for KeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

/// Implements [`Key`] for a service trait object, with the trait object as the value type.
///
/// ```
/// pub trait Tools: Send + Sync {}
/// cordis::key!(dyn Tools = "tools");
/// assert_eq!(cordis::KeyId::of::<dyn Tools>().name(), "tools");
/// ```
#[macro_export]
macro_rules! key {
    (dyn $service:path = $name:literal) => {
        impl $crate::Key for dyn $service {
            type Value = dyn $service;
            const NAME: &'static str = $name;
        }
    };
}
