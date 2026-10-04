//! Keys: named slots with a declared value type.

use std::any::TypeId;
use std::fmt;
use std::sync::Arc;

/// A named slot with a declared value type.
///
/// Consumers and providers meet at a key without naming each other. Consumers receive
/// `Arc<Self::Value>`. Implement it for a service trait object with [`key!`](crate::key!), or
/// by hand for a marker type that names a second slot of an existing interface:
///
/// ```
/// use cordis::Key;
///
/// pub trait Database: Send + Sync {}
///
/// pub struct ReplicaDb;
/// impl Key for ReplicaDb {
///     type Value = dyn Database;
///     type Meta = ();
///     const NAME: &'static str = "db.replica";
/// }
/// ```
pub trait Key: 'static {
    /// The type consumers receive behind an `Arc`.
    type Value: ?Sized + Send + Sync + 'static;
    /// Interception metadata: how a consumer uses the provider, as opposed to which provider it
    /// reaches. `()` for a key that takes none.
    type Meta: Merge;
    /// Name used by configuration and diagnostics. Lookups use the key's `TypeId`.
    const NAME: &'static str;

    /// Applies interception metadata to the provider value a consumer receives.
    ///
    /// The paper's provider function from metadata to value (Def 26). The reactor calls it once
    /// per activation of a consumer, and only when the consumer declares metadata for the key
    /// or the context it was instantiated in carries some; otherwise the consumer receives the
    /// provider value as is. The default ignores the metadata, which suits `Meta = ()`.
    fn bind(value: Arc<Self::Value>, meta: &Self::Meta) -> Arc<Self::Value> {
        let _ = meta;
        value
    }
}

/// Interception metadata: a monoid with [`Default`] as its identity.
///
/// [`merge`](Merge::merge) must be associative and right-biased: the right operand takes
/// priority, so metadata a context carries overrides what a component declares, and a later
/// [`intercept`](crate::Cx::intercept) overrides an earlier one. Scalar fields are typically
/// overwritten and set-valued fields unioned.
pub trait Merge: Clone + Default + Send + Sync + 'static {
    /// Merges `right` onto `self`, with `right` taking priority.
    fn merge(&mut self, right: Self);
}

impl Merge for () {
    fn merge(&mut self, (): ()) {}
}

/// A scalar field: `Some` on the right overwrites, `None` keeps the left.
impl<T: Clone + Send + Sync + 'static> Merge for Option<T> {
    fn merge(&mut self, right: Self) {
        if right.is_some() {
            *self = right;
        }
    }
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
///
/// A key that takes interception metadata names its [`Merge`] type and the function that
/// applies it, [`Key::bind`]:
///
/// ```
/// use std::sync::Arc;
///
/// pub trait Fs: Send + Sync {
///     fn write(&self, path: &str) -> Result<(), String>;
/// }
///
/// #[derive(Clone, Default)]
/// pub struct FsMeta {
///     pub read_only: Option<bool>,
/// }
///
/// impl cordis::Merge for FsMeta {
///     fn merge(&mut self, right: Self) {
///         self.read_only.merge(right.read_only);
///     }
/// }
///
/// struct ReadOnly(Arc<dyn Fs>);
/// impl Fs for ReadOnly {
///     fn write(&self, path: &str) -> Result<(), String> {
///         Err(format!("{path}: read-only"))
///     }
/// }
///
/// cordis::key!(dyn Fs = "fs", meta = FsMeta, bind = |fs, meta| {
///     if meta.read_only == Some(true) { Arc::new(ReadOnly(fs)) } else { fs }
/// });
/// ```
#[macro_export]
macro_rules! key {
    (dyn $service:path = $name:literal) => {
        impl $crate::Key for dyn $service {
            type Value = dyn $service;
            type Meta = ();
            const NAME: &'static str = $name;
        }
    };
    (dyn $service:path = $name:literal, meta = $meta:ty, bind = $bind:expr $(,)?) => {
        impl $crate::Key for dyn $service {
            type Value = dyn $service;
            type Meta = $meta;
            const NAME: &'static str = $name;

            fn bind(value: ::std::sync::Arc<dyn $service>, meta: &$meta) -> ::std::sync::Arc<dyn $service> {
                let bind: fn(::std::sync::Arc<dyn $service>, &$meta) -> ::std::sync::Arc<dyn $service> = $bind;
                bind(value, meta)
            }
        }
    };
}
