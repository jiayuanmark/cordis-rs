//! Realms and interception: contexts derived to change how keys resolve for the fibers
//! instantiated through them.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use tokio::sync::oneshot;

use crate::erased::Erased;
use crate::erased::Factory;
use crate::reactor::{Command, Spawner};
use crate::{Component, Fiber, Intercept, Key, KeyId, Merge, Result};

/// A namespace of provisions.
///
/// Every key resolves in the shared realm unless a context isolates it. A fiber provides and
/// depends on each key in the realm its context assigns to that key, so two fibers can provide
/// one key in different realms, and a consumer sees only the provider in its own realm.
///
/// One realm can hold several keys: isolating `dyn Tools` and `dyn Fs` into the same realm
/// gives a tenant its own tool registry and file system. Each key keeps its own binding there;
/// a realm never makes two keys share a value.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Realm(Id);

#[derive(Clone, PartialEq, Eq, Hash)]
enum Id {
    Shared,
    Fresh(u64),
    Named(Arc<str>),
}

impl Realm {
    /// Returns a realm no other call returns. It is discarded once no fiber is in it.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(Id::Fresh(NEXT.fetch_add(1, Ordering::Relaxed)))
    }

    /// Returns the realm called `name`. Every call with the same name, from any fiber, returns
    /// the same realm.
    pub fn named(name: &str) -> Self {
        Self(Id::Named(name.into()))
    }

    /// Returns the shared realm, where every key resolves unless a context isolates it.
    /// Isolating a key into it undoes an inherited isolation.
    pub fn shared() -> Self {
        Self(Id::Shared)
    }
}

impl fmt::Debug for Realm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Id::Shared => f.write_str("shared"),
            Id::Fresh(id) => write!(f, "#{id}"),
            Id::Named(name) => write!(f, "{name:?}"),
        }
    }
}

/// Where a key resolves: the key in one realm.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Slot {
    pub(crate) key: KeyId,
    pub(crate) realm: Realm,
}

impl fmt::Debug for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.realm.0 {
            Id::Shared => write!(f, "{:?}", self.key),
            _ => write!(f, "{:?}@{:?}", self.key, self.realm),
        }
    }
}

/// The tables a context carries: the realm of each isolated key (the paper's ρ, Def 24) and
/// the interception metadata of each intercepted key (ι, Def 26).
///
/// A fiber's context is fixed when the fiber is inserted, and its children inherit it. Both
/// tables are copied on write, so deriving a context leaves the one it derives from intact.
#[derive(Clone, Default)]
pub(crate) struct Env {
    realms: Arc<HashMap<KeyId, Realm>>,
    meta: Arc<HashMap<KeyId, Carried>>,
}

/// Metadata a context carries for one key, with the key's binder, so a consumer that declares
/// no metadata of its own can apply it without naming [`Intercept`].
#[derive(Clone)]
struct Carried {
    meta: Arc<dyn Any + Send + Sync>,
    bind: fn(&Erased, &(dyn Any + Send + Sync)) -> Erased,
}

fn bind_erased<K: Intercept + ?Sized>(value: &Erased, meta: &(dyn Any + Send + Sync)) -> Erased {
    let meta = meta.downcast_ref::<K::Meta>().expect("metadata is stored only under its own key");
    Erased::new::<K>(K::bind(value.get::<K>(), meta))
}

impl Env {
    /// Returns where `key` resolves under this context.
    pub(crate) fn slot(&self, key: KeyId) -> Slot {
        Slot { key, realm: self.realms.get(&key).cloned().unwrap_or_else(Realm::shared) }
    }

    /// Returns the metadata this context carries for `K`, if any.
    pub(crate) fn meta<K: Intercept + ?Sized>(&self) -> Option<&K::Meta> {
        let carried = self.meta.get(&KeyId::of::<K>())?;
        Some(carried.meta.downcast_ref().expect("metadata is stored only under its own key"))
    }

    /// Applies the metadata this context carries for `key`, if any, to a provider value.
    pub(crate) fn bind(&self, key: KeyId, value: Erased) -> Erased {
        match self.meta.get(&key) {
            Some(carried) => (carried.bind)(&value, &*carried.meta),
            None => value,
        }
    }

    pub(crate) fn isolate(&mut self, key: KeyId, realm: Realm) {
        let realms = Arc::make_mut(&mut self.realms);
        if realm == Realm::shared() {
            realms.remove(&key);
        } else {
            realms.insert(key, realm);
        }
    }

    fn intercept<K: Intercept + ?Sized>(&mut self, meta: K::Meta) {
        let mut merged = self.meta::<K>().cloned().unwrap_or_default();
        merged.merge(meta);
        let carried = Carried { meta: Arc::new(merged), bind: bind_erased::<K> };
        Arc::make_mut(&mut self.meta).insert(KeyId::of::<K>(), carried);
    }
}

/// A context derived from a fiber's [`Cx`](crate::Cx) or from an [`App`](crate::App), which
/// changes how keys resolve for the fibers instantiated through it.
///
/// [`isolate`](Scope::isolate) moves a key into another realm and
/// [`intercept`](Scope::intercept) attaches metadata to it; each returns a new scope and
/// leaves this one as it was. [`plugin`](Scope::plugin) instantiates a fiber under the scope.
/// That fiber's own context, and so its children's, inherits every adjustment.
///
/// A scope is a derived context in the paper's sense (Def 23): it changes nothing shared, so
/// dropping it has nothing to revert. Fibers instantiated through a scope taken from a `Cx` are
/// children of that fiber's activation, exactly as with [`Cx::plugin`](crate::Cx::plugin).
#[derive(Clone)]
pub struct Scope {
    env: Env,
    spawner: Spawner,
}

impl Scope {
    pub(crate) fn new(env: Env, spawner: Spawner) -> Self {
        Self { env, spawner }
    }

    /// Returns a scope where `K` resolves in a fresh realm, private to the fibers instantiated
    /// through it and their descendants.
    ///
    /// Inside, consumers of `K` see only a provider of `K` instantiated inside, and such a
    /// provider is invisible outside. Other keys still resolve as they do here.
    pub fn isolate<K: Key + ?Sized>(&self) -> Scope {
        self.isolate_in::<K>(&Realm::new())
    }

    /// Returns a scope where `K` resolves in `realm`, which other scopes may name as well.
    ///
    /// A key already isolated is reassigned, and [`Realm::shared`] undoes the isolation.
    pub fn isolate_in<K: Key + ?Sized>(&self, realm: &Realm) -> Scope {
        let mut env = self.env.clone();
        env.isolate(KeyId::of::<K>(), realm.clone());
        Scope { env, spawner: self.spawner.clone() }
    }

    /// Returns a scope that carries `meta` for `K`, merged onto what this scope carries and
    /// taking priority over it.
    ///
    /// A consumer of `K` instantiated through the scope receives the provider value with the
    /// metadata applied by [`Intercept::bind`]. The context's metadata takes priority over what the
    /// consumer declares in `deps(...)`, so an enclosing component can constrain how a
    /// component uses a key without changing that component.
    pub fn intercept<K: Intercept + ?Sized>(&self, meta: K::Meta) -> Scope {
        let mut env = self.env.clone();
        env.intercept::<K>(meta);
        Scope { env, spawner: self.spawner.clone() }
    }

    /// Instantiates a fiber of component `D` with `config` under this scope.
    ///
    /// From a scope taken from a `Cx`, the fiber is a child of that fiber's current activation;
    /// see [`Cx::plugin`](crate::Cx::plugin). From a scope taken from an `App`, it is a root
    /// fiber, which [`Fiber::dispose`] or [`App::remove`](crate::App::remove) retires.
    ///
    /// # Errors
    ///
    /// Returns an error when `config` fails to serialize, when the fiber the scope was taken
    /// from can no longer instantiate children, or when the app has stopped. In each case no
    /// fiber runs.
    pub async fn plugin<D: Component>(&self, config: impl Serialize) -> Result<Fiber> {
        let config = serde_json::to_value(config)?;
        self.spawner.insert(Arc::new(Factory::of::<D>()), config, self.env.clone()).await
    }

    /// Returns the current active provider of `K` in the realm this scope resolves it in, with
    /// no metadata applied.
    pub async fn get<K: Key + ?Sized>(&self) -> Option<Arc<K::Value>> {
        let commands = self.spawner.commands()?;
        let (reply, answer) = oneshot::channel();
        commands.send(Command::Get { slot: self.env.slot(KeyId::of::<K>()), reply }).ok()?;
        answer.await.ok().flatten().map(|value| value.get::<K>())
    }
}
