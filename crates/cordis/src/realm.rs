//! Realms and interception: contexts derived to change how keys resolve for the fibers
//! instantiated through them.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::component::WhenRun;
use crate::erased::{Config, Erased, Factory};
use crate::orchestrator::Spawner;
use crate::{Component, Fiber, Guard, Key, KeyId, Optional, Provide, Result, When, WhenAction};

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

/// The tables a context carries: the realm of each isolated key (the paper's ρ, Def 24),
/// the interception of each intercepted key (ι, Def 26), and an optional parent [`tracing::Span`]
/// for fibers instantiated under it.
///
/// A fiber's context is fixed when the fiber is inserted, and its children inherit it. Both
/// tables are copied on write, so deriving a context leaves the one it derives from intact.
#[derive(Clone, Default)]
pub(crate) struct Env {
    realms: Arc<HashMap<KeyId, Realm>>,
    intercepts: Arc<HashMap<KeyId, Remap>>,
    span: Option<tracing::Span>,
}

/// An interception with the key's type erased: maps the provider value to the value consumers
/// under the context receive.
type Remap = Arc<dyn Fn(Erased) -> Erased + Send + Sync>;

impl Env {
    /// Returns where `key` resolves under this context.
    pub(crate) fn slot(&self, key: KeyId) -> Slot {
        Slot { key, realm: self.realms.get(&key).cloned().unwrap_or_else(Realm::shared) }
    }

    /// Applies this context's interception of `key`, if any, to a provider value.
    pub(crate) fn remap(&self, key: KeyId, value: Erased) -> Erased {
        match self.intercepts.get(&key) {
            Some(remap) => remap(value),
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

    /// Adds `remap` after any interception of `K` this context already carries.
    fn intercept<K: Key + ?Sized>(&mut self, remap: impl Fn(Arc<K::Value>) -> Arc<K::Value> + Send + Sync + 'static) {
        let key = KeyId::of::<K>();
        let outer = self.intercepts.get(&key).cloned();
        let composed: Remap = Arc::new(move |value: Erased| {
            let value = match &outer {
                Some(outer) => outer(value),
                None => value,
            };
            Erased::new::<K>(remap(value.get::<K>()))
        });
        Arc::make_mut(&mut self.intercepts).insert(key, composed);
    }

    /// Returns the explicit parent span attached to this context, if any.
    pub(crate) fn span(&self) -> Option<&tracing::Span> {
        self.span.as_ref()
    }

    /// Sets the parent span for fibers under this context, or clears it when `span` is
    /// [`tracing::Span::none`].
    fn set_span(&mut self, span: tracing::Span) {
        self.span = if span.is_none() { None } else { Some(span) };
    }
}

/// A context derived from a fiber's [`Cx`](crate::Cx) or from an [`App`](crate::App), which
/// changes how keys resolve and how lifecycle spans are parented for the fibers instantiated
/// through it.
///
/// [`isolate`](Scope::isolate) moves a key into another realm, [`intercept`](Scope::intercept)
/// changes the value consumers of a key receive, and [`in_span`](Scope::in_span) sets a
/// parent [`tracing::Span`] for lifecycle transitions; each returns a new scope and leaves this
/// one as it was. [`plugin`](Scope::plugin), [`provide`](Scope::provide),
/// [`when`](Scope::when), and [`optional`](Scope::optional) instantiate fibers under the scope.
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

    /// Returns a scope whose consumers of `K` receive `remap(provider)` instead of the provider.
    ///
    /// The shared provider and every other consumer are untouched: `remap` runs once per
    /// activation of each consumer instantiated through the scope, and its result becomes that
    /// consumer's field in [`Cx::deps`](crate::Cx::deps). Usually it wraps the provider in a type
    /// that implements the same trait and forwards to it, so an enclosing component can
    /// constrain how a component uses a key without changing that component or the provider.
    /// The closure receives the provider through an `Arc`, which other consumers share, so it
    /// can call only `&self` methods and returns a new value rather than changing the provider.
    ///
    /// A scope that already intercepts `K` applies its remap first, then this one.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use cordis::{Component, Cx, Result, component, key};
    ///
    /// pub trait Fs: Send + Sync {
    ///     fn read(&self, path: &str) -> Result<String>;
    ///     fn write(&self, path: &str, data: &str) -> Result<()>;
    /// }
    /// key!(dyn Fs = "fs");
    ///
    /// // Written by the sandbox's author; providers of `dyn Fs` need not know about it.
    /// struct ReadOnly(Arc<dyn Fs>);
    /// impl Fs for ReadOnly {
    ///     fn read(&self, path: &str) -> Result<String> {
    ///         self.0.read(path)
    ///     }
    ///     fn write(&self, path: &str, _: &str) -> Result<()> {
    ///         Err(format!("{path}: read-only").into())
    ///     }
    /// }
    ///
    /// #[component(deps(fs: dyn Fs))]
    /// struct Agent;
    /// # impl Component for Agent {
    /// #     async fn activate(_: Cx<Self>, _: ()) -> Result<Self> { Ok(Agent) }
    /// # }
    ///
    /// #[component]
    /// struct Sandbox;
    ///
    /// impl Component for Sandbox {
    ///     async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
    ///         // This agent's `cx.deps().fs` refuses writes; everyone else's Fs is unchanged.
    ///         cx.intercept::<dyn Fs>(|fs| Arc::new(ReadOnly(fs))).plugin::<Agent>(()).await?;
    ///         Ok(Sandbox)
    ///     }
    /// }
    /// ```
    pub fn intercept<K: Key + ?Sized>(
        &self,
        remap: impl Fn(Arc<K::Value>) -> Arc<K::Value> + Send + Sync + 'static,
    ) -> Scope {
        let mut env = self.env.clone();
        env.intercept::<K>(remap);
        Scope { env, spawner: self.spawner.clone() }
    }

    /// Returns a scope whose fibers (and their descendants) parent their `cordis.activate` and
    /// `cordis.deactivate` tracing spans to `span`.
    ///
    /// Pass [`tracing::Span::none()`] to detach from an inherited parent span.
    ///
    /// Because a fiber keeps a clone of `span` until the fiber is discarded, only attach a
    /// short-lived span (such as an agent turn or HTTP request) to fibers whose lifetime is
    /// bounded by that span (for example a per-turn subagent retired when the turn ends).
    /// Long-lived fibers without an explicit parent span still link back to the span that
    /// triggered their insert, update, or removal via `follows_from` without holding it open.
    pub fn in_span(&self, span: tracing::Span) -> Scope {
        let mut env = self.env.clone();
        env.set_span(span);
        Scope { env, spawner: self.spawner.clone() }
    }

    /// Instantiates a fiber of component `D` with `config` under this scope.
    ///
    /// `config` is passed directly in memory (`D::Config: Clone + Send + Sync + 'static`), without
    /// serialization. From a scope taken from a `Cx`, the fiber is a child of that fiber's current
    /// activation; see [`Cx::plugin`](crate::Cx::plugin). From a scope taken from an `App`, it is
    /// a root fiber, which [`Fiber::dispose`] or [`App::remove`](crate::App::remove) retires.
    ///
    /// # Errors
    ///
    /// Returns an error when the fiber the scope was taken from can no longer instantiate
    /// children, or when the app has stopped. In each case no fiber runs.
    pub async fn plugin<D>(&self, config: D::Config) -> Result<Fiber<D>>
    where
        D: Component,
        D::Config: Clone + Sync,
    {
        let config = Config::Typed(Arc::new(config));
        self.spawner.insert(Arc::new(Factory::typed::<D>()), config, self.env.clone()).await
    }

    /// Instantiates a [`Provide<K>`] fiber under this scope that publishes an existing `value`
    /// under key `K` in this scope's realm for `K`.
    ///
    /// Like [`plugin`](Scope::plugin), from a scope taken from a `Cx` the fiber is a child of that
    /// fiber's current activation; from a scope taken from an `App` it is a root fiber. The
    /// returned [`Fiber`] can replace the value with [`Fiber::update`] (reloading consumers) or
    /// withdraw it with [`Fiber::dispose`]. See [`Cx::provide`](crate::Cx::provide).
    ///
    /// # Errors
    ///
    /// Returns an error when the fiber the scope was taken from can no longer instantiate
    /// children, or when the app has stopped. In each case no fiber runs.
    pub fn provide<K: Key + ?Sized>(
        &self,
        value: Arc<K::Value>,
    ) -> impl Future<Output = Result<Fiber<Provide<K>>>> + Send + use<K> {
        // Plain `fn` with `config` and `factory` built before `async move` so the returned future
        // never holds `<dyn Trait as Key>::Value` across `.await` and stays unconditionally `Send`.
        let spawner = self.spawner.clone();
        let env = self.env.clone();
        let config = Config::Typed(Arc::new(value));
        let factory = Arc::new(Factory::typed::<Provide<K>>());
        async move { spawner.insert(factory, config, env).await }
    }

    /// Instantiates a fiber under this scope that runs `run` whenever `K` has an active provider
    /// in this scope's realm for `K`.
    ///
    /// Like [`plugin`](Scope::plugin), the fiber waits while `K` has no provider, runs `run` once
    /// `K` is active, and undoes the run's [`on_dispose`](crate::Cx::on_dispose) list—before `K`'s
    /// provider tears down—when `K` leaves or reloads, or when the parent activation ends. See
    /// [`Cx::when`](crate::Cx::when).
    ///
    /// # Errors
    ///
    /// Returns an error when the fiber the scope was taken from can no longer instantiate
    /// children, or when the app has stopped. In each case no fiber runs.
    pub async fn when<K: Key + ?Sized>(&self, run: impl WhenAction<K>) -> Result<Fiber<When<K>>> {
        // `plugin::<When<K>>` without its `Config: Clone + Sync` bound, which the compiler cannot
        // prove for every lifetime when `K` is a trait object.
        let config = Config::Typed(Arc::new(WhenRun::new(run)));
        self.spawner.insert(Arc::new(Factory::typed::<When<K>>()), config, self.env.clone()).await
    }

    /// Returns a live [`Optional<K>`] handle backed by a [`when`](Scope::when) fiber under this
    /// scope.
    ///
    /// The handle tracks the active provider of `K` in this scope's realm (with any interception
    /// on `K` applied once per activation of `K`'s provider) without restarting the owning fiber.
    ///
    /// Create the handle once and store or clone it: each call starts a new [`When<K>`] fiber
    /// that lives until the owning activation ends or the last clone of the handle is dropped.
    /// See [`Optional`].
    ///
    /// # Errors
    ///
    /// Returns an error when the fiber the scope was taken from can no longer instantiate
    /// children, or when the app has stopped. In each case no fiber runs.
    pub fn optional<K: Key + ?Sized>(&self) -> impl Future<Output = Result<Optional<K>>> + Send + use<K> {
        // Plain `fn` rather than `async fn`: rustc cannot prove an `async fn` generic over
        // `dyn Trait` keys `Send` when awaited inside `Component::activate` or `tokio::spawn`.
        let scope = self.clone();
        async move {
            let slot: Arc<RwLock<Option<Erased>>> = Arc::new(RwLock::new(None));
            let cell = slot.clone();
            let fiber = scope
                .when::<K>(move |cx, provider| {
                    let cell = cell.clone();
                    async move {
                        *cell.write().expect("optional slot lock is never poisoned") = Some(Erased::new::<K>(provider));
                        let clear = cell.clone();
                        cx.on_dispose(Guard::new(move || {
                            *clear.write().expect("optional slot lock is never poisoned") = None;
                        }));
                        Ok(())
                    }
                })
                .await?;
            Ok(Optional::new(slot, fiber))
        }
    }
}
