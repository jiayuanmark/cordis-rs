//! Type-erased values and component operations used by the orchestrator.

use std::any::Any;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::dispose::Undo;
use crate::orchestrator::{Control, WeakTable};
use crate::realm::Env;
use crate::{Component, Cx, Events, Key, KeyId, Manifest, Optional, Result, When, WhenAction};

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The provider value of each dependency key, resolved for one activation.
pub(crate) type Resolved = Vec<(KeyId, Erased)>;

/// Configuration of a fiber: either a JSON value (for fibers inserted through [`App::insert`] or
/// [`Loader`](crate::Loader)) or a typed Rust value (for fibers instantiated through
/// [`Cx::plugin`], [`Scope::plugin`], or [`App::plugin`](crate::App::plugin)).
#[derive(Clone)]
pub(crate) enum Config {
    Json(Value),
    Typed(Arc<dyn Any + Send + Sync>),
}

type Activate = Box<dyn Fn(Resolved, Config, Events, Control) -> BoxFuture<Result<Box<dyn Instance>>> + Send + Sync>;

/// A provided value with its type erased.
///
/// Holds an `Arc<K::Value>` and is read back only under the same `K`, so the internal
/// downcast cannot fail.
#[derive(Clone)]
pub struct Erased(Arc<dyn Any + Send + Sync>);

impl Erased {
    /// Wraps the value provided for `K`.
    pub fn new<K: Key + ?Sized>(value: Arc<K::Value>) -> Self {
        Self(Arc::new(value))
    }

    pub(crate) fn get<K: Key + ?Sized>(&self) -> Arc<K::Value> {
        self.0
            .downcast_ref::<Arc<K::Value>>()
            .expect("an Erased value is read only under the key that wrote it")
            .clone()
    }
}

/// The provider values resolved for one activation, and the context they are read under.
///
/// Passed to the code `#[component]` generates for [`Manifest::resolve`].
#[doc(hidden)]
pub struct Resolve<'a> {
    values: &'a [(KeyId, Erased)],
    env: &'a Env,
    table: &'a WeakTable,
}

impl Resolve<'_> {
    fn find(&self, id: KeyId) -> &Erased {
        &self
            .values
            .iter()
            .find(|(key, _)| *key == id)
            .expect("the orchestrator resolves every declared key before activation")
            .1
    }

    /// Returns the provider of `K`, with the context's interception of `K` applied, if any.
    pub fn get<K: Key + ?Sized>(&self) -> Arc<K::Value> {
        let id = KeyId::of::<K>();
        self.env.remap(id, self.find(id).clone()).get::<K>()
    }

    /// Returns a handle that reads the current provider of an optional dependency `K` under the
    /// context's realm and interception.
    pub fn optional<K: Key + ?Sized>(&self) -> Optional<K> {
        Optional::new(self.env.clone(), self.table.clone())
    }
}

/// The operations of one component type, with the type erased.
pub(crate) struct Factory {
    pub(crate) deps: Vec<KeyId>,
    pub(crate) optional: Vec<KeyId>,
    pub(crate) provides: Vec<KeyId>,
    pub(crate) validate: fn(&Value) -> Result<(), String>,
    pub(crate) activate: Activate,
}

impl Factory {
    pub(crate) fn of<C>() -> Self
    where
        C: Component,
        C::Config: DeserializeOwned,
    {
        Self {
            deps: <C as Manifest>::deps(),
            optional: <C as Manifest>::optional(),
            provides: <C as Manifest>::provides(),
            validate: validate::<C>,
            activate: Box::new(activate_json::<C>),
        }
    }

    pub(crate) fn typed<C>() -> Self
    where
        C: Component,
        C::Config: Clone + Sync,
    {
        Self {
            deps: <C as Manifest>::deps(),
            optional: <C as Manifest>::optional(),
            provides: <C as Manifest>::provides(),
            validate: |_| Ok(()),
            activate: Box::new(activate_typed::<C>),
        }
    }

    pub(crate) fn when<K: Key + ?Sized>(run: impl WhenAction<K>) -> Self {
        let run = Arc::new(run);
        Self {
            deps: <When<K> as Manifest>::deps(),
            optional: Vec::new(),
            provides: Vec::new(),
            validate: validate::<When<K>>,
            activate: Box::new(move |deps, _config, events, control| {
                let run = run.clone();
                Box::pin(async move {
                    let provider = <When<K> as Manifest>::resolve(&Resolve {
                        values: &deps,
                        env: control.env(),
                        table: control.table(),
                    });
                    let undo = control.undo().upgrade().expect("the activation task holds its run's undo list");
                    let cx = Cx::new(Arc::new(provider.clone()), events, control);
                    run(cx, provider).await?;
                    Ok(Box::new(Active { component: Arc::new(When::<K>(PhantomData)), undo }) as Box<dyn Instance>)
                })
            }),
        }
    }
}

fn validate<C>(config: &Value) -> Result<(), String>
where
    C: Component,
    C::Config: DeserializeOwned,
{
    serde_json::from_value::<C::Config>(config.clone()).map(drop).map_err(|error| error.to_string())
}

fn activate_json<C>(
    deps: Resolved,
    config: Config,
    events: Events,
    control: Control,
) -> BoxFuture<Result<Box<dyn Instance>>>
where
    C: Component,
    C::Config: DeserializeOwned,
{
    Box::pin(async move {
        let Config::Json(config) = config else {
            return Err("expected JSON configuration".into());
        };
        let config: C::Config = serde_json::from_value(config)?;
        activate_with::<C>(deps, config, events, control).await
    })
}

fn activate_typed<C>(
    deps: Resolved,
    config: Config,
    events: Events,
    control: Control,
) -> BoxFuture<Result<Box<dyn Instance>>>
where
    C: Component,
    C::Config: Clone + Sync,
{
    Box::pin(async move {
        let Config::Typed(config) = config else {
            return Err("expected typed configuration".into());
        };
        let config = config
            .downcast_ref::<C::Config>()
            .ok_or("configuration type does not match the component's Config type")?
            .clone();
        activate_with::<C>(deps, config, events, control).await
    })
}

async fn activate_with<C: Component>(
    deps: Resolved,
    config: C::Config,
    events: Events,
    control: Control,
) -> Result<Box<dyn Instance>> {
    let deps = Arc::new(C::resolve(&Resolve { values: &deps, env: control.env(), table: control.table() }));
    let undo = control.undo().upgrade().expect("the activation task holds its run's undo list");
    let component = C::activate(Cx::new(deps, events, control), config).await?;
    Ok(Box::new(Active { component: Arc::new(component), undo }) as Box<dyn Instance>)
}

/// An activated component, with its type erased.
pub(crate) trait Instance: Send + Sync {
    /// Returns the value to publish under each provided key.
    fn provisions(&self) -> Vec<(KeyId, Erased)>;

    /// Runs the fiber run's undo list, then releases the orchestrator's reference to the
    /// component.
    fn dispose(self: Box<Self>) -> BoxFuture<()>;
}

struct Active<C> {
    component: Arc<C>,
    /// The run's undo list, which every `Cx` clone of the run adds to.
    undo: Undo,
}

impl<C: Component> Instance for Active<C> {
    fn provisions(&self) -> Vec<(KeyId, Erased)> {
        <C as Manifest>::publish(&self.component)
    }

    fn dispose(self: Box<Self>) -> BoxFuture<()> {
        let Active { component, undo } = *self;
        Box::pin(async move {
            undo.run().await;
            // A consumer may still hold the value; its effects are undone either way, and it
            // drops when the last reference is released.
            drop(component);
        })
    }
}
