//! Type-erased values and component operations used by the orchestrator.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

use crate::dispose::Undo;
use crate::orchestrator::Control;
use crate::realm::Env;
use crate::{Component, Cx, Events, Key, KeyId, Manifest, Result};

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The provider value of each dependency key, resolved for one activation.
pub(crate) type Resolved = Vec<(KeyId, Erased)>;

type Activate = fn(Resolved, Value, Events, Control) -> BoxFuture<Result<Box<dyn Instance>>>;

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
}

/// The operations of one component type, with the type erased.
pub(crate) struct Factory {
    pub(crate) deps: Vec<KeyId>,
    pub(crate) provides: Vec<KeyId>,
    pub(crate) validate: fn(&Value) -> Result<(), String>,
    pub(crate) activate: Activate,
}

impl Factory {
    pub(crate) fn of<C: Component>() -> Self {
        Self {
            deps: <C as Manifest>::deps(),
            provides: <C as Manifest>::provides(),
            validate: validate::<C>,
            activate: activate::<C>,
        }
    }
}

fn validate<C: Component>(config: &Value) -> Result<(), String> {
    serde_json::from_value::<C::Config>(config.clone()).map(drop).map_err(|error| error.to_string())
}

fn activate<C: Component>(
    deps: Resolved,
    config: Value,
    events: Events,
    control: Control,
) -> BoxFuture<Result<Box<dyn Instance>>> {
    Box::pin(async move {
        let config: C::Config = serde_json::from_value(config)?;
        let deps = Arc::new(C::resolve(&Resolve { values: &deps, env: control.env() }));
        let undo = control.undo().upgrade().expect("the activation task holds its run's undo list");
        let component = C::activate(Cx::new(deps, events, control), config).await?;
        Ok(Box::new(Active { component: Arc::new(component), undo }) as Box<dyn Instance>)
    })
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
