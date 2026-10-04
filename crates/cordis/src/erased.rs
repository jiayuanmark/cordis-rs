//! Type-erased values and component operations used by the reactor.

use std::any::{Any, type_name};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

use crate::guard::ScopeOwner;
use crate::reactor::Control;
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

    /// Returns the value resolved for `K`, which the reactor resolves before activation.
    ///
    /// Called by the code `#[component]` generates for [`Manifest::resolve`].
    pub fn find<K: Key + ?Sized>(resolved: &[(KeyId, Erased)]) -> Arc<K::Value> {
        let id = KeyId::of::<K>();
        resolved
            .iter()
            .find(|(key, _)| *key == id)
            .expect("the reactor resolves every declared key before activation")
            .1
            .get::<K>()
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
        let scope = ScopeOwner::new();
        let deps = Arc::new(C::resolve(&deps));
        let component = C::activate(Cx::new(deps, events, scope.scope(), control), config).await?;
        Ok(Box::new(Active { component: Arc::new(component), scope }) as Box<dyn Instance>)
    })
}

/// An activated component, with its type erased.
pub(crate) trait Instance: Send + Sync {
    /// Returns the value to publish under each provided key.
    fn provisions(&self) -> Vec<(KeyId, Erased)>;

    /// Runs the component's inverse.
    fn deactivate(self: Box<Self>) -> BoxFuture<()>;
}

struct Active<C> {
    component: Arc<C>,
    /// Registrations made through `Cx`, including clones the component kept; closed before
    /// `deactivate` so no listener runs during teardown.
    scope: ScopeOwner,
}

impl<C: Component> Instance for Active<C> {
    fn provisions(&self) -> Vec<(KeyId, Erased)> {
        <C as Manifest>::publish(&self.component)
    }

    fn deactivate(self: Box<Self>) -> BoxFuture<()> {
        let Active { component, scope } = *self;
        drop(scope);
        match Arc::into_inner(component) {
            Some(component) => Box::pin(component.deactivate()),
            None => {
                tracing::warn!(
                    component = type_name::<C>(),
                    "a reference outlived every consumer's teardown; the component drops when it is released",
                );
                Box::pin(async {})
            }
        }
    }
}
