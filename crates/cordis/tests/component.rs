//! The `#[component]` attribute on generic and plain types.

use std::marker::PhantomData;

use cordis::{App, Component, Cx, KeyId, Manifest, Result, Status, component, key};
use serde_json::json;

pub trait Named: Send + Sync {
    fn name(&self) -> &'static str;
}
key!(dyn Named = "named");

pub trait Kind: Send + Sync + 'static {
    const NAME: &'static str;
}

struct Alpha;
impl Kind for Alpha {
    const NAME: &'static str = "alpha";
}

#[component(provides(dyn Named))]
struct Provider<T: Kind>(PhantomData<T>);

impl<T: Kind> Named for Provider<T> {
    fn name(&self) -> &'static str {
        T::NAME
    }
}

impl<T: Kind> Component for Provider<T> {
    async fn activate(_: Cx<Self>, _: ()) -> Result<Self> {
        Ok(Self(PhantomData))
    }
}

#[component]
struct Bare;

impl Component for Bare {
    async fn activate(_: Cx<Self>, _: ()) -> Result<Self> {
        Ok(Bare)
    }
}

pub trait Counter: Send + Sync {
    fn count(&self) -> usize;
}
key!(dyn Counter = "counter");

#[component(provides(dyn Counter))]
struct Seven;

impl Counter for Seven {
    fn count(&self) -> usize {
        7
    }
}

impl Component for Seven {
    async fn activate(_: Cx<Self>, _: ()) -> Result<Self> {
        Ok(Seven)
    }
}

pub trait Report: Send + Sync {
    fn describe(&self) -> String;
}
key!(dyn Report = "report");

#[component(deps(named: dyn Named, counter: dyn Counter), provides(dyn Report))]
struct Consumer {
    cx: Cx<Self>,
}

impl Report for Consumer {
    fn describe(&self) -> String {
        let ConsumerDeps { named, counter } = self.cx.deps();
        format!("{} {}", named.name(), counter.count())
    }
}

impl Component for Consumer {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        Ok(Self { cx })
    }
}

#[test]
fn the_attribute_declares_keys_in_order() {
    assert_eq!(Provider::<Alpha>::provides(), [KeyId::of::<dyn Named>()]);
    assert!(Provider::<Alpha>::deps().is_empty());
    assert!(Bare::deps().is_empty() && Bare::provides().is_empty());
    assert_eq!(Consumer::deps(), [KeyId::of::<dyn Named>(), KeyId::of::<dyn Counter>()]);
    let _: <Bare as Manifest>::Deps = ();
}

#[tokio::test]
async fn a_generic_component_provides_through_its_declared_key() {
    let app = App::new();
    let provider = app.insert::<Provider<Alpha>>(json!(null)).await;
    let bare = app.insert::<Bare>(json!(null)).await;
    app.settled().await;
    assert_eq!(app.status(provider).await, Some(Status::Active));
    assert_eq!(app.status(bare).await, Some(Status::Active));
    assert_eq!(app.get::<dyn Named>().await.unwrap().name(), "alpha");
}

#[tokio::test]
async fn deps_resolve_into_the_generated_struct() {
    let app = App::new();
    let consumer = app.insert::<Consumer>(json!(null)).await;
    app.insert::<Provider<Alpha>>(json!(null)).await;
    app.insert::<Seven>(json!(null)).await;
    app.settled().await;
    assert_eq!(app.status(consumer).await, Some(Status::Active));
    assert_eq!(app.get::<dyn Report>().await.unwrap().describe(), "alpha 7");
}
