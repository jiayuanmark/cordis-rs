//! Reaching a fiber's context from ordinary component methods, after `activate` returns.

use std::sync::{Arc, Mutex};

use cordis::{App, Component, Cx, Result, component, event, key};
use serde::Deserialize;
use serde_json::json;

static JOURNAL: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn record(entry: impl Into<String>) {
    JOURNAL.lock().unwrap().push(entry.into());
}

fn journal() -> Vec<String> {
    JOURNAL.lock().unwrap().clone()
}

event!(Named = "test/named", Emit(String));

pub trait Salutation: Send + Sync {
    fn text(&self) -> String;
}
key!(dyn Salutation = "salutation");

pub trait Greet: Send + Sync {
    fn greet(&self, name: &str) -> String;
    fn greet_everyone_named(&self);
}
key!(dyn Greet = "greet");

#[derive(Deserialize)]
struct SaluteConfig {
    text: String,
}

#[component(config = SaluteConfig, provides(dyn Salutation))]
struct Salute(String);

impl Salutation for Salute {
    fn text(&self) -> String {
        self.0.clone()
    }
}

impl Component for Salute {
    async fn activate(_: Cx<Self>, config: SaluteConfig) -> Result<Self> {
        Ok(Salute(config.text))
    }
}

/// Keeps its `Cx` and uses it from trait methods called by consumers.
#[component(deps(dyn Salutation), provides(dyn Greet))]
struct Greeter {
    cx: Cx<Self>,
}

/// A clone of the greeter's `Cx`, kept past the fiber's lifetime.
static LEFTOVER: Mutex<Option<Cx<Greeter>>> = Mutex::new(None);

impl Greet for Greeter {
    fn greet(&self, name: &str) -> String {
        format!("{} {name}", self.cx.dep::<dyn Salutation>().text())
    }

    fn greet_everyone_named(&self) {
        // The listener captures a clone of the `Cx` that owns it.
        let cx = self.cx.clone();
        self.cx.on::<Named>().listen(move |name| record(format!("{} {name}", cx.dep::<dyn Salutation>().text())));
    }
}

impl Component for Greeter {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        *LEFTOVER.lock().unwrap() = Some(cx.clone());
        Ok(Greeter { cx })
    }
}

impl Drop for Greeter {
    fn drop(&mut self) {
        record("drop greeter");
    }
}

#[component(deps(dyn Greet))]
struct Caller;

impl Component for Caller {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        let greeter = cx.dep::<dyn Greet>();
        record(greeter.greet("caller"));
        greeter.greet_everyone_named();
        Ok(Caller)
    }
}

#[tokio::test]
async fn methods_reach_the_context_and_their_listeners_unload_with_the_fiber() {
    let app = App::new();
    app.insert::<Salute>(json!({ "text": "hello" })).await;
    let greeter = app.insert::<Greeter>(json!(null)).await;
    app.insert::<Caller>(json!(null)).await;
    app.settled().await;
    app.events().emit::<Named>("x".into());
    assert_eq!(journal(), ["hello caller", "hello x"]);

    let leftover = LEFTOVER.lock().unwrap().clone().unwrap();
    assert!(leftover.is_live());

    app.remove(greeter);
    app.settled().await;
    app.events().emit::<Named>("y".into());
    // The listener held a `Cx` clone, yet the greeter still dropped and stopped listening.
    assert_eq!(journal(), ["hello caller", "hello x", "drop greeter"]);

    // A clone that outlives the fiber can no longer register anything.
    assert!(!leftover.is_live());
    let late = Arc::new(Mutex::new(0));
    let counted = late.clone();
    leftover.on::<Named>().listen(move |_| *counted.lock().unwrap() += 1);
    app.events().emit::<Named>("z".into());
    assert_eq!(*late.lock().unwrap(), 0);
}
