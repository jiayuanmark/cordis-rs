//! Layered configuration: one base layer, two profiles, and a user patch compose different
//! applications from the same components, and switching profiles reconciles a running app.
//!
//! Run with `cargo run --example profiles`.

use cordis::{App, Catalog, Component, Cx, Layer, Loader, Manifest, Result, Tree, component, key};
use serde::Deserialize;

pub trait Sessions: Send + Sync {}
key!(dyn Sessions = "sessions");

pub trait Tools: Send + Sync {}
key!(dyn Tools = "tools");

/// Makes a component print when it activates and when it is dropped.
macro_rules! printing {
    ($name:ident, $describe:expr) => {
        impl Component for $name {
            async fn activate(_: Cx<Self>, config: Self::Config) -> Result<Self> {
                let describe: fn(&<Self as Manifest>::Config) -> String = $describe;
                let label = format!("{} {}", stringify!($name), describe(&config)).trim_end().to_owned();
                println!("  + {label}");
                Ok(Self(label))
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                println!("  - {}", self.0);
            }
        }
    };
}

#[derive(Deserialize)]
struct Dir {
    dir: String,
}

#[derive(Deserialize)]
struct Approval {
    mode: String,
}

#[derive(Deserialize)]
struct Port {
    port: u16,
}

#[component(config = Dir, provides(dyn Sessions))]
struct JsonlSessions(String);

#[component(provides(dyn Sessions))]
struct MemorySessions(String);

#[component(deps(dyn Sessions), provides(dyn Tools))]
struct ToolRuntime(String);

#[component(config = Approval, deps(dyn Tools))]
struct ApprovalPolicy(String);

#[component(config = Port, deps(dyn Sessions, dyn Tools))]
struct WebServer(String);

printing!(JsonlSessions, |c| format!("(dir {})", c.dir));
printing!(MemorySessions, |_| String::new());
printing!(ToolRuntime, |_| String::new());
printing!(ApprovalPolicy, |c| format!("(mode {})", c.mode));
printing!(WebServer, |c| format!("(port {})", c.port));

impl Sessions for JsonlSessions {}
impl Sessions for MemorySessions {}
impl Tools for ToolRuntime {}

fn layer(name: &str, text: &str) -> Layer {
    Layer::from_yaml(name, text).expect("the example layers are valid")
}

fn print_tree(tree: &Tree) {
    for row in &tree.plugins {
        let state = if row.disabled { " (disabled)" } else { "" };
        println!("  {:<9} use {:<16} config {}{state}", row.id, row.component, row.config);
    }
}

#[tokio::main]
async fn main() {
    let base = layer("base.yml", include_str!("config/base.yml"));
    let headless = layer("headless.yml", include_str!("config/headless.yml"));
    let web = layer("web.yml", include_str!("config/web.yml"));
    let user = layer("user.patch.yml", include_str!("config/user.patch.yml"));

    let mut catalog = Catalog::new();
    catalog
        .register::<JsonlSessions>("jsonl-sessions")
        .register::<MemorySessions>("memory-sessions")
        .register::<ToolRuntime>("tool-runtime")
        .register::<ApprovalPolicy>("approval-policy")
        .register::<WebServer>("web-server");
    let app = App::new();
    let mut loader = Loader::new(app.clone(), catalog);

    let profiles = [("headless", vec![&base, &headless]), ("web + user patch", vec![&base, &web, &user])];
    for (name, layers) in profiles {
        let names: Vec<&str> = layers.iter().map(|layer| layer.name()).collect();
        let tree = Tree::layered(layers).expect("the example layers merge");
        println!("== profile {name}: {}", names.join(" -> "));
        print_tree(&tree);
        println!("lifecycle:");
        loader.apply(&tree).await.expect("the merged tree is valid");
        app.settled().await;
        println!();
    }

    // A patch that adds a row without naming its component fails before anything changes.
    let broken = layer("broken.yml", "plugins:\n  - { id: metrics, config: { port: 9090 } }\n");
    println!("== broken patch: {}", Tree::layered([&base, &broken]).unwrap_err());

    println!("\n== shutdown");
    app.shutdown().await;
}
