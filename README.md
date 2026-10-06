# cordis-rs

A Rust realization of the context paradigm from *A Programming Paradigm for Spatiotemporal Composability* (arXiv:2608.25512): components whose effects are undone when they unload, and an orchestrator that activates and deactivates them as their dependencies come and go. [design.md](design.md) describes the model.

## Layout

| Path | Contents |
|---|---|
| `crates/cordis/src/key.rs` | `Key`, `KeyId`, `key!` |
| `crates/cordis/src/guard.rs` | `Guard`, `Guards` |
| `crates/cordis/src/dispose.rs` | `Dispose` and the undo list of a fiber run |
| `crates/cordis/src/component.rs` | `Manifest`, `Component`, `Cx` |
| `crates/cordis-macros/src/lib.rs` | The `#[component]` attribute, re-exported as `cordis::component` |
| `crates/cordis/src/events.rs` | `Event`, `Events`, the five dispatch modes, `event!` |
| `crates/cordis/src/erased.rs` | Type-erased values and component operations |
| `crates/cordis/src/orchestrator.rs` | `App` and the orchestrator |
| `crates/cordis/src/realm.rs` | `Realm` and `Scope`: isolation and interception |
| `crates/cordis/src/loader.rs` | `Catalog`, `Loader`, YAML trees and layers |
| `crates/cordis/tests/lifecycle.rs` | Lifecycle behavior tests |
| `crates/cordis/tests/dispose.rs` | Undo list tests: order, timing, panics |
| `crates/cordis/tests/tree.rs` | Child fiber tests |
| `crates/cordis/tests/component.rs` | `#[component]` on generic and plain types |
| `crates/cordis/tests/events.rs` | Event bus behavior tests |
| `crates/cordis/tests/layers.rs` | Layer merge tests |
| `crates/cordis/tests/realms.rs` | Isolation and interception tests |
| `crates/cordis/examples/harness.rs` | A miniature agent harness built on events (`cargo run --example harness`) |
| `crates/cordis/examples/profiles.rs` | Profiles composed from YAML layers in `examples/config/` (`cargo run --example profiles`) |

## Example

```rust
pub trait Tools: Send + Sync { fn register(&self, name: &str) -> Guard; }
key!(dyn Tools = "tools");

#[component(deps(tools: dyn Tools))]
struct GrepTool;

impl Component for GrepTool {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        // Unregistered when the fiber unloads, even if someone still holds the GrepTool.
        cx.on_dispose(cx.deps().tools.register("grep"));
        Ok(GrepTool)
    }
}
```

## Status

Implemented:

- Keys as types, with compile-time rejection of undeclared dependencies and invalid provisions.
- One undo list per fiber run: `cx.on_dispose` takes a `Guard`, a Tokio `JoinHandle`, or an async closure; listeners and children join the same list, undone newest first, one at a time, when the fiber unloads or its activation is cancelled or fails.
- Reactive activation and deactivation, including cascades, the "relied upon" teardown guard, cancellation of in-flight activations (signalled through `cx.cancelled()`, never by dropping `activate` mid-step), exclusive providers with takeover, configuration reloads, and per-fiber failure.
- Typed events with `Emit`, `Bail`, `Parallel`, `Serial`, and `Waterfall` dispatch; listeners registered through `Cx` are removed when their fiber unloads or its activation is cancelled or fails.
- Layered configuration: `Tree::layered` merges YAML layers by row id (override set fields, replace `config` whole, toggle `disabled`, append new rows).
- A YAML loader that validates a whole tree (unknown components, invalid configuration, missing providers, cycles) before reconciling it.
- Child fibers instantiated through `cx.plugin`, retired with the activation that made them.
- Realms: `cx.isolate::<K>()` gives a subtree its own provider of `K`, in a fresh or named realm, and rows can name realms with `isolate`.
- Interception: `cx.intercept::<K>(|v| ...)` hands consumers under the scope a remapped view of a provider, such as a read-only wrapper, leaving the shared provider untouched.

Not yet implemented: `many` keys, loader-managed local realms and interception in configuration, listener ordering beyond registration order, the named event edge, `compose!`, and WASM components.

## Commands

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

`rust-toolchain.toml` pins Rust 1.95.0.
