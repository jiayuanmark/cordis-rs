# cordis-rs

A Rust realization of the context paradigm from *A Programming Paradigm for Spatiotemporal Composability* (arXiv:2608.25512): components whose effects revert when they unload, and a reactor that activates and deactivates them as their dependencies come and go. [design.md](design.md) describes the model.

## Layout

| Path | Contents |
|---|---|
| `crates/cordis/src/key.rs` | `Key`, `KeyId`, `key!` |
| `crates/cordis/src/guard.rs` | `Guard`, `Guards` |
| `crates/cordis/src/component.rs` | `Manifest`, `Component`, `Cx` |
| `crates/cordis-macros/src/lib.rs` | The `#[component]` attribute, re-exported as `cordis::component` |
| `crates/cordis/src/events.rs` | `Event`, `Events`, the five dispatch modes, `event!` |
| `crates/cordis/src/erased.rs` | Type-erased values and component operations |
| `crates/cordis/src/reactor.rs` | `App` and the reactor |
| `crates/cordis/src/loader.rs` | `Catalog`, `Loader`, YAML trees and layers |
| `crates/cordis/tests/lifecycle.rs` | Lifecycle behavior tests |
| `crates/cordis/tests/component.rs` | `#[component]` on generic and plain types |
| `crates/cordis/tests/events.rs` | Event bus behavior tests |
| `crates/cordis/tests/layers.rs` | Layer merge tests |
| `crates/cordis/examples/harness.rs` | A miniature agent harness built on events (`cargo run --example harness`) |
| `crates/cordis/examples/profiles.rs` | Profiles composed from YAML layers in `examples/config/` (`cargo run --example profiles`) |

## Example

```rust
pub trait Tools: Send + Sync { fn register(&self, name: &str) -> Guard; }
key!(dyn Tools = "tools");

#[component(deps(tools: dyn Tools))]
struct GrepTool { _registration: Guard }

impl Component for GrepTool {
    async fn activate(cx: Cx<Self>, _: ()) -> Result<Self> {
        Ok(Self { _registration: cx.deps().tools.register("grep") })
    }
}
```

## Status

Implemented:

- Keys as types, with compile-time rejection of undeclared dependencies and invalid provisions.
- Reactive activation and deactivation, including cascades, the "relied upon" teardown guard, cancellation of in-flight activations, exclusive providers with takeover, configuration reloads, and per-fiber failure.
- Typed events with `Emit`, `Bail`, `Parallel`, `Serial`, and `Waterfall` dispatch; listeners registered through `Cx` are removed when their fiber unloads or its activation is cancelled or fails.
- Layered configuration: `Tree::layered` merges YAML layers by row id (override set fields, replace `config` whole, toggle `disabled`, append new rows).
- A YAML loader that validates a whole tree (unknown components, invalid configuration, missing providers, cycles) before reconciling it.

Not yet implemented: `many` keys, realms, interception metadata, listener ordering beyond registration order, the named event edge, `compose!`, and WASM components.

## Commands

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

`rust-toolchain.toml` pins Rust 1.95.0.
