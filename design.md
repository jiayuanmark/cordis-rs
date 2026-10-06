# Cordis in Rust: Design

This document describes a native Rust realization of the context paradigm from *A Programming Paradigm for Spatiotemporal Composability* (Shi, Zhang, Cui; arXiv:2608.25512), and an agent harness built on it. It does not aim for compatibility with the TypeScript Cordis implementation.

## Thesis

TypeScript Cordis enforces the paper's guarantees with runtime machinery: a `Proxy` that mediates every context access, an explicit effect accumulator, and a string-keyed event bus. In Rust, language features replace most of that machinery:

- **Revertible effects** are ownership and `Drop`.
- **Effect iterators** are `async fn`.
- **Coeffect specifications** are associated types.
- **Coeffect provisions** are checked coercions to `Arc<dyn Trait>`.
- **Event names** are types, so payloads and dispatch modes are checked at compile time.

The runtime keeps only what is inherently dynamic: an orchestrator that activates and deactivates components as providers and configuration change.

Rust fixes the *vocabulary* at compile time: which service traits exist, which component types exist, and what each one declares. Configuration still decides the *composition*: which components run, with what configuration, and so which implementation serves each key.

## Concept mapping

| Paper concept | Rust realization |
|---|---|
| Key *k* with value type *V_k* | A type implementing `Key`, with `Key::Value` as *V_k* |
| Component (*d*, *p*, *e*), Def 48 | A type implementing `Component`, with *d* and *p* declared by `#[component]` |
| Revertible effect and inverse, §3.1 | Owning a value whose `Drop` performs the inverse |
| Accumulator φ | The component's state value; during a cancelled activation, the future's locals |
| Effect iterator and iteration boundaries, Def 17 | `async fn activate`; each `.await` is an iteration boundary |
| Fiber and fiber tree, Defs 49 and 50 | One configured instance of a component, owned by the orchestrator; a child records the activation it was instantiated under |
| Instantiation and retirement, Def 52 | `cx.plugin::<C>(config)`; the orchestrator retires a child when its parent's activation ends (O-Retire) |
| Target view and committed view, Def 53 | `View`: the provider fiber of each dependency key |
| Satisfaction and `notify`, Def 22 | The orchestrator's settle step |
| The "relied upon" guard, Def 54 | Per-fiber count of committed views that name the fiber |
| Derived realization, Def 23 | `Scope`: a context derived from a `Cx` or the `App`, discarded by dropping it |
| Isolation, Defs 24 and 25 | `cx.isolate::<K>()`: the table is keyed by (key, `Realm`), and a fiber's realms are fixed at insertion |
| Interception, Defs 26 and 27 | `cx.intercept::<K>(\|v\| ...)`: a closure that remaps the provider value for consumers under the scope |
| Service multiplexing, §6.2 | Cardinality declared on the key: `one` or `many` |
| Dependency typing and versioning, §6.6 | `TypeId` keys plus trait upcasting between interface versions |
| Cordis events | Types implementing `Event`, dispatched through the built-in `Events` bus |

## Keys

A consumer and a provider never name each other. They meet at a **key**: a named slot with a declared value type.

In TypeScript Cordis a key is a string (`'tools'`), and the value's type is only asserted by declaration merging. In Rust a key is a **type**. The registry is a map from the key's `TypeId` to a value of the key's declared type:

```rust
pub trait Key: 'static {
    type Value: ?Sized + Send + Sync + 'static; // consumers receive Arc<Self::Value>
    const NAME: &'static str;                   // configuration and diagnostics only
}

pub trait Tools: Send + Sync { fn register(&self, name: &str) -> Guard; }
key!(dyn Tools = "tools");   // impl Key for dyn Tools { type Value = dyn Tools; ... }
```

- **The consumer names only the interface.** `cx.deps().tools` is an `Arc<dyn Tools>`; which implementation stands behind it is decided at runtime.
- **The lookup returns the declared type.** The key is the type, so user code contains no casts. The registry downcasts once internally, and that downcast cannot fail because only a provision of `K` writes under `TypeId::of::<K>()`.
- **No collisions, no silent drift.** The `TypeId` of `dyn Tools` includes its defining crate. Unrelated crates cannot collide, and two semver-incompatible versions of one crate are two different keys.
- **A key is not always an interface.** A marker type can name a second slot that uses the same interface:

```rust
pub struct ReplicaDb;
impl Key for ReplicaDb { type Value = dyn Database; const NAME: &'static str = "db.replica"; }
```

`Key::NAME` appears only at the edges, where people write text: configuration and logs. The loader maps names to `TypeId`s through the keys its registered components declare. With isolation, the lookup key becomes `(TypeId, Realm)`.

## Components

A component is a type. Its state value is its accumulator: dropping the value reverts everything the component did.

```rust
pub trait Manifest: Sized + 'static {
    type Config: DeserializeOwned + Send + 'static;
    type Deps: Send + Sync + 'static; // one typed field per key in d
    fn deps() -> Vec<KeyId>;      // d
    fn provides() -> Vec<KeyId>;  // p
}

pub trait Component: Manifest + Send + Sync {
    fn activate(cx: Cx<Self>, cfg: Self::Config) -> impl Future<Output = Result<Self>> + Send;
    fn deactivate(self) -> impl Future<Output = ()> + Send { async {} } // Drop does the rest
}
```

```rust
#[component(config = GrepConfig, deps(tools: dyn Tools, fs: dyn Fs))]
struct GrepTool { _reg: Guard }

// generated:
// struct GrepToolDeps { pub tools: Arc<dyn Tools>, pub fs: Arc<dyn Fs> }

impl Component for GrepTool {
    async fn activate(cx: Cx<Self>, cfg: GrepConfig) -> Result<Self> {
        let GrepToolDeps { tools, fs } = cx.deps();
        Ok(Self { _reg: tools.register(grep_tool(fs.clone(), cfg)) })
    }
}
```

- **One declaration, on the type.** The `#[component]` attribute implements `Manifest` and takes `config`, `deps`, and `provides` in any order, each optional. It reads the type's name and generics from the item it is attached to. The `impl Component` block then holds only behavior, so it stays ordinary, rustfmt-formatted Rust.
- **Dependencies are a typed struct.** `deps(name: Key, ...)` generates `{Type}Deps` with one `Arc<Key::Value>` field per entry and sets it as `Manifest::Deps`. The registry stays type-erased, because configuration decides at runtime which fiber provides each key; generated code downcasts each entry once, before `activate`, and `Cx<Self>` holds the resulting `Arc<Self::Deps>`. After that, reading a dependency is a field access, with no lookup or cast. This is the pattern of Bevy's `SystemParam` and axum's extractors: the compiler checks the declaration and the runtime only fills it in.
- **Undeclared access does not compile.** Only declared keys have a field in `{Type}Deps`, so `cx.deps()` is the one way to read a dependency, and reading anything else is a missing-field error. `#[component]` also rejects a key listed twice.
- **Inactive access cannot be expressed.** The orchestrator constructs `Cx<Self>` only when every dependency has an active provider.
- **Methods reach the context through a stored `Cx`.** `Cx` is a cheap clone, so a component keeps one as a field and calls `self.cx.deps()` or `self.cx.on::<E>()` from ordinary methods. Every clone registers into the fiber's own set, which is reverted when the fiber unloads; after that `cx.is_live()` is `false` and new registrations revert at once. Closing the set breaks the cycle when a listener captures a `Cx` clone.
- **A small proc-macro crate.** `cordis-macros` holds only the attribute. A declarative `component!(Name { ... })` would avoid `syn` but has to be told the type's name, and wrapping the struct inside it would take the struct out of rustfmt's reach.

### Effects

- **Unloading drops the component value.** Every `Guard` field drops with it and reverts its registration.
- **External resources already follow this pattern.** `AbortOnDropHandle`, `Command::kill_on_drop`, `TempDir`, and listener sockets release their resource on drop, which covers most of the system boundary in paper §6.1.
- **Attribution is possession.** An effect belongs to whoever holds its guard.
- **A guard holds a `Weak` reference to its registry**, so it never keeps its provider alive and does nothing if the registry is already gone.
- **LIFO revert.** Dropping an `activate` future drops its locals in reverse declaration order, which realizes rule L-Divert and Theorem 16. `Vec` drops front to back, so `Guards` pops from the back.
- **Async cleanup goes in `deactivate`.** Rust has no stable async `Drop`, so a cancelled activation receives only synchronous cleanup.
- **Registrations made through `Cx` belong to the fiber.** The fiber holds their guards and drops them before `deactivate` runs, or when the activation is cancelled or fails.

## Provision

The component value is the service. `#[component(provides(...))]` publishes `Arc<Self>` as `Arc<K::Value>` for each listed key. Generic unsizing is unstable, so the macro writes one coercion per key, and each coercion fails to compile if the component does not implement the trait. Publishing through `Manifest::publish` is the only way to bind a key, so a component binds no key outside *p* (Def 48).

```rust
#[component(provides(dyn Tools))]
struct ToolRegistry { tools: Arc<Mutex<Vec<String>>> }
impl Tools for ToolRegistry { fn register(&self, name: &str) -> Guard { /* ... */ } }
```

- **Versions.** Trait upcasting lets one provider serve several interface versions: with `trait ToolsV2: Tools`, the provider lists both `dyn ToolsV2` and `dyn Tools`.
- **Cardinality.** A `one` key has one provider at a time, counted from the start of its activation until its teardown has finished; a second provider waits and takes over once the first has fully stopped. A provider that reloads, for a configuration change or a replaced dependency, keeps the key. For a `many` key, adding or removing a provider does not reload consumers, which observe a live set instead. This gives load balancing and rolling updates (paper §6.2).

## Events

Events follow TypeScript Cordis: a built-in bus that every component can reach, listeners registered from any component, and the same five dispatch modes. The difference is that an event is a **type**, not a string:

```rust
pub trait Event: 'static {
    const NAME: &'static str;          // diagnostics and the named edge
    type Mode: Mode;                   // Emit, Bail, Parallel, Serial, or Waterfall
    type Args: Send + Sync + 'static;
    type Output: Send + 'static;
}

event!(pub ToolsPreExecute = "tools/pre-execute", Waterfall(ToolCall) -> ToolResult);
```

A listener registers through the component's `Cx`, and an emitter dispatches through the same bus:

```rust
// listener, in activate
cx.on::<ToolsPreExecute>().listen(|call, next| async move {
    if denied(&call) { return Flow::short_circuit(ToolResult::denied()) }
    next.run(call).await
});

// emitter
let result = cx.events().waterfall::<ToolsPreExecute>(call, |call| run_tool(call)).await;
```

| Mode | Dispatch | Listener | Result |
|---|---|---|---|
| `Emit` | Synchronous, in registration order | `Fn(&Args)` | none |
| `Bail` | Synchronous, in order until one returns `Some` | `Fn(&Args) -> Option<Output>` | first `Some` |
| `Parallel` | Awaited, all listeners concurrently | `Fn(Arc<Args>) -> impl Future<Output = ()>` | none |
| `Serial` | Awaited, in order until one returns `Some` | `Fn(Arc<Args>) -> impl Future<Output = Option<Output>>` | first `Some` |
| `Waterfall` | Awaited middleware around a default action | `Fn(Args, Next) -> impl Future<Output = Flow<Output>>` | the chain's output |

- **Typed, like TypeScript's declaration merging.** A listener with the wrong payload, a dispatch with the wrong mode, and a listener with the wrong mode's signature do not compile, so the `@mode` tag and its catalog gate are unnecessary. Event types from different crates cannot collide, and dispatch involves no serialization or downcast.
- **Each event has exactly one dispatch mode**, because the mode fixes the listener signature the event's listeners are checked against. The paper defines no dispatch modes, and TypeScript Cordis lets any event be dispatched by any method; the one-mode rule is the DeepSeek Harness convention (`@mode` plus its catalog gate), expressed as a type. When the same payload needs a second dispatch style, declare a second event type:

  ```rust
  event!(pub ToolsExecute = "tools/execute", Waterfall(ToolCall) -> ToolResult);
  event!(pub ToolsExecuted = "tools/executed", Emit(ToolCall));
  ```

- **`on::<E>()` returns a registrar** whose `listen` signature depends on the event's mode. Putting the signature on a method with a direct `Fn` bound lets the compiler infer closure argument types.
- **Registrations are effects.** `cx.on` hands its guard to the fiber, so listeners are removed when the fiber unloads, and when its activation is cancelled or fails. Host code outside a component uses `app.events().on::<E>().listen(...)`, which returns the `Guard`.
- **Payloads move instead of being shared mutably.** A waterfall passes `Args` by value down the chain; each listener may change it before calling `next.run(args)`. Parallel and serial listeners share an `Arc<Args>`.
- **A waterfall listener returns `Flow<Output>`**, which only `next.run()` or an explicit `Flow::short_circuit` can produce, so an accidental short circuit does not compile. `Flow::map` post-processes the downstream result.
- **TODO: listener order.** Listeners run in registration order, and registration happens during activation, so components that activate concurrently register in an unspecified order, and a reloaded component moves to the end. TypeScript Cordis has the same behaviour and offers `prepend`; the harness uses it where position matters. Planned: an explicit priority, with ties broken by the fiber's row position, so the order is deterministic and survives reloads.
- **Dispatch uses a snapshot** of the listener list. A registration or removal during dispatch applies to the next dispatch.
- **Emitters and listeners stay loosely coupled**, as in TypeScript. A listener that should run only while a particular service is active declares that service in `deps`, and then unloads with it.
- **The named edge (planned).** `Event::NAME` plus serde payloads will let WASM components, out-of-process plugins, and wildcard observers use events by name, with payloads validated where they enter. Events whose payloads hold live objects, such as an `Agent`, stay in-process.

## Isolation and interception

Both adjust how keys resolve for part of the fiber tree without touching the shared table, so both are **derived contexts** (Def 23): `cx.isolate` and `cx.intercept` return a `Scope`, a new context that leaves the `Cx` it came from as it was. Dropping a scope has nothing to revert. Fibers instantiated through it, with `scope.plugin::<C>(config)`, run under it, and their own `Cx`, and so their children, inherit it. `App` offers the same three methods for root fibers.

```rust
// A reviewer runs its own tool registry; the agent inside sees it, nothing outside does.
let sub = cx.isolate::<dyn Tools>();
sub.plugin::<ToolRegistry>(()).await?;
sub.plugin::<AgentLoop>(json!({ "role": "reviewer" })).await?;

// A sandboxed agent receives a read-only file system without being changed.
cx.intercept::<dyn Fs>(|fs| Arc::new(ReadOnly(fs)))
    .plugin::<AgentLoop>(config)
    .await?;
```

### Realms

- **The table is keyed by key and realm.** A context carries the paper's realm table ρ: the `Realm` of each isolated key, every other key resolving in the shared realm. A fiber depends on and provides each key `k` at `(k, ρ(k))`. This is §4.4's reading of isolation as the same calculus over pairs: one provider per key *per realm*, and a consumer sees only the provider in its own realm.
- **Realms are fixed when a fiber is inserted**, again following §4.4: a fiber whose realms change is a different fiber. The loader rebuilds a row whose `isolate` changes.
- **Three kinds of realm.** `isolate::<K>()` makes a fresh one, private to the scope's subtree. `Realm::named("tenant-a")` is shared by every scope that names it, from anywhere in the tree. `Realm::shared()` undoes an inherited isolation, which matches Def 25's "a key already isolated is reassigned rather than refused".
- **A realm holds several keys, never one binding for two.** In TypeScript Cordis two keys isolated to the same symbol share a store slot. Here a binding is typed by its key, so `(dyn Tools, r)` and `(dyn Fs, r)` are separate slots; a named realm works as a tenant namespace instead.
- **Configuration** names realms per key: `isolate: { tools: sandbox }` places the row's `dyn Tools` in the realm `sandbox`, shared with every row naming it. The loader checks providers within each realm. TypeScript's local realms (`isolate: { tools: true }`) are private to an entry and its group; this loader has no groups yet, so it accepts names only.

### Interception

- **A closure that remaps the value.** `cx.intercept::<K>(remap)` takes `Fn(Arc<K::Value>) -> Arc<K::Value>`. A consumer instantiated under the scope receives `remap(provider)` in its `cx.deps()` instead of the provider. Usually the closure wraps the provider in a type that implements the same trait and forwards to it (`|fs| Arc::new(ReadOnly(fs))`). Whoever intercepts writes that wrapper; the provider's author needs to know nothing about interception, and any key can be intercepted. If a trait already has a method that returns a view, the closure can call it instead. The cost of a wrapper is forwarding every method of the trait.
- **The shared provider is untouched.** Consumers hold the provider through `Arc`, which only hands out `&self`, so the closure cannot change it in place. It returns a new value, usually a wrapper that delegates to the provider, and only consumers under the scope see that value. Everyone else, including `scope.get::<K>()`, still sees the provider itself.
- **Nested intercepts apply outermost first.** `scope.intercept::<K>(a).intercept::<K>(b)` hands consumers `b(a(provider))`, and a fiber's children inherit its intercepts, so an enclosing component's constraint is applied before the one a nested component adds.
- **Fixed per activation.** The closure runs once each time a consumer activates, and its result is stored in that consumer's `Deps`. TypeScript Cordis reads interception metadata at every call through its context proxy, so changing it needs no restart; here a consumer must restart to see a different remap. Interception does not affect the orchestrator's rules: it changes what a consumer receives, not which fiber provides the key (§4.4).
- **Why a closure and not metadata.** The paper binds each key to a function from metadata to value (Def 26), and TypeScript passes metadata that each service reads. In Rust the remap itself is the natural unit: it is typed by the key, needs no merge rules or per-key metadata type, and leaves the provider in charge of what views it offers through its trait.

## Configuration

YAML composes the application from the components linked into the binary:

```yaml
plugins:
  - id: prompt
    use: system-prompt            # registered component name
  - id: tools
    use: tool-registry
  - id: grep
    use: grep-tool
    config: { max_matches: 200 }
```

- **`use` names a registered component.** Native components are registered by name; WASM and out-of-process components name a file or endpoint.
- **Configuration is validated at load** by deserializing into the component's `Config` type.
- **The loader checks the whole tree before activating anything.** Every row's dependencies and provisions are static metadata, so unknown components, invalid configuration, missing providers, and dependency cycles are load errors.
- **Reapplying a tree reconciles it.** Removed rows retire their fibers, changed configuration reloads them, and new rows insert fibers.
- **Profiles and overlays** remain layers of plain data merged by row id before the loader sees the tree.
- **Optional static composition.** A `compose!` macro can fix a whole application in Rust code, making missing providers and cycles compile errors. A build script can apply the same check to shipped profiles while users still layer patches at runtime.

## Reactive lifecycle

The orchestrator is one task that owns every fiber. It decides *when* a component runs; the component's types already fixed *what* it receives.

### Fiber states

```rust
enum State {
    Inactive,
    Loading { token: u64, cancel: CancellationToken }, // activate() is running
    Active { inst: Box<dyn Instance> },                 // its provisions are in the table
    Leaving { inst: Box<dyn Instance> },                // out of the table; waiting until no longer relied upon
    Deactivating,                                       // deactivate() is running
    Failed { view: View, error: String },
}
```

Each fiber also records its **committed view**: the provider fiber of each dependency key that it activated against. Its **target view** is computed from the table: the current provider of each dependency key, or none if a key has no active provider. Views compare providers by fiber identity, so a replaced provider changes the view even when the new value looks the same.

### Rules

After every command or task completion, the orchestrator settles each affected fiber until nothing changes:

| Fiber is | Condition | Action | Paper rule |
|---|---|---|---|
| `Inactive` | target view exists | spawn `activate` | L-Begin |
| `Loading` | target differs from committed | cancel the task; dropping the future reverts its partial effects | L-Divert |
| `Loading` completes | target still matches | publish provisions to the table, become `Active`, mark dependents | L-Finish |
| `Active` | target differs from committed | remove provisions from the table, become `Leaving`, mark dependents | L-Leave |
| `Leaving` | no committed view names this fiber | run `deactivate`, then release its own providers | L-Unload |

- **Only active fibers are in the table.** A provider value does not exist until `activate` returns it, so dependents can never see a half-built provider.
- **Dependents are found through a reverse index** from each key to the fibers that depend on it.
- **Configuration changes** bump a per-fiber epoch that is part of the committed view, so an update reloads the fiber through the same rules.
- **A panic or error in `activate`** moves only that fiber to `Failed`. It retries when its target view or configuration changes.

### Cascade

`G` depends on `dyn Tools`, provided by `T`; `T` depends on `dyn Prompt`, provided by `P`.

```text
P active   → publish dyn Prompt → mark T
T active   → publish dyn Tools  → mark G
G active

remove P   → P leaves: dyn Prompt out of the table → mark T
T leaves   → dyn Tools out of the table          → mark G
G leaves   → nothing relies on G → G deactivates → release T
T deactivates → release P
P deactivates
```

Activation runs in dependency order, and independent branches activate concurrently. Teardown order is never computed by sorting. It emerges from the "relied upon" guard, which keeps each provider in place until its consumers finish their own teardown (Theorem 70).

### Fiber tree

A component instantiates a child fiber with `cx.plugin::<C>(config)`, from `activate` or from any method of a value that keeps its `Cx`. The child is an effect of the parent's current activation:

- **The orchestrator owns the link.** Each fiber records the activation it was instantiated under (`parent: Option<Owner>`, an id plus the activation token). The parent's `Cx` carries a weak sender to the orchestrator, so a kept `Cx` never keeps the orchestrator alive.
- **Retirement follows the parent's activation.** When that activation ends, whether it is cancelled, fails, or unloads (at the start of `deactivate`), the orchestrator sets the retirement flag on each of its children. A reload is an unload and a fresh activation, so a reloaded parent gets fresh children.
- **The parent does not wait.** As in the paper's O-Retire, retirement is unconditional, and a parent may finish unloading while a child is still unloading (§4.2.1). Children do not count toward the parent's "relied upon" guard.
- **A child is an ordinary fiber otherwise.** It activates when its own dependencies have providers and reloads when they change. `plugin` returns a `Fiber` handle with `status`, `update`, and `dispose`, so a parent can retire a child early, such as an agent that finished its task. Like a TS fiber, the handle is a view: dropping it changes nothing. `App::parent` reports the link.
- **Late requests are refused.** A fiber that is leaving or whose activation has ended gets an error from `plugin`; in the paper's `Unloading` state a fiber performs no more effects. The orchestrator makes the check, so a race cannot leave an orphan.

### Leaks

The orchestrator holds each instance as an `Arc<Self>`. After the guard releases, it calls `Arc::into_inner` to recover the owned value for `deactivate`. `None` means some consumer kept a clone past its own teardown. The orchestrator logs the leak, and `Drop` runs when the last clone is released.

## Execution tiers

| Tier | Code | Reload | Use |
|---|---|---|---|
| Native | Compiled into the binary | Configuration reconciliation | Core services |
| WASM components | `wasmtime` + WIT | Dropping the instance retracts its code | Third-party and generated components |
| Out of process | JSON-RPC broker (paper §6.2) | Process restart | Components in other languages |

A WIT world is the paper's component triple: imports are *d*, exports are *p*, and the instance is *e*. A WASM component can reach only its imports, which supplies the sandboxing of paper §6.3.

```wit
package dsh:tool-grep@0.1.0;
world plugin {
  import dsh:fs/read@0.1.0;          // d
  export dsh:tools/tool@0.1.0;       // p
}
```

## Agent harness on top

The harness is a set of crates, each a service trait plus components that provide it.

| Crate | Depends on | Provides |
|---|---|---|
| `dsh-session` | — | `dyn Sessions` |
| `dsh-llm` | `many dyn LlmAdapter` | `dyn Llm` |
| `dsh-tools` | `dyn SystemPrompt` | `dyn Tools` |
| `dsh-agent-loop` | `dyn Llm`, `dyn Tools`, `dyn Sessions` | `dyn Agents` |

- **Interception points** (`agent/pre-step`, `agent/request`, `tools/*`, `agent/turn-stopping`, `session/event`) are event types declared by the crate that dispatches them.
- **Presets and subagents** are realms that override `dyn Tools` or `dyn Fs` for one agent.
- **Self-modification** compiles a generated component to `wasm32-wasip2` and mounts it as a fiber.
- **Model-visible means logged.** Only the session crate can construct `Logged<T>`, and the request builder accepts only `Logged` values.
- **Session events** are serde types registered with a decoder table. A log with an unknown event type that is not marked ignorable is refused. Format migrations are an `impl From<V1> for V2` chain.
- **Tools** derive their JSON Schema from the argument type (`schemars`) and their description from the doc comment.

## Trade-offs

| Gains | Costs |
|---|---|
| Effect tracking is ownership | No async `Drop`: cancelled activations get synchronous cleanup only |
| Undeclared access and invalid provisions are compile errors | Macro-heavy; an undeclared read surfaces as a missing field on the generated `{Type}Deps` |
| Event payloads and modes are checked at compile time | Async listeners take owned or `Arc` arguments, not borrowed ones |
| Lifecycle transitions are serialized by one owner | No native code replacement; new code arrives through WASM or another process |
| Versioned, collision-free keys | Width compatibility is limited to what supertraits declare |
