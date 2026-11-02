//! The paper's guarantees, checked under random changes.
//!
//! Each seed inserts, updates and removes fibers at random, with activations and undos that
//! yield a random number of times, and without waiting for one change to settle before the
//! next. The components report what they do to a [`World`], which checks, as it happens:
//!
//! 1. A key has at most one provider at a time, counted from the start of its activation until
//!    its last undo has finished (Def 50; one provider per realm, and these keys stay in the
//!    shared realm).
//! 2. A provider starts undoing only after every run that read it has finished undoing
//!    (Theorem 70).
//! 3. A run's effects are undone before its last undo finishes (Corollary 69).
//!
//! At checkpoints the test waits for the app to settle (Theorem 73) and checks that every fiber
//! is in the state its providers allow: active when it can run, failed only when it fails
//! against them, inactive only when a provider is missing or another fiber holds its key. An
//! active fiber's run must have read exactly the active providers of its keys, including an
//! optional key's provider when there is one.
//!
//! The changes come from a seeded generator, so re-running a failing seed replays the same
//! changes. The runtime may interleave task completions with them differently from run to run,
//! so a failure can take a few runs to show again.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use cordis::{App, Component, Cx, FiberId, Guard, Key, Result, Status, component};
use serde::{Deserialize, Serialize};

/// What each run reports about itself, and what providers publish.
pub trait Node: Send + Sync {
    fn run(&self) -> u64;
    fn poisoned(&self) -> bool;
}

macro_rules! keys {
    ($($key:ident = $name:literal),*) => {$(
        struct $key;
        impl Key for $key {
            type Value = dyn Node;
            const NAME: &'static str = $name;
        }
    )*};
}
keys!(A = "a", B = "b", C = "c");

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    world: u64,
    /// The fiber's name in the test, kept across updates.
    name: u32,
    /// Turns `activate` yields before and after its effect.
    delay: u8,
    /// Turns the run's undo yields.
    undo_delay: u8,
    /// Whether `activate` returns early once its run is cancelled.
    cancellable: bool,
    fail: bool,
    /// Makes every run that reads this provider fail.
    poison: bool,
}

struct Life {
    run: u64,
    poison: bool,
}

macro_rules! components {
    ($($component:ident: deps($($dep:ident: $key:ident),*), provides($($provided:ident),*);)*) => {$(
        #[component(config = Config, deps($($dep: $key),*), provides($($provided),*))]
        struct $component(Life);

        impl Node for $component {
            fn run(&self) -> u64 {
                self.0.run
            }
            fn poisoned(&self) -> bool {
                self.0.poison
            }
        }

        impl Component for $component {
            async fn activate(cx: Cx<Self>, config: Config) -> Result<Self> {
                let reads: Vec<Arc<dyn Node>> = vec![$(cx.deps().$dep.clone()),*];
                live(&cx, config, &[$($provided::NAME),*], reads).await.map(Self)
            }
        }
    )*};
}

components! {
    Source: deps(), provides(A);
    Middle: deps(a: A), provides(B);
    Top: deps(a: A, b: B), provides(C);
}

// `Leaf` uses `C` when it has a provider, so it reloads when one appears or leaves.
#[component(config = Config, deps(b: B, c?: C))]
struct Leaf;

impl Component for Leaf {
    async fn activate(cx: Cx<Self>, config: Config) -> Result<Self> {
        let deps = cx.deps();
        let reads: Vec<Arc<dyn Node>> = std::iter::once(deps.b.clone()).chain(deps.c.clone()).collect();
        live(&cx, config, &[], reads).await.map(|_| Leaf)
    }
}

#[derive(Clone, Copy, Debug)]
enum Kind {
    Source,
    Middle,
    Top,
    Leaf,
}

impl Kind {
    const ALL: [Kind; 4] = [Kind::Source, Kind::Middle, Kind::Top, Kind::Leaf];

    fn deps(self) -> &'static [&'static str] {
        match self {
            Kind::Source => &[],
            Kind::Middle => &["a"],
            Kind::Top => &["a", "b"],
            Kind::Leaf => &["b"],
        }
    }

    fn optional(self) -> &'static [&'static str] {
        match self {
            Kind::Leaf => &["c"],
            _ => &[],
        }
    }

    fn provides(self) -> &'static [&'static str] {
        match self {
            Kind::Source => &["a"],
            Kind::Middle => &["b"],
            Kind::Top => &["c"],
            Kind::Leaf => &[],
        }
    }

    async fn insert(self, app: &App, config: &Config) -> FiberId {
        let config = serde_json::to_value(config).unwrap();
        match self {
            Kind::Source => app.insert::<Source>(config).await,
            Kind::Middle => app.insert::<Middle>(config).await,
            Kind::Top => app.insert::<Top>(config).await,
            Kind::Leaf => app.insert::<Leaf>(config).await,
        }
    }
}

/// The body of every component's `activate`: a run that holds an effect and reports its steps.
async fn live<C: Component>(
    cx: &Cx<C>,
    config: Config,
    provides: &'static [&'static str],
    reads: Vec<Arc<dyn Node>>,
) -> Result<Life> {
    let world = config.world;
    let run = with(world, |w| w.begin(config.name, provides, reads.iter().map(|node| node.run()).collect()));
    let undo_delay = config.undo_delay;
    // Added first, so it runs last: when it finishes, the run has let go of everything.
    cx.on_dispose(move || async move {
        for _ in 0..undo_delay {
            tokio::task::yield_now().await;
        }
        with(world, |w| w.end(run));
    });
    pause(cx, config.delay, config.cancellable).await?;
    // An effect whose undo arrives only after an `.await`, like a guard returned by
    // `registry.add(name).await`.
    with(world, |w| w.effects.insert(run));
    tokio::task::yield_now().await;
    cx.on_dispose(Guard::new(move || {
        with(world, |w| w.effects.remove(&run));
    }));
    pause(cx, config.delay, config.cancellable).await?;
    if config.fail || reads.iter().any(|node| node.poisoned()) {
        return Err("failed".into());
    }
    // Added last, so it runs first: the run starts letting go.
    cx.on_dispose(move || async move { with(world, |w| w.undoing(run)) });
    Ok(Life { run, poison: config.poison })
}

async fn pause<C: Component>(cx: &Cx<C>, turns: u8, cancellable: bool) -> Result<()> {
    for _ in 0..turns {
        if cancellable {
            tokio::select! {
                biased;
                () = cx.cancelled() => return Err("cancelled".into()),
                () = tokio::task::yield_now() => {}
            }
        } else {
            tokio::task::yield_now().await;
        }
    }
    Ok(())
}

// The world: what the runs of one seed report, and the checks made as they report it.

static WORLDS: Mutex<BTreeMap<u64, World>> = Mutex::new(BTreeMap::new());

fn with<T>(world: u64, f: impl FnOnce(&mut World) -> T) -> T {
    let mut worlds = WORLDS.lock().unwrap_or_else(PoisonError::into_inner);
    f(worlds.get_mut(&world).expect("the seed's world exists"))
}

#[derive(Default)]
struct World {
    next_run: u64,
    /// Runs whose `activate` has started and whose last undo has not finished.
    runs: BTreeMap<u64, Run>,
    /// Runs whose last undo has finished, with their fiber.
    ended: BTreeMap<u64, u32>,
    /// The runs holding each key.
    holders: BTreeMap<&'static str, Vec<u64>>,
    /// The runs whose effect is in place.
    effects: BTreeSet<u64>,
    violations: Vec<String>,
}

struct Run {
    fiber: u32,
    provides: &'static [&'static str],
    /// The provider runs it read through `cx.deps()`.
    reads: Vec<u64>,
}

impl World {
    fn begin(&mut self, fiber: u32, provides: &'static [&'static str], reads: Vec<u64>) -> u64 {
        let run = self.next_run;
        self.next_run += 1;
        for key in provides {
            let holders = self.holders.entry(key).or_default();
            for other in holders.iter() {
                let other = self.runs[other].fiber;
                self.violations
                    .push(format!("fiber {fiber} started providing `{key}` while fiber {other} still held it"));
            }
            holders.push(run);
        }
        self.runs.insert(run, Run { fiber, provides, reads });
        run
    }

    fn undoing(&mut self, run: u64) {
        match self.ended.get(&run) {
            Some(fiber) => self.violations.push(format!("fiber {fiber} ran its last undo before its first")),
            None => self.no_readers(run, "started undoing"),
        }
    }

    fn end(&mut self, run: u64) {
        self.no_readers(run, "finished undoing");
        let Run { fiber, provides, .. } = self.runs.remove(&run).expect("a run ends once");
        self.ended.insert(run, fiber);
        if self.effects.contains(&run) {
            self.violations.push(format!("fiber {fiber} finished undoing with its effect in place"));
        }
        for key in provides {
            self.holders.get_mut(key).unwrap().retain(|holder| *holder != run);
        }
    }

    fn no_readers(&mut self, provider: u64, when: &str) {
        let fiber = self.runs[&provider].fiber;
        for reader in self.runs.values().filter(|reader| reader.reads.contains(&provider)) {
            let message =
                format!("provider fiber {fiber} {when} while fiber {}, which read it, still ran", reader.fiber);
            self.violations.push(message);
        }
    }
}

// The test: random changes, and checkpoints.

/// A small seeded generator (SplitMix64), so the test needs no extra dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    fn config(&mut self, world: u64, name: u32) -> Config {
        Config {
            world,
            name,
            delay: self.below(4) as u8,
            undo_delay: self.below(4) as u8,
            cancellable: self.chance(50),
            fail: self.chance(10),
            poison: self.chance(15),
        }
    }
}

struct Planted {
    id: FiberId,
    kind: Kind,
    config: Config,
}

const SEEDS: u64 = 300;
const STEPS: usize = 40;
const CHECK_EVERY: usize = 8;

#[tokio::test]
async fn the_guarantees_hold_under_random_changes() {
    for seed in 0..SEEDS {
        if let Err(failure) = run_seed(seed).await {
            panic!("seed {seed}: {failure}");
        }
    }
}

async fn run_seed(seed: u64) -> std::result::Result<(), String> {
    static NEXT_WORLD: AtomicU64 = AtomicU64::new(0);
    let world = NEXT_WORLD.fetch_add(1, Ordering::Relaxed);
    WORLDS.lock().unwrap().insert(world, World::default());
    let mut rng = Rng(seed);
    let app = App::new();
    let mut planted: BTreeMap<u32, Planted> = BTreeMap::new();
    let mut log = Vec::new();
    let mut next_name = 0;

    let result = async {
        for step in 1..=STEPS {
            let names: Vec<u32> = planted.keys().copied().collect();
            match rng.below(10) {
                0..=3 => {
                    let kind = Kind::ALL[rng.below(Kind::ALL.len())];
                    let config = rng.config(world, next_name);
                    log.push(format!("insert {kind:?} as fiber {next_name}: {config:?}"));
                    let id = kind.insert(&app, &config).await;
                    planted.insert(next_name, Planted { id, kind, config });
                    next_name += 1;
                }
                4..=6 if !names.is_empty() => {
                    let name = names[rng.below(names.len())];
                    let config = rng.config(world, name);
                    log.push(format!("update fiber {name}: {config:?}"));
                    let fiber = planted.get_mut(&name).unwrap();
                    app.update(fiber.id, serde_json::to_value(&config).unwrap());
                    fiber.config = config;
                }
                7..=8 if !names.is_empty() => {
                    let name = names[rng.below(names.len())];
                    log.push(format!("remove fiber {name}"));
                    app.remove(planted.remove(&name).unwrap().id);
                }
                _ => {}
            }
            for _ in 0..rng.below(8) {
                tokio::task::yield_now().await;
            }
            violations(world)?;
            if step % CHECK_EVERY == 0 {
                settle(&app).await?;
                violations(world)?;
                check_settled(&app, world, &planted).await?;
            }
        }
        app.shutdown().await;
        violations(world)?;
        let worlds = WORLDS.lock().unwrap();
        let w = &worlds[&world];
        if !w.runs.is_empty() || !w.effects.is_empty() {
            return Err(format!("after shutdown, {} runs and {} effects remain", w.runs.len(), w.effects.len()));
        }
        Ok(())
    }
    .await;
    // A failed seed keeps its world: its fibers may still be running.
    if result.is_ok() {
        WORLDS.lock().unwrap().remove(&world);
    }
    result.map_err(|failure| format!("{failure}\n\nchanges:\n  {}", log.join("\n  ")))
}

fn violations(world: u64) -> std::result::Result<(), String> {
    let violations = with(world, |w| std::mem::take(&mut w.violations));
    if violations.is_empty() { Ok(()) } else { Err(violations.join("\n")) }
}

async fn settle(app: &App) -> std::result::Result<(), String> {
    tokio::time::timeout(Duration::from_secs(10), app.settled()).await.map_err(|_| "the app did not settle".to_owned())
}

/// Checks that every fiber is in the state its providers allow, and that exactly the runs of
/// active fibers are live, each with its effect in place.
async fn check_settled(app: &App, world: u64, planted: &BTreeMap<u32, Planted>) -> std::result::Result<(), String> {
    let mut statuses = BTreeMap::new();
    for (name, fiber) in planted {
        statuses.insert(*name, app.status(fiber.id).await.ok_or(format!("fiber {name} is gone"))?);
    }
    let mut active: BTreeMap<&str, u32> = BTreeMap::new();
    for (name, status) in &statuses {
        if *status == Status::Active {
            for key in planted[name].kind.provides() {
                if let Some(other) = active.insert(key, *name) {
                    return Err(format!("fibers {other} and {name} are both active providers of `{key}`"));
                }
            }
        }
    }
    for (name, fiber) in planted {
        let status = &statuses[name];
        let missing = fiber.kind.deps().iter().any(|key| !active.contains_key(key));
        let held = fiber.kind.provides().iter().any(|key| active.get(key).is_some_and(|holder| holder != name));
        let fails = fiber.config.fail
            || fiber
                .kind
                .deps()
                .iter()
                .chain(fiber.kind.optional())
                .any(|key| active.get(key).is_some_and(|provider| planted[provider].config.poison));
        let allowed = match status {
            _ if missing || held => matches!(status, Status::Inactive | Status::Failed(_)),
            Status::Failed(_) => fails,
            Status::Active => !fails,
            _ => false,
        };
        if !allowed {
            let why = if missing {
                "a provider is missing"
            } else if held {
                "another fiber holds its key"
            } else if fails {
                "it fails against its providers"
            } else {
                "it can run"
            };
            return Err(format!("fiber {name} settled {status:?}, but {why}"));
        }
    }

    let worlds = WORLDS.lock().unwrap();
    let w = &worlds[&world];
    let mut live: BTreeMap<u32, usize> = BTreeMap::new();
    for run in w.runs.values() {
        *live.entry(run.fiber).or_default() += 1;
    }
    for (name, status) in &statuses {
        let expected = usize::from(*status == Status::Active);
        let runs = live.remove(name).unwrap_or(0);
        if runs != expected {
            return Err(format!("fiber {name} settled {status:?} with {runs} live runs"));
        }
    }
    if let Some((name, runs)) = live.into_iter().next() {
        return Err(format!("removed fiber {name} still has {runs} live runs"));
    }
    // Each active fiber's run read the current run of each active provider of its keys, and
    // nothing else.
    let run_of = |name: u32| w.runs.iter().find(|(_, run)| run.fiber == name).map(|(id, _)| *id);
    for (name, fiber) in planted {
        if statuses[name] != Status::Active {
            continue;
        }
        let run = run_of(*name).expect("an active fiber has one live run");
        let mut expected: Vec<u64> = fiber
            .kind
            .deps()
            .iter()
            .chain(fiber.kind.optional())
            .filter_map(|key| active.get(key))
            .map(|provider| run_of(*provider).expect("an active provider has one live run"))
            .collect();
        let mut reads = w.runs[&run].reads.clone();
        expected.sort_unstable();
        reads.sort_unstable();
        if reads != expected {
            return Err(format!("fiber {name} runs against providers {reads:?}, but the active ones are {expected:?}"));
        }
    }
    if w.effects.len() != w.runs.len() || w.runs.keys().any(|run| !w.effects.contains(run)) {
        return Err("the effects in place are not those of the live runs".to_owned());
    }
    Ok(())
}
