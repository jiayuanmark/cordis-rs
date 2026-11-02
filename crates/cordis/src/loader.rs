//! Configuration trees: validate them, then reconcile them against a running [`App`].

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;

use crate::erased::Factory;
use crate::realm::{Env, Slot};
use crate::{App, Component, FiberId, KeyId, Realm};

/// Component types available to configuration, by name.
#[derive(Clone, Default)]
pub struct Catalog {
    entries: HashMap<String, Arc<Factory>>,
}

impl Catalog {
    /// Returns an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `C` under `name`, the value rows use in their `use` field.
    ///
    /// # Returns
    ///
    /// The catalog, for chaining.
    pub fn register<C: Component>(&mut self, name: &str) -> &mut Self {
        self.entries.insert(name.to_owned(), Arc::new(Factory::of::<C>()));
        self
    }

    /// Returns the key whose configuration name is `name`, among the keys registered
    /// components depend on or provide.
    fn key(&self, name: &str) -> Option<KeyId> {
        self.entries
            .values()
            .flat_map(|factory| factory.deps.iter().chain(&factory.optional).chain(&factory.provides))
            .copied()
            .find(|key| key.name() == name)
    }
}

/// A configuration tree.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tree {
    /// The rows, each mounting one fiber unless disabled.
    pub plugins: Vec<Row>,
}

/// One configuration row.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Row {
    /// Stable row id; reconciliation matches rows across trees by id.
    pub id: String,
    /// Catalog name of the component to mount.
    #[serde(rename = "use")]
    pub component: String,
    /// Component configuration, deserialized into the component's `Config`. Absent is `null`.
    #[serde(default)]
    pub config: Value,
    /// Keeps the row in the tree without mounting it.
    #[serde(default)]
    pub disabled: bool,
    /// Realms by key name: the row depends on and provides each listed key in the named realm,
    /// which every row naming it shares. Other keys resolve in the shared realm.
    #[serde(default)]
    pub isolate: BTreeMap<String, String>,
}

impl Tree {
    /// Merges layers, lowest first, into one tree.
    ///
    /// A layer row whose id already exists overrides only the fields it sets: `use`, `config`
    /// and `isolate` (each replaced whole, never deep-merged), and `disabled`. A row with a new id is appended and
    /// must set `use`. Switching a row's `use` usually needs a new `config` as well; the merged
    /// tree is validated when it is applied.
    ///
    /// # Errors
    ///
    /// Returns [`LoadError::DuplicateId`] when one layer repeats an id, and
    /// [`LoadError::NewRowWithoutUse`] when a layer adds a row without naming its component.
    pub fn layered<'a>(layers: impl IntoIterator<Item = &'a Layer>) -> Result<Tree, LoadError> {
        let mut rows: Vec<Row> = Vec::new();
        for layer in layers {
            let mut ids = HashSet::new();
            for patch in &layer.rows {
                if !ids.insert(patch.id.as_str()) {
                    return Err(LoadError::DuplicateId(patch.id.clone()));
                }
                match rows.iter_mut().find(|row| row.id == patch.id) {
                    Some(row) => {
                        if let Some(component) = &patch.component {
                            row.component = component.clone();
                        }
                        if let Some(config) = &patch.config {
                            row.config = config.clone();
                        }
                        if let Some(disabled) = patch.disabled {
                            row.disabled = disabled;
                        }
                        if let Some(isolate) = &patch.isolate {
                            row.isolate = isolate.clone();
                        }
                    }
                    None => {
                        let component = patch.component.clone().ok_or_else(|| LoadError::NewRowWithoutUse {
                            layer: layer.name.clone(),
                            row: patch.id.clone(),
                        })?;
                        rows.push(Row {
                            id: patch.id.clone(),
                            component,
                            config: patch.config.clone().unwrap_or(Value::Null),
                            disabled: patch.disabled.unwrap_or(false),
                            isolate: patch.isolate.clone().unwrap_or_default(),
                        });
                    }
                }
            }
        }
        Ok(Tree { plugins: rows })
    }
}

/// One configuration layer: rows that override or extend the layers below it.
#[derive(Clone, Debug)]
pub struct Layer {
    name: String,
    rows: Vec<LayerRow>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LayerFile {
    plugins: Vec<LayerRow>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LayerRow {
    id: String,
    #[serde(rename = "use")]
    component: Option<String>,
    /// `None` when absent; an explicit `null` replaces the config with `null`.
    #[serde(default, deserialize_with = "present")]
    config: Option<Value>,
    disabled: Option<bool>,
    isolate: Option<BTreeMap<String, String>>,
}

fn present<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

impl Layer {
    /// Parses a layer from YAML text with the same `plugins` list as a [`Tree`], where every
    /// field except `id` is optional.
    ///
    /// # Errors
    ///
    /// Returns [`LoadError::Layer`], naming the layer, when the text is not a valid layer.
    pub fn from_yaml(name: &str, text: &str) -> Result<Self, LoadError> {
        let file: LayerFile =
            serde_yaml::from_str(text).map_err(|source| LoadError::Layer { layer: name.to_owned(), source })?;
        Ok(Self { name: name.to_owned(), rows: file.plugins })
    }

    /// Returns the layer's name, used in error messages.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A configuration tree that cannot be applied. Nothing in the running app changes.
///
/// A tree whose rows only lack providers is not an error; see [`LoadWarning`].
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// The YAML text does not describe a tree.
    #[error("invalid configuration: {0}")]
    Parse(#[from] serde_yaml::Error),
    /// A layer's YAML text is not a valid layer.
    #[error("invalid layer `{layer}`: {source}")]
    Layer {
        layer: String,
        #[source]
        source: serde_yaml::Error,
    },
    /// A layer adds a row without naming its component.
    #[error("layer `{layer}` adds row `{row}` without `use`")]
    NewRowWithoutUse { layer: String, row: String },
    /// Two rows share an id.
    #[error("row id `{0}` appears more than once")]
    DuplicateId(String),
    /// A row names a component the catalog does not contain.
    #[error("row `{row}` uses unknown component `{component}`")]
    UnknownComponent { row: String, component: String },
    /// A row's configuration does not deserialize into its component's `Config`.
    #[error("row `{row}` has invalid config: {message}")]
    InvalidConfig { row: String, message: String },
    /// A row isolates a key that no registered component depends on or provides.
    #[error("row `{row}` isolates unknown key `{key}`")]
    UnknownKey { row: String, key: String },
}

/// Something an applied tree leaves its rows waiting on, unless a provider outside the tree fills
/// the gap. The tree is applied anyway: the rows' fibers wait, as any fiber waits for a provider
/// that isn't there yet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadWarning {
    /// A row depends on a key that no enabled row provides in the realm the row resolves it in.
    /// The check reads the tree only: the row still activates if something else provides the
    /// key there, such as a fiber the host inserts or a child fiber of another row.
    MissingProvider {
        row: String,
        key: &'static str,
        /// The realm the row's `isolate` names for the key, or `None` for the shared realm.
        realm: Option<String>,
    },
    /// Enabled rows depend on each other in a cycle, so none of them activates unless
    /// something outside the cycle provides one of its keys.
    Cycle(Vec<String>),
}

impl fmt::Display for LoadWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadWarning::MissingProvider { row, key, realm } => {
                write!(f, "row `{row}` depends on `{key}`")?;
                if let Some(realm) = realm {
                    write!(f, " in realm `{realm}`")?;
                }
                write!(f, ", which no enabled row provides there; it waits unless something outside the tree does")
            }
            LoadWarning::Cycle(rows) => write!(f, "dependency cycle: {}", rows.join(" -> ")),
        }
    }
}

/// Applies configuration trees to an [`App`].
///
/// Each application validates the whole tree first. It then retires fibers whose rows are
/// gone, disabled, switched to another component, or moved to other realms; updates fibers
/// whose configuration differs from their row; and inserts fibers for new rows. A row's realms
/// are fixed when its fiber is inserted, so moving a row rebuilds its fiber, and its dependents
/// follow.
///
/// The loader compares rows against what the app is running, not against the trees it applied
/// before: applying a tree again mounts a new fiber for a row whose fiber the host removed, and
/// restores a row's configuration after the host updated its fiber.
pub struct Loader {
    app: App,
    catalog: Catalog,
    mounted: HashMap<String, Mounted>,
}

/// An enabled row with its component and the context its fiber is inserted under.
type Checked<'t> = (&'t Row, Arc<Factory>, Env);

struct Mounted {
    component: String,
    isolate: BTreeMap<String, String>,
    fiber: FiberId,
}

impl Loader {
    /// Returns a loader with nothing mounted.
    pub fn new(app: App, catalog: Catalog) -> Self {
        Self { app, catalog, mounted: HashMap::new() }
    }

    /// Parses YAML text into a [`Tree`] and applies it.
    ///
    /// # Returns
    ///
    /// The tree's warnings, as [`apply`](Loader::apply) returns them.
    ///
    /// # Errors
    ///
    /// Returns a [`LoadError`] when the text or the tree is invalid; nothing is applied then.
    pub async fn apply_yaml(&mut self, text: &str) -> Result<Vec<LoadWarning>, LoadError> {
        let tree: Tree = serde_yaml::from_str(text)?;
        self.apply(&tree).await
    }

    /// Validates `tree`, then reconciles the app against it.
    ///
    /// # Returns
    ///
    /// What the tree alone leaves its rows waiting on: keys no enabled row provides, and
    /// dependency cycles among the rows (every cycle shares a row with a reported one). Each is
    /// also logged as a warning. The rows are mounted anyway, and a provider from outside the
    /// tree, such as a fiber the host inserts, still satisfies them.
    ///
    /// # Errors
    ///
    /// Returns a [`LoadError`] when the tree is invalid; nothing is applied then.
    pub async fn apply(&mut self, tree: &Tree) -> Result<Vec<LoadWarning>, LoadError> {
        let (rows, warnings) = self.check(tree)?;
        for warning in &warnings {
            tracing::warn!("{warning}");
        }
        let app = &self.app;
        self.mounted.retain(|id, mounted| {
            let kept = rows
                .iter()
                .any(|(row, ..)| row.id == *id && row.component == mounted.component && row.isolate == mounted.isolate);
            if !kept {
                app.remove(mounted.fiber);
            }
            kept
        });
        for (row, factory, env) in rows {
            let mounted = self.mounted.get(&row.id).map(|mounted| mounted.fiber);
            let running = match mounted {
                Some(fiber) => self.app.config(fiber).await.map(|config| (fiber, config)),
                None => None,
            };
            match running {
                Some((_, config)) if config == row.config => {}
                Some((fiber, _)) => self.app.update(fiber, row.config.clone()),
                None => {
                    let fiber = self.app.insert_factory(factory, row.config.clone(), env).await;
                    let mounted = Mounted { component: row.component.clone(), isolate: row.isolate.clone(), fiber };
                    self.mounted.insert(row.id.clone(), mounted);
                }
            }
        }
        Ok(warnings)
    }

    /// Returns the fiber the loader last mounted for a row id. The host may have removed it
    /// since; the next application then mounts a new one.
    pub fn fiber(&self, row: &str) -> Option<FiberId> {
        self.mounted.get(row).map(|mounted| mounted.fiber)
    }

    /// Validates `tree`, returning its enabled rows and what keeps some of them from activating.
    fn check<'t>(&self, tree: &'t Tree) -> Result<(Vec<Checked<'t>>, Vec<LoadWarning>), LoadError> {
        let mut ids = HashSet::new();
        let mut rows = Vec::new();
        for row in &tree.plugins {
            if !ids.insert(row.id.as_str()) {
                return Err(LoadError::DuplicateId(row.id.clone()));
            }
            if row.disabled {
                continue;
            }
            let factory = self
                .catalog
                .entries
                .get(&row.component)
                .ok_or_else(|| LoadError::UnknownComponent { row: row.id.clone(), component: row.component.clone() })?;
            (factory.validate)(&row.config)
                .map_err(|message| LoadError::InvalidConfig { row: row.id.clone(), message })?;
            let mut env = Env::default();
            for (key, realm) in &row.isolate {
                let key = self
                    .catalog
                    .key(key)
                    .ok_or_else(|| LoadError::UnknownKey { row: row.id.clone(), key: key.clone() })?;
                env.isolate(key, Realm::named(realm));
            }
            rows.push((row, factory.clone(), env));
        }

        let mut providers: HashMap<Slot, Vec<usize>> = HashMap::new();
        for (index, (_, factory, env)) in rows.iter().enumerate() {
            for key in &factory.provides {
                providers.entry(env.slot(*key)).or_default().push(index);
            }
        }
        let mut warnings = Vec::new();
        let mut edges = Vec::with_capacity(rows.len());
        for (row, factory, env) in &rows {
            let mut targets = Vec::new();
            for key in &factory.deps {
                match providers.get(&env.slot(*key)) {
                    Some(provided_by) => targets.extend(provided_by),
                    None => warnings.push(LoadWarning::MissingProvider {
                        row: row.id.clone(),
                        key: key.name(),
                        realm: row.isolate.get(key.name()).cloned(),
                    }),
                }
            }
            edges.push(targets);
        }
        for cycle in find_cycles(&edges) {
            warnings.push(LoadWarning::Cycle(cycle.into_iter().map(|index| rows[index].0.id.clone()).collect()));
        }
        Ok((rows, warnings))
    }
}

/// Returns dependency cycles, each with its first row repeated at the end. Every cycle shares a
/// row with a returned one: each search skips the rows of the cycles already found.
fn find_cycles(edges: &[Vec<usize>]) -> Vec<Vec<usize>> {
    let mut found = vec![false; edges.len()];
    let mut cycles = Vec::new();
    while let Some(cycle) = find_cycle(edges, &found) {
        for &row in &cycle {
            found[row] = true;
        }
        cycles.push(cycle);
    }
    cycles
}

/// Returns the rows of one dependency cycle among the rows not `skipped`, first row repeated at
/// the end, if any exists.
fn find_cycle(edges: &[Vec<usize>], skipped: &[bool]) -> Option<Vec<usize>> {
    #[derive(Clone, Copy, PartialEq)]
    enum Mark {
        New,
        OnStack,
        Done,
    }

    fn visit(node: usize, edges: &[Vec<usize>], marks: &mut [Mark], stack: &mut Vec<usize>) -> Option<Vec<usize>> {
        marks[node] = Mark::OnStack;
        stack.push(node);
        for &next in &edges[node] {
            match marks[next] {
                Mark::OnStack => {
                    let start = stack.iter().position(|&entry| entry == next).expect("on the stack");
                    let mut cycle = stack[start..].to_vec();
                    cycle.push(next);
                    return Some(cycle);
                }
                Mark::New => {
                    if let Some(cycle) = visit(next, edges, marks, stack) {
                        return Some(cycle);
                    }
                }
                Mark::Done => {}
            }
        }
        stack.pop();
        marks[node] = Mark::Done;
        None
    }

    let mut marks: Vec<Mark> = skipped.iter().map(|&skip| if skip { Mark::Done } else { Mark::New }).collect();
    let mut stack = Vec::new();
    (0..edges.len()).find_map(
        |node| {
            if marks[node] == Mark::New { visit(node, edges, &mut marks, &mut stack) } else { None }
        },
    )
}
