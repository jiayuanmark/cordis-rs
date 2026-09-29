//! Configuration trees: validate them, then reconcile them against a running [`App`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value;

use crate::erased::Factory;
use crate::{App, Component, FiberId, KeyId};

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
}

impl Tree {
    /// Merges layers, lowest first, into one tree.
    ///
    /// A layer row whose id already exists overrides only the fields it sets: `use`, `config`
    /// (replaced whole, never deep-merged), and `disabled`. A row with a new id is appended and
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
    /// A row depends on a key that no enabled row provides.
    #[error("row `{row}` depends on `{key}`, which no enabled row provides")]
    MissingProvider { row: String, key: &'static str },
    /// Enabled rows depend on each other in a cycle, so none of them could activate.
    #[error("dependency cycle: {}", .0.join(" -> "))]
    Cycle(Vec<String>),
}

/// Applies configuration trees to an [`App`].
///
/// Each application validates the whole tree first. It then retires fibers whose rows are
/// gone, disabled, or switched to another component; updates fibers whose configuration
/// changed; and inserts fibers for new rows.
pub struct Loader {
    app: App,
    catalog: Catalog,
    mounted: HashMap<String, Mounted>,
}

struct Mounted {
    component: String,
    config: Value,
    fiber: FiberId,
}

impl Loader {
    /// Returns a loader with nothing mounted.
    pub fn new(app: App, catalog: Catalog) -> Self {
        Self { app, catalog, mounted: HashMap::new() }
    }

    /// Parses YAML text into a [`Tree`] and applies it.
    ///
    /// # Errors
    ///
    /// Returns a [`LoadError`] when the text or the tree is invalid; nothing is applied then.
    pub async fn apply_yaml(&mut self, text: &str) -> Result<(), LoadError> {
        let tree: Tree = serde_yaml::from_str(text)?;
        self.apply(&tree).await
    }

    /// Validates `tree`, then reconciles the app against it.
    ///
    /// # Errors
    ///
    /// Returns a [`LoadError`] when the tree is invalid; nothing is applied then.
    pub async fn apply(&mut self, tree: &Tree) -> Result<(), LoadError> {
        let rows = self.check(tree)?;
        let app = &self.app;
        self.mounted.retain(|id, mounted| {
            let kept = rows.iter().any(|(row, _)| row.id == *id && row.component == mounted.component);
            if !kept {
                app.remove(mounted.fiber);
            }
            kept
        });
        for (row, factory) in rows {
            match self.mounted.get_mut(&row.id) {
                Some(mounted) if mounted.config != row.config => {
                    self.app.update(mounted.fiber, row.config.clone());
                    mounted.config = row.config.clone();
                }
                Some(_) => {}
                None => {
                    let fiber = self.app.insert_factory(factory, row.config.clone()).await;
                    let mounted = Mounted { component: row.component.clone(), config: row.config.clone(), fiber };
                    self.mounted.insert(row.id.clone(), mounted);
                }
            }
        }
        Ok(())
    }

    /// Returns the fiber mounted for a row id.
    pub fn fiber(&self, row: &str) -> Option<FiberId> {
        self.mounted.get(row).map(|mounted| mounted.fiber)
    }

    fn check<'t>(&self, tree: &'t Tree) -> Result<Vec<(&'t Row, Arc<Factory>)>, LoadError> {
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
            rows.push((row, factory.clone()));
        }

        let mut providers: HashMap<KeyId, Vec<usize>> = HashMap::new();
        for (index, (_, factory)) in rows.iter().enumerate() {
            for key in &factory.provides {
                providers.entry(*key).or_default().push(index);
            }
        }
        let mut edges = Vec::with_capacity(rows.len());
        for (row, factory) in &rows {
            let mut targets = Vec::new();
            for key in &factory.deps {
                let provided_by = providers
                    .get(key)
                    .ok_or_else(|| LoadError::MissingProvider { row: row.id.clone(), key: key.name() })?;
                targets.extend(provided_by);
            }
            edges.push(targets);
        }
        if let Some(cycle) = find_cycle(&edges) {
            return Err(LoadError::Cycle(cycle.into_iter().map(|index| rows[index].0.id.clone()).collect()));
        }
        Ok(rows)
    }
}

/// Returns the rows of one dependency cycle, first row repeated at the end, if any exists.
fn find_cycle(edges: &[Vec<usize>]) -> Option<Vec<usize>> {
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

    let mut marks = vec![Mark::New; edges.len()];
    let mut stack = Vec::new();
    (0..edges.len()).find_map(
        |node| {
            if marks[node] == Mark::New { visit(node, edges, &mut marks, &mut stack) } else { None }
        },
    )
}
