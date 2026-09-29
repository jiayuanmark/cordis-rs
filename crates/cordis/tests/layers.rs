//! Layer merging: overrides by row id, insertion of new rows, and layer errors.

use cordis::{Layer, LoadError, Tree};
use serde_json::json;

fn layer(name: &str, text: &str) -> Layer {
    Layer::from_yaml(name, text).unwrap()
}

fn base() -> Layer {
    layer(
        "base",
        "plugins:
  - { id: sessions, use: jsonl-sessions, config: { dir: /tmp, fsync: true } }
  - { id: tools, use: tool-runtime }
  - { id: approval, use: approval-policy, config: { mode: ask } }
",
    )
}

#[test]
fn a_patch_overrides_only_the_fields_it_sets_and_replaces_config_whole() {
    let patch = layer("patch", "plugins:\n  - { id: sessions, config: { dir: /data } }\n");
    let tree = Tree::layered([&base(), &patch]).unwrap();
    let sessions = &tree.plugins[0];
    assert_eq!(sessions.component, "jsonl-sessions");
    assert_eq!(sessions.config, json!({ "dir": "/data" }));
}

#[test]
fn a_patch_can_switch_the_component_while_the_row_keeps_its_position() {
    let patch = layer("patch", "plugins:\n  - { id: sessions, use: memory-sessions, config: null }\n");
    let tree = Tree::layered([&base(), &patch]).unwrap();
    let ids: Vec<&str> = tree.plugins.iter().map(|row| row.id.as_str()).collect();
    assert_eq!(ids, ["sessions", "tools", "approval"]);
    assert_eq!(tree.plugins[0].component, "memory-sessions");
    assert_eq!(tree.plugins[0].config, json!(null));
}

#[test]
fn later_layers_can_disable_and_re_enable_a_row() {
    let off = layer("off", "plugins:\n  - { id: approval, disabled: true }\n");
    let on = layer("on", "plugins:\n  - { id: approval, disabled: false }\n");
    assert!(Tree::layered([&base(), &off]).unwrap().plugins[2].disabled);
    let tree = Tree::layered([&base(), &off, &on]).unwrap();
    assert!(!tree.plugins[2].disabled);
    assert_eq!(tree.plugins[2].config, json!({ "mode": "ask" }));
}

#[test]
fn new_rows_are_appended_in_layer_order() {
    let web = layer("web", "plugins:\n  - { id: server, use: web-server, config: { port: 1 } }\n");
    let user = layer("user", "plugins:\n  - { id: metrics, use: metrics }\n  - { id: server, config: { port: 2 } }\n");
    let tree = Tree::layered([&base(), &web, &user]).unwrap();
    let ids: Vec<&str> = tree.plugins.iter().map(|row| row.id.as_str()).collect();
    assert_eq!(ids, ["sessions", "tools", "approval", "server", "metrics"]);
    assert_eq!(tree.plugins[3].config, json!({ "port": 2 }));
}

#[test]
fn invalid_layers_are_rejected_with_the_layer_named() {
    let no_use = layer("user", "plugins:\n  - { id: metrics }\n");
    assert!(matches!(
        Tree::layered([&base(), &no_use]),
        Err(LoadError::NewRowWithoutUse { layer, row }) if layer == "user" && row == "metrics"
    ));

    let repeated = layer("user", "plugins:\n  - { id: tools }\n  - { id: tools }\n");
    assert!(matches!(Tree::layered([&base(), &repeated]), Err(LoadError::DuplicateId(id)) if id == "tools"));

    let unknown_field = Layer::from_yaml("user", "plugins:\n  - { id: tools, enabled: true }\n");
    assert!(matches!(unknown_field, Err(LoadError::Layer { layer, .. }) if layer == "user"));
}
