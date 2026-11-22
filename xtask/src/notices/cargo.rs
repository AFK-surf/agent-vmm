use super::{Notice, fallback, files};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    resolve: Resolve,
}
#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    version: Option<String>,
    source: Option<String>,
    manifest_path: Option<PathBuf>,
    license: Option<String>,
}
#[derive(Deserialize)]
struct Resolve {
    root: Option<String>,
    nodes: Vec<Node>,
}
#[derive(Deserialize)]
struct Node {
    id: String,
    deps: Vec<Dependency>,
}
#[derive(Deserialize)]
struct Dependency {
    pkg: String,
    dep_kinds: Vec<Kind>,
}
#[derive(Deserialize)]
struct Kind {
    kind: Option<String>,
}

pub(super) fn notices(data: &[u8], licenses: &Path) -> Result<Vec<Notice>> {
    let meta: Metadata = serde_json::from_slice(data)?;
    let graph: HashMap<_, _> = meta
        .resolve
        .nodes
        .into_iter()
        .map(|n| (n.id, n.deps))
        .collect();
    let mut stack = vec![
        meta.resolve
            .root
            .context("cargo metadata has no root package")?,
    ];
    let mut visited = HashSet::new();
    while let Some(id) = stack.pop() {
        if !visited.insert(id.clone()) {
            continue;
        }
        if let Some(deps) = graph.get(&id) {
            stack.extend(
                deps.iter()
                    .filter(|d| d.dep_kinds.iter().any(|k| k.kind.is_none()))
                    .map(|d| d.pkg.clone()),
            );
        }
    }
    let mut rows = Vec::new();
    for pkg in meta.packages {
        if !visited.contains(&pkg.id) || pkg.source.as_deref().unwrap_or_default().is_empty() {
            continue;
        }
        let version = pkg.version.unwrap_or_default();
        let license = pkg.license.unwrap_or_default();
        let dir = pkg
            .manifest_path
            .as_deref()
            .and_then(Path::parent)
            .context("missing crate manifest path")?;
        let mut texts = files(dir)?;
        if texts.is_empty() {
            texts = fallback(&license, licenses, true)
                .with_context(|| format!("Rust crate {}@{version}", pkg.name))?;
        }
        rows.push(Notice {
            section: "Rust",
            name: pkg.name,
            version,
            license,
            texts,
            ..Default::default()
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn walks_normal_deps_and_requires_fallback_text_for_external_crates() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = temp.path().join("Cargo.toml");
        let license = temp.path().join("MIT.txt");
        std::fs::write(&license, "MIT license text").unwrap();
        let meta = json!({
            "packages": [
                {"id":"root","name":"root"},
                {"id":"runtime","name":"local"},
                {"id":"crate","name":"crate","version":"1","source":"registry+example","license":"MIT","manifest_path":manifest},
                {"id":"ignored","name":"dev","source":"registry+example"}
            ],
            "resolve": {"root":"root","nodes":[
                {"id":"root","deps":[{"pkg":"runtime","dep_kinds":[{"kind":null}]},{"pkg":"ignored","dep_kinds":[{"kind":"build"},{"kind":"dev"}]}]},
                {"id":"runtime","deps":[{"pkg":"crate","dep_kinds":[{"kind":"dev"},{"kind":null}]}]}
            ]}
        });
        let bytes = serde_json::to_vec(&meta).unwrap();
        let rows = notices(&bytes, temp.path()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].texts["MIT"], b"MIT license text");
        std::fs::remove_file(license).unwrap();
        let err = notices(&bytes, temp.path())
            .err()
            .expect("missing fallback");
        assert!(format!("{err:#}").contains("Rust crate crate@1: missing license text MIT"));
    }
}
