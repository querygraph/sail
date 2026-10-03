//! Read build declarations from installed distribution files without importing
//! extension packages. Discovery is completed before the first entry is loaded.
use std::collections::HashSet;

use datafusion_common::{Result, plan_err};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};
use serde::Deserialize;

use super::manifest::{Manifest, validate_build};
use super::py_error;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticMetadata {
    schema_version: u32,
    extensions: Vec<BuildCompatibility>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BuildCompatibility {
    entry_point: String,
    name: String,
    version: String,
    api_version: u32,
    datafusion_version: String,
    arrow_version: String,
}

impl BuildCompatibility {
    fn parse(json: &str, entry_name: &str) -> Result<Self> {
        let metadata: StaticMetadata = serde_json::from_str(json).map_err(|error| {
            py_error(format!(
                "invalid static compatibility metadata for entry point {entry_name}: {error}"
            ))
        })?;
        if metadata.schema_version != 1 {
            return plan_err!(
                "unsupported static compatibility schema {} for entry point {entry_name}; expected 1",
                metadata.schema_version
            );
        }
        let mut names = HashSet::new();
        for build in &metadata.extensions {
            if build.entry_point.is_empty() || !names.insert(&build.entry_point) {
                return plan_err!(
                    "invalid static compatibility metadata for entry point {entry_name}: empty or duplicate entry point {}",
                    build.entry_point
                );
            }
        }
        let build = metadata
            .extensions
            .into_iter()
            .find(|build| build.entry_point == entry_name)
            .ok_or_else(|| {
                py_error(format!(
                    "missing static compatibility entry for entry point {entry_name}"
                ))
            })?;
        validate_build(
            &build.name,
            &build.version,
            build.api_version,
            &build.datafusion_version,
            &build.arrow_version,
        )?;
        Ok(build)
    }

    /// Runtime options remain dynamic, but cannot rewrite the admitted build.
    pub(super) fn check_manifest(&self, manifest: &Manifest) -> Result<()> {
        for (field, agrees) in [
            ("name", self.name == manifest.name),
            ("version", self.version == manifest.version),
            ("api_version", self.api_version == manifest.api_version),
            (
                "datafusion_version",
                self.datafusion_version == manifest.datafusion_version,
            ),
            (
                "arrow_version",
                self.arrow_version == manifest.arrow_version,
            ),
        ] {
            if !agrees {
                return plan_err!(
                    "static compatibility metadata disagrees with manifest for entry point {}: {field}",
                    self.entry_point
                );
            }
        }
        Ok(())
    }
}

fn read_build(entry: &Bound<'_, PyAny>, entry_name: &str) -> Result<BuildCompatibility> {
    let module = entry
        .getattr("module")
        .and_then(|value| value.extract::<String>())
        .map_err(py_error)?;
    // Do not resolve a module with importlib.resources or find_spec: resolving a
    // submodule may import its parent. Derive only relative paths from its name.
    for part in module.split('.') {
        let valid = PyString::new(entry.py(), part)
            .call_method0("isidentifier")
            .and_then(|value| value.extract::<bool>())
            .map_err(py_error)?;
        if !valid {
            return plan_err!("invalid extension module name for entry point {entry_name}");
        }
    }
    let module_path = module.replace('.', "/");
    let candidates = [
        format!("{module_path}/sail-extension.json"),
        format!("{module_path}.sail-extension.json"),
    ];
    let distribution = entry.getattr("dist").map_err(py_error)?;
    let files = distribution.getattr("files").map_err(py_error)?;
    if files.is_none() {
        return plan_err!(
            "missing static compatibility metadata for entry point {entry_name}: installed distribution has no file manifest"
        );
    }
    let mut recorded = HashSet::new();
    for file in files.try_iter().map_err(py_error)? {
        recorded.insert(
            file.and_then(|file| file.str())
                .and_then(|name| name.extract::<String>())
                .map_err(py_error)?,
        );
    }
    let found = candidates
        .iter()
        .filter(|path| recorded.contains(*path))
        .collect::<Vec<_>>();
    let path = match found.as_slice() {
        [path] => *path,
        [] => {
            return plan_err!(
                "missing static compatibility metadata for entry point {entry_name}: record {} in the installed wheel",
                candidates.join(" or ")
            );
        }
        _ => {
            return plan_err!(
                "ambiguous static compatibility metadata for entry point {entry_name}: both package and module files are recorded"
            );
        }
    };
    let kwargs = PyDict::new(entry.py());
    kwargs.set_item("encoding", "utf-8").map_err(py_error)?;
    let json = distribution
        .call_method1("locate_file", (path,))
        .and_then(|path| path.call_method("read_text", (), Some(&kwargs)))
        .and_then(|value| value.extract::<String>())
        .map_err(|error| {
            py_error(format!(
                "cannot read static compatibility metadata for entry point {entry_name} at {path}: {error}"
            ))
        })?;
    BuildCompatibility::parse(&json, entry_name)
}

pub(super) struct PreparedEntry<'py> {
    pub name: String,
    pub entry: Bound<'py, PyAny>,
    pub build: BuildCompatibility,
}

/// Both driver and worker must finish this pass before running package code.
pub(super) fn discover(py: Python<'_>) -> Result<Vec<PreparedEntry<'_>>> {
    let kwargs = PyDict::new(py);
    kwargs
        .set_item("group", "pysail.extensions")
        .map_err(py_error)?;
    let entries = py
        .import("importlib.metadata")
        .and_then(|module| module.call_method("entry_points", (), Some(&kwargs)))
        .map_err(py_error)?;
    let mut prepared = Vec::new();
    let mut named = Vec::new();
    for entry in entries.try_iter().map_err(py_error)? {
        let entry = entry.map_err(py_error)?;
        let name = entry
            .getattr("name")
            .and_then(|name| name.extract::<String>())
            .map_err(py_error)?;
        named.push((name, entry));
    }
    named.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, entry) in named {
        let build = read_build(&entry, &name)?;
        prepared.push(PreparedEntry { name, entry, build });
    }
    Ok(prepared)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn static_metadata() -> Value {
        json!({"schema_version": 1, "extensions": [{
            "entry_point": "example", "name": "example", "version": "1",
            "api_version": 1, "datafusion_version": "55.1.0", "arrow_version": "59.3.0"
        }]})
    }

    #[test]
    fn admits_compatible_build_without_freezing_runtime_options() -> Result<()> {
        let data = static_metadata();
        let build = BuildCompatibility::parse(&data.to_string(), "example")?;
        let mut runtime = data["extensions"][0].clone();
        if let Some(object) = runtime.as_object_mut() {
            object.remove("entry_point");
        }
        runtime["placement"] = json!("driver");
        runtime["relation_types"] = json!([]);
        for bytes in [64, 128] {
            runtime["memory_bytes"] = json!(bytes);
            let manifest: Manifest = serde_json::from_value(runtime.clone()).map_err(py_error)?;
            build.check_manifest(&manifest)?;
            manifest.validate()?;
        }
        Ok(())
    }

    #[test]
    fn rejects_unknown_schema_and_ambiguous_mapping() {
        let mut data = static_metadata();
        data["schema_version"] = json!(2);
        assert!(BuildCompatibility::parse(&data.to_string(), "example").is_err());
        data["schema_version"] = json!(1);
        data["extensions"] = json!([data["extensions"][0], data["extensions"][0]]);
        assert!(BuildCompatibility::parse(&data.to_string(), "example").is_err());
        assert!(BuildCompatibility::parse(&static_metadata().to_string(), "other").is_err());
    }
}
