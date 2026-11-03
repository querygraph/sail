use std::path::Path;

/// The version Cargo.lock resolves for a package, read without a TOML parser:
/// the lockfile's `[[package]]` entries are `name = "..."` then `version = "..."`.
fn locked_version(lock: &str, package: &str) -> Option<String> {
    let mut lines = lock.lines();
    while let Some(line) = lines.next() {
        if line.trim() == format!("name = \"{package}\"")
            && let Some(version) = lines.next()
            && let Some(version) = version.trim().strip_prefix("version = \"")
        {
            return version.strip_suffix('"').map(str::to_string);
        }
    }
    None
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto";
    println!("cargo:rerun-if-changed={proto}/gf/utils/v1/utils.proto");
    prost_build::compile_protos(&[format!("{proto}/gf/utils/v1/utils.proto")], &[proto])?;

    // The host's build identity for native extensions is what the workspace lockfile
    // resolves for the FFI boundary (`datafusion-ffi`) and Arrow, not a literal: the
    // extension packages must be built against exactly these.
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let lock_path = Path::new(&manifest_dir).join("../../Cargo.lock");
    println!("cargo:rerun-if-changed={}", lock_path.display());
    let lock = std::fs::read_to_string(&lock_path)?;
    for (variable, package) in [
        ("SAIL_HOST_DATAFUSION_VERSION", "datafusion-ffi"),
        ("SAIL_HOST_ARROW_VERSION", "arrow-array"),
    ] {
        let version = locked_version(&lock, package)
            .ok_or_else(|| format!("{package} is not in {}", lock_path.display()))?;
        println!("cargo:rustc-env={variable}={version}");
    }
    Ok(())
}
