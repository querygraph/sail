#!/usr/bin/env bash
# Run this from a detached worktree. Receipt applies only to the immutable SHA.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../../.." && pwd)
venv=${SAIL_EXTENSION_VENV:-"$repo/.venv"}
target=${SAIL_EXTENSION_TARGET:-"$repo/target/extensions-poc"}
fmt_toolchain=${SAIL_EXTENSION_FMT_TOOLCHAIN:-nightly-2026-05-28}
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4}
export PYO3_PYTHON="$venv/bin/python"
export PYTHONHOME=$("$venv/bin/python" -c 'import sys; print(sys.base_prefix)')
export PYTHONPATH=$("$venv/bin/python" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
if [[ "$(uname -s)" == Darwin ]]; then
    export DYLD_LIBRARY_PATH=$("$venv/bin/python" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')
else
    export LD_LIBRARY_PATH=$("$venv/bin/python" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))')
fi
cd "$repo"
if git symbolic-ref -q HEAD >/dev/null; then
    printf 'Run verification in a detached worktree at the candidate commit.\n' >&2
    exit 1
fi
sha=$(git rev-parse HEAD)
if [[ -n "$(git status --porcelain)" ]]; then
    printf 'Tracked files must be clean before verification.\n' >&2
    exit 1
fi
df -h "$repo"
cargo "+$fmt_toolchain" fmt --all -- --check
vendor=examples/extensions/vendor/nutmeg-graph
for package in sedona nutmeg; do
    cargo "+$fmt_toolchain" fmt --manifest-path "examples/extensions/$package/Cargo.toml" -- --check
done
cargo "+$fmt_toolchain" fmt --manifest-path "$vendor/Cargo.toml" -- --check
# Run full host libraries: scalar codec and scheduler tests do not all contain
# "extension" in their names, so a substring filter would omit required checks.
CARGO_TARGET_DIR="$target/host" cargo test --locked --lib \
    -p sail-common-datafusion -p sail-function -p sail-plan -p sail-execution \
    -p sail-session -p sail-spark-connect -p sail-native-resource-ffi
CARGO_TARGET_DIR="$target/host" cargo clippy --locked -p sail-common-datafusion \
    -p sail-function -p sail-plan -p sail-execution -p sail-session \
    -p sail-spark-connect -p sail-native-resource-ffi --all-targets -- -D warnings
for wheel in "$target"/wheels/sail_sedona_extension-*.whl; do
    "$venv/bin/python" examples/extensions/scripts/check_wheel.py "$wheel" \
        --output "$target/sedona-native-dependencies.json"
done
for package in sedona nutmeg; do
    CARGO_TARGET_DIR="$target/$package" cargo test --locked --manifest-path "examples/extensions/$package/Cargo.toml"
done
CARGO_TARGET_DIR="$target/nutmeg-core" cargo test --locked --manifest-path "$vendor/Cargo.toml"
"$venv/bin/python" examples/extensions/nutmeg/scripts/stress_cancel.py \
    --target-dir "$target/nutmeg" --jobs "$CARGO_BUILD_JOBS" \
    --output "$target/cancellation-stress.json"
"$venv/bin/python" examples/extensions/nutmeg/scripts/stress_cancel.py \
    --manifest-path "$vendor/Cargo.toml" --target-dir "$target/nutmeg-core" \
    --jobs "$CARGO_BUILD_JOBS" \
    --test-name graph_tables::tests::separately_planned_concurrent_reads_share_one_revision_cache \
    --output "$target/snapshot-stress.json"
"$venv/bin/python" -m pytest examples/extensions/nutmeg/tests -q
"$venv/bin/python" -m unittest discover -s examples/extensions/scripts/tests -v
for mode in local local-cluster process-cluster; do
    "$venv/bin/python" -m pytest examples/extensions/tests --sail-binary "$target/host/debug/sail" --execution-mode "$mode" --basetemp "$target/pytest-$mode" -q
    "$venv/bin/python" examples/extensions/scripts/test_graph_algorithms.py \
        --sail-binary "$target/host/debug/sail" --execution-mode "$mode" \
        --output "$target/graph-algorithms-$mode"
done
[[ "$(git rev-parse HEAD)" == "$sha" ]]
[[ -z "$(git status --porcelain)" ]]
"$venv/bin/python" examples/extensions/scripts/record_evidence.py --target "$target" --output "$target/receipt.json"
printf 'extensions-poc: PASSED local and distributed native extension gates at %s\n' "$sha"
