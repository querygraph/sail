#!/usr/bin/env bash
# Runs the Sail Declarative Pipelines demo against a Sail binary built from this branch.
#
#   examples/declarative-pipelines/run.sh                   # uses target/release/sail, else target/debug/sail
#   SAIL_BINARY=/path/to/sail examples/declarative-pipelines/run.sh
#   IGGY_SERVER_BINARY=/path/to/iggy-server examples/declarative-pipelines/run.sh   # also proves Iggy
#
# PYTHON names the interpreter that has `pyspark[connect]==4.2.1` and `pyyaml` installed.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"

binary="${SAIL_BINARY:-}"
if [[ -z "$binary" ]]; then
    for candidate in "$root/target/release/sail" "$root/target/debug/sail"; do
        if [[ -x "$candidate" ]]; then
            binary="$candidate"
            break
        fi
    done
fi
if [[ -z "$binary" || ! -x "$binary" ]]; then
    echo "No sail binary found. Build one first: cargo build --release -p sail-cli" >&2
    exit 2
fi

python="${PYTHON:-python3}"
if ! "$python" -c 'import pyspark, yaml, grpc; assert pyspark.__version__ == "4.2.1", pyspark.__version__' 2>/dev/null; then
    echo "$python needs pyspark[connect]==4.2.1 and pyyaml: pip install 'pyspark[connect]==4.2.1' pyyaml" >&2
    exit 2
fi

arguments=(--sail "$binary")
if [[ -n "${IGGY_SERVER_BINARY:-}" ]]; then
    arguments+=(--iggy "$IGGY_SERVER_BINARY")
fi
# Servers started from a shell that blocks SIGINT inherit the block and cannot be stopped with it.
exec "$python" -c 'import signal, sys, runpy
signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT, signal.SIGTERM})
signal.signal(signal.SIGINT, signal.default_int_handler)
sys.argv = sys.argv[1:]
runpy.run_path(sys.argv[0], run_name="__main__")' "$here/demo.py" "${arguments[@]}" "$@"
