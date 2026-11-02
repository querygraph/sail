#!/usr/bin/env bash
# Run the pipeline tests that cross catalogs against a fresh Sail server with a second memory catalog.
# usage: run-second-catalog.sh SAIL_BINARY [PORT]
set -euo pipefail

binary=$1
port=${2:-15260}
root=$(cd "$(dirname "$0")/../.." && pwd)

export SAIL_CATALOG__LIST='[{name="sail",type="memory",initial_database=["default"],initial_database_comment="default database"},{name="sdp_other",type="memory",initial_database=["default"]}]'
export SAIL_CATALOG__DEFAULT_CATALOG=sail
export SPARK_REMOTE="sc://localhost:$port"
export PYTHONPATH="$root/python${PYTHONPATH:+:$PYTHONPATH}"

"$binary" spark server --port "$port" &
server=$!
trap 'kill -INT "$server" 2>/dev/null || true; wait "$server" 2>/dev/null || true' EXIT

for _ in $(seq 1 600); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
        break
    fi
    kill -0 "$server"
    sleep 0.1
done

cd "$root"
python -m pytest python/pysail/tests/spark/pipelines/test_second_catalog.py -q -p no:cacheprovider
