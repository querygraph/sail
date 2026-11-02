#!/usr/bin/env bash
# Run the pipeline materialization and DDL tests against a Sail server whose catalog is a standalone
# Hive metastore, with embedded Derby and a local file warehouse. The test modules are the default
# profile's own, unchanged: Spark's HiveMaterializeTablesSuite also reuses the default suite's bodies.
#
# usage: run-hive-metastore.sh SAIL_BINARY METASTORE_DIR [PORT]
# METASTORE_DIR is filled on first use, and each download is checked against its Maven SHA-1.
# PORT is the Sail server, and PORT + 1 is the metastore.
set -euo pipefail

binary=$1
store=$2
port=${3:-15270}
metastore_port=$((port + 1))
root=$(cd "$(dirname "$0")/../.." && pwd)
maven=https://repo1.maven.org/maven2
java_home=$(/usr/libexec/java_home -v 17)

fetch() {
    local url=$1 sha1=$2 file
    file=$store/downloads/$(basename "$url")
    if [[ ! -f $file ]]; then
        curl -fsSL -o "$file.part" "$url"
        echo "$sha1  $file.part" | shasum -a 1 -c - >/dev/null
        mv "$file.part" "$file"
    fi
}

mkdir -p "$store/downloads"
fetch "$maven/org/apache/hive/hive-standalone-metastore-server/4.0.1/hive-standalone-metastore-server-4.0.1-bin.tar.gz" \
    e6fd257ef9871e9c5d2e01528b85a47e0db78322
fetch "$maven/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar" \
    12ac6f103a0ff29fce17a078c7c64d25320b6165
fetch "$maven/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar" \
    81065531e63fccbe85fb04a3274709593fb00d3c
fetch "$maven/javax/servlet/javax.servlet-api/3.1.0/javax.servlet-api-3.1.0.jar" \
    3cd63d075497751784b2fa84be59432f4905bf7c

home=$store/apache-hive-metastore-4.0.1-bin
if [[ ! -d $home ]]; then
    tar -xzf "$store/downloads/hive-standalone-metastore-server-4.0.1-bin.tar.gz" -C "$store" --no-same-owner
fi

run=$(mktemp -d "$store/run.XXXXXX")
mkdir "$run/conf" "$run/warehouse"
cat >"$run/conf/metastore-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>metastore.thrift.port</name><value>$metastore_port</value></property>
  <property><name>metastore.warehouse.dir</name><value>file://$run/warehouse</value></property>
  <property><name>metastore.warehouse.external.dir</name><value>file://$run/warehouse</value></property>
  <property><name>javax.jdo.option.ConnectionURL</name><value>jdbc:derby:;databaseName=$run/metastore_db;create=true</value></property>
  <property><name>javax.jdo.option.ConnectionDriverName</name><value>org.apache.derby.jdbc.EmbeddedDriver</value></property>
  <property><name>metastore.expression.proxy</name><value>org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy</value></property>
  <property><name>metastore.disallow.incompatible.col.type.changes</name><value>false</value></property>
</configuration>
EOF
classpath="$run/conf:$home/lib/*:$store/downloads/*"

wait_for_port() {
    local target=$1 owner=$2
    for _ in $(seq 1 1200); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$target") 2>/dev/null; then
            return 0
        fi
        kill -0 "$owner"
        sleep 0.1
    done
    echo "nothing listening on $target" >&2
    return 1
}

metastore=
server=
stop() {
    if [[ -n $server ]]; then
        kill -INT "$server" 2>/dev/null || true
        wait "$server" 2>/dev/null || true
    fi
    if [[ -n $metastore ]]; then
        kill "$metastore" 2>/dev/null || true
        wait "$metastore" 2>/dev/null || true
    fi
}
trap stop EXIT

cd "$run"
"$java_home/bin/java" -cp "$classpath" org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool \
    -dbType derby -initSchema >schema.log 2>&1
"$java_home/bin/java" --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED \
    -cp "$classpath" org.apache.hadoop.hive.metastore.HiveMetaStore -p "$metastore_port" >metastore.log 2>&1 &
metastore=$!
wait_for_port "$metastore_port" "$metastore"

export SAIL_CATALOG__LIST="[{name=\"sail\",type=\"hms\",uris=[\"127.0.0.1:$metastore_port\"]}]"
export SPARK_REMOTE="sc://localhost:$port"
export PYTHONPATH="$root/python${PYTHONPATH:+:$PYTHONPATH}"
"$binary" spark server --port "$port" >server.log 2>&1 &
server=$!
wait_for_port "$port" "$server"

cd "$root"
tests=python/pysail/tests/spark/pipelines
python -m pytest \
    "$tests/test_materialize_tables_more.py" \
    "$tests/test_nested_materialization.py" \
    "$tests/test_direct_execution.py" \
    "$tests/test_properties.py" \
    "$tests/test_execution.py" \
    "$tests/test_audit_exec.py" \
    -q -p no:cacheprovider --junitxml="$run/pytest.xml"
