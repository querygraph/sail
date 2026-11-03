"""The engine-neutral, zero-input relation protocol for staging ownership."""

import json
import uuid

from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.connect.plan import LogicalPlan

from . import utils_pb2 as wire

TYPE_URL = "type.googleapis.com/gf.utils.v1.Request"
CLIENT_VERSION = "0.1.0"


class CapabilityError(RuntimeError):
    """The server does not implement the required versioned utils contract."""


class _UtilsRelation(LogicalPlan):
    def __init__(self, request):
        super().__init__(None)
        self.payload = request.SerializeToString()

    def plan(self, session):
        relation = self._create_proto_relation()
        relation.extension.type_url = TYPE_URL
        relation.extension.value = self.payload
        return relation


class GraphUtils:
    """Filesystem operations scoped by a server-issued run capability.

    All returned paths name storage visible to the server. Python never opens
    them. A successful exists/list keeps the session active; closing a Spark
    session or letting it expire invalidates its retained graph results.
    """

    def __init__(self, spark):
        self.spark = spark
        try:
            rows = self._request(wire.Request(ping=wire.Ping(client_version=CLIENT_VERSION)))
        except Exception as exc:
            raise CapabilityError(
                "graph utils Ping failed: enable the server's owned-run storage service"
            ) from exc
        if len(rows) != 1 or rows[0].kind != "pong":
            raise CapabilityError("invalid graph utils Ping receipt")
        pong = rows[0]
        try:
            self.capabilities = frozenset(json.loads(pong.capabilities))
        except (ValueError, TypeError) as exc:
            raise CapabilityError("invalid graph utils capabilities JSON") from exc
        if pong.protocol_version != 1 or not {"fs", "owned_runs_v1"} <= self.capabilities:
            raise CapabilityError("graph utils requires protocol 1 and fs, owned_runs_v1 capabilities")
        self.root = pong.path
        self.engine = pong.engine
        self.lease_seconds = pong.lease_seconds

    def _request(self, request):
        # Receipt collection is bounded. Graph rows are never returned here.
        return DataFrame(_UtilsRelation(request), self.spark).collect()

    def allocate(self, *, request_id=None):
        request_id = request_id or str(uuid.uuid4())
        row = self._request(wire.Request(mkdir=wire.Mkdir(root="", request_id=request_id)))[0]
        return row.path, row.token

    def exists(self, path, token):
        return self._request(wire.Request(exists=wire.Exists(path=path, token=token)))[0].value

    def ls(self, path, token, *, limit=1000):
        return self._request(wire.Request(ls=wire.Ls(path=path, limit=limit, token=token)))

    def remove(self, path, token):
        # Row inherits tuple.count; field access must not resolve that method.
        return self._request(wire.Request(rm=wire.Rm(path=path, token=token)))[0]["count"]
