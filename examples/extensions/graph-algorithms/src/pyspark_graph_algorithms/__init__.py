"""Compatibility imports for Pecan's former Python package name.

Use ``pyspark_pecan`` in new code. Both names resolve to the same classes and
protobuf modules; the gf.utils.v1 wire contract has not changed.
"""

import importlib as _importlib
import sys as _sys

from pyspark_pecan import *  # noqa: F403
from pyspark_pecan import __all__

for _name in ("algorithms", "lifecycle", "staging", "utils", "utils_pb2"):
    _module = _importlib.import_module(f"pyspark_pecan.{_name}")
    _sys.modules[f"{__name__}.{_name}"] = _module
    globals()[_name] = _module

del _name, _module, _importlib, _sys
