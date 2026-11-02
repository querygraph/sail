"""Shared Unity Catalog container fixtures."""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

import pytest
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from pysail.testing.containers.unity_defaults import DEFAULT_CATALOG, UNITY_CATALOG_IMAGE

# The REST API of a Unity Catalog server already running, such as one built from source.
EXTERNAL_URL = "PYSAIL_UNITY_CATALOG_URL"

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path


@pytest.fixture(scope="module")
def unity_storage_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Return the host path mounted for managed Unity Catalog table storage."""
    return tmp_path_factory.mktemp("unity_storage_root")


@pytest.fixture(scope="module")
def unity_container(
    tmp_path_factory: pytest.TempPathFactory,
    unity_storage_root: Path,
) -> Generator[DockerContainer | None, None, None]:
    """Start a Unity Catalog container with its embedded H2 backend.

    With `PYSAIL_UNITY_CATALOG_URL` set, a server that is already running is used instead, which
    needs no Docker.
    """
    if os.environ.get(EXTERNAL_URL):
        yield None
        return
    tmp_dir = tmp_path_factory.mktemp("unity")
    server_config = "server.env=dev\nserver.authorization=disable\nserver.managed-table.enabled=true\n"
    server_path = tmp_dir / "server.properties"
    server_path.write_text(server_config)

    container = (
        DockerContainer(UNITY_CATALOG_IMAGE)
        .with_exposed_ports(8080)
        .with_volume_mapping(str(server_path), "/home/unitycatalog/etc/conf/server.properties", "ro")
        .with_volume_mapping(str(unity_storage_root), str(unity_storage_root), "rw")
        .waiting_for(
            LogMessageWaitStrategy(
                "###################################################################"
            ).with_startup_timeout(120)
        )
    )
    with container:
        yield container


@pytest.fixture(scope="module")
def unity_rest_url(unity_container: DockerContainer | None) -> str:
    """Return the host-accessible Unity Catalog REST API URL."""
    if unity_container is None:
        return os.environ[EXTERNAL_URL]
    host = unity_container.get_container_host_ip()
    port = unity_container.get_exposed_port(8080)
    return f"http://{host}:{port}/api/2.1/unity-catalog"


@pytest.fixture(scope="module")
def unity_catalog_initialized(unity_rest_url: str, unity_storage_root: Path) -> None:
    """Create the test catalog in Unity Catalog via its REST API."""
    url = f"{unity_rest_url}/catalogs"
    if os.environ.get(EXTERNAL_URL):
        # A shared server keeps the catalog of the last module, with that module's storage root.
        requests.delete(f"{url}/{DEFAULT_CATALOG}", params={"force": "true"}, timeout=10)
    payload = {
        "name": DEFAULT_CATALOG,
        "comment": "Main catalog for testing",
        "storage_root": str(unity_storage_root),
    }
    max_retries = 10
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code in (200, 201, 409):
                return
            response.raise_for_status()
        except Exception:
            if attempt == max_retries - 1:
                raise
            time.sleep(2)
        else:
            return
