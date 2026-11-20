"""
Optional read-only test against a real OPC UA server.

Skipped unless ASYNCUA_LIVE_URL is set. Never writes to the server.

    ASYNCUA_LIVE_URL=opc.tcp://192.168.4.201:4840/ ASYNCUA_LIVE_USER=dev \
    ASYNCUA_LIVE_PASSWORD=... ASYNCUA_LIVE_NAMESPACES=6 pytest tests/test_live_plc.py -s
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import Counter
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import pytest
import pytest_socket

from .conftest import integration_module

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

URL = os.environ.get("ASYNCUA_LIVE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="ASYNCUA_LIVE_URL not set")

hub_mod = integration_module("hub")
discovery = integration_module("discovery")


async def test_live_read_only(hass: HomeAssistant, socket_enabled: None) -> None:
    """Discover, subscribe and receive values from a real server (no write)."""
    # The HA test plugin only allows localhost: open the PLC address for this test.
    pytest_socket.socket_allow_hosts(
        ["127.0.0.1", urlparse(URL).hostname], allow_unix_socket=True
    )
    hub = hub_mod.OpcuaHub(
        hass,
        hub_mod.HubConfig(
            name="live",
            url=URL,
            username=os.environ.get("ASYNCUA_LIVE_USER"),
            password=os.environ.get("ASYNCUA_LIVE_PASSWORD"),
        ),
    )
    await hub.async_start()
    try:
        assert await hub.async_wait_connected(15), hub.last_error
        namespaces = {
            int(n)
            for n in os.environ.get("ASYNCUA_LIVE_NAMESPACES", "").split(",")
            if n.strip()
        }
        start = time.monotonic()
        found, truncated = await discovery.async_discover(
            hub.client, discovery.DiscoveryFilter(namespaces=namespaces)
        )
        print(
            f"\ndiscovered {len(found)} variables in {time.monotonic() - start:.1f}s "
            f"(truncated={truncated}): {dict(Counter(v.platform for v in found))}"
        )
        assert found
        assert all(
            v.platform in ("sensor", "binary_sensor") for v in found
        )  # read-only default

        sample = [v.nodeid for v in found[:300]]
        start = time.monotonic()
        hub.async_register_nodes(sample)
        async with asyncio.timeout(20):
            while sum(hub.get(n) is not None for n in sample) < len(sample) - len(
                hub.failed_nodes
            ):
                await asyncio.sleep(0.1)
        print(
            f"received {len(sample)} initial values in {time.monotonic() - start:.1f}s, "
            f"failed={len(hub.failed_nodes)}, bad quality="
            f"{sum(not hub.get(n).good for n in sample if hub.get(n))}"
        )
        for nodeid in [n for n in sample if hub.get(n) and not hub.get(n).good]:
            print(f"  BAD {nodeid} {hub.get(nodeid).status} {hub.get(nodeid).value!r}")
        for nodeid in sample[:5]:
            print(f"  {nodeid} = {hub.value(nodeid)!r}")
    finally:
        await hub.async_stop()
