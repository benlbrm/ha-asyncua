"""Tests for value coercion, the hub and discovery against an embedded server."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from asyncua import ua
from homeassistant.exceptions import HomeAssistantError

from .conftest import integration_module

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .conftest import OpcServer

hub_mod = integration_module("hub")
discovery = integration_module("discovery")


@pytest.mark.parametrize(
    ("value", "vtype", "expected"),
    [
        (50.0, ua.VariantType.Int16, 50),
        ("42", ua.VariantType.UInt32, 42),
        ("0x10", ua.VariantType.Byte, 16),
        ("12.0", ua.VariantType.Int32, 12),
        (True, ua.VariantType.Int16, 1),
        ("on", ua.VariantType.Boolean, True),
        ("False", ua.VariantType.Boolean, False),
        (0, ua.VariantType.Boolean, False),
        ("21.5", ua.VariantType.Float, 21.5),
        (3, ua.VariantType.String, "3"),
    ],
)
def test_coerce_value(value, vtype, expected) -> None:
    """Values from Home Assistant are converted to the node type."""
    result = hub_mod.coerce_value(value, vtype)
    assert result == expected
    assert type(result) is type(expected)


@pytest.mark.parametrize(
    ("value", "vtype"),
    [
        (1.5, ua.VariantType.Int16),
        (300, ua.VariantType.Byte),
        (-1, ua.VariantType.UInt16),
        ("maybe", ua.VariantType.Boolean),
    ],
)
def test_coerce_value_rejects(value, vtype) -> None:
    """Invalid or out of range values raise ValueError."""
    with pytest.raises(ValueError):
        hub_mod.coerce_value(value, vtype)


def test_normalize_nodeid() -> None:
    """Equivalent node id spellings map to the same key."""
    assert hub_mod.normalize_nodeid("ns=0;i=85") == "i=85"
    assert (
        hub_mod.normalize_nodeid(" ns=2;s=Machine.Running ") == "ns=2;s=Machine.Running"
    )


async def _wait_for(predicate, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.05)


async def test_hub_subscribe_and_write(
    hass: HomeAssistant, opc_server: OpcServer
) -> None:
    """The hub monitors nodes incrementally, pushes values and writes typed values."""
    hub = hub_mod.OpcuaHub(hass, hub_mod.HubConfig(name="plc", url=opc_server.url))
    calls: list[str] = []
    running = opc_server.nodeid("Running")
    hub.async_add_node_listener(running, lambda: calls.append("running"))

    hub.async_register_nodes([running])
    await hub.async_start()
    assert await hub.async_wait_connected(10)
    await _wait_for(lambda: hub.value(running) is True)
    assert calls

    # Registering more nodes later subscribes only the new ones.
    setpoint = opc_server.nodeid("Setpoint")
    hub.async_register_nodes([setpoint, "ns=2;s=does.not.exist"])
    await _wait_for(lambda: hub.value(setpoint) == 10)
    await _wait_for(lambda: "ns=2;s=does.not.exist" in hub.failed_nodes)
    assert hub.subscribed_count == 2

    # A float coming from a number entity is written to an Int16 node.
    await hub.async_write(setpoint, 55.0)
    assert await opc_server.read("Setpoint") == 55
    await _wait_for(lambda: hub.value(setpoint) == 55)

    with pytest.raises(HomeAssistantError):
        await hub.async_write(setpoint, 1.5)
    with pytest.raises(HomeAssistantError):
        await hub.async_write(opc_server.nodeid("Running"), "maybe")

    await hub.async_stop()
    assert not hub.connected
    with pytest.raises(HomeAssistantError):
        await hub.async_write(setpoint, 1)


async def test_discovery(hass: HomeAssistant, opc_server: OpcServer) -> None:
    """Variables are mapped to platforms; writable ones only when allowed."""
    hub = hub_mod.OpcuaHub(hass, hub_mod.HubConfig(name="plc", url=opc_server.url))
    await hub.async_start()
    assert await hub.async_wait_connected(10)

    flt = discovery.DiscoveryFilter(namespaces={opc_server.ns})
    found, truncated = await discovery.async_discover(hub.client, flt)
    by_name = {v.name: v for v in found}
    assert not truncated
    # Read-only by default, even for writable variables.
    assert by_name["cmdStart"].platform == "binary_sensor"
    assert by_name["Setpoint"].platform == "sensor"
    assert by_name["Temperature"].unit == "°C"
    assert by_name["Setpoint"].min_value == 0.0
    assert by_name["Setpoint"].max_value == 100.0
    assert "Motors" not in by_name  # structure, not a value
    # Members of array elements are grouped in one device per element.
    speed = next(v for v in found if v.nodeid.endswith("Motors[1].Speed"))
    assert speed.device_name == "Motors[1]"
    assert speed.name == "Speed"
    assert by_name["Running"].device_name == "Machine"

    flt = discovery.DiscoveryFilter(
        namespaces={opc_server.ns}, writable=r"cmd|Setpoint|Label", exclude="Motors"
    )
    found, _ = await discovery.async_discover(hub.client, flt)
    platforms = {v.name: v.platform for v in found}
    assert platforms == {
        "Running": "binary_sensor",
        "cmdStart": "switch",
        "Setpoint": "number",
        "Temperature": "sensor",
        "Label": "text",
        "Position": "sensor",
    }

    flt = discovery.DiscoveryFilter(namespaces={opc_server.ns + 1})
    assert (await discovery.async_discover(hub.client, flt))[0] == []
    await hub.async_stop()
