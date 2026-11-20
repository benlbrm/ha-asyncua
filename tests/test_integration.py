"""End-to-end tests of the integration inside Home Assistant."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from homeassistant.config_entries import SOURCE_IMPORT, SOURCE_USER, ConfigEntryState
from homeassistant.const import STATE_OFF, STATE_ON, STATE_UNAVAILABLE
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry

from .conftest import DOMAIN

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .conftest import OpcServer


async def _wait_state(
    hass: HomeAssistant, entity_id: str, expected: str, timeout: float = 5
) -> None:
    async with asyncio.timeout(timeout):
        while (state := hass.states.get(entity_id)) is None or state.state != expected:
            await asyncio.sleep(0.05)


def _entity_id(hass: HomeAssistant, domain: str, server: OpcServer, key: str) -> str:
    unique_id = f"{DOMAIN}.plc.{server.nodeid(key)}"
    entity_id = er.async_get(hass).async_get_entity_id(domain, DOMAIN, unique_id)
    assert entity_id, f"no {domain} entity for {key}"
    return entity_id


async def test_user_flow(hass: HomeAssistant, opc_server: OpcServer) -> None:
    """The user flow tests the connection then asks for discovery options."""
    result = await hass.config_entries.flow.async_init(
        DOMAIN, context={"source": SOURCE_USER}
    )
    assert result["type"] is FlowResultType.FORM

    bad = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"name": "plc", "url": "opc.tcp://127.0.0.1:1/"}
    )
    assert bad["errors"] == {"base": "cannot_connect"}

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {"name": "plc", "url": opc_server.url}
    )
    assert result["step_id"] == "discovery"
    invalid = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {"discovery_enabled": True, "discovery_root": "nope", "discovery_max_depth": 5},
    )
    assert invalid["errors"] == {"discovery_root": "invalid_nodeid"}

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {
            "discovery_enabled": True,
            "discovery_root": "i=85",
            "discovery_max_depth": 10,
            "discovery_namespaces": str(opc_server.ns),
            "discovery_writable": "cmd|Setpoint",
        },
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    entry = result["result"]
    assert entry.unique_id == "plc"
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    await hass.config_entries.async_unload(entry.entry_id)


@pytest.fixture
async def loaded_entry(hass: HomeAssistant, opc_server: OpcServer) -> MockConfigEntry:
    """A UI hub with discovery restricted to the test namespace."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="plc",
        title="plc",
        data={"name": "plc", "url": opc_server.url},
        options={
            "discovery_enabled": True,
            "discovery_root": "i=85",
            "discovery_max_depth": 10,
            "discovery_namespaces": str(opc_server.ns),
            "discovery_writable": "cmd|Setpoint|Motors",
            "discovery_exclude": "Label",
        },
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    yield entry
    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_discovered_entities(
    hass: HomeAssistant, opc_server: OpcServer, loaded_entry: MockConfigEntry
) -> None:
    """Discovered variables become entities attached to devices and follow the server."""
    running = _entity_id(hass, "binary_sensor", opc_server, "Running")
    temperature = _entity_id(hass, "sensor", opc_server, "Temperature")
    start = _entity_id(hass, "switch", opc_server, "cmdStart")
    setpoint = _entity_id(hass, "number", opc_server, "Setpoint")

    await _wait_state(hass, running, STATE_ON)
    await _wait_state(hass, temperature, "21.5")
    assert hass.states.get(temperature).attributes["unit_of_measurement"] == "°C"
    assert hass.states.get(setpoint).attributes["max"] == 100.0

    # Server-side change is pushed by the subscription.
    await opc_server.write(
        "Running", False, __import__("asyncua").ua.VariantType.Boolean
    )
    await _wait_state(hass, running, STATE_OFF)

    # Writes from Home Assistant reach the server with the right type.
    await hass.services.async_call(
        "switch", "turn_on", {"entity_id": start}, blocking=True
    )
    assert await opc_server.read("cmdStart") is True
    await _wait_state(hass, start, STATE_ON)
    await hass.services.async_call(
        "number", "set_value", {"entity_id": setpoint, "value": 42}, blocking=True
    )
    assert await opc_server.read("Setpoint") == 42

    # Devices: hub, Machine (via hub) and one per Motors[n] element.
    dev_reg = dr.async_get(hass)
    devices = {
        d.name: d
        for d in dr.async_entries_for_config_entry(dev_reg, loaded_entry.entry_id)
    }
    assert {"plc", "Machine", "Motors[0]", "Motors[1]"} <= set(devices)
    assert devices["Machine"].via_device_id == devices["plc"].id
    ent_reg = er.async_get(hass)
    assert ent_reg.async_get(running).device_id == devices["Machine"].id

    # Diagnostic entities on the hub device.
    connected = ent_reg.async_get_entity_id("binary_sensor", DOMAIN, "plc_connected")
    assert hass.states.get(connected).state == STATE_ON
    assert ent_reg.async_get_entity_id("button", DOMAIN, "plc_rediscover")

    # Excluded variable is not created.
    assert (
        ent_reg.async_get_entity_id(
            "text", DOMAIN, f"{DOMAIN}.plc.{opc_server.nodeid('Label')}"
        )
        is None
    )


async def test_set_value_service(
    hass: HomeAssistant, opc_server: OpcServer, loaded_entry: MockConfigEntry
) -> None:
    """The set_value service converts values and reports errors."""
    await hass.services.async_call(
        DOMAIN,
        "set_value",
        {"hub": "plc", "nodeid": opc_server.nodeid("Setpoint"), "value": "7"},
        blocking=True,
    )
    assert await opc_server.read("Setpoint") == 7
    with pytest.raises(ServiceValidationError):
        await hass.services.async_call(
            DOMAIN,
            "set_value",
            {"hub": "nope", "nodeid": "i=1", "value": 1},
            blocking=True,
        )
    with pytest.raises(HomeAssistantError):
        await hass.services.async_call(
            DOMAIN,
            "set_value",
            {"hub": "plc", "nodeid": opc_server.nodeid("Setpoint"), "value": "abc"},
            blocking=True,
        )


async def test_options_reload_removes_stale_entities(
    hass: HomeAssistant, opc_server: OpcServer, loaded_entry: MockConfigEntry
) -> None:
    """Narrowing the filters removes entities and devices that are no longer discovered."""
    ent_reg = er.async_get(hass)
    speed_uid = f"{DOMAIN}.plc.{opc_server.nodeid('Motors[0].Speed')}"
    assert ent_reg.async_get_entity_id("number", DOMAIN, speed_uid)

    result = await hass.config_entries.options.async_init(loaded_entry.entry_id)
    assert result["type"] is FlowResultType.FORM
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {
            "discovery_enabled": True,
            "discovery_root": "i=85",
            "discovery_max_depth": 10,
            "discovery_namespaces": str(opc_server.ns),
            "discovery_exclude": "Motors|Label",
        },
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    await hass.async_block_till_done()

    assert ent_reg.async_get_entity_id("number", DOMAIN, speed_uid) is None
    names = {
        d.name
        for d in dr.async_entries_for_config_entry(
            dr.async_get(hass), loaded_entry.entry_id
        )
    }
    assert "Motors[0]" not in names
    # cmdStart is no longer controllable: it is now a binary sensor.
    assert _entity_id(hass, "binary_sensor", opc_server, "cmdStart")


async def test_yaml_setup(hass: HomeAssistant, opc_server: OpcServer) -> None:
    """YAML hubs are imported as config entries and YAML entities keep working."""
    nodeid = opc_server.nodeid
    config = {
        DOMAIN: [{"name": "plc", "url": opc_server.url, "scan_interval": 10}],
        "sensor": [
            {
                "platform": DOMAIN,
                "nodes": [
                    {
                        "name": "Temp",
                        "hub": "plc",
                        "nodeid": nodeid("Temperature"),
                        "unit_of_measurement": "°C",
                    }
                ],
            }
        ],
        "switch": [
            {
                "platform": DOMAIN,
                "nodes": [
                    {
                        "name": "Start",
                        "hub": "plc",
                        "nodeid": nodeid("cmdStart"),
                        "nodeid_switch_di": nodeid("Running"),
                    }
                ],
            }
        ],
        "cover": [
            {
                "platform": DOMAIN,
                "nodes": [
                    {
                        "name": "Shutter",
                        "hub": "plc",
                        "nodeid_cover_position": nodeid("Position"),
                        "nodeid_cover_open": nodeid("cmdStart"),
                    }
                ],
            }
        ],
        "light": [
            {
                "platform": DOMAIN,
                "nodes": [
                    {
                        "name": "Lamp",
                        "hub": "plc",
                        "nodeid": nodeid("Running"),
                        "nodeid_light_on": nodeid("cmdStart"),
                        "nodeid_light_off": nodeid("cmdStart"),
                        "pulse_ms": 50,
                    }
                ],
            }
        ],
    }
    # Like HA does at startup, every component sees the full configuration.
    for domain in ("sensor", "switch", "cover", "light", DOMAIN):
        assert await async_setup_component(hass, domain, config)
    await hass.async_block_till_done()

    entries = hass.config_entries.async_entries(DOMAIN)
    assert len(entries) == 1
    assert entries[0].source == SOURCE_IMPORT
    assert entries[0].options["discovery_enabled"] is False

    await _wait_state(hass, "sensor.temp", "21.5")
    await _wait_state(hass, "switch.start", STATE_ON)  # state from the feedback node
    await _wait_state(hass, "cover.shutter", "open")
    assert hass.states.get("cover.shutter").attributes["current_position"] == 40
    await _wait_state(hass, "light.lamp", STATE_ON)

    await hass.services.async_call(
        "switch", "turn_off", {"entity_id": "switch.start"}, blocking=True
    )
    assert await opc_server.read("cmdStart") is False

    # Light with a distinct command node: pulse True then back to False.
    await hass.services.async_call(
        "light", "turn_off", {"entity_id": "light.lamp"}, blocking=True
    )
    assert await opc_server.read("cmdStart") is False

    # Cover without set-position node only supports OPEN.
    assert hass.states.get("cover.shutter").attributes["supported_features"] == 1

    await opc_server.server.stop()
    await _wait_state(hass, "sensor.temp", STATE_UNAVAILABLE, timeout=20)
    await opc_server.server.start()
    await _wait_state(hass, "sensor.temp", "21.5", timeout=40)

    await hass.config_entries.async_unload(entries[0].entry_id)
    await hass.async_stop()
