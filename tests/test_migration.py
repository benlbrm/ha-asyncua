"""Migration from the former ``ha-asyncua`` domain."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from homeassistant.config_entries import SOURCE_IMPORT, SOURCE_USER, ConfigEntryState
from homeassistant.const import STATE_ON
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry

from .conftest import DOMAIN

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .conftest import OpcServer

OLD = "ha-asyncua"


async def _wait(predicate, timeout: float = 10) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.05)


async def test_migrate_ui_entry(hass: HomeAssistant, opc_server: OpcServer) -> None:
    """A UI hub of the old domain is re-created; entities keep their entity ids."""
    old = MockConfigEntry(
        domain=OLD,
        source=SOURCE_USER,
        unique_id="plc",
        title="plc",
        data={
            "name": "plc",
            "url": opc_server.url,
            "security_policy": "None",
            "security_mode": "SignAndEncrypt",
        },
        options={
            "discovery_enabled": True,
            "discovery_root": "i=85",
            "discovery_max_depth": 10,
            "discovery_namespaces": str(opc_server.ns),
            "discovery_writable": "",
            "discovery_exclude": "Motors|Label",
        },
    )
    old.add_to_hass(hass)
    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)
    running = ent_reg.async_get_or_create(
        "binary_sensor",
        OLD,
        f"{OLD}.plc.{opc_server.nodeid('Running')}",
        config_entry=old,
        suggested_object_id="my_running",
    )
    hub_device = dev_reg.async_get_or_create(
        config_entry_id=old.entry_id, identifiers={(OLD, "plc")}, name="plc"
    )

    assert await async_setup_component(hass, DOMAIN, {})
    await _wait(lambda: not hass.config_entries.async_entries(OLD))
    await hass.async_block_till_done()

    (entry,) = hass.config_entries.async_entries(DOMAIN)
    assert entry.unique_id == "plc"
    assert entry.state is ConfigEntryState.LOADED
    assert entry.options["discovery_exclude"] == "Motors|Label"
    assert "security_mode" not in entry.data

    migrated = ent_reg.async_get(running.entity_id)
    assert migrated.platform == DOMAIN
    assert migrated.unique_id == f"{DOMAIN}.plc.{opc_server.nodeid('Running')}"
    assert migrated.config_entry_id == entry.entry_id
    await _wait(
        lambda: (
            (s := hass.states.get("binary_sensor.my_running")) and s.state == STATE_ON
        )
    )

    device = dev_reg.async_get(hub_device.id)
    assert device.identifiers == {(DOMAIN, "plc")}
    assert device.config_entry_id == entry.entry_id

    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


async def test_old_yaml_still_works(hass: HomeAssistant, opc_server: OpcServer) -> None:
    """YAML written with the old domain is loaded and raises a repair issue."""
    old_import = MockConfigEntry(
        domain=OLD,
        source=SOURCE_IMPORT,
        unique_id="plc",
        title="plc",
        data={"name": "plc", "url": opc_server.url},
        options={"discovery_enabled": False},
    )
    old_import.add_to_hass(hass)
    ent_reg = er.async_get(hass)
    temp = ent_reg.async_get_or_create(
        "sensor",
        OLD,
        f"{OLD}.plc.{opc_server.nodeid('Temperature')}",
        suggested_object_id="boiler",
    )
    # Custom YAML unique id (no domain prefix): must not conflict with itself.
    vmc = ent_reg.async_get_or_create(
        "switch", OLD, "vmc_switch", suggested_object_id="vmc_switch"
    )
    # Half-migrated registry (a previous start stopped midway).
    already = ent_reg.async_get_or_create(
        "binary_sensor",
        DOMAIN,
        f"{DOMAIN}.plc.{opc_server.nodeid('Running')}",
        suggested_object_id="running",
    )
    config = {
        OLD: [{"name": "plc", "url": opc_server.url}],
        "sensor": [
            {
                "platform": OLD,
                "nodes": [
                    {
                        "name": "Temp",
                        "hub": "plc",
                        "nodeid": opc_server.nodeid("Temperature"),
                    }
                ],
            }
        ],
        "switch": [
            {
                "platform": OLD,
                "nodes": [
                    {
                        "name": "VMC",
                        "hub": "plc",
                        "unique_id": "vmc_switch",
                        "nodeid": opc_server.nodeid("cmdStart"),
                    }
                ],
            }
        ],
        "binary_sensor": [
            {
                "platform": DOMAIN,
                "nodes": [
                    {
                        "name": "Running",
                        "hub": "plc",
                        "nodeid": opc_server.nodeid("Running"),
                    }
                ],
            }
        ],
    }
    for domain in ("sensor", "switch", "binary_sensor", DOMAIN):
        assert await async_setup_component(hass, domain, config)
    await _wait(lambda: not hass.config_entries.async_entries(OLD))
    await hass.async_block_till_done()

    assert ent_reg.async_get(temp.entity_id).platform == DOMAIN
    migrated_vmc = ent_reg.async_get(vmc.entity_id)
    assert migrated_vmc.platform == DOMAIN
    assert migrated_vmc.unique_id == "vmc_switch"
    await _wait(
        lambda: (s := hass.states.get("switch.vmc_switch")) and s.state == "off"
    )
    await _wait(lambda: (s := hass.states.get(already.entity_id)) and s.state == "on")
    await _wait(lambda: (s := hass.states.get("sensor.boiler")) and s.state == "21.5")
    (entry,) = hass.config_entries.async_entries(DOMAIN)
    assert entry.source == SOURCE_IMPORT
    issue = ir.async_get(hass).async_get_issue(DOMAIN, "old_yaml_domain")
    assert issue
    assert "sensor: platform ha-asyncua" in issue.translation_placeholders["keys"]

    await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_stop()
