"""Light platform for ha-asyncua."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.light import LightEntity, ColorMode
from homeassistant.const import STATE_OK, STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.config_validation import PLATFORM_SCHEMA
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import AsyncuaCoordinator
from .const import (
    CONF_NODE_HUB,
    CONF_NODE_ID,
    CONF_NODE_ID_LIGHT_ON,
    CONF_NODE_ID_LIGHT_OFF,
    CONF_NODE_NAME,
    CONF_NODE_UNIQUE_ID,
    CONF_NODES,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

NODE_SCHEMA = {
    CONF_NODES: [
        {
            vol.Required(CONF_NODE_HUB): cv.string,
            vol.Required(CONF_NODE_NAME): cv.string,
            vol.Required(CONF_NODE_ID): cv.string,
            vol.Required(CONF_NODE_ID_LIGHT_ON): cv.string,
            vol.Required(CONF_NODE_ID_LIGHT_OFF): cv.string,
            vol.Optional(CONF_NODE_UNIQUE_ID): cv.string,
        }
    ]
}

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(schema=NODE_SCHEMA, extra=vol.ALLOW_EXTRA)


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Set up asyncua Light coordinator_nodes from YAML configuration."""
    coordinator_nodes: dict[str, list[dict[str, str]]] = {}
    entities: list = []

    for val_node in config[CONF_NODES]:
        coordinator_nodes.setdefault(val_node[CONF_NODE_HUB], []).append(val_node)

    for hub_name, nodes in coordinator_nodes.items():
        if hub_name not in hass.data[DOMAIN]:
            raise ConfigEntryError(f"Asyncua hub {hub_name} not found.")
        coordinator: AsyncuaCoordinator = hass.data[DOMAIN][hub_name]
        coordinator.add_sensors(sensors=nodes)

        for node in nodes:
            entities.append(
                AsyncuaLight(
                    coordinator=coordinator,
                    name=node[CONF_NODE_NAME],
                    hub=hub_name,
                    node_id=node[CONF_NODE_ID],
                    node_id_on=node.get(CONF_NODE_ID_LIGHT_ON),
                    node_id_off=node.get(CONF_NODE_ID_LIGHT_OFF),
                    unique_id=node.get(CONF_NODE_UNIQUE_ID),
                )
            )

    async_add_entities(entities)


class AsyncuaLight(CoordinatorEntity[AsyncuaCoordinator], LightEntity):
    """OPC UA Light using coordinator cache."""

    _attr_supported_color_modes = {ColorMode.ONOFF}
    _attr_color_mode = ColorMode.ONOFF

    def __init__(
        self,
        coordinator: AsyncuaCoordinator,
        name: str,
        hub: str,
        node_id: str,
        node_id_on: str | None = None,
        node_id_off: str | None = None,
        unique_id: str | None = None,
    ) -> None:
        super().__init__(coordinator=coordinator)
        self._attr_name = name
        self._node_id = node_id
        self._node_id_on = node_id_on or node_id
        self._node_id_off = node_id_off or node_id
        self._attr_unique_id = unique_id or f"{DOMAIN}.{hub}.{node_id}"
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, hub)})
        self._attr_available = STATE_UNAVAILABLE
        self._attr_is_on: bool | None = None

    @property
    def available(self) -> bool:
        return self.coordinator.hub.connected

    @property
    def is_on(self) -> bool | None:
        if not self.coordinator.hub.connected:
            self._attr_available = STATE_UNAVAILABLE
            return None
        val = self.coordinator.hub.cache_val.get(self._node_id)
        self._attr_is_on = bool(val) if val is not None else False
        self._attr_available = STATE_OK
        return self._attr_is_on

    async def _write_and_refresh(self, nodeid: str, value: bool) -> None:
        hub = self.coordinator.hub
        if not hub.connected:
            _LOGGER.debug("Hub disconnected, attempting reconnect before write")
            try:
                await asyncio.wait_for(hub.connect(), timeout=5)
            except Exception as e:
                _LOGGER.error("Reconnect before write failed: %s", e)
                self._attr_available = STATE_UNAVAILABLE
                self.async_write_ha_state()
                return

        # optimistic UI update
        self._attr_is_on = value
        self.async_write_ha_state()

        try:
            await asyncio.wait_for(hub.set_value(nodeid=nodeid, value=value), timeout=5)
        except Exception as e:
            _LOGGER.error("Write failed for node %s: %s", nodeid, e)
            asyncio.create_task(hub.schedule_reconnect())
            self._attr_available = STATE_UNAVAILABLE
            self.async_write_ha_state()
            return

        # delayed refresh to pick up any subscription notification
        async def delayed_refresh() -> None:
            await asyncio.sleep(0.5)
            await self.coordinator.async_request_refresh()

        asyncio.create_task(delayed_refresh())

    async def async_turn_on(self, **kwargs: Any) -> None:
        _LOGGER.debug("Turning ON %s", self.name)
        await self._write_and_refresh(nodeid=self._node_id_on, value=True)

    async def async_turn_off(self, **kwargs: Any) -> None:
        _LOGGER.debug("Turning OFF %s", self.name)
        await self._write_and_refresh(nodeid=self._node_id_off, value=True)
