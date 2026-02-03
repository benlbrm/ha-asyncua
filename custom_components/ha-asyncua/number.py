"""Number platform for ha-asyncua."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.number import NumberEntity, NumberMode
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
    CONF_NODE_ID_NUMBER_MAX,
    CONF_NODE_ID_NUMBER_MIN,
    CONF_NODE_ID_NUMBER_MODE,
    CONF_NODE_ID_NUMBER_STEP,
    CONF_NODE_NAME,
    CONF_NODE_UNIQUE_ID,
    CONF_NODE_UNIT_OF_MEASUREMENT,
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
            vol.Optional(CONF_NODE_UNIQUE_ID): cv.string,
            vol.Optional(CONF_NODE_UNIT_OF_MEASUREMENT): cv.string,
            vol.Optional(CONF_NODE_ID_NUMBER_MIN): vol.Coerce(float),
            vol.Optional(CONF_NODE_ID_NUMBER_MAX): vol.Coerce(float),
            vol.Optional(CONF_NODE_ID_NUMBER_STEP): vol.Coerce(float),
            vol.Optional(CONF_NODE_ID_NUMBER_MODE, default="auto"): cv.string,
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
    """Set up asyncua numbers from YAML configuration."""
    coordinator_nodes: dict[str, list[dict[str, str]]] = {}
    entities: list[AsyncuaNumber] = []

    for val_node in config[CONF_NODES]:
        coordinator_nodes.setdefault(val_node[CONF_NODE_HUB], []).append(val_node)

    for hub_name, nodes in coordinator_nodes.items():
        if hub_name not in hass.data[DOMAIN]:
            raise ConfigEntryError(f"Asyncua hub {hub_name} not found.")
        coordinator: AsyncuaCoordinator = hass.data[DOMAIN][hub_name]
        coordinator.add_sensors(sensors=nodes)

        for node in nodes:
            entities.append(
                AsyncuaNumber(
                    coordinator=coordinator,
                    name=node[CONF_NODE_NAME],
                    hub=hub_name,
                    node_id=node[CONF_NODE_ID],
                    unique_id=node.get(CONF_NODE_UNIQUE_ID),
                    unit_of_measurement=node.get(CONF_NODE_UNIT_OF_MEASUREMENT),
                    min_value=node.get(CONF_NODE_ID_NUMBER_MIN),
                    max_value=node.get(CONF_NODE_ID_NUMBER_MAX),
                    step=node.get(CONF_NODE_ID_NUMBER_STEP),
                    mode=node.get(CONF_NODE_ID_NUMBER_MODE, "auto"),
                )
            )

    async_add_entities(entities)


class AsyncuaNumber(CoordinatorEntity[AsyncuaCoordinator], NumberEntity):
    """OPC UA number entity using coordinator cache."""

    def __init__(
        self,
        coordinator: AsyncuaCoordinator,
        name: str,
        hub: str,
        node_id: str,
        unique_id: str | None = None,
        unit_of_measurement: str | None = None,
        min_value: float | None = None,
        max_value: float | None = None,
        step: float | None = None,
        mode: str = "auto",
    ) -> None:
        super().__init__(coordinator=coordinator)
        self._attr_name = name
        self._node_id = node_id
        self._attr_unique_id = unique_id or f"{DOMAIN}.{hub}.{node_id}"
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, hub)})
        self._attr_available = STATE_UNAVAILABLE
        self._attr_native_value: float | None = None
        self._attr_native_unit_of_measurement = unit_of_measurement

        # Number specific attributes
        if min_value is not None:
            self._attr_native_min_value = min_value
        if max_value is not None:
            self._attr_native_max_value = max_value
        if step is not None:
            self._attr_native_step = step

        # Mode: auto, box, or slider
        if mode == "box":
            self._attr_mode = NumberMode.BOX
        elif mode == "slider":
            self._attr_mode = NumberMode.SLIDER
        else:
            self._attr_mode = NumberMode.AUTO

    @property
    def available(self) -> bool:
        return self.coordinator.hub.connected

    @property
    def native_value(self) -> float | None:
        if not self.coordinator.hub.connected:
            self._attr_available = STATE_UNAVAILABLE
            return None
        val = self.coordinator.hub.cache_val.get(self._node_id)
        if val is not None:
            try:
                self._attr_native_value = float(val)
            except (ValueError, TypeError):
                self._attr_native_value = None
        self._attr_available = STATE_OK
        return self._attr_native_value

    async def async_set_native_value(self, value: float) -> None:
        """Set the value of the number on the OPC UA server."""
        hub = self.coordinator.hub

        # Ensure connection
        if not hub.connected:
            _LOGGER.debug("Hub disconnected, attempting reconnect before writing number")
            try:
                await asyncio.wait_for(hub.connect(), timeout=5)
            except Exception as e:
                _LOGGER.error("Reconnect before write failed: %s", e)
                self._attr_available = STATE_UNAVAILABLE
                self.async_write_ha_state()
                return

        # Optimistic update
        self._attr_native_value = value
        self.async_write_ha_state()

        # Perform write with timeout
        try:
            await asyncio.wait_for(
                hub.set_value(nodeid=self._node_id, value=value), timeout=5
            )
        except Exception as e:
            _LOGGER.error("Write failed for %s: %s", self._attr_name, e)
            asyncio.create_task(hub.schedule_reconnect())
            self._attr_available = STATE_UNAVAILABLE
            self.async_write_ha_state()
            return

        # Delayed refresh for eventual subscription push
        async def delayed_refresh() -> None:
            await asyncio.sleep(0.5)
            await self.coordinator.async_request_refresh()

        asyncio.create_task(delayed_refresh())
