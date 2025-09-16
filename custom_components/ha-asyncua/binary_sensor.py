"""Binary sensor platform for ha-asyncua."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.binary_sensor import (
    BinarySensorEntity,
    BinarySensorDeviceClass,
)
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
    """Set up asyncua Binary Sensors from YAML configuration."""
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
                AsyncuaBinarySensor(
                    coordinator=coordinator,
                    name=node[CONF_NODE_NAME],
                    hub=hub_name,
                    node_id=node[CONF_NODE_ID],
                    unique_id=node.get(CONF_NODE_UNIQUE_ID),
                )
            )

    async_add_entities(entities)


class AsyncuaBinarySensor(CoordinatorEntity[AsyncuaCoordinator], BinarySensorEntity):
    """OPC UA Binary sensor using coordinator cache."""

    _attr_device_class = BinarySensorDeviceClass.OCCUPANCY

    def __init__(
        self,
        coordinator: AsyncuaCoordinator,
        name: str,
        hub: str,
        node_id: str,
        unique_id: str | None = None,
    ) -> None:
        super().__init__(coordinator=coordinator)
        self._attr_name = name
        self._node_id = node_id
        self._attr_unique_id = unique_id or f"{DOMAIN}.{hub}.{node_id}"
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, hub)})
        self._attr_available = STATE_UNAVAILABLE
        self._attr_is_on: bool | None = None

    @property
    def available(self) -> bool:
        """Availability based on hub connection."""
        return self.coordinator.hub.connected

    @property
    def is_on(self) -> bool | None:
        """Return True if the cached value is truthy."""
        if not self.coordinator.hub.connected:
            self._attr_available = STATE_UNAVAILABLE
            return None
        val = self.coordinator.hub.cache_val.get(self._node_id)
        try:
            self._attr_is_on = bool(val) if val is not None else False
        except Exception:
            _LOGGER.warning("Cannot convert %s to bool for node %s", val, self._node_id)
            self._attr_is_on = False
        self._attr_available = STATE_OK
        return self._attr_is_on

    async def async_added_to_hass(self) -> None:
        """Request coordinator refresh when added."""
        await self.coordinator.async_request_refresh()
