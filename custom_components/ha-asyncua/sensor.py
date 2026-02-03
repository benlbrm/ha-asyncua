"""Sensor platform for ha-asyncua."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.sensor import SensorDeviceClass, SensorEntity
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.config_validation import PLATFORM_SCHEMA
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import AsyncuaCoordinator
from .const import (
    CONF_NODE_DEVICE_CLASS,
    CONF_NODE_HUB,
    CONF_NODE_ID,
    CONF_NODE_NAME,
    CONF_NODE_STATE_CLASS,
    CONF_NODE_UNIQUE_ID,
    CONF_NODE_UNIT_OF_MEASUREMENT,
    CONF_NODES,
    DOMAIN,
)

if TYPE_CHECKING:
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

_LOGGER = logging.getLogger(__name__)

NODE_SCHEMA = {
    CONF_NODES: [
        {
            vol.Optional(CONF_NODE_DEVICE_CLASS): cv.string,
            vol.Optional(CONF_NODE_STATE_CLASS, default="measurement"): cv.string,
            vol.Optional(CONF_NODE_UNIT_OF_MEASUREMENT): cv.string,
            vol.Optional(CONF_NODE_UNIQUE_ID): cv.string,
            vol.Required(CONF_NODE_ID): cv.string,
            vol.Required(CONF_NODE_NAME): cv.string,
            vol.Required(CONF_NODE_HUB): cv.string,
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
    """Set up asyncua sensors from YAML configuration."""
    coordinator_nodes: dict[str, list[dict[str, str]]] = {}
    entities: list[AsyncuaSensor] = []

    # Regrouper les nodes par hub
    for val_node in config[CONF_NODES]:
        coordinator_nodes.setdefault(val_node[CONF_NODE_HUB], []).append(val_node)

    for hub_name, nodes in coordinator_nodes.items():
        if hub_name not in hass.data[DOMAIN]:
            msg = f"Asyncua hub {hub_name} not found."
            raise ConfigEntryError(msg)
        coordinator: AsyncuaCoordinator = hass.data[DOMAIN][hub_name]
        coordinator.add_sensors(sensors=nodes)

        for node in nodes:
            entities.append(
                AsyncuaSensor(
                    coordinator=coordinator,
                    name=node[CONF_NODE_NAME],
                    hub=hub_name,
                    node_id=node[CONF_NODE_ID],
                    device_class=node.get(CONF_NODE_DEVICE_CLASS),
                    state_class=node.get(CONF_NODE_STATE_CLASS, "measurement"),
                    unique_id=node.get(CONF_NODE_UNIQUE_ID),
                    unit_of_measurement=node.get(CONF_NODE_UNIT_OF_MEASUREMENT),
                )
            )

    async_add_entities(entities)


class AsyncuaSensor(CoordinatorEntity[AsyncuaCoordinator], SensorEntity):
    """Representation of an OPCUA sensor."""

    def __init__(
        self,
        coordinator: AsyncuaCoordinator,
        name: str,
        hub: str,
        node_id: str,
        device_class: str | None = None,
        state_class: str = "measurement",
        unique_id: str | None = None,
        precision: int = 2,
        unit_of_measurement: str | None = None,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator=coordinator)
        self._attr_name = name
        self._attr_unique_id = unique_id or f"{DOMAIN}.{hub}.{node_id}"
        self._attr_device_class = (
            SensorDeviceClass(device_class)
            if device_class and device_class in SensorDeviceClass.__members__.values()
            else None
        )
        self._attr_state_class = state_class
        self._attr_suggested_display_precision = precision
        self._attr_native_unit_of_measurement = unit_of_measurement
        self._attr_native_value: Any = None
        self._hub_name = hub
        self._node_id = node_id
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, hub)})

    @property
    def available(self) -> bool:
        """Return True if the hub is connected and the node has a value."""
        return self.coordinator.hub.connected

    @property
    def native_value(self) -> Any:
        """Return the cached OPCUA value for this node."""
        value = self.coordinator.hub.cache_val.get(self._node_id)
        self._attr_native_value = value
        return value

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from coordinator (triggered by subscription)."""
        _LOGGER.debug(
            "Sensor %s (%s) updated with value: %s",
            self._attr_name,
            self._node_id,
            self.coordinator.hub.cache_val.get(self._node_id),
        )
        self.async_write_ha_state()
