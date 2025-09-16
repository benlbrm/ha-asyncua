"""Binary sensor platform for ha-asyncua."""

from __future__ import annotations

import logging

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.components.switch import SwitchDeviceClass
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

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    schema=NODE_SCHEMA,
    extra=vol.ALLOW_EXTRA,
)


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Set up asyncua_Binary_Sensor coordinator_nodes."""
    coordinator_nodes: dict[str, list[dict[str, str]]] = {}
    coordinators: dict[str, AsyncuaCoordinator] = {}
    entities: list = []

    for val_node in config[CONF_NODES]:
        coordinator_nodes.setdefault(val_node[CONF_NODE_HUB], []).append(val_node)

    for hub_name, nodes in coordinator_nodes.items():
        if hub_name not in hass.data[DOMAIN]:
            msg = f"Asyncua hub {hub_name} not found. Specify a valid asyncua hub in the configuration."
            raise ConfigEntryError(msg)
        coordinators[hub_name] = hass.data[DOMAIN][hub_name]
        coordinators[hub_name].add_sensors(sensors=nodes)

        for val_sensor in nodes:
            entities.append(
                AsyncuaBinarySensor(
                    coordinator=coordinators[hub_name],
                    name=val_sensor[CONF_NODE_NAME],
                    hub=val_sensor[CONF_NODE_HUB],
                    node_id=val_sensor[CONF_NODE_ID],
                    unique_id=val_sensor.get(CONF_NODE_UNIQUE_ID),
                )
            )

    async_add_entities(entities)


class AsyncuaBinarySensor(BinarySensorEntity, CoordinatorEntity[AsyncuaCoordinator]):
    """Representation of an OPCUA binary sensor."""

    def __init__(
        self,
        coordinator: AsyncuaCoordinator,
        name: str,
        hub: str,
        node_id: str,
        addr_di: str | None = None,
        unique_id: str | None = None,
    ) -> None:
        """Initialize the switch."""
        super().__init__(coordinator=coordinator)
        self._attr_name = name
        self._attr_unique_id = (
            unique_id if unique_id is not None else f"{DOMAIN}.{hub}.{node_id}"
        )
        self._attr_available = STATE_UNAVAILABLE
        self._attr_device_class = SwitchDeviceClass.SWITCH
        self._attr_is_on: bool | None = None
        self._hub = hub
        self._coordinator = coordinator
        self._node_id = node_id
        self._addr_di = addr_di if addr_di is not None else node_id

    @property
    def is_on(self) -> bool:
        """Return True if sensor is ON (value truthy)."""
        val = self._coordinator.hub.cache_val.get(self._node_id)
        if val is None:
            return False
        try:
            return bool(val)
        except Exception:
            _LOGGER.warning(
                "Cannot convert value %s of node %s to bool", val, self._node_id
            )
            return False

    @property
    def available(self) -> bool:
        """Return True if hub is connected."""
        return self._coordinator.hub.connected
