"""Light platform for ha-asyncua."""

from __future__ import annotations

import logging
import asyncio
from typing import Any
import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.light import LightEntity

# from homeassistant.components.light import LightDeviceClass
from homeassistant.components.light import ColorMode
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
    """Set up asyncua_Light coordinator_nodes."""
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
                AsyncuaLight(
                    coordinator=coordinators[hub_name],
                    name=val_sensor[CONF_NODE_NAME],
                    hub=val_sensor[CONF_NODE_HUB],
                    node_id=val_sensor[CONF_NODE_ID],
                    node_id_on=val_sensor[CONF_NODE_ID_LIGHT_ON],
                    node_id_off=val_sensor[CONF_NODE_ID_LIGHT_OFF],
                    unique_id=val_sensor.get(CONF_NODE_UNIQUE_ID),
                )
            )

    async_add_entities(entities)


class AsyncuaLight(LightEntity, CoordinatorEntity[AsyncuaCoordinator]):
    """Representation of an OPCUA light."""

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
        """Initialize the light."""
        super().__init__(coordinator=coordinator)
        self._attr_name = name
        self._attr_unique_id = (
            unique_id if unique_id is not None else f"{DOMAIN}.{hub}.{node_id}"
        )
        self._attr_available = STATE_UNAVAILABLE
        self._attr_is_on: bool | None = None
        self._hub = hub
        self._coordinator = coordinator
        self._node_id = node_id
        self._node_id_on = node_id_on if node_id_on is not None else node_id
        self._node_id_off = node_id_off if node_id_off is not None else node_id

    @property
    def attr_name(self) -> str:
        """Return switch name."""
        return self._attr_name

    @property
    def is_on(self) -> bool | None:
        """Return the current state from subscription cache."""
        if not self.coordinator.hub.connected:
            self._attr_available = STATE_UNAVAILABLE
            return None

        val = self.coordinator.hub.cache_val.get(self._node_id)
        self._attr_is_on = bool(val) if val is not None else None
        self._attr_available = STATE_OK
        return self._attr_is_on

    async def async_init(self) -> None:
        """Initialize light state from OPCUA node."""
        await self._async_refresh_state()

    async def _async_refresh_state(self) -> None:
        """Read current state from OPCUA server."""
        # 1. On écrit l'état dans HA immédiatement pour réactivité
        val = self._coordinator.hub.cache_val.get(self._node_id)
        self._attr_is_on = bool(val) if val is not None else False
        self._attr_available = (
            STATE_OK if self._coordinator.hub.connected else STATE_UNAVAILABLE
        )
        self.async_write_ha_state()

        # 2. On force un refresh manuel après un léger délai
        #    pour synchroniser si le serveur n'a pas émis de DataChange
        async def delayed_refresh() -> None:
            await asyncio.sleep(0.5)  # délai très court (500 ms)
            await self.coordinator.async_request_refresh()

    # @property
    # def is_on(self) -> bool | None:
    #     """Return True if the light is ON."""
    #     return self._attr_is_on

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the light on via OPCUA write."""
        await self._coordinator.hub.set_value(nodeid=self._node_id_on, value=True)
        await self._async_refresh_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the light off via OPCUA write."""
        await self._coordinator.hub.set_value(nodeid=self._node_id_off, value=True)
        await self._async_refresh_state()
