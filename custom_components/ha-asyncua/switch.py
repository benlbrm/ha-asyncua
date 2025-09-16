"""Platform for switch integration with subscription-based updates + forced refresh."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.switch import SwitchDeviceClass, SwitchEntity
from homeassistant.const import STATE_OK, STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.config_validation import PLATFORM_SCHEMA
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import AsyncuaCoordinator
from .const import (
    CONF_NODE_HUB,
    CONF_NODE_ID,
    CONF_NODE_NAME,
    CONF_NODE_SWITCH_DI,
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
            vol.Optional(CONF_NODE_SWITCH_DI): cv.string,
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
    """Set up asyncua_switch coordinator_nodes."""
    coordinator_nodes: dict[str, list[dict[str, str]]] = {}
    coordinators: dict[str, AsyncuaCoordinator] = {}
    asyncua_switches: list = []

    for val_node in config[CONF_NODES]:
        coordinator_nodes.setdefault(val_node[CONF_NODE_HUB], []).append(val_node)

    for hub_name, nodes in coordinator_nodes.items():
        if hub_name not in hass.data[DOMAIN]:
            msg = f"Asyncua hub {hub_name} not found. Specify a valid asyncua hub in the configuration."
            raise ConfigEntryError(msg)
        coordinators[hub_name] = hass.data[DOMAIN][hub_name]
        coordinators[hub_name].add_sensors(sensors=nodes)

        for val_sensor in nodes:
            asyncua_switches.append(
                AsyncuaSwitch(
                    coordinator=coordinators[hub_name],
                    name=val_sensor[CONF_NODE_NAME],
                    hub=val_sensor[CONF_NODE_HUB],
                    node_id=val_sensor[CONF_NODE_ID],
                    addr_di=val_sensor.get(CONF_NODE_SWITCH_DI),
                    unique_id=val_sensor.get(CONF_NODE_UNIQUE_ID),
                )
            )

    async_add_entities(asyncua_switches)
    # for idx_switch, switch in enumerate(asyncua_switches):
    #     await switch.async_init()
    #     _LOGGER.debug("Initialized switch %s - %s", idx_switch, switch.attr_name)


class AsyncuaSwitch(SwitchEntity, CoordinatorEntity[AsyncuaCoordinator]):
    """A switch implementation for Asyncua OPCUA nodes with subscription cache."""

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
    def attr_name(self) -> str:
        """Return switch name."""
        return self._attr_name

    @property
    def is_on(self) -> bool | None:
        """Return the current state from subscription cache."""
        if not self.coordinator.hub.connected:
            self._attr_available = STATE_UNAVAILABLE
            return None

        val = self.coordinator.hub.cache_val.get(self._addr_di)
        self._attr_is_on = bool(val) if val is not None else None
        self._attr_available = STATE_OK
        return self._attr_is_on

    async def _async_set_value(self, val: bool) -> None:
        """Write value to OPCUA node and refresh state if no notification is received."""
        await self.coordinator.hub.set_value(nodeid=self._node_id, value=val)

        # 1. On écrit l'état dans HA immédiatement pour réactivité
        self._attr_is_on = val
        self.async_write_ha_state()

        # 2. On force un refresh manuel après un léger délai
        #    pour synchroniser si le serveur n'a pas émis de DataChange
        async def delayed_refresh() -> None:
            await asyncio.sleep(0.5)  # délai très court (500 ms)
            await self.coordinator.async_request_refresh()

        # await asyncio.create_task(delayed_refresh())

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the switch on."""
        await self._async_set_value(True)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the switch off."""
        await self._async_set_value(False)
