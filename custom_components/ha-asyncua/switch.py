"""Platform for switch integration with subscription-based updates + forced refresh."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.switch import SwitchDeviceClass, SwitchEntity
from homeassistant.const import STATE_OK, STATE_UNAVAILABLE
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.config_validation import PLATFORM_SCHEMA
from homeassistant.helpers.entity import DeviceInfo
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

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

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

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(schema=NODE_SCHEMA, extra=vol.ALLOW_EXTRA)


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Set up asyncua switches from YAML configuration."""
    coordinator_nodes: dict[str, list[dict[str, str]]] = {}
    entities: list = []

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
                AsyncuaSwitch(
                    coordinator=coordinator,
                    name=node[CONF_NODE_NAME],
                    hub=hub_name,
                    node_id=node[CONF_NODE_ID],
                    addr_di=node.get(CONF_NODE_SWITCH_DI),
                    unique_id=node.get(CONF_NODE_UNIQUE_ID),
                )
            )

    async_add_entities(entities)


class AsyncuaSwitch(CoordinatorEntity[AsyncuaCoordinator], SwitchEntity):
    """OPC UA switch using coordinator cache."""

    _attr_device_class = SwitchDeviceClass.SWITCH

    def __init__(
        self,
        coordinator: AsyncuaCoordinator,
        name: str,
        hub: str,
        node_id: str,
        addr_di: str | None = None,
        unique_id: str | None = None,
    ) -> None:
        super().__init__(coordinator=coordinator)
        self._attr_name = name
        self._node_id = node_id
        self._addr_di = addr_di or node_id
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
        val = self.coordinator.hub.cache_val.get(self._addr_di)
        self._attr_is_on = bool(val) if val is not None else None
        self._attr_available = STATE_OK
        return self._attr_is_on

    async def _write_and_refresh(self, value: bool) -> None:
        hub = self.coordinator.hub

        # ensure connection: try quick reconnect if disconnected
        if not hub.connected:
            _LOGGER.debug("Hub disconnected, attempt reconnect before writing switch")
            try:
                await asyncio.wait_for(hub.connect(), timeout=5)
            except Exception as e:
                _LOGGER.exception("Reconnect before write failed: %s", e)
                self._attr_available = STATE_UNAVAILABLE
                self.async_write_ha_state()
                return

        # optimistic update
        self._attr_is_on = value
        self.async_write_ha_state()

        # perform write with timeout
        try:
            await asyncio.wait_for(
                hub.set_value(nodeid=self._node_id, value=value), timeout=5
            )
        except Exception as e:
            _LOGGER.exception("Write failed for %s: %s", self._attr_name, e)
            asyncio.create_task(hub.schedule_reconnect())
            self._attr_available = STATE_UNAVAILABLE
            self.async_write_ha_state()
            return

        # delayed refresh for eventual subscription push
        async def delayed_refresh() -> None:
            await asyncio.sleep(0.5)
            await self.coordinator.async_request_refresh()

        asyncio.create_task(delayed_refresh())

    async def async_turn_on(self, **kwargs: Any) -> None:
        await self._write_and_refresh(True)

    async def async_turn_off(self, **kwargs: Any) -> None:
        await self._write_and_refresh(False)
