"""Cover platform for ha-asyncua."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.cover import (
    CoverEntity,
    CoverEntityFeature,
)
from homeassistant.const import STATE_UNAVAILABLE
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.config_validation import PLATFORM_SCHEMA
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from . import AsyncuaCoordinator
from .const import (
    CONF_NODE_HUB,
    CONF_NODE_ID_COVER_CLOSE,
    CONF_NODE_ID_COVER_OPEN,
    CONF_NODE_ID_COVER_POSITION,
    CONF_NODE_ID_COVER_SET_POSITION,
    CONF_NODE_ID_COVER_STOP,
    CONF_NODE_NAME,
    CONF_NODE_UNIQUE_ID,
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
            vol.Required(CONF_NODE_HUB): cv.string,
            vol.Required(CONF_NODE_NAME): cv.string,
            vol.Required(CONF_NODE_ID_COVER_POSITION): cv.string,
            vol.Optional(CONF_NODE_ID_COVER_OPEN): cv.string,
            vol.Optional(CONF_NODE_ID_COVER_CLOSE): cv.string,
            vol.Optional(CONF_NODE_ID_COVER_STOP): cv.string,
            vol.Optional(CONF_NODE_ID_COVER_SET_POSITION): cv.string,
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
    """Set up AsyncUA covers from YAML configuration."""
    coordinator_nodes: dict[str, list[dict[str, str]]] = {}
    entities: list = []

    # Regroup nodes by hub
    for node in config[CONF_NODES]:
        coordinator_nodes.setdefault(node[CONF_NODE_HUB], []).append(node)

    for hub_name, nodes in coordinator_nodes.items():
        if hub_name not in hass.data[DOMAIN]:
            msg = f"Asyncua hub {hub_name} not found."
            raise ConfigEntryError(msg)
        coordinator: AsyncuaCoordinator = hass.data[DOMAIN][hub_name]
        coordinator.add_sensors(sensors=nodes)

        for node in nodes:
            entities.append(
                AsyncuaCover(
                    coordinator=coordinator,
                    name=node[CONF_NODE_NAME],
                    hub=hub_name,
                    node_id_position=node[CONF_NODE_ID_COVER_POSITION],
                    node_id_open=node.get(CONF_NODE_ID_COVER_OPEN),
                    node_id_close=node.get(CONF_NODE_ID_COVER_CLOSE),
                    node_id_stop=node.get(CONF_NODE_ID_COVER_STOP),
                    node_id_set_position=node.get(CONF_NODE_ID_COVER_SET_POSITION),
                    unique_id=node.get(CONF_NODE_UNIQUE_ID),
                )
            )

    async_add_entities(entities)


class AsyncuaCover(CoordinatorEntity[AsyncuaCoordinator], CoverEntity):
    """Representation of an OPCUA cover (shutter, blind, gate...)."""

    _attr_supported_features = (
        CoverEntityFeature.OPEN
        | CoverEntityFeature.CLOSE
        | CoverEntityFeature.STOP
        | CoverEntityFeature.SET_POSITION
    )

    def __init__(
        self,
        coordinator: AsyncuaCoordinator,
        name: str,
        hub: str,
        node_id_position: str,
        node_id_open: str | None = None,
        node_id_close: str | None = None,
        node_id_stop: str | None = None,
        node_id_set_position: str | None = None,
        unique_id: str | None = None,
    ) -> None:
        """Initialize the cover entity."""
        super().__init__(coordinator=coordinator)
        self._attr_name = name
        self._attr_unique_id = unique_id or f"{DOMAIN}.{hub}.{node_id_position}"
        self._hub_name = hub
        self._node_id_position = node_id_position
        self._node_id_open = node_id_open
        self._node_id_close = node_id_close
        self._node_id_stop = node_id_stop
        self._node_id_set_position = node_id_set_position
        self._attr_available = STATE_UNAVAILABLE
        self._attr_device_info = DeviceInfo(identifiers={(DOMAIN, hub)})
        self._attr_is_closed: bool | None = None

    @property
    def available(self) -> bool:
        """Return True if the hub is connected."""
        return self.coordinator.hub.connected

    @property
    def current_cover_position(self) -> int | None:
        """Return current position (0-100)."""
        if not self.coordinator.hub.connected:
            return None
        val = self.coordinator.hub.cache_val.get(self._node_id_position)
        try:
            pos = int(val) if val is not None else None
            _LOGGER.debug(
                "Current Cover %s Position is: %s", self._node_id_position, pos
            )
            if pos is not None:
                pos = max(0, min(100, pos))
                self._attr_is_closed = pos == 0  # ✅ Mise à jour de l'état fermé/ouvert
            return pos
        except (ValueError, TypeError):
            _LOGGER.warning(
                "Invalid position value for %s: %s", self._node_id_position, val
            )
            return None

    @property
    def is_closed(self) -> bool | None:
        """Return True if the cover is fully closed."""
        # Si on n'a pas encore de position connue, on retourne la dernière valeur connue
        return self._attr_is_closed

    async def _write_and_refresh(self, nodeid: str, value: Any) -> None:
        """Write a value and refresh state safely."""
        hub = self.coordinator.hub
        if not hub.connected:
            _LOGGER.debug("Hub disconnected, attempting reconnect before write...")
            try:
                await asyncio.wait_for(hub.connect(), timeout=5)
            except Exception as ex:
                _LOGGER.exception("Reconnection failed before write: %s", ex)
                self._attr_available = STATE_UNAVAILABLE
                self.async_write_ha_state()
                return

        try:
            await asyncio.wait_for(hub.set_value(nodeid=nodeid, value=value), timeout=5)
            _LOGGER.debug("Write to node %s succeeded", nodeid)
        except Exception as ex:
            _LOGGER.exception("Write to node %s failed: %s", nodeid, ex)
            await hub.schedule_reconnect()
            self._attr_available = STATE_UNAVAILABLE
            self.async_write_ha_state()
            return

        # Trigger refresh after writing
        asyncio.create_task(self.coordinator.async_request_refresh())

    async def async_open_cover(self, **kwargs: Any) -> None:
        """Open the cover (fallback on position if no dedicated node)."""
        if self._node_id_open:
            await self._write_and_refresh(self._node_id_open, True)
        else:
            _LOGGER.debug("Fallback: setting position of %s to 100", self.name)
            await self.async_set_cover_position(position=100)

    async def async_close_cover(self, **kwargs: Any) -> None:
        """Close the cover (fallback on position if no dedicated node)."""
        if self._node_id_close:
            await self._write_and_refresh(self._node_id_close, True)
        else:
            _LOGGER.debug("Fallback: setting position of %s to 0", self.name)
            await self.async_set_cover_position(position=0)

    async def async_stop_cover(self, **kwargs: Any) -> None:
        """Stop the cover (no-op if no node provided)."""
        if self._node_id_stop:
            await self._write_and_refresh(self._node_id_stop, True)
        else:
            _LOGGER.info(
                "No stop node configured for %s, ignoring stop request", self.name
            )

    async def async_set_cover_position(self, **kwargs: Any) -> None:
        """Set cover position (0-100)."""
        position = kwargs.get("position")
        if position is None:
            return
        try:
            position = max(0, min(100, int(position)))
        except (ValueError, TypeError):
            _LOGGER.exception("Invalid position value for %s: %s", self.name, position)
            return
        await self._write_and_refresh(self._node_id_set_position, position)

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle coordinator data updates."""
        self.async_write_ha_state()
