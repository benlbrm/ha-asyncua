"""Cover platform for ha-asyncua (YAML only)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.cover import (
    ATTR_POSITION,
    CoverDeviceClass,
    CoverEntity,
    CoverEntityFeature,
)
from homeassistant.components.cover import (
    PLATFORM_SCHEMA as COVER_PLATFORM_SCHEMA,
)
from homeassistant.core import callback

from .const import (
    CONF_NODE_DEVICE_CLASS,
    CONF_NODE_HUB,
    CONF_NODE_ID_COVER_CLOSE,
    CONF_NODE_ID_COVER_OPEN,
    CONF_NODE_ID_COVER_POSITION,
    CONF_NODE_ID_COVER_SET_POSITION,
    CONF_NODE_ID_COVER_STOP,
    CONF_NODE_NAME,
    CONF_NODE_PULSE,
    CONF_NODE_UNIQUE_ID,
    CONF_NODES,
)
from .entity import AsyncuaEntity, async_setup_yaml_platform, nodeid_validator

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

    from .hub import OpcuaHub

PLATFORM_SCHEMA = COVER_PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_NODES): [
            {
                vol.Required(CONF_NODE_HUB): cv.string,
                vol.Required(CONF_NODE_NAME): cv.string,
                vol.Required(CONF_NODE_ID_COVER_POSITION): nodeid_validator,
                vol.Optional(CONF_NODE_ID_COVER_OPEN): nodeid_validator,
                vol.Optional(CONF_NODE_ID_COVER_CLOSE): nodeid_validator,
                vol.Optional(CONF_NODE_ID_COVER_STOP): nodeid_validator,
                vol.Optional(CONF_NODE_ID_COVER_SET_POSITION): nodeid_validator,
                vol.Optional(CONF_NODE_DEVICE_CLASS): vol.Coerce(CoverDeviceClass),
                vol.Optional(CONF_NODE_PULSE): vol.All(
                    vol.Coerce(int), vol.Range(10, 10000)
                ),
                vol.Optional(CONF_NODE_UNIQUE_ID): cv.string,
            }
        ]
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Set up covers from YAML."""

    def _factory(hub: OpcuaHub, conf: dict[str, Any]) -> AsyncuaCover:
        return AsyncuaCover(
            hub,
            conf[CONF_NODE_ID_COVER_POSITION],
            name=conf[CONF_NODE_NAME],
            unique_id=conf.get(CONF_NODE_UNIQUE_ID),
            nodeid_open=conf.get(CONF_NODE_ID_COVER_OPEN),
            nodeid_close=conf.get(CONF_NODE_ID_COVER_CLOSE),
            nodeid_stop=conf.get(CONF_NODE_ID_COVER_STOP),
            nodeid_set_position=conf.get(CONF_NODE_ID_COVER_SET_POSITION),
            device_class=conf.get(CONF_NODE_DEVICE_CLASS),
            pulse_ms=conf.get(CONF_NODE_PULSE),
        )

    await async_setup_yaml_platform(hass, config, async_add_entities, _factory)


class AsyncuaCover(AsyncuaEntity, CoverEntity):
    """OPC UA cover: position feedback (0-100) plus optional command nodes."""

    def __init__(
        self,
        hub: OpcuaHub,
        nodeid_position: str,
        *,
        nodeid_open: str | None = None,
        nodeid_close: str | None = None,
        nodeid_stop: str | None = None,
        nodeid_set_position: str | None = None,
        device_class: CoverDeviceClass | None = None,
        pulse_ms: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(hub, nodeid_position, **kwargs)
        self._nodeid_open = nodeid_open
        self._nodeid_close = nodeid_close
        self._nodeid_stop = nodeid_stop
        self._nodeid_set_position = nodeid_set_position
        self._pulse_ms = pulse_ms
        self._attr_device_class = device_class

        features = CoverEntityFeature(0)
        if nodeid_open or nodeid_set_position:
            features |= CoverEntityFeature.OPEN
        if nodeid_close or nodeid_set_position:
            features |= CoverEntityFeature.CLOSE
        if nodeid_stop:
            features |= CoverEntityFeature.STOP
        if nodeid_set_position:
            features |= CoverEntityFeature.SET_POSITION
        self._attr_supported_features = features

    @callback
    def _update_from_hub(self) -> None:
        value = self.raw_value
        try:
            position = None if value is None else max(0, min(100, int(value)))
        except (TypeError, ValueError):
            position = None
        self._attr_current_cover_position = position
        self._attr_is_closed = None if position is None else position == 0

    async def async_open_cover(self, **kwargs: Any) -> None:
        """Open the cover."""
        if self._nodeid_open:
            await self._async_pulse(self._nodeid_open, self._pulse_ms)
        else:
            await self.async_set_cover_position(**{ATTR_POSITION: 100})

    async def async_close_cover(self, **kwargs: Any) -> None:
        """Close the cover."""
        if self._nodeid_close:
            await self._async_pulse(self._nodeid_close, self._pulse_ms)
        else:
            await self.async_set_cover_position(**{ATTR_POSITION: 0})

    async def async_stop_cover(self, **kwargs: Any) -> None:
        """Stop the cover."""
        if self._nodeid_stop:
            await self._async_pulse(self._nodeid_stop, self._pulse_ms)

    async def async_set_cover_position(self, **kwargs: Any) -> None:
        """Move the cover to a position (0-100)."""
        if self._nodeid_set_position is None:
            return
        position = max(0, min(100, int(kwargs[ATTR_POSITION])))
        await self.hub.async_write(self._nodeid_set_position, position)
