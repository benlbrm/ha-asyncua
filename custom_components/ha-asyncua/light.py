"""Light platform for ha-asyncua (YAML only)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.light import (
    PLATFORM_SCHEMA as LIGHT_PLATFORM_SCHEMA,
)
from homeassistant.components.light import ColorMode, LightEntity
from homeassistant.core import callback

from .const import (
    CONF_NODE_HUB,
    CONF_NODE_ID,
    CONF_NODE_ID_LIGHT_OFF,
    CONF_NODE_ID_LIGHT_ON,
    CONF_NODE_NAME,
    CONF_NODE_PULSE,
    CONF_NODE_UNIQUE_ID,
    CONF_NODES,
)
from .entity import AsyncuaEntity, async_setup_yaml_platform, nodeid_validator
from .hub import normalize_nodeid

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

    from .hub import OpcuaHub

PLATFORM_SCHEMA = LIGHT_PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_NODES): [
            {
                vol.Required(CONF_NODE_HUB): cv.string,
                vol.Required(CONF_NODE_NAME): cv.string,
                vol.Required(CONF_NODE_ID): nodeid_validator,
                vol.Optional(CONF_NODE_ID_LIGHT_ON): nodeid_validator,
                vol.Optional(CONF_NODE_ID_LIGHT_OFF): nodeid_validator,
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
    """Set up lights from YAML."""

    def _factory(hub: OpcuaHub, conf: dict[str, Any]) -> AsyncuaLight:
        return AsyncuaLight(
            hub,
            conf[CONF_NODE_ID],
            name=conf[CONF_NODE_NAME],
            unique_id=conf.get(CONF_NODE_UNIQUE_ID),
            nodeid_on=conf.get(CONF_NODE_ID_LIGHT_ON),
            nodeid_off=conf.get(CONF_NODE_ID_LIGHT_OFF),
            pulse_ms=conf.get(CONF_NODE_PULSE),
        )

    await async_setup_yaml_platform(hass, config, async_add_entities, _factory)


class AsyncuaLight(AsyncuaEntity, LightEntity):
    """
    OPC UA on/off light.

    State is read from nodeid. Commands go to nodeid_light_on/off: when such a
    command node is distinct from the state node, True is written to it (a
    command bit, optionally reset after pulse_ms); otherwise the target state
    is written directly to the state node.
    """

    _attr_supported_color_modes = frozenset({ColorMode.ONOFF})
    _attr_color_mode = ColorMode.ONOFF

    def __init__(
        self,
        hub: OpcuaHub,
        nodeid: str,
        *,
        nodeid_on: str | None = None,
        nodeid_off: str | None = None,
        pulse_ms: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(hub, nodeid, **kwargs)
        self._nodeid_on = nodeid_on or nodeid
        self._nodeid_off = nodeid_off or nodeid
        self._pulse_ms = pulse_ms

    @callback
    def _update_from_hub(self) -> None:
        value = self.raw_value
        self._attr_is_on = None if value is None else bool(value)

    def _is_state_node(self, nodeid: str) -> bool:
        return normalize_nodeid(nodeid) == normalize_nodeid(self._state_nodeid)

    async def _command(self, nodeid: str, target: bool) -> None:
        if self._is_state_node(nodeid):
            await self.hub.async_write(nodeid, target)
        else:
            await self._async_pulse(nodeid, self._pulse_ms)

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the light on."""
        await self._command(self._nodeid_on, target=True)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the light off."""
        await self._command(self._nodeid_off, target=False)
