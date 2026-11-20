"""Switch platform for ha-asyncua."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.switch import (
    PLATFORM_SCHEMA as SWITCH_PLATFORM_SCHEMA,
)
from homeassistant.components.switch import SwitchDeviceClass, SwitchEntity
from homeassistant.core import callback

from .const import (
    CONF_NODE_HUB,
    CONF_NODE_ID,
    CONF_NODE_NAME,
    CONF_NODE_SWITCH_DI,
    CONF_NODE_UNIQUE_ID,
    CONF_NODES,
)
from .entity import (
    AsyncuaConfigEntry,
    AsyncuaEntity,
    async_setup_yaml_platform,
    default_unique_id,
    discovered_for,
    nodeid_validator,
)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

    from .hub import OpcuaHub

PLATFORM_SCHEMA = SWITCH_PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_NODES): [
            {
                vol.Required(CONF_NODE_HUB): cv.string,
                vol.Required(CONF_NODE_NAME): cv.string,
                vol.Required(CONF_NODE_ID): nodeid_validator,
                vol.Optional(CONF_NODE_SWITCH_DI): nodeid_validator,
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
    """Set up switches from YAML."""

    def _factory(hub: OpcuaHub, conf: dict[str, Any]) -> AsyncuaSwitch:
        return AsyncuaSwitch(
            hub,
            command_nodeid=conf[CONF_NODE_ID],
            state_nodeid=conf.get(CONF_NODE_SWITCH_DI),
            name=conf[CONF_NODE_NAME],
            unique_id=conf.get(CONF_NODE_UNIQUE_ID),
        )

    await async_setup_yaml_platform(hass, config, async_add_entities, _factory)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: AsyncuaConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up discovered switches."""
    hub = entry.runtime_data.hub
    async_add_entities(
        AsyncuaSwitch(
            hub,
            command_nodeid=var.nodeid,
            name=var.name,
            device_info=device,
            enabled_default=enabled,
        )
        for var, device, enabled in discovered_for(entry, "switch")
    )


class AsyncuaSwitch(AsyncuaEntity, SwitchEntity):
    """
    OPC UA switch.

    The command is written to command_nodeid; the state is read from
    state_nodeid (a feedback input, e.g. a contactor auxiliary contact) when
    configured, otherwise from the command node itself.
    """

    _attr_device_class = SwitchDeviceClass.SWITCH

    def __init__(
        self,
        hub: OpcuaHub,
        *,
        command_nodeid: str,
        state_nodeid: str | None = None,
        unique_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            hub,
            state_nodeid or command_nodeid,
            unique_id=unique_id or default_unique_id(hub.name, command_nodeid),
            **kwargs,
        )
        self._command_nodeid = command_nodeid

    @callback
    def _update_from_hub(self) -> None:
        value = self.raw_value
        self._attr_is_on = None if value is None else bool(value)

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn the switch on."""
        await self.hub.async_write(self._command_nodeid, True)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn the switch off."""
        await self.hub.async_write(self._command_nodeid, False)
