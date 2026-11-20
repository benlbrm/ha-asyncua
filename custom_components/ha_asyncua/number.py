"""Number platform for ha_asyncua."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from asyncua import ua
from homeassistant.components.number import (
    PLATFORM_SCHEMA as NUMBER_PLATFORM_SCHEMA,
)
from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.core import callback

from .const import (
    CONF_NODE_HUB,
    CONF_NODE_ID,
    CONF_NODE_ID_NUMBER_MAX,
    CONF_NODE_ID_NUMBER_MIN,
    CONF_NODE_ID_NUMBER_MODE,
    CONF_NODE_ID_NUMBER_STEP,
    CONF_NODE_NAME,
    CONF_NODE_UNIQUE_ID,
    CONF_NODE_UNIT_OF_MEASUREMENT,
    CONF_NODES,
)
from .discovery import FLOAT_TYPES
from .entity import (
    AsyncuaConfigEntry,
    AsyncuaEntity,
    async_setup_yaml_platform,
    discovered_for,
    nodeid_validator,
)
from .hub import _INT_RANGES

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

    from .hub import OpcuaHub

PLATFORM_SCHEMA = NUMBER_PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_NODES): [
            {
                vol.Required(CONF_NODE_HUB): cv.string,
                vol.Required(CONF_NODE_NAME): cv.string,
                vol.Required(CONF_NODE_ID): nodeid_validator,
                vol.Optional(CONF_NODE_UNIQUE_ID): cv.string,
                vol.Optional(CONF_NODE_UNIT_OF_MEASUREMENT): cv.string,
                vol.Optional(CONF_NODE_ID_NUMBER_MIN): vol.Coerce(float),
                vol.Optional(CONF_NODE_ID_NUMBER_MAX): vol.Coerce(float),
                vol.Optional(CONF_NODE_ID_NUMBER_STEP): vol.Coerce(float),
                vol.Optional("min_value"): vol.Coerce(float),
                vol.Optional("max_value"): vol.Coerce(float),
                vol.Optional("step"): vol.Coerce(float),
                vol.Optional("mode"): vol.Coerce(NumberMode),
                vol.Optional(CONF_NODE_ID_NUMBER_MODE, default="auto"): vol.Coerce(
                    NumberMode
                ),
            }
        ]
    },
    extra=vol.ALLOW_EXTRA,
)

# Bounds used for float variables without a usable EURange.
FLOAT_BOUND = 1e9


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Set up numbers from YAML."""

    def _factory(hub: OpcuaHub, conf: dict[str, Any]) -> AsyncuaNumber:
        return AsyncuaNumber(
            hub,
            conf[CONF_NODE_ID],
            name=conf[CONF_NODE_NAME],
            unique_id=conf.get(CONF_NODE_UNIQUE_ID),
            unit=conf.get(CONF_NODE_UNIT_OF_MEASUREMENT),
            # README keys (min_value...) and legacy nodeid_number_* keys are both accepted.
            min_value=conf.get("min_value", conf.get(CONF_NODE_ID_NUMBER_MIN)),
            max_value=conf.get("max_value", conf.get(CONF_NODE_ID_NUMBER_MAX)),
            step=conf.get("step", conf.get(CONF_NODE_ID_NUMBER_STEP)),
            mode=conf.get("mode", conf.get(CONF_NODE_ID_NUMBER_MODE, NumberMode.AUTO)),
        )

    await async_setup_yaml_platform(
        hass,
        config,
        async_add_entities,
        _factory,
        discovery_info=discovery_info,
        schema=PLATFORM_SCHEMA,
    )


async def async_setup_entry(
    hass: HomeAssistant,
    entry: AsyncuaConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up discovered numbers."""
    hub = entry.runtime_data.hub
    entities = []
    for var, device, enabled in discovered_for(entry, "number"):
        vtype = ua.VariantType[var.vtype]
        if vtype in FLOAT_TYPES:
            low, high, step = -FLOAT_BOUND, FLOAT_BOUND, 0.001
        else:
            low, high = _INT_RANGES[vtype]
            step = 1.0
        entities.append(
            AsyncuaNumber(
                hub,
                var.nodeid,
                name=var.name,
                device_info=device,
                enabled_default=enabled,
                unit=var.unit,
                min_value=var.min_value if var.min_value is not None else float(low),
                max_value=var.max_value if var.max_value is not None else float(high),
                step=step,
                mode=NumberMode.BOX,
            )
        )
    async_add_entities(entities)


class AsyncuaNumber(AsyncuaEntity, NumberEntity):
    """OPC UA number (setpoint)."""

    def __init__(
        self,
        hub: OpcuaHub,
        nodeid: str,
        *,
        unit: str | None = None,
        min_value: float | None = None,
        max_value: float | None = None,
        step: float | None = None,
        mode: NumberMode = NumberMode.AUTO,
        **kwargs: Any,
    ) -> None:
        super().__init__(hub, nodeid, **kwargs)
        self._attr_native_unit_of_measurement = unit
        if min_value is not None:
            self._attr_native_min_value = min_value
        if max_value is not None:
            self._attr_native_max_value = max_value
        if step is not None:
            self._attr_native_step = step
        self._attr_mode = mode

    @callback
    def _update_from_hub(self) -> None:
        value = self.raw_value
        try:
            self._attr_native_value = None if value is None else float(value)
        except TypeError, ValueError:
            self._attr_native_value = None

    async def async_set_native_value(self, value: float) -> None:
        """Write the value (converted to the node data type by the hub)."""
        await self.hub.async_write(self._state_nodeid, value)
