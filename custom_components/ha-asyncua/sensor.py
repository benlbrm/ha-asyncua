"""Sensor platform for ha-asyncua."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from asyncua import ua
from homeassistant.components.sensor import (
    PLATFORM_SCHEMA as SENSOR_PLATFORM_SCHEMA,
)
from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.core import callback

from .const import (
    CONF_NODE_DEVICE_CLASS,
    CONF_NODE_HUB,
    CONF_NODE_ID,
    CONF_NODE_NAME,
    CONF_NODE_PRECISION,
    CONF_NODE_STATE_CLASS,
    CONF_NODE_UNIQUE_ID,
    CONF_NODE_UNIT_OF_MEASUREMENT,
    CONF_NODES,
)
from .discovery import FLOAT_TYPES, NUMERIC_TYPES
from .entity import (
    AsyncuaConfigEntry,
    AsyncuaEntity,
    async_setup_yaml_platform,
    discovered_for,
    nodeid_validator,
)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

    from .hub import OpcuaHub

PLATFORM_SCHEMA = SENSOR_PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_NODES): [
            {
                vol.Required(CONF_NODE_HUB): cv.string,
                vol.Required(CONF_NODE_NAME): cv.string,
                vol.Required(CONF_NODE_ID): nodeid_validator,
                vol.Optional(CONF_NODE_UNIQUE_ID): cv.string,
                vol.Optional(CONF_NODE_DEVICE_CLASS): vol.Coerce(SensorDeviceClass),
                vol.Optional(CONF_NODE_STATE_CLASS): vol.Coerce(SensorStateClass),
                vol.Optional(CONF_NODE_UNIT_OF_MEASUREMENT): cv.string,
                vol.Optional(CONF_NODE_PRECISION): vol.All(
                    vol.Coerce(int), vol.Range(0, 10)
                ),
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
    """Set up sensors from YAML."""

    def _factory(hub: OpcuaHub, conf: dict[str, Any]) -> AsyncuaSensor:
        unit = conf.get(CONF_NODE_UNIT_OF_MEASUREMENT)
        state_class = conf.get(CONF_NODE_STATE_CLASS)
        if state_class is None and (unit or CONF_NODE_DEVICE_CLASS in conf):
            # Previous versions always used "measurement"; keep it for numeric sensors.
            state_class = SensorStateClass.MEASUREMENT
        return AsyncuaSensor(
            hub,
            conf[CONF_NODE_ID],
            name=conf[CONF_NODE_NAME],
            unique_id=conf.get(CONF_NODE_UNIQUE_ID),
            device_class=conf.get(CONF_NODE_DEVICE_CLASS),
            state_class=state_class,
            unit=unit,
            precision=conf.get(CONF_NODE_PRECISION, 2),
        )

    await async_setup_yaml_platform(hass, config, async_add_entities, _factory)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: AsyncuaConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up discovered sensors."""
    hub = entry.runtime_data.hub
    entities = []
    for var, device, enabled in discovered_for(entry, "sensor"):
        vtype = ua.VariantType[var.vtype]
        numeric = vtype in NUMERIC_TYPES
        entities.append(
            AsyncuaSensor(
                hub,
                var.nodeid,
                name=var.name,
                device_info=device,
                enabled_default=enabled,
                device_class=SensorDeviceClass.TIMESTAMP
                if vtype is ua.VariantType.DateTime
                else None,
                state_class=SensorStateClass.MEASUREMENT if numeric else None,
                unit=var.unit if numeric else None,
                precision=2 if vtype in FLOAT_TYPES else (0 if numeric else None),
            )
        )
    async_add_entities(entities)


class AsyncuaSensor(AsyncuaEntity, SensorEntity):
    """OPC UA sensor."""

    def __init__(
        self,
        hub: OpcuaHub,
        nodeid: str,
        *,
        device_class: SensorDeviceClass | None = None,
        state_class: SensorStateClass | None = None,
        unit: str | None = None,
        precision: int | None = 2,
        **kwargs: Any,
    ) -> None:
        super().__init__(hub, nodeid, **kwargs)
        self._attr_device_class = device_class
        self._attr_state_class = state_class
        self._attr_native_unit_of_measurement = unit
        self._attr_suggested_display_precision = precision

    @callback
    def _update_from_hub(self) -> None:
        value = self.raw_value
        if value is None:
            self._attr_native_value = None
        elif self._attr_device_class is SensorDeviceClass.TIMESTAMP:
            self._attr_native_value = (
                value.replace(tzinfo=UTC)
                if isinstance(value, datetime) and value.tzinfo is None
                else value
            )
        elif isinstance(value, bool):
            self._attr_native_value = int(value)
        elif hasattr(value, "Text"):  # LocalizedText
            self._attr_native_value = value.Text
        elif self._attr_state_class is not None and not isinstance(value, int | float):
            self._attr_native_value = None
        else:
            self._attr_native_value = value
