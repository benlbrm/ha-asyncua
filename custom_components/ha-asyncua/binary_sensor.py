"""Binary sensor platform for ha-asyncua."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.components.binary_sensor import (
    PLATFORM_SCHEMA as BINARY_SENSOR_PLATFORM_SCHEMA,
)
from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.const import EntityCategory
from homeassistant.core import callback

from .const import (
    CONF_NODE_DEVICE_CLASS,
    CONF_NODE_HUB,
    CONF_NODE_ID,
    CONF_NODE_NAME,
    CONF_NODE_UNIQUE_ID,
    CONF_NODES,
)
from .entity import (
    AsyncuaConfigEntry,
    AsyncuaEntity,
    async_setup_yaml_platform,
    discovered_for,
    hub_device_info,
    nodeid_validator,
)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

    from .hub import OpcuaHub

PLATFORM_SCHEMA = BINARY_SENSOR_PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_NODES): [
            {
                vol.Required(CONF_NODE_HUB): cv.string,
                vol.Required(CONF_NODE_NAME): cv.string,
                vol.Required(CONF_NODE_ID): nodeid_validator,
                vol.Optional(CONF_NODE_UNIQUE_ID): cv.string,
                vol.Optional(CONF_NODE_DEVICE_CLASS): vol.Coerce(
                    BinarySensorDeviceClass
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
    """Set up binary sensors from YAML."""

    def _factory(hub: OpcuaHub, conf: dict[str, Any]) -> AsyncuaBinarySensor:
        return AsyncuaBinarySensor(
            hub,
            conf[CONF_NODE_ID],
            name=conf[CONF_NODE_NAME],
            unique_id=conf.get(CONF_NODE_UNIQUE_ID),
            device_class=conf.get(CONF_NODE_DEVICE_CLASS),
        )

    await async_setup_yaml_platform(hass, config, async_add_entities, _factory)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: AsyncuaConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the connectivity sensor and discovered binary sensors."""
    hub = entry.runtime_data.hub
    entities: list[BinarySensorEntity] = [AsyncuaConnectivitySensor(hub)]
    entities.extend(
        AsyncuaBinarySensor(
            hub, var.nodeid, name=var.name, device_info=device, enabled_default=enabled
        )
        for var, device, enabled in discovered_for(entry, "binary_sensor")
    )
    async_add_entities(entities)


class AsyncuaBinarySensor(AsyncuaEntity, BinarySensorEntity):
    """OPC UA binary sensor."""

    def __init__(
        self,
        hub: OpcuaHub,
        nodeid: str,
        *,
        device_class: BinarySensorDeviceClass | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(hub, nodeid, **kwargs)
        self._attr_device_class = device_class

    @callback
    def _update_from_hub(self) -> None:
        value = self.raw_value
        self._attr_is_on = None if value is None else bool(value)


class AsyncuaConnectivitySensor(BinarySensorEntity):
    """Connection status of the OPC UA server."""

    _attr_should_poll = False
    _attr_has_entity_name = True
    _attr_translation_key = "connected"
    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, hub: OpcuaHub) -> None:
        self._hub = hub
        self._attr_unique_id = f"{hub.name}_connected"
        self._attr_device_info = hub_device_info(hub)

    async def async_added_to_hass(self) -> None:
        """Follow the hub connection status."""
        self.async_on_remove(
            self._hub.async_add_connection_listener(self.async_write_ha_state)
        )

    @property
    def is_on(self) -> bool:
        """Return True when connected."""
        return self._hub.connected

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Expose subscription counters."""
        return {
            "monitored_nodes": self._hub.subscribed_count,
            "failed_nodes": len(self._hub.failed_nodes),
            "last_error": self._hub.last_error,
        }
