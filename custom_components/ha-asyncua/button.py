"""Button platform for ha-asyncua: re-run discovery."""

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.button import ButtonEntity
from homeassistant.const import EntityCategory

from .const import CONF_DISCOVERY_ENABLED
from .entity import AsyncuaConfigEntry, hub_device_info

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback


async def async_setup_entry(
    hass: HomeAssistant,
    entry: AsyncuaConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the rediscover button (only useful when discovery is enabled)."""
    if entry.options.get(CONF_DISCOVERY_ENABLED, False):
        async_add_entities([AsyncuaRediscoverButton(entry)])


class AsyncuaRediscoverButton(ButtonEntity):
    """Reload the entry, which browses the server again."""

    _attr_has_entity_name = True
    _attr_translation_key = "rediscover"
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, entry: AsyncuaConfigEntry) -> None:
        hub = entry.runtime_data.hub
        self._entry_id = entry.entry_id
        self._attr_unique_id = f"{hub.name}_rediscover"
        self._attr_device_info = hub_device_info(hub)

    async def async_press(self) -> None:
        """Reload the config entry."""
        self.hass.config_entries.async_schedule_reload(self._entry_id)
