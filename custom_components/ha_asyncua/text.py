"""Text platform for ha_asyncua (discovered writable strings)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.text import TextEntity
from homeassistant.core import callback

from .entity import AsyncuaConfigEntry, AsyncuaEntity, discovered_for

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback


async def async_setup_entry(
    hass: HomeAssistant,
    entry: AsyncuaConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up discovered text entities."""
    hub = entry.runtime_data.hub
    async_add_entities(
        AsyncuaText(
            hub, var.nodeid, name=var.name, device_info=device, enabled_default=enabled
        )
        for var, device, enabled in discovered_for(entry, "text")
    )


class AsyncuaText(AsyncuaEntity, TextEntity):
    """OPC UA writable string."""

    _attr_native_max = 255

    @callback
    def _update_from_hub(self) -> None:
        value = self.raw_value
        self._attr_native_value = (
            None if value is None else str(value)[: self._attr_native_max]
        )

    async def async_set_value(self, value: str) -> None:
        """Write the string."""
        await self.hub.async_write(self._state_nodeid, value)
