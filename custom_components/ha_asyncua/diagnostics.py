"""Diagnostics for ha_asyncua."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING, Any

from homeassistant.components.diagnostics import async_redact_data

from .const import CONF_HUB_PASSWORD, CONF_HUB_USERNAME

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .entity import AsyncuaConfigEntry

TO_REDACT = {CONF_HUB_PASSWORD, CONF_HUB_USERNAME}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: AsyncuaConfigEntry,
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    runtime = entry.runtime_data
    return {
        "entry": {
            "source": entry.source,
            "data": async_redact_data(dict(entry.data), TO_REDACT),
            "options": dict(entry.options),
        },
        "hub": runtime.hub.diagnostics(),
        "discovery": {
            "live": runtime.discovered_live,
            "truncated": runtime.truncated,
            "variables": len(runtime.variables),
            "per_platform": dict(Counter(v.platform for v in runtime.variables)),
            "devices": len({v.device_id for v in runtime.variables}),
        },
    }
