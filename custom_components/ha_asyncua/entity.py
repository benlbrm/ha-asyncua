"""Base entity and helpers shared by every ha_asyncua platform."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import PlatformNotReady
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import Entity

from .const import CONF_NODE_HUB, CONF_NODES, DISCOVERY_ENABLE_THRESHOLD, DOMAIN
from .hub import OpcuaHub, normalize_nodeid

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from homeassistant.helpers.entity_platform import AddEntitiesCallback
    from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

    from .discovery import DiscoveredVariable

_LOGGER = logging.getLogger(__name__)


def nodeid_validator(value: Any) -> str:
    """Voluptuous validator for OPC UA node id strings."""
    text = str(value)
    try:
        normalize_nodeid(text)
    except Exception as err:
        msg = f"invalid OPC UA node id {text!r}: {err}"
        raise vol.Invalid(msg) from err
    return text


@dataclass
class AsyncuaData:
    """Integration-wide data stored in hass.data[DOMAIN]."""

    hubs: dict[str, OpcuaHub] = field(default_factory=dict)
    yaml_hubs: set[str] = field(default_factory=set)
    # Node ids referenced by YAML entities, per hub (excluded from discovery).
    yaml_nodeids: dict[str, set[str]] = field(default_factory=dict)


@dataclass
class EntryRuntime:
    """Runtime data of a config entry."""

    hub: OpcuaHub
    variables: list[DiscoveredVariable] = field(default_factory=list)
    truncated: bool = False
    discovered_live: bool = False
    hub_device_id: str | None = None


type AsyncuaConfigEntry = ConfigEntry[EntryRuntime]


def get_data(hass: HomeAssistant) -> AsyncuaData:
    """Return the integration data."""
    return hass.data.setdefault(DOMAIN, AsyncuaData())


def hub_device_info(hub: OpcuaHub) -> DeviceInfo:
    """Device representing the OPC UA server."""
    return DeviceInfo(
        identifiers={(DOMAIN, hub.name)},
        name=hub.name,
        manufacturer=hub.config.manufacturer or None,
        model=hub.config.model or None,
        sw_version=None,
    )


def variable_device_info(
    hub: OpcuaHub, var: DiscoveredVariable, hub_device_id: str | None
) -> DeviceInfo:
    """Device of a discovered variable: its parent OPC UA object, or the hub."""
    if not var.device_id:
        return hub_device_info(hub)
    info = DeviceInfo(
        identifiers={(DOMAIN, f"{hub.name}:{var.device_id}")},
        name=var.device_name,
    )
    if hub_device_id is not None:
        info["via_device_id"] = hub_device_id
    return info


def default_unique_id(hub_name: str, nodeid: str) -> str:
    """Unique id scheme shared by YAML and discovered entities (kept for compatibility)."""
    return f"{DOMAIN}.{hub_name}.{nodeid}"


class AsyncuaEntity(Entity):
    """Push-updated entity bound to one or more OPC UA nodes of a hub."""

    _attr_should_poll = False

    def __init__(
        self,
        hub: OpcuaHub,
        state_nodeid: str,
        *,
        name: str | None,
        unique_id: str | None = None,
        extra_nodeids: Iterable[str | None] = (),
        device_info: DeviceInfo | None = None,
        enabled_default: bool = True,
    ) -> None:
        self.hub = hub
        self._state_nodeid = state_nodeid
        self._nodeids = [state_nodeid, *(n for n in extra_nodeids if n)]
        self._attr_name = name
        self._attr_unique_id = unique_id or default_unique_id(hub.name, state_nodeid)
        self._attr_entity_registry_enabled_default = enabled_default
        if device_info is not None:
            self._attr_device_info = device_info
            self._attr_has_entity_name = True
        self._attr_extra_state_attributes = {"nodeid": state_nodeid, "hub": hub.name}

    async def async_added_to_hass(self) -> None:
        """Subscribe to node and connection updates."""
        await super().async_added_to_hass()
        for nodeid in dict.fromkeys(self._nodeids):
            self.async_on_remove(
                self.hub.async_add_node_listener(nodeid, self._handle_update)
            )
        self.async_on_remove(
            self.hub.async_add_connection_listener(self._handle_update)
        )
        self.hub.async_register_nodes(self._nodeids)
        self._update_from_hub()

    @callback
    def _handle_update(self) -> None:
        self._update_from_hub()
        self.async_write_ha_state()

    @callback
    def _update_from_hub(self) -> None:
        """Refresh _attr_* from the hub cache (override in subclasses)."""

    @property
    def available(self) -> bool:
        """Available when connected and the state node has a good value."""
        return self.hub.is_node_ok(self._state_nodeid)

    @property
    def raw_value(self) -> Any:
        """Last good value of the state node."""
        return self.hub.value(self._state_nodeid)

    async def _async_pulse(self, nodeid: str, pulse_ms: int | None) -> None:
        """Write True to a command node, then False after pulse_ms (if set)."""
        await self.hub.async_write(nodeid, True)
        if pulse_ms:
            await asyncio.sleep(pulse_ms / 1000)
            await self.hub.async_write(nodeid, False)


def yaml_nodeids(node_conf: dict[str, Any]) -> set[str]:
    """Return every node id referenced by a YAML entity config."""
    return {
        str(v)
        for k, v in node_conf.items()
        if k.startswith("nodeid") and isinstance(v, str)
    }


async def async_setup_yaml_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    factory: Callable[[OpcuaHub, dict[str, Any]], Entity],
    *,
    discovery_info: DiscoveryInfoType | None = None,
    schema: Callable[[Any], ConfigType] | None = None,
) -> None:
    """Create YAML-configured entities, grouped by hub."""
    if discovery_info is not None and schema is not None:
        # Loaded by the integration (YAML written with the former domain).
        config = schema(discovery_info)
    data = get_data(hass)
    entities: list[Entity] = []
    not_ready: set[str] = set()
    for node_conf in config[CONF_NODES]:
        hub_name = node_conf[CONF_NODE_HUB]
        hub = data.hubs.get(hub_name)
        if hub is None:
            known = {e.unique_id for e in hass.config_entries.async_entries(DOMAIN)}
            if hub_name in known:
                not_ready.add(hub_name)
            else:
                _LOGGER.error(
                    "OPC UA hub %r not found for entity %s", hub_name, node_conf
                )
            continue
        entities.append(factory(hub, node_conf))
    if not_ready:
        msg = f"OPC UA hubs not ready yet: {', '.join(sorted(not_ready))}"
        raise PlatformNotReady(msg)
    async_add_entities(entities)


def discovered_for(
    entry: AsyncuaConfigEntry, platform: str
) -> list[tuple[DiscoveredVariable, DeviceInfo, bool]]:
    """Return (variable, device_info, enabled_default) discovered for a platform."""
    runtime = entry.runtime_data
    enabled = len(runtime.variables) <= DISCOVERY_ENABLE_THRESHOLD
    return [
        (var, variable_device_info(runtime.hub, var, runtime.hub_device_id), enabled)
        for var in runtime.variables
        if var.platform == platform
    ]
