"""
ha_asyncua: connect OPC UA servers (PLCs) to Home Assistant.

Hubs can be created from the UI (config flow) or from YAML. YAML hubs are
imported as config entries so they get a device and optional discovery,
while YAML entity platforms keep working unchanged.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.config_entries import SOURCE_IMPORT
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.exceptions import ServiceValidationError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import discovery as ha_discovery
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.storage import Store

try:
    from homeassistant.config import config_per_platform
except ImportError:  # Home Assistant < 2026.x
    from homeassistant.helpers import config_per_platform  # type: ignore[attr-defined, no-redef]

from .const import (
    ATTR_NODE_HUB,
    ATTR_NODE_ID,
    ATTR_VALUE,
    CONF_DISCOVERY_ENABLED,
    CONF_DISCOVERY_EXCLUDE,
    CONF_DISCOVERY_INCLUDE,
    CONF_DISCOVERY_MAX_DEPTH,
    CONF_DISCOVERY_NAMESPACES,
    CONF_DISCOVERY_ROOT,
    CONF_DISCOVERY_WRITABLE,
    CONF_HUB_CERTIFICATE,
    CONF_HUB_ID,
    CONF_HUB_MANUFACTURER,
    CONF_HUB_MODEL,
    CONF_HUB_PASSWORD,
    CONF_HUB_PRIVATE_KEY,
    CONF_HUB_PUBLISHING_INTERVAL,
    CONF_HUB_SCAN_INTERVAL,
    CONF_HUB_SECURITY_MODE,
    CONF_HUB_SECURITY_POLICY,
    CONF_HUB_URL,
    CONF_HUB_USERNAME,
    CONF_NODE_HUB,
    CONF_NODES,
    DEFAULT_DISCOVERY_MAX_DEPTH,
    DEFAULT_DISCOVERY_ROOT,
    DEFAULT_PUBLISHING_INTERVAL,
    DEFAULT_SECURITY_MODE,
    DOMAIN,
    ENTRY_PLATFORMS,
    OLD_DOMAIN,
    SECURITY_MODES,
    SECURITY_POLICIES,
    SECURITY_POLICY_NONE,
    SERVICE_REDISCOVER,
    SERVICE_SET_VALUE,
    YAML_PLATFORMS,
)
from .discovery import DiscoveredVariable, DiscoveryFilter, async_discover
from .entity import (
    AsyncuaConfigEntry,
    EntryRuntime,
    default_unique_id,
    get_data,
    nodeid_validator,
    yaml_nodeids,
)
from .hub import HubConfig, OpcuaHub, normalize_nodeid, without_unused_security
from .migration import (
    async_adopt_old_entry,
    async_migrate_config_entries,
    async_migrate_registries,
    async_report_old_yaml,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from homeassistant.helpers.typing import ConfigType

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 1
DISCOVERY_CONNECT_TIMEOUT = 15

HUB_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HUB_ID): cv.string,
        vol.Required(CONF_HUB_URL): cv.string,
        vol.Optional(CONF_HUB_MANUFACTURER, default=""): cv.string,
        vol.Optional(CONF_HUB_MODEL, default=""): cv.string,
        # Deprecated: values are pushed by subscription, polling is not used.
        vol.Optional(CONF_HUB_SCAN_INTERVAL): vol.Coerce(int),
        vol.Optional(
            CONF_HUB_PUBLISHING_INTERVAL, default=DEFAULT_PUBLISHING_INTERVAL
        ): vol.All(vol.Coerce(int), vol.Range(min=10)),
        vol.Inclusive(CONF_HUB_USERNAME, "credentials"): cv.string,
        vol.Inclusive(CONF_HUB_PASSWORD, "credentials"): cv.string,
        vol.Optional(CONF_HUB_SECURITY_POLICY, default=SECURITY_POLICY_NONE): vol.In(
            SECURITY_POLICIES
        ),
        vol.Optional(CONF_HUB_SECURITY_MODE, default=DEFAULT_SECURITY_MODE): vol.In(
            SECURITY_MODES
        ),
        vol.Inclusive(CONF_HUB_CERTIFICATE, "certificate"): cv.isfile,
        vol.Inclusive(CONF_HUB_PRIVATE_KEY, "certificate"): cv.isfile,
    }
)

CONFIG_SCHEMA = vol.Schema(
    {DOMAIN: vol.All(cv.ensure_list, [HUB_SCHEMA])}, extra=vol.ALLOW_EXTRA
)

SERVICE_SET_VALUE_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_NODE_HUB): cv.string,
        vol.Required(ATTR_NODE_ID): nodeid_validator,
        vol.Required(ATTR_VALUE): vol.Any(bool, int, float, str),
    }
)
SERVICE_REDISCOVER_SCHEMA = vol.Schema({vol.Required(ATTR_NODE_HUB): cv.string})

HUB_KEYS = (
    CONF_HUB_ID,
    CONF_HUB_URL,
    CONF_HUB_MANUFACTURER,
    CONF_HUB_MODEL,
    CONF_HUB_PUBLISHING_INTERVAL,
    CONF_HUB_USERNAME,
    CONF_HUB_PASSWORD,
    CONF_HUB_SECURITY_POLICY,
    CONF_HUB_SECURITY_MODE,
    CONF_HUB_CERTIFICATE,
    CONF_HUB_PRIVATE_KEY,
)


def hub_config_from(data: Mapping[str, Any]) -> HubConfig:
    """Build a HubConfig from YAML or config entry data."""
    return HubConfig(
        name=data[CONF_HUB_ID],
        url=data[CONF_HUB_URL],
        username=data.get(CONF_HUB_USERNAME) or None,
        password=data.get(CONF_HUB_PASSWORD) or None,
        manufacturer=data.get(CONF_HUB_MANUFACTURER, ""),
        model=data.get(CONF_HUB_MODEL, ""),
        publishing_interval=int(
            data.get(CONF_HUB_PUBLISHING_INTERVAL, DEFAULT_PUBLISHING_INTERVAL)
        ),
        security_policy=data.get(CONF_HUB_SECURITY_POLICY, SECURITY_POLICY_NONE),
        security_mode=data.get(CONF_HUB_SECURITY_MODE, DEFAULT_SECURITY_MODE),
        certificate=data.get(CONF_HUB_CERTIFICATE) or None,
        private_key=data.get(CONF_HUB_PRIVATE_KEY) or None,
    )


def _collect_yaml_nodeids(config: ConfigType) -> dict[str, set[str]]:
    """Return node ids used by YAML entities of this integration, per hub."""
    result: dict[str, set[str]] = {}
    for domain in YAML_PLATFORMS:
        for platform, p_conf in config_per_platform(config, domain):
            if platform not in (DOMAIN, OLD_DOMAIN):
                continue
            for node in p_conf.get(CONF_NODES, []) or []:
                if not isinstance(node, dict) or CONF_NODE_HUB not in node:
                    continue
                ids = result.setdefault(str(node[CONF_NODE_HUB]), set())
                for nodeid in yaml_nodeids(node):
                    ids.add(nodeid)
                    try:
                        ids.add(normalize_nodeid(nodeid))
                    except Exception:  # noqa: S112 - invalid ids are reported by the platform
                        continue
    return result


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up YAML hubs and the integration services."""
    # Before any platform loads, so migrated entities keep their entity ids.
    try:
        async_migrate_registries(hass)
    except Exception:
        _LOGGER.exception("Migration from the former domain failed")

    data = get_data(hass)
    data.yaml_nodeids = _collect_yaml_nodeids(config)
    hub_confs: list[dict[str, Any]] = list(config.get(DOMAIN, []))
    old_keys: list[str] = []
    if OLD_DOMAIN in config:
        old_keys.append(f"{OLD_DOMAIN}:")
        try:
            hub_confs += vol.All(cv.ensure_list, [HUB_SCHEMA])(config[OLD_DOMAIN])
        except vol.Invalid as err:
            _LOGGER.error(  # noqa: TRY400 - validation error, no traceback needed
                "Invalid %s configuration: %s", OLD_DOMAIN, err
            )

    names = [h[CONF_HUB_ID] for h in hub_confs]
    if len(names) != len(set(names)):
        _LOGGER.error("Duplicated OPC UA hub names in configuration: %s", names)
        return False

    for hub_conf in hub_confs:
        if CONF_HUB_SCAN_INTERVAL in hub_conf:
            _LOGGER.info(
                "Hub %s: 'scan_interval' is ignored (values are pushed by subscription)",
                hub_conf[CONF_HUB_ID],
            )
        hub = OpcuaHub(hass, hub_config_from(hub_conf))
        data.hubs[hub.name] = hub
        data.yaml_hubs.add(hub.name)
        await hub.async_start()
        import_data = without_unused_security(
            {k: hub_conf[k] for k in HUB_KEYS if hub_conf.get(k) is not None}
        )
        hass.async_create_task(
            hass.config_entries.flow.async_init(
                DOMAIN, context={"source": SOURCE_IMPORT}, data=import_data
            )
        )

    # Hubs removed from YAML: drop their imported config entries.
    for domain in (DOMAIN, OLD_DOMAIN):
        for entry in hass.config_entries.async_entries(domain):
            if entry.source == SOURCE_IMPORT and entry.unique_id not in data.yaml_hubs:
                hass.async_create_task(hass.config_entries.async_remove(entry.entry_id))
    try:
        async_migrate_config_entries(hass)
    except Exception:
        _LOGGER.exception("Migration of former config entries failed")

    # YAML entities still declared with the former domain: load them anyway.
    for domain in YAML_PLATFORMS:
        for platform, p_conf in config_per_platform(config, domain):
            if platform != OLD_DOMAIN:
                continue
            old_keys.append(f"{domain}: platform {OLD_DOMAIN}")
            hass.async_create_task(
                ha_discovery.async_load_platform(
                    hass, domain, DOMAIN, {**p_conf, "platform": DOMAIN}, config
                )
            )
    async_report_old_yaml(hass, sorted(set(old_keys)))

    async def _stop_yaml_hubs(_event: Any) -> None:
        for name in list(data.yaml_hubs):
            await data.hubs[name].async_stop()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _stop_yaml_hubs)
    _register_services(hass)
    return True


def _register_services(hass: HomeAssistant) -> None:
    data = get_data(hass)

    def _hub(name: str) -> OpcuaHub:
        hub = data.hubs.get(name)
        if hub is None:
            msg = f"OPC UA hub {name!r} not found (known: {', '.join(data.hubs) or 'none'})"
            raise ServiceValidationError(msg)
        return hub

    async def _set_value(call: ServiceCall) -> None:
        hub = _hub(call.data[ATTR_NODE_HUB])
        await hub.async_write(call.data[ATTR_NODE_ID], call.data[ATTR_VALUE])

    async def _rediscover(call: ServiceCall) -> None:
        name = call.data[ATTR_NODE_HUB]
        _hub(name)
        entry = hass.config_entries.async_entry_for_domain_unique_id(DOMAIN, name)
        if entry is None:
            msg = f"OPC UA hub {name!r} has no config entry"
            raise ServiceValidationError(msg)
        if not entry.options.get(CONF_DISCOVERY_ENABLED, False):
            msg = (
                f"Discovery is disabled for OPC UA hub {name!r}: enable it in "
                "Settings > Devices & services > OPC UA (asyncua) > Configure"
            )
            raise ServiceValidationError(msg)
        hass.config_entries.async_schedule_reload(entry.entry_id)

    hass.services.async_register(
        DOMAIN, SERVICE_SET_VALUE, _set_value, schema=SERVICE_SET_VALUE_SCHEMA
    )
    hass.services.async_register(
        DOMAIN, SERVICE_REDISCOVER, _rediscover, schema=SERVICE_REDISCOVER_SCHEMA
    )


def discovery_filter(entry: AsyncuaConfigEntry, skip: set[str]) -> DiscoveryFilter:
    """Build the discovery filter from the entry options."""
    opts = entry.options
    namespaces = {
        int(ns)
        for ns in str(opts.get(CONF_DISCOVERY_NAMESPACES, "") or "")
        .replace(" ", "")
        .split(",")
        if ns.isdigit()
    }
    return DiscoveryFilter(
        root=opts.get(CONF_DISCOVERY_ROOT) or DEFAULT_DISCOVERY_ROOT,
        max_depth=int(opts.get(CONF_DISCOVERY_MAX_DEPTH, DEFAULT_DISCOVERY_MAX_DEPTH)),
        include=opts.get(CONF_DISCOVERY_INCLUDE) or None,
        exclude=opts.get(CONF_DISCOVERY_EXCLUDE) or None,
        namespaces=namespaces,
        skip_nodeids=skip,
        writable=opts.get(CONF_DISCOVERY_WRITABLE) or None,
    )


async def _async_run_discovery(
    hass: HomeAssistant, entry: AsyncuaConfigEntry, runtime: EntryRuntime
) -> None:
    """Discover variables, falling back to the last stored result when offline."""
    hub = runtime.hub
    store: Store[dict[str, Any]] = Store(
        hass, STORAGE_VERSION, f"{DOMAIN}.{entry.entry_id}.discovery"
    )
    if await hub.async_wait_connected(DISCOVERY_CONNECT_TIMEOUT):
        skip = get_data(hass).yaml_nodeids.get(hub.name, set())
        try:
            variables, truncated = await async_discover(
                hub.client, discovery_filter(entry, skip)
            )
        except Exception:
            _LOGGER.exception("OPC UA discovery failed on hub %s", hub.name)
        else:
            runtime.variables, runtime.truncated, runtime.discovered_live = (
                variables,
                truncated,
                True,
            )
            await store.async_save(
                {"variables": [v.as_dict() for v in variables], "truncated": truncated}
            )
            _LOGGER.info("Hub %s: discovered %d variables", hub.name, len(variables))
            return

    cached = await store.async_load()
    if cached:
        runtime.variables = [
            DiscoveredVariable.from_dict(v) for v in cached["variables"]
        ]
        runtime.truncated = cached.get("truncated", False)
        _LOGGER.info(
            "Hub %s unreachable: using %d cached discovered variables",
            hub.name,
            len(runtime.variables),
        )
    else:
        _LOGGER.warning(
            "Hub %s unreachable: discovery postponed until connected", hub.name
        )

    # Re-run a live discovery as soon as the server is reachable.
    @callback
    def _on_connection() -> None:
        if hub.connected:
            remove()
            hass.config_entries.async_schedule_reload(entry.entry_id)

    remove = hub.async_add_connection_listener(_on_connection)
    entry.async_on_unload(remove)


def _cleanup_registries(hass: HomeAssistant, entry: AsyncuaConfigEntry) -> None:
    """Remove entities/devices of nodes that disappeared from the server."""
    runtime = entry.runtime_data
    hub = runtime.hub
    expected_entities = {
        default_unique_id(hub.name, v.nodeid) for v in runtime.variables
    }
    expected_entities.add(f"{hub.name}_connected")
    if entry.options.get(CONF_DISCOVERY_ENABLED, False):
        expected_entities.add(f"{hub.name}_rediscover")
    ent_reg = er.async_get(hass)
    for ent in er.async_entries_for_config_entry(ent_reg, entry.entry_id):
        if ent.unique_id not in expected_entities:
            _LOGGER.info("Removing stale OPC UA entity %s", ent.entity_id)
            ent_reg.async_remove(ent.entity_id)

    expected_devices = {(DOMAIN, hub.name)} | {
        (DOMAIN, f"{hub.name}:{v.device_id}") for v in runtime.variables if v.device_id
    }
    dev_reg = dr.async_get(hass)
    for device in dr.async_entries_for_config_entry(dev_reg, entry.entry_id):
        if not device.identifiers & expected_devices:
            dev_reg.async_update_device(
                device.id, remove_config_entry_id=entry.entry_id
            )


async def async_setup_entry(hass: HomeAssistant, entry: AsyncuaConfigEntry) -> bool:
    """Set up an OPC UA hub from a config entry."""
    data = get_data(hass)
    name = entry.data[CONF_HUB_ID]
    if name in data.yaml_hubs:
        # The hub is owned by YAML (already started in async_setup).
        hub = data.hubs[name]
    else:
        hub = OpcuaHub(hass, hub_config_from(entry.data))
        data.hubs[name] = hub
        await hub.async_start()

    runtime = EntryRuntime(hub=hub)
    entry.runtime_data = runtime
    try:
        await async_adopt_old_entry(hass, entry)
    except Exception:
        _LOGGER.exception("Migration of the former %s entry failed", hub.name)

    runtime.hub_device_id = (
        dr.async_get(hass)
        .async_get_or_create(
            config_entry_id=entry.entry_id,
            identifiers={(DOMAIN, hub.name)},
            name=hub.name,
            manufacturer=hub.config.manufacturer or None,
            model=hub.config.model or None,
        )
        .id
    )

    if entry.options.get(CONF_DISCOVERY_ENABLED, False):
        await _async_run_discovery(hass, entry, runtime)
    else:
        _LOGGER.info(
            "Discovery is disabled for OPC UA hub %s%s: enable it in the "
            "integration options (Configure) to add the server variables",
            hub.name,
            " (imported from YAML)" if entry.source == SOURCE_IMPORT else "",
        )
    if runtime.discovered_live or not entry.options.get(CONF_DISCOVERY_ENABLED, False):
        _cleanup_registries(hass, entry)

    await hass.config_entries.async_forward_entry_setups(entry, ENTRY_PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))
    return True


async def _async_update_listener(
    hass: HomeAssistant, entry: AsyncuaConfigEntry
) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: AsyncuaConfigEntry) -> bool:
    """Unload a config entry."""
    unloaded = await hass.config_entries.async_unload_platforms(entry, ENTRY_PLATFORMS)
    if unloaded:
        data = get_data(hass)
        hub = entry.runtime_data.hub
        if hub.name not in data.yaml_hubs:
            await hub.async_stop()
            data.hubs.pop(hub.name, None)
    return unloaded


async def async_remove_entry(hass: HomeAssistant, entry: AsyncuaConfigEntry) -> None:
    """Delete the stored discovery cache when an entry is removed."""
    await Store(
        hass, STORAGE_VERSION, f"{DOMAIN}.{entry.entry_id}.discovery"
    ).async_remove()
