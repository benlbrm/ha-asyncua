"""AsyncUA integration with persistent connection and subscription support."""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from asyncua import Client, ua
from asyncua.common import ua_utils
from asyncua.ua.uatypes import DataValue
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.typing import ConfigType
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import (
    ATTR_NODE_HUB,
    ATTR_NODE_ID,
    ATTR_VALUE,
    CONF_HUB_ID,
    CONF_HUB_MANUFACTURER,
    CONF_HUB_MODEL,
    CONF_HUB_PASSWORD,
    CONF_HUB_SCAN_INTERVAL,
    CONF_HUB_URL,
    CONF_HUB_USERNAME,
    CONF_NODE_ID,
    CONF_NODE_NAME,
    CONF_NODE_ID_LIGHT_ON,
    CONF_NODE_ID_LIGHT_OFF,
    DOMAIN,
    SERVICE_SET_VALUE,
)

_LOGGER = logging.getLogger(DOMAIN)
_LOGGER.setLevel(logging.INFO)

BASE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HUB_ID): cv.string,
        vol.Required(CONF_HUB_URL): cv.string,
        vol.Optional(CONF_HUB_MANUFACTURER, default=""): cv.string,
        vol.Optional(CONF_HUB_MODEL, default=""): cv.string,
        vol.Optional(CONF_HUB_SCAN_INTERVAL, default=5): int,
        vol.Inclusive(CONF_HUB_USERNAME, None): cv.string,
        vol.Inclusive(CONF_HUB_PASSWORD, None): cv.string,
    }
)

SERVICE_SET_VALUE_SCHEMA = vol.Schema(
    {
        vol.Required(ATTR_NODE_HUB): cv.string,
        vol.Required(ATTR_NODE_ID): cv.string,
        vol.Required(ATTR_VALUE): vol.Any(
            float, int, str, cv.byte, cv.boolean, cv.time
        ),
    }
)

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.All(
            cv.ensure_list,
            [vol.Any(BASE_SCHEMA)],
        ),
    },
    extra=vol.ALLOW_EXTRA,
)


class AsyncuaSubscriptionHandler:
    """Handle subscription events from OPC UA server."""

    def __init__(self, hub: OpcuaHub):
        self.hub = hub

    async def datachange_notification(self, node, val, data) -> None:
        nodeid = node.nodeid.to_string()
        self.hub.cache_val[nodeid] = val
        _LOGGER.info("DataChange: %s = %s", nodeid, val)
        # Notify coordinator that data changed
        await self.hub.notify_update()

    async def event_notification(self, event) -> None:
        _LOGGER.info("Event notification: %s", event)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up asyncua integration."""
    hass.data[DOMAIN] = {}

    async def _set_value(service):
        hub = hass.data[DOMAIN][service.data.get(ATTR_NODE_HUB)].hub
        await hub.set_value(
            nodeid=service.data[ATTR_NODE_ID],
            value=service.data[ATTR_VALUE],
        )
        return True

    async def _configure_hub(hub_conf: dict):
        if hub_conf[CONF_HUB_ID] in hass.data[DOMAIN]:
            raise ConfigEntryError(f"Duplicated hub ID {hub_conf[CONF_HUB_ID]}")

        opcua_hub = OpcuaHub(
            hub_name=hub_conf[CONF_HUB_ID],
            hub_manufacturer=hub_conf[CONF_HUB_MANUFACTURER],
            hub_model=hub_conf[CONF_HUB_MODEL],
            hub_url=hub_conf[CONF_HUB_URL],
            username=hub_conf.get(CONF_HUB_USERNAME),
            password=hub_conf.get(CONF_HUB_PASSWORD),
        )

        coordinator = AsyncuaCoordinator(
            hass=hass,
            name=hub_conf[CONF_HUB_ID],
            hub=opcua_hub,
        )

        hass.data[DOMAIN][hub_conf[CONF_HUB_ID]] = coordinator

        await opcua_hub.connect()

    await asyncio.gather(*[_configure_hub(h) for h in config[DOMAIN]])

    hass.services.async_register(
        domain=DOMAIN,
        service=f"{SERVICE_SET_VALUE}",
        service_func=_set_value,
        schema=SERVICE_SET_VALUE_SCHEMA,
    )

    return True


class OpcuaHub:
    """Manage connection and subscription to an OPCUA server."""

    def __init__(
        self,
        hub_name: str,
        hub_manufacturer: str,
        hub_model: str,
        hub_url: str,
        username: str | None = None,
        password: str | None = None,
        timeout: float = 4,
    ):
        self._hub_name = hub_name
        self._hub_url = hub_url
        self._username = username
        self._password = password
        self._timeout = timeout
        self._connected = False
        self.device_info = DeviceInfo(
            configuration_url=hub_url,
            manufacturer=hub_manufacturer,
            model=hub_model,
        )
        self.client = Client(url=hub_url, timeout=5)
        self.client.secure_channel_timeout = 30000
        self.client.session_timeout = 30000
        if username:
            self.client.set_user(username=username)
        if password:
            self.client.set_password(pwd=password)

        self.cache_val: dict[str, Any] = {}
        self._subscription = None
        self._lock = asyncio.Lock()
        self._reconnect_task = None
        self._coordinator = None  # assigned later

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self):
        async with self._lock:
            if self._connected:
                return
            try:
                await self.client.connect()
                self._connected = True
                _LOGGER.info("Connected to OPCUA server at %s", self._hub_url)
            except Exception as e:
                self._connected = False
                _LOGGER.error("Failed to connect: %s", e)
                await self.schedule_reconnect()

    async def disconnect(self):
        async with self._lock:
            if not self._connected:
                return
            await self.client.disconnect()
            self._connected = False
            _LOGGER.info("Disconnected from OPCUA server.")

    async def subscribe_nodes(self, node_key_pair: dict[str, str]):
        """Subscribe to nodes and keep connection alive."""
        await self.connect()
        try:
            handler = AsyncuaSubscriptionHandler(self)
            self._subscription = await self.client.create_subscription(100, handler)
            for nodeid in node_key_pair.values():
                node = self.client.get_node(nodeid)
                await self._subscription.subscribe_data_change(node)
                try:
                    initial_value = await node.read_value()
                    self.cache_val[nodeid] = initial_value
                    _LOGGER.info("Nodeid: %s Initial Value: %s", nodeid, initial_value)
                except Exception as e:
                    _LOGGER.warning(
                        "Failed to read initial value for %s: %s", nodeid, e
                    )
            _LOGGER.info("Subscribed to %d nodes", len(node_key_pair))
        except Exception as e:
            _LOGGER.error("Subscription failed: %s", e)
            await self.schedule_reconnect()

    async def set_value(self, nodeid: str, value: Any) -> bool:
        await self.connect()
        try:
            node = self.client.get_node(nodeid=nodeid)
            node_type = await node.read_data_type_as_variant_type()
            var = ua.Variant(
                ua_utils.string_to_variant(string=str(value), vtype=node_type)
            )
            await node.write_value(DataValue(var))
            return True
        except Exception as e:
            _LOGGER.error("Failed to write value: %s", e)
            await self.schedule_reconnect()
            return False

    async def schedule_reconnect(self, delay: int = 5):
        """Schedule a reconnect task if not already running."""
        if self._reconnect_task and not self._reconnect_task.done():
            return

        async def reconnect_loop():
            while not self._connected:
                _LOGGER.warning("Trying to reconnect in %s seconds...", delay)
                await asyncio.sleep(delay)
                await self.connect()
                if self._connected and self._coordinator:
                    await self._coordinator.setup_subscription()

        self._reconnect_task = asyncio.create_task(reconnect_loop())

    async def notify_update(self):
        """Notify coordinator when new data is available."""
        if self._coordinator:
            await self._coordinator.async_request_refresh()


class AsyncuaCoordinator(DataUpdateCoordinator):
    """Coordinator that stores node values (no polling)."""

    def __init__(self, hass: HomeAssistant, name: str, hub: OpcuaHub):
        self._hub = hub
        self._hub._coordinator = self
        self._node_key_pair: dict[str, str] = {}
        self._sensors: list[dict[str, str]] = []
        self._subscription_task: asyncio.Task | None = None
        super().__init__(hass, _LOGGER, name=name, update_interval=None)

    @property
    def hub(self):
        return self._hub

    def add_sensors(self, sensors: list[dict[str, str]]) -> bool:
        self._sensors.extend(sensors)
        for val_sensor in self._sensors:
            _LOGGER.info("OPCUA Sensor: %s", val_sensor)
            node_id = (
                val_sensor.get(CONF_NODE_ID)
                or val_sensor.get(CONF_NODE_ID_LIGHT_ON)
                or val_sensor.get(CONF_NODE_ID_LIGHT_OFF)
            )
            if node_id:  # on évite d'ajouter des None
                self._node_key_pair[node_id] = node_id  # clé = NodeId, valeur = NodeId
                _LOGGER.info("OPCUA Sensor nodeid: %s", node_id)

        # Schedule subscription setup after sensors are registered.
        # Use create_task so platform setup (sync) doesn't need to await.
        if not self._subscription_task or self._subscription_task.done():
            self._subscription_task = asyncio.create_task(self.setup_subscription())
        return True

    async def setup_subscription(self):
        await self._hub.subscribe_nodes(self._node_key_pair)

    async def _async_update_data(self):
        return {**self._hub.cache_val}
