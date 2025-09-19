"""
ha-asyncua: core module with OpcuaHub and AsyncuaCoordinator.

Provides:
- async_setup(...) to register hubs from YAML
- OpcuaHub : manages asyncua.Client, connection, subscription, reconnect
- AsyncuaCoordinator : stores node list and cached values and triggers HA refreshes
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from asyncua import Client, ua
from asyncua.common import ua_utils
from asyncua.ua.uaerrors import UaError
from asyncua.ua.uatypes import DataValue
from homeassistant.core import HomeAssistant, callback
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
    DOMAIN,
    SERVICE_SET_VALUE,
)

# logging.getLogger("asyncua").setLevel(logging.WARNING)
_LOGGER = logging.getLogger(__name__)
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
    {DOMAIN: vol.All(cv.ensure_list, [BASE_SCHEMA])}, extra=vol.ALLOW_EXTRA
)


class AsyncuaSubscriptionHandler:
    """Handle subscription events from OPC UA server."""

    def __init__(self, hub: "OpcuaHub"):
        self.hub = hub

    async def datachange_notification(self, node, val, data) -> None:
        """Called when a subscribed node value changes."""
        nodeid = node.nodeid.to_string()
        self.hub.cache_val[nodeid] = val
        _LOGGER.debug("DataChange: %s = %s", nodeid, val)
        await self.hub.notify_update()

    async def event_notification(self, event) -> None:
        """Handle event notifications (not used widely here)."""
        _LOGGER.debug("Event: %s", event)

    async def status_change_notification(self, status) -> None:
        """Handle subscription status changes (e.g., BadSessionClosed)."""
        _LOGGER.warning("Subscription status changed: %s", status)
        # Trigger hub to handle it (disconnect -> reconnect -> resubscribe)
        asyncio.create_task(self.hub.handle_subscription_status_change(status))


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up the ha-asyncua integration and hubs from YAML config."""
    hass.data.setdefault(DOMAIN, {})

    async def _set_value(service):
        """Service handler to set a node value."""
        hub_id = service.data[ATTR_NODE_HUB]
        hub_coordinator = hass.data[DOMAIN].get(hub_id)
        if not hub_coordinator:
            raise ConfigEntryError(f"Hub {hub_id} not found")
        hub = hub_coordinator.hub
        await hub.set_value(
            nodeid=service.data[ATTR_NODE_ID], value=service.data[ATTR_VALUE]
        )
        return True

    async def _configure_hub(hub_conf: dict[str, Any]) -> None:
        hub_id = hub_conf[CONF_HUB_ID]
        if hub_id in hass.data[DOMAIN]:
            raise ConfigEntryError(f"Duplicated hub ID {hub_id}")

        opcua_hub = OpcuaHub(
            hub_name=hub_id,
            hub_manufacturer=hub_conf.get(CONF_HUB_MANUFACTURER, ""),
            hub_model=hub_conf.get(CONF_HUB_MODEL, ""),
            hub_url=hub_conf[CONF_HUB_URL],
            username=hub_conf.get(CONF_HUB_USERNAME),
            password=hub_conf.get(CONF_HUB_PASSWORD),
        )

        coordinator = AsyncuaCoordinator(hass=hass, name=hub_id, hub=opcua_hub)
        hass.data[DOMAIN][hub_id] = coordinator

        # connect immediately (fire and forget inside because connect has its own backoff)
        await opcua_hub.connect()

        # ensure clean disconnect at shutdown
        @callback
        async def _async_stop(_event):
            await opcua_hub.shutdown()

        hass.bus.async_listen_once("homeassistant_stop", _async_stop)

    # configure all hubs
    await asyncio.gather(*[_configure_hub(h) for h in config.get(DOMAIN, [])])

    hass.services.async_register(
        domain=DOMAIN,
        service=SERVICE_SET_VALUE,
        service_func=_set_value,
        schema=SERVICE_SET_VALUE_SCHEMA,
    )

    _LOGGER.info(
        "ha-asyncua setup complete with hubs: %s", list(hass.data[DOMAIN].keys())
    )
    return True


class OpcuaHub:
    """Manage connection + subscription + reconnection to an OPC UA server."""

    def __init__(
        self,
        hub_name: str,
        hub_manufacturer: str,
        hub_model: str,
        hub_url: str,
        username: str | None = None,
        password: str | None = None,
        timeout: float = 5.0,
    ) -> None:
        self._hub_name = hub_name
        self._hub_url = hub_url
        self._username = username
        self._password = password
        self._timeout = timeout
        self._connected = False
        self.device_info = DeviceInfo(
            configuration_url=hub_url, manufacturer=hub_manufacturer, model=hub_model
        )
        self.client = Client(url=hub_url, timeout=int(timeout))
        # tune timeouts
        self.client.secure_channel_timeout = 60000
        self.client.session_timeout = 60000
        if username:
            self.client.set_user(username=username)
        if password:
            self.client.set_password(pwd=password)

        self.cache_val: dict[str, Any] = {}
        self._subscription = None
        self._lock = asyncio.Lock()
        self._reconnect_task: asyncio.Task | None = None
        self._coordinator: AsyncuaCoordinator | None = None
        self._backoff = 5
        self._shutdown = False

    @property
    def hub_name(self) -> str:
        """Return hub id/name."""
        return self._hub_name

    @property
    def connected(self) -> bool:
        """Return connection status."""
        return self._connected

    async def connect(self) -> None:
        """Establish a connection to the OPC UA server.

        This method is idempotent and uses a lock to avoid concurrent connects.
        It resets backoff on success and clears stale cache.
        """
        async with self._lock:
            if self._shutdown:
                _LOGGER.debug("Not connecting: hub marked for shutdown")
                return
            if self._connected:
                return
            try:
                _LOGGER.info("Connecting to OPC UA server %s", self._hub_url)
                await asyncio.wait_for(self.client.connect(), timeout=10)
                self._connected = True
                self._backoff = 5
                # clear stale cache on new connection
                self.cache_val.clear()
                _LOGGER.info("Connected to OPC UA server %s", self._hub_url)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._connected = False
                _LOGGER.error("Failed to connect to %s: %s", self._hub_url, exc)
                # schedule reconnect loop if not already scheduled
                await self.schedule_reconnect()

    async def disconnect(self) -> None:
        """Disconnect and clean up subscription (if any)."""
        async with self._lock:
            if self._subscription:
                try:
                    await self._subscription.delete()
                except Exception as e:
                    _LOGGER.debug("Error while deleting subscription: %s", e)
                self._subscription = None
            if self._connected:
                try:
                    await asyncio.wait_for(self.client.disconnect(), timeout=5)
                except Exception as e:
                    _LOGGER.debug("Error during client disconnect: %s", e)
            self._connected = False
            _LOGGER.info("Disconnected from OPC UA server %s", self._hub_url)

    async def subscribe_nodes(self, node_key_pair: dict[str, str]) -> None:
        """
        Create subscription for provided node ids and read initial values.

        Safe: will attempt connect first. If not connected after attempt, returns.
        Improved: detect server ServiceFault / BadNoSubscription and force a full
        reconnect/resubscribe attempt once. Iterate over a stable snapshot of nodes
        to avoid concurrent-dict mutation errors.
        """
        await self.connect()
        if not self._connected or self._shutdown:
            _LOGGER.debug("Not subscribing: hub not connected or shutting down")
            return

        # clean previous subscription if present
        if self._subscription:
            try:
                await self._subscription.delete()
            except Exception as e:
                _LOGGER.debug("Error deleting old subscription: %s", e)
            self._subscription = None

        # Use a stable snapshot of node ids to avoid "dictionary changed size during iteration"
        node_ids = list(node_key_pair.values())

        async def _create_subscription() -> bool:
            """Try to create a subscription and return True on success."""
            try:
                handler = AsyncuaSubscriptionHandler(self)
                # create subscription with handler that will call back into hub
                self._subscription = await self.client.create_subscription(100, handler)
                _LOGGER.info("Subscription created (hub=%s)", self._hub_name)
                return True
            except Exception as exc:
                _LOGGER.warning("Failed to create subscription: %s", exc)
                return False

        created = await _create_subscription()
        if not created:
            # On first failure attempt a defensive reconnect then retry once.
            _LOGGER.debug(
                "First subscription creation failed, attempting defensive reconnect (hub=%s)",
                self._hub_name,
            )
            try:
                await self.disconnect()
            except Exception:
                _LOGGER.debug("Disconnect during recovery failed", exc_info=True)

            try:
                await self.connect()
            except Exception:
                _LOGGER.debug("Reconnect attempt failed", exc_info=True)

            created = await _create_subscription()
            if not created:
                _LOGGER.error(
                    "Could not create subscription after retry, scheduling reconnect (hub=%s)",
                    self._hub_name,
                )
                await self.schedule_reconnect()
                return

        # subscribe to nodes using the stable snapshot
        for nodeid in node_ids:
            try:
                node = self.client.get_node(nodeid)
                try:
                    # subscribe_data_change may raise if server-side subscription state is invalid
                    await self._subscription.subscribe_data_change(node)
                except Exception as exc:
                    msg = str(exc)
                    _LOGGER.warning(
                        "subscribe_data_change failed for %s: %s", nodeid, exc
                    )

                    # Detect server-side subscription errors that require full reconnect.
                    if any(
                        x in msg
                        for x in (
                            "BadNoSubscription",
                            "BadSubscriptionIdInvalid",
                            "ServiceFault",
                        )
                    ):
                        _LOGGER.warning(
                            "Server reports invalid/stale subscription for %s: %s. Forcing full reconnect/resubscribe.",
                            nodeid,
                            msg,
                        )
                        # try to cleanup and schedule a reconnect/resubscribe
                        try:
                            await self.disconnect()
                        except Exception:
                            _LOGGER.debug(
                                "Disconnect while handling stale subscription failed",
                                exc_info=True,
                            )
                        await asyncio.sleep(1)
                        await self.schedule_reconnect()
                        # stop processing further nodes now — reconnect task will resubscribe whole node list
                        return
                    # otherwise continue with other nodes
                    continue

                # attempt to read initial value with timeout
                try:
                    val = await asyncio.wait_for(node.read_value(), timeout=3)
                    self.cache_val[nodeid] = val
                    _LOGGER.debug("Initial read %s = %s", nodeid, val)
                except Exception as e:
                    _LOGGER.debug("Initial read failed for %s: %s", nodeid, e)
            except Exception as e:
                _LOGGER.warning(
                    "Failed to prepare subscription for node %s: %s", nodeid, e
                )

        _LOGGER.info("Subscribed to %d nodes for hub %s", len(node_ids), self._hub_name)

    async def set_value(self, nodeid: str, value: Any) -> bool:
        """Write a value to a node, with safety: connect first, timeout, and reconnect on failure."""
        await self.connect()
        if not self._connected:
            _LOGGER.error("Cannot write: hub not connected")
            return False
        try:
            node = self.client.get_node(nodeid=nodeid)
            node_type = await asyncio.wait_for(
                node.read_data_type_as_variant_type(), timeout=3
            )
            var = ua.Variant(
                ua_utils.string_to_variant(string=str(value), vtype=node_type)
            )
            await asyncio.wait_for(node.write_value(DataValue(var)), timeout=4)
            _LOGGER.debug("Wrote %s to %s", value, nodeid)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _LOGGER.error("Write failed to %s: %s", nodeid, exc)
            # schedule reconnect/resubscribe
            asyncio.create_task(self.schedule_reconnect())
            return False

    async def handle_subscription_status_change(self, status: Any) -> None:
        """Called by subscription handler on status change — force full reconnect/resubscribe."""
        _LOGGER.warning("Handling subscription status change: %s", status)
        # try to disconnect cleanly and start reconnect loop
        try:
            await self.disconnect()
        except Exception as e:
            _LOGGER.debug("Disconnect during status change handler failed: %s", e)
        await self.schedule_reconnect()

    async def schedule_reconnect(self) -> None:
        """Start a reconnect loop with exponential backoff (non-blocking)."""
        if self._shutdown:
            _LOGGER.debug("Not scheduling reconnect: hub is shutting down")
            return
        if self._reconnect_task and not self._reconnect_task.done():
            _LOGGER.debug("Reconnect already scheduled - skipping")
            return

        async def _reconnect_loop() -> None:
            backoff = self._backoff
            while not self._connected and not self._shutdown:
                _LOGGER.info(
                    "Reconnect attempt in %s seconds (hub=%s)", backoff, self._hub_name
                )
                await asyncio.sleep(backoff)
                try:
                    # if previous client objects are in weird state, try a graceful disconnect first
                    try:
                        await self.disconnect()
                    except Exception:
                        pass
                    await self.connect()
                    if self._connected:
                        _LOGGER.info(
                            "Reconnected (hub=%s), will resubscribe nodes",
                            self._hub_name,
                        )
                        # reset backoff and resubscribe
                        self._backoff = 5
                        if self._coordinator:
                            await self._coordinator.setup_subscription()
                        return
                except asyncio.CancelledError:
                    _LOGGER.debug("Reconnect loop cancelled")
                    return
                except Exception as exc:
                    _LOGGER.warning("Reconnect attempt failed: %s", exc)
                backoff = min(backoff * 2, 60)

        self._reconnect_task = asyncio.create_task(_reconnect_loop())

    async def notify_update(self) -> None:
        """Notify coordinator (if present) that data changed."""
        if self._coordinator:
            # Immediately push the latest cache to the coordinator and notify listeners.
            # Use a shallow copy to avoid concurrent mutation of the shared cache.
            self._coordinator.async_set_updated_data({**self.cache_val})

    async def shutdown(self) -> None:
        """Shutdown hub: cancel reconnect task and disconnect client."""
        self._shutdown = True
        # cancel reconnect task
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except Exception:
                pass
        # disconnect client
        try:
            await self.disconnect()
        except Exception as e:
            _LOGGER.debug("Error during hub.shutdown(): %s", e)


class AsyncuaCoordinator(DataUpdateCoordinator):
    """Coordinator storing cached node values and managing subscriptions."""

    def __init__(self, hass: HomeAssistant, name: str, hub: OpcuaHub) -> None:
        self._hub = hub
        # let hub reference coordinator for resubscribe
        self._hub._coordinator = self
        self._node_key_pair: dict[str, str] = {}
        self._sensors: list[dict[str, str]] = []
        self._subscription_task: asyncio.Task | None = None
        super().__init__(hass=hass, logger=_LOGGER, name=name, update_interval=None)

    @property
    def hub(self) -> OpcuaHub:
        """Return the linked hub."""
        return self._hub

    def add_sensors(self, sensors: list[dict[str, str]]) -> bool:
        """
        Register sensors and schedule a subscription setup.

        The node_key_pair stores mapping nodeid->nodeid so we can subscribe easily.
        """
        # extend only with provided sensors (avoid duplicating)
        for val in sensors:
            if val not in self._sensors:
                self._sensors.append(val)
                _LOGGER.debug(
                    "add_sensors incoming sensor dict keys: %s", list(val.keys())
                )
                _LOGGER.debug(
                    "add_sensors incoming sensor dict val: %s", list(val.values())
                )

                # collect all node ids present in the sensor dict (not only the first)
                expected_keys = {
                    #     CONF_NODE_ID,
                    #     CONF_NODE_ID_LIGHT_ON,
                    #     CONF_NODE_ID_LIGHT_OFF,
                    #     CONF_NODE_ID_COVER_CLOSE,
                    #     CONF_NODE_ID_COVER_OPEN,
                    #     CONF_NODE_ID_COVER_STOP,
                    #     CONF_NODE_ID_COVER_POSITION,
                    #     CONF_NODE_ID_COVER_SET_POSITION,
                }

                for key, value in val.items():
                    if not value:
                        continue
                    # accept known constant keys or legacy/user keys starting with nodeid_ / node_id_
                    if key in expected_keys or key.startswith(
                        (CONF_NODE_ID, "node_id_")
                    ):
                        node_id = value
                        if node_id:
                            if node_id not in self._node_key_pair:
                                self._node_key_pair[node_id] = node_id
                                _LOGGER.debug(
                                    "Registered node_id %s (source key=%s) for hub %s",
                                    node_id,
                                    key,
                                    self._hub.hub_name,
                                )

        # schedule subscription setup in background
        if not self._subscription_task or self._subscription_task.done():
            self._subscription_task = asyncio.create_task(self.setup_subscription())
        return True

    async def setup_subscription(self) -> None:
        """Instruct hub to (re)create the subscription for registered nodes."""
        await self._hub.subscribe_nodes(self._node_key_pair)

    async def _async_update_data(self) -> dict[str, Any]:
        """Return cached values to Home Assistant on refresh."""
        return {**self._hub.cache_val}
