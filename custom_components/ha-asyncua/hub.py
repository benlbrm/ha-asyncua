"""OPC UA hub: connection, subscription, per-node listeners and writes."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from asyncua import Client, ua
from asyncua.client.ua_client import UaClientState
from asyncua.common import ua_utils
from asyncua.crypto import security_policies
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError

from .const import (
    CONF_HUB_SECURITY_MODE,
    CONF_HUB_SECURITY_POLICY,
    DEFAULT_PUBLISHING_INTERVAL,
    DEFAULT_SECURITY_MODE,
    DOMAIN,
    SECURITY_POLICY_NONE,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from asyncua.common.node import Node
    from asyncua.common.subscription import DataChangeNotif, Subscription

_LOGGER = logging.getLogger(__name__)

CONNECT_TIMEOUT = 10
REQUEST_TIMEOUT = 5
INITIAL_BACKOFF = 5
MAX_BACKOFF = 60
SUBSCRIBE_CHUNK = 500

SECURITY_POLICY_CLASSES: dict[str, type[security_policies.SecurityPolicy]] = {
    "Basic256Sha256": security_policies.SecurityPolicyBasic256Sha256,
    "Aes128Sha256RsaOaep": security_policies.SecurityPolicyAes128Sha256RsaOaep,
    "Aes256Sha256RsaPss": security_policies.SecurityPolicyAes256Sha256RsaPss,
}

_INT_RANGES: dict[ua.VariantType, tuple[int, int]] = {
    ua.VariantType.SByte: (-(2**7), 2**7 - 1),
    ua.VariantType.Byte: (0, 2**8 - 1),
    ua.VariantType.Int16: (-(2**15), 2**15 - 1),
    ua.VariantType.UInt16: (0, 2**16 - 1),
    ua.VariantType.Int32: (-(2**31), 2**31 - 1),
    ua.VariantType.UInt32: (0, 2**32 - 1),
    ua.VariantType.Int64: (-(2**63), 2**63 - 1),
    ua.VariantType.UInt64: (0, 2**64 - 1),
}
_FLOAT_TYPES = (ua.VariantType.Float, ua.VariantType.Double)
_TRUE_STRINGS = frozenset(("1", "true", "on", "yes"))
_FALSE_STRINGS = frozenset(("0", "false", "off", "no"))


def without_unused_security(data: Mapping[str, Any]) -> dict[str, Any]:
    """Drop the security mode when no security policy is used (it has no effect)."""
    result = dict(data)
    if (
        result.get(CONF_HUB_SECURITY_POLICY, SECURITY_POLICY_NONE)
        == SECURITY_POLICY_NONE
    ):
        result.pop(CONF_HUB_SECURITY_MODE, None)
    return result


def normalize_nodeid(nodeid: str) -> str:
    """Return the canonical string form of a node id ("ns=0;i=85" -> "i=85")."""
    return ua.NodeId.from_string(nodeid.strip()).to_string()


def coerce_value(value: Any, vtype: ua.VariantType) -> Any:  # noqa: PLR0911
    """Convert a Home Assistant value to the Python type expected by an OPC UA variant."""
    if vtype is ua.VariantType.Boolean:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in _TRUE_STRINGS:
                return True
            if lowered in _FALSE_STRINGS:
                return False
            msg = f"Cannot convert {value!r} to Boolean"
            raise ValueError(msg)
        return bool(value)
    if vtype in _INT_RANGES:
        if isinstance(value, str):
            value = value.strip()
            try:
                value = int(value, 0)
            except ValueError:
                value = float(value)
        if isinstance(value, float):
            if not value.is_integer():
                msg = f"{value} is not an integer, required by {vtype.name}"
                raise ValueError(msg)
            value = int(value)
        value = int(value)
        low, high = _INT_RANGES[vtype]
        if not low <= value <= high:
            msg = f"{value} is out of range for {vtype.name} [{low}, {high}]"
            raise ValueError(msg)
        return value
    if vtype in _FLOAT_TYPES:
        return float(value)
    if vtype is ua.VariantType.String:
        return str(value)
    if vtype is ua.VariantType.DateTime:
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value))
    return ua_utils.string_to_val(str(value), vtype)


@dataclass(slots=True)
class NodeValue:
    """Last value received for a node."""

    value: Any
    good: bool
    status: str


@dataclass(slots=True)
class HubConfig:
    """Connection parameters of a hub."""

    name: str
    url: str
    username: str | None = None
    password: str | None = None
    manufacturer: str = ""
    model: str = ""
    publishing_interval: int = DEFAULT_PUBLISHING_INTERVAL
    security_policy: str = SECURITY_POLICY_NONE
    security_mode: str = DEFAULT_SECURITY_MODE
    certificate: str | None = None
    private_key: str | None = None


class _SubscriptionHandler:
    """Route asyncua subscription callbacks to the hub."""

    def __init__(self, hub: OpcuaHub) -> None:
        self._hub = hub

    def datachange_notification(
        self, node: Node, val: Any, data: DataChangeNotif
    ) -> None:
        status = data.monitored_item.Value.StatusCode
        self._hub.handle_value(node.nodeid.to_string(), val, status)

    def status_change_notification(self, status: Any) -> None:
        _LOGGER.info("Subscription status change on hub %s: %s", self._hub.name, status)

    def event_notification(self, event: Any) -> None:
        _LOGGER.debug("Event on hub %s: %s", self._hub.name, event)


class OpcuaHub:
    """
    Manage one OPC UA server.

    The first connection is retried with backoff by the hub. Once connected,
    asyncua's own supervisor (auto_reconnect) keeps the session alive and
    recreates the subscription and its monitored items after a loss.
    """

    def __init__(self, hass: HomeAssistant, config: HubConfig) -> None:
        self.hass = hass
        self.config = config
        self.client = self._make_client()
        self._connected = False
        self._stopping = False
        self._subscription: Subscription | None = None
        self._wanted: set[str] = set()
        self._subscribed: set[str] = set()
        self.failed_nodes: dict[str, str] = {}
        self._values: dict[str, NodeValue] = {}
        self._vtypes: dict[str, ua.VariantType] = {}
        self._node_listeners: dict[str, list[CALLBACK_TYPE]] = {}
        self._connection_listeners: list[CALLBACK_TYPE] = []
        self._connect_task: asyncio.Task | None = None
        self._state_task: asyncio.Task | None = None
        self._sync_task: asyncio.Task | None = None
        self._sync_lock = asyncio.Lock()
        self._needs_refresh = False
        self.last_error: str | None = None

    # ------------------------------------------------------------------ props

    @property
    def name(self) -> str:
        """Return the hub name."""
        return self.config.name

    @property
    def connected(self) -> bool:
        """Return True when the session with the server is up."""
        return self._connected

    @property
    def subscribed_count(self) -> int:
        """Return the number of monitored nodes."""
        return len(self._subscribed)

    # -------------------------------------------------------------- lifecycle

    def _make_client(self) -> Client:
        client = Client(
            url=self.config.url,
            timeout=REQUEST_TIMEOUT,
            watchdog_intervall=5.0,
            auto_reconnect=True,
            reconnect_max_delay=MAX_BACKOFF,
            reconnect_request_timeout=REQUEST_TIMEOUT,
        )
        client.secure_channel_timeout = 600_000
        client.session_timeout = 60_000
        client.name = f"Home Assistant {DOMAIN} ({self.config.name})"
        client.application_uri = (
            f"urn:{socket.gethostname()}:{DOMAIN}:{self.config.name}"
        )
        if self.config.username:
            client.set_user(self.config.username)
        if self.config.password:
            client.set_password(self.config.password)
        return client

    def certificate_paths(self) -> tuple[Path, Path]:
        """Return (certificate, private key) paths used for secure connections."""
        if self.config.certificate and self.config.private_key:
            return Path(self.config.certificate), Path(self.config.private_key)
        base = Path(self.hass.config.path(DOMAIN, self.config.name))
        return base / "client_cert.der", base / "client_key.pem"

    async def _setup_security(self) -> None:
        policy = SECURITY_POLICY_CLASSES[self.config.security_policy]
        cert, key = self.certificate_paths()
        if not (self.config.certificate and self.config.private_key):
            await self.client.setup_self_signed_certificate(
                key_file=key, cert_file=cert
            )
        await self.client.set_security(
            policy,
            certificate=str(cert),
            private_key=str(key),
            mode=ua.MessageSecurityMode[self.config.security_mode],
        )

    async def async_connect_once(self) -> None:
        """Connect once, raising on failure (used by the config flow and the start loop)."""
        if self.config.security_policy != SECURITY_POLICY_NONE:
            await self._setup_security()
        await asyncio.wait_for(self.client.connect(), timeout=CONNECT_TIMEOUT)

    async def async_start(self) -> None:
        """Start connecting in the background (never raises)."""
        self._stopping = False
        if self._connect_task is None or self._connect_task.done():
            self._connect_task = self.hass.async_create_background_task(
                self._connect_loop(), f"{DOMAIN} connect {self.name}"
            )

    async def async_wait_connected(self, timeout: float) -> bool:  # noqa: ASYNC109
        """Wait until connected or timeout; return the connection status."""
        if self._connected:
            return True
        if self._connect_task is None:
            return False
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(asyncio.shield(self._connect_task), timeout)
        return self._connected

    async def _connect_loop(self) -> None:
        backoff = INITIAL_BACKOFF
        while not self._stopping:
            try:
                _LOGGER.info(
                    "Connecting to OPC UA server %s (%s)", self.name, self.config.url
                )
                await self.async_connect_once()
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self.last_error = repr(err)
                _LOGGER.warning(
                    "Connection to %s failed: %r, retrying in %ss",
                    self.name,
                    err,
                    backoff,
                )
                with contextlib.suppress(Exception):
                    await self.client.disconnect()
                self.client = self._make_client()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue
            _LOGGER.info("Connected to OPC UA server %s", self.name)
            self.last_error = None
            self._state_task = self.hass.async_create_background_task(
                self._watch_state(), f"{DOMAIN} state {self.name}"
            )
            self._set_connected(True)
            self._schedule_sync()
            return

    async def _watch_state(self) -> None:
        """
        Mirror asyncua client state into the hub availability.

        With auto_reconnect, asyncua's supervisor retries forever and passes
        through DISCONNECTED transiently on each failed attempt, so the hub
        never recreates the client here: it only follows the state.
        """
        async with self.client.uaclient.subscribe_state() as states:
            while not self._stopping:
                state = await states.next_change()
                _LOGGER.debug("Hub %s client state: %s", self.name, state.value)
                if state is UaClientState.CONNECTED:
                    # A reused session does not resend initial values: re-read them.
                    self._needs_refresh = True
                    self._set_connected(True)
                    self._schedule_sync()
                elif state in (UaClientState.RECONNECTING, UaClientState.DISCONNECTED):
                    self._set_connected(False)

    async def async_stop(self) -> None:
        """Stop background tasks and disconnect."""
        self._stopping = True
        for task in (self._connect_task, self._state_task, self._sync_task):
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        if self._connected:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(
                    self.client.disconnect(), timeout=REQUEST_TIMEOUT
                )
        self._subscription = None
        self._subscribed.clear()
        self._set_connected(False)

    @callback
    def _set_connected(self, connected: bool) -> None:
        if connected == self._connected:
            return
        self._connected = connected
        for listener in list(self._connection_listeners):
            listener()

    # ----------------------------------------------------------- subscription

    @callback
    def async_register_nodes(self, nodeids: Iterable[str]) -> None:
        """Ask the hub to monitor these nodes (incremental, safe to call any time)."""
        new = {normalize_nodeid(n) for n in nodeids} - self._wanted
        if not new:
            return
        self._wanted |= new
        self._schedule_sync()

    @callback
    def _schedule_sync(self) -> None:
        if not self._connected or self._stopping:
            return
        if self._sync_task is None or self._sync_task.done():
            self._sync_task = self.hass.async_create_background_task(
                self._sync_subscription(), f"{DOMAIN} subscribe {self.name}"
            )

    async def _sync_subscription(self) -> None:
        """Subscribe every wanted node that is not monitored yet (loops until stable)."""
        async with self._sync_lock:
            if self._needs_refresh and self._subscribed:
                self._needs_refresh = False
                await self._refresh_values(sorted(self._subscribed))
            while self._connected and not self._stopping:
                pending = sorted(
                    self._wanted - self._subscribed - set(self.failed_nodes)
                )
                if not pending:
                    return
                try:
                    if self._subscription is None:
                        self._subscription = await self.client.create_subscription(
                            self.config.publishing_interval, _SubscriptionHandler(self)
                        )
                    for start in range(0, len(pending), SUBSCRIBE_CHUNK):
                        chunk = pending[start : start + SUBSCRIBE_CHUNK]
                        nodes = [self.client.get_node(n) for n in chunk]
                        results = await self._subscription.subscribe_data_change(
                            nodes, queuesize=1
                        )
                        for nodeid, result in zip(chunk, results, strict=True):
                            if isinstance(result, ua.StatusCode):
                                self.failed_nodes[nodeid] = result.name
                                _LOGGER.warning(
                                    "Cannot monitor node %s on hub %s: %s",
                                    nodeid,
                                    self.name,
                                    result.name,
                                )
                                self._notify_node(nodeid)
                            else:
                                self._subscribed.add(nodeid)
                except asyncio.CancelledError:
                    raise
                except Exception as err:
                    # Connection-level problem: asyncua's supervisor reconnects and
                    # the CONNECTED transition schedules a new sync.
                    _LOGGER.warning(
                        "Subscription setup failed on hub %s: %r", self.name, err
                    )
                    return
                _LOGGER.info(
                    "Hub %s monitors %d nodes (%d failed)",
                    self.name,
                    len(self._subscribed),
                    len(self.failed_nodes),
                )

    async def _refresh_values(self, nodeids: list[str]) -> None:
        """Read current values of nodes in bulk and dispatch them."""
        try:
            for start in range(0, len(nodeids), SUBSCRIBE_CHUNK):
                chunk = nodeids[start : start + SUBSCRIBE_CHUNK]
                params = ua.ReadParameters()
                for nodeid in chunk:
                    rv = ua.ReadValueId()
                    rv.NodeId = ua.NodeId.from_string(nodeid)
                    rv.AttributeId = ua.AttributeIds.Value
                    params.NodesToRead.append(rv)
                results = await self.client.uaclient.read(params)
                for nodeid, data_value in zip(chunk, results, strict=True):
                    value = data_value.Value.Value if data_value.Value else None
                    self.handle_value(
                        nodeid, value, data_value.StatusCode or ua.StatusCode()
                    )
        except asyncio.CancelledError:
            raise
        except Exception as err:
            _LOGGER.warning("Refreshing values on hub %s failed: %r", self.name, err)

    @callback
    def handle_value(self, nodeid: str, value: Any, status: ua.StatusCode) -> None:
        """Store a pushed value and notify the entities using it."""
        good = status.is_good()
        self._values[nodeid] = NodeValue(value=value, good=good, status=status.name)
        _LOGGER.debug("Hub %s: %s = %r (%s)", self.name, nodeid, value, status.name)
        self._notify_node(nodeid)

    @callback
    def _notify_node(self, nodeid: str) -> None:
        for listener in list(self._node_listeners.get(nodeid, ())):
            listener()

    # -------------------------------------------------------------- listeners

    @callback
    def async_add_node_listener(
        self, nodeid: str, listener: CALLBACK_TYPE
    ) -> CALLBACK_TYPE:
        """Call listener each time the value of nodeid changes."""
        key = normalize_nodeid(nodeid)
        self._node_listeners.setdefault(key, []).append(listener)

        @callback
        def _remove() -> None:
            listeners = self._node_listeners.get(key, [])
            if listener in listeners:
                listeners.remove(listener)

        return _remove

    @callback
    def async_add_connection_listener(self, listener: CALLBACK_TYPE) -> CALLBACK_TYPE:
        """Call listener each time the connection status changes."""
        self._connection_listeners.append(listener)

        @callback
        def _remove() -> None:
            if listener in self._connection_listeners:
                self._connection_listeners.remove(listener)

        return _remove

    # ----------------------------------------------------------- read / write

    def get(self, nodeid: str) -> NodeValue | None:
        """Return the last value of a node (None if never received)."""
        return self._values.get(normalize_nodeid(nodeid))

    def value(self, nodeid: str) -> Any:
        """Return the last good value of a node, or None."""
        node_value = self.get(nodeid)
        if node_value is None or not node_value.good:
            return None
        return node_value.value

    def is_node_ok(self, nodeid: str) -> bool:
        """Return True if the node has a good value and the hub is connected."""
        node_value = self.get(nodeid)
        return self._connected and node_value is not None and node_value.good

    async def async_write(self, nodeid: str, value: Any) -> None:
        """Write a value to a node, raising HomeAssistantError on any failure."""
        if not self._connected:
            msg = f"OPC UA hub {self.name} is not connected"
            raise HomeAssistantError(msg)
        try:
            key = normalize_nodeid(nodeid)
        except Exception as err:
            msg = f"Invalid node id {nodeid!r}: {err}"
            raise HomeAssistantError(msg) from err
        node = self.client.get_node(key)
        try:
            vtype = self._vtypes.get(key)
            if vtype is None:
                vtype = await asyncio.wait_for(
                    node.read_data_type_as_variant_type(), timeout=REQUEST_TIMEOUT
                )
                self._vtypes[key] = vtype
            try:
                variant = ua.Variant(coerce_value(value, vtype), vtype)
            except (ValueError, TypeError) as err:
                msg = f"Cannot write {value!r} to {key} ({vtype.name}): {err}"
                raise HomeAssistantError(msg) from err
            await asyncio.wait_for(
                node.write_value(ua.DataValue(variant)), timeout=REQUEST_TIMEOUT
            )
        except HomeAssistantError:
            raise
        except ua.UaStatusCodeError as err:
            msg = f"Server refused write to {key} on hub {self.name}: {err}"
            raise HomeAssistantError(msg) from err
        except (TimeoutError, ConnectionError, OSError) as err:
            msg = f"Write to {key} on hub {self.name} failed: {err!r}"
            raise HomeAssistantError(msg) from err
        _LOGGER.debug("Hub %s: wrote %r to %s", self.name, value, key)

    # ------------------------------------------------------------ diagnostics

    def diagnostics(self) -> dict[str, Any]:
        """Return a diagnostics snapshot (no secrets)."""
        return {
            "connected": self._connected,
            "client_state": self.client.uaclient.state.value,
            "last_error": self.last_error,
            "wanted_nodes": len(self._wanted),
            "subscribed_nodes": len(self._subscribed),
            "failed_nodes": dict(self.failed_nodes),
            "bad_quality_nodes": sorted(
                n for n, v in self._values.items() if not v.good
            ),
            "security_policy": self.config.security_policy,
            "publishing_interval": self.config.publishing_interval,
            "snapshot_time": datetime.now(UTC).isoformat(),
        }
