"""Config flow for ha_asyncua."""

from __future__ import annotations

import contextlib
import logging
import re
from typing import TYPE_CHECKING, Any

import voluptuous as vol
from asyncua import ua
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.core import callback
from homeassistant.helpers import selector

from .const import (
    CONF_DISCOVERY_ENABLED,
    CONF_DISCOVERY_EXCLUDE,
    CONF_DISCOVERY_INCLUDE,
    CONF_DISCOVERY_MAX_DEPTH,
    CONF_DISCOVERY_NAMESPACES,
    CONF_DISCOVERY_ROOT,
    CONF_DISCOVERY_WRITABLE,
    CONF_HUB_ID,
    CONF_HUB_MANUFACTURER,
    CONF_HUB_MODEL,
    CONF_HUB_PASSWORD,
    CONF_HUB_PUBLISHING_INTERVAL,
    CONF_HUB_SECURITY_MODE,
    CONF_HUB_SECURITY_POLICY,
    CONF_HUB_URL,
    CONF_HUB_USERNAME,
    DEFAULT_DISCOVERY_MAX_DEPTH,
    DEFAULT_DISCOVERY_ROOT,
    DEFAULT_PUBLISHING_INTERVAL,
    DEFAULT_SECURITY_MODE,
    DOMAIN,
    SECURITY_MODES,
    SECURITY_POLICIES,
    SECURITY_POLICY_NONE,
)
from .hub import OpcuaHub, without_unused_security

if TYPE_CHECKING:
    from collections.abc import Mapping

_LOGGER = logging.getLogger(__name__)

_AUTH_ERRORS = ("BadUserAccessDenied", "BadIdentityToken")
_SECURITY_ERRORS = ("BadCertificate", "BadSecurity", "BadSecurityPolicyRejected")

DEFAULT_OPTIONS = {
    CONF_DISCOVERY_ENABLED: True,
    CONF_DISCOVERY_ROOT: DEFAULT_DISCOVERY_ROOT,
    CONF_DISCOVERY_MAX_DEPTH: DEFAULT_DISCOVERY_MAX_DEPTH,
    CONF_DISCOVERY_INCLUDE: "",
    CONF_DISCOVERY_EXCLUDE: "",
    CONF_DISCOVERY_NAMESPACES: "",
    CONF_DISCOVERY_WRITABLE: "",
}


def _connection_schema(defaults: Mapping[str, Any], *, with_name: bool) -> vol.Schema:
    fields: dict[Any, Any] = {}
    if with_name:
        fields[
            vol.Required(CONF_HUB_ID, default=defaults.get(CONF_HUB_ID, "plc-01"))
        ] = str
    fields.update(
        {
            vol.Required(
                CONF_HUB_URL,
                default=defaults.get(CONF_HUB_URL, "opc.tcp://192.168.0.1:4840"),
            ): str,
            vol.Optional(
                CONF_HUB_USERNAME,
                description={"suggested_value": defaults.get(CONF_HUB_USERNAME)},
            ): str,
            vol.Optional(
                CONF_HUB_PASSWORD,
                description={"suggested_value": defaults.get(CONF_HUB_PASSWORD)},
            ): selector.TextSelector(
                selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
            ),
            vol.Required(
                CONF_HUB_SECURITY_POLICY,
                default=defaults.get(CONF_HUB_SECURITY_POLICY, SECURITY_POLICY_NONE),
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(options=list(SECURITY_POLICIES))
            ),
            vol.Required(
                CONF_HUB_SECURITY_MODE,
                default=defaults.get(CONF_HUB_SECURITY_MODE, DEFAULT_SECURITY_MODE),
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(options=list(SECURITY_MODES))
            ),
            vol.Required(
                CONF_HUB_PUBLISHING_INTERVAL,
                default=defaults.get(
                    CONF_HUB_PUBLISHING_INTERVAL, DEFAULT_PUBLISHING_INTERVAL
                ),
            ): vol.All(vol.Coerce(int), vol.Range(min=10, max=60000)),
            vol.Optional(
                CONF_HUB_MANUFACTURER,
                description={"suggested_value": defaults.get(CONF_HUB_MANUFACTURER)},
            ): str,
            vol.Optional(
                CONF_HUB_MODEL,
                description={"suggested_value": defaults.get(CONF_HUB_MODEL)},
            ): str,
        }
    )
    return vol.Schema(fields)


def _discovery_schema(defaults: Mapping[str, Any]) -> vol.Schema:
    def _suggest(key: str) -> dict[str, Any]:
        return {"suggested_value": defaults.get(key, DEFAULT_OPTIONS[key])}

    return vol.Schema(
        {
            vol.Required(
                CONF_DISCOVERY_ENABLED,
                default=defaults.get(CONF_DISCOVERY_ENABLED, True),
            ): bool,
            vol.Required(
                CONF_DISCOVERY_ROOT,
                default=defaults.get(CONF_DISCOVERY_ROOT, DEFAULT_DISCOVERY_ROOT),
            ): str,
            vol.Required(
                CONF_DISCOVERY_MAX_DEPTH,
                default=defaults.get(
                    CONF_DISCOVERY_MAX_DEPTH, DEFAULT_DISCOVERY_MAX_DEPTH
                ),
            ): vol.All(vol.Coerce(int), vol.Range(min=1, max=50)),
            vol.Optional(
                CONF_DISCOVERY_INCLUDE, description=_suggest(CONF_DISCOVERY_INCLUDE)
            ): str,
            vol.Optional(
                CONF_DISCOVERY_EXCLUDE, description=_suggest(CONF_DISCOVERY_EXCLUDE)
            ): str,
            vol.Optional(
                CONF_DISCOVERY_NAMESPACES,
                description=_suggest(CONF_DISCOVERY_NAMESPACES),
            ): str,
            vol.Optional(
                CONF_DISCOVERY_WRITABLE, description=_suggest(CONF_DISCOVERY_WRITABLE)
            ): str,
        }
    )


def validate_discovery(user_input: dict[str, Any]) -> dict[str, str]:
    """Validate discovery options; return form errors."""
    errors: dict[str, str] = {}
    try:
        ua.NodeId.from_string(user_input[CONF_DISCOVERY_ROOT])
    except Exception:
        errors[CONF_DISCOVERY_ROOT] = "invalid_nodeid"
    for key in (
        CONF_DISCOVERY_INCLUDE,
        CONF_DISCOVERY_EXCLUDE,
        CONF_DISCOVERY_WRITABLE,
    ):
        if user_input.get(key):
            try:
                re.compile(user_input[key])
            except re.error:
                errors[key] = "invalid_regex"
    namespaces = str(user_input.get(CONF_DISCOVERY_NAMESPACES) or "").replace(" ", "")
    if namespaces and not all(ns.isdigit() for ns in namespaces.split(",")):
        errors[CONF_DISCOVERY_NAMESPACES] = "invalid_namespaces"
    return errors


class AsyncuaConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for an OPC UA server."""

    VERSION = 1

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    async def _async_test_connection(
        self, data: dict[str, Any]
    ) -> tuple[str | None, str]:
        """Try to connect; return (error key or None, details)."""
        from . import hub_config_from  # noqa: PLC0415 - avoid import cycle

        hub = OpcuaHub(self.hass, hub_config_from(data))
        try:
            await hub.async_connect_once()
        except Exception as err:
            text = repr(err)
            _LOGGER.debug("Connection test to %s failed: %s", data[CONF_HUB_URL], text)
            if any(code in text for code in _AUTH_ERRORS):
                return "invalid_auth", text
            if any(code in text for code in _SECURITY_ERRORS):
                return "security_error", text
            return "cannot_connect", text
        finally:
            with contextlib.suppress(Exception):
                await hub.client.disconnect()
        return None, ""

    def _cert_placeholder(self, data: dict[str, Any]) -> str:
        from . import hub_config_from  # noqa: PLC0415

        cert, _ = OpcuaHub(self.hass, hub_config_from(data)).certificate_paths()
        return str(cert)

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Ask for the server connection parameters."""
        errors: dict[str, str] = {}
        details = ""
        if user_input is not None:
            await self.async_set_unique_id(user_input[CONF_HUB_ID])
            self._abort_if_unique_id_configured()
            error, details = await self._async_test_connection(user_input)
            if error is None:
                self._data = without_unused_security(user_input)
                return await self.async_step_discovery()
            errors["base"] = error
        defaults = user_input or {}
        return self.async_show_form(
            step_id="user",
            data_schema=_connection_schema(defaults, with_name=True),
            errors=errors,
            description_placeholders={
                "details": details,
                "certificate": self._cert_placeholder(
                    {CONF_HUB_ID: defaults.get(CONF_HUB_ID, "plc-01"), CONF_HUB_URL: ""}
                ),
            },
        )

    async def async_step_discovery(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Choose what to discover on the server."""
        errors: dict[str, str] = {}
        if user_input is not None:
            errors = validate_discovery(user_input)
            if not errors:
                return self.async_create_entry(
                    title=self._data[CONF_HUB_ID],
                    data=self._data,
                    options={**DEFAULT_OPTIONS, **user_input},
                )
        return self.async_show_form(
            step_id="discovery",
            data_schema=_discovery_schema(user_input or DEFAULT_OPTIONS),
            errors=errors,
        )

    async def async_step_migrate(
        self, migrate_data: dict[str, Any]
    ) -> ConfigFlowResult:
        """Re-create a UI hub of the former ``ha-asyncua`` domain."""
        await self.async_set_unique_id(migrate_data["unique_id"])
        self._abort_if_unique_id_configured()
        return self.async_create_entry(
            title=migrate_data["title"],
            data=without_unused_security(migrate_data["data"]),
            options=migrate_data["options"],
        )

    async def async_step_import(self, import_data: dict[str, Any]) -> ConfigFlowResult:
        """Import a hub defined in configuration.yaml (YAML stays the source of truth)."""
        await self.async_set_unique_id(import_data[CONF_HUB_ID])
        for entry in self._async_current_entries(include_ignore=False):
            if entry.unique_id == import_data[CONF_HUB_ID]:
                if entry.data != import_data:
                    self.hass.config_entries.async_update_entry(entry, data=import_data)
                return self.async_abort(reason="already_configured")
        return self.async_create_entry(
            title=import_data[CONF_HUB_ID],
            data=import_data,
            options={**DEFAULT_OPTIONS, CONF_DISCOVERY_ENABLED: False},
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Change connection parameters of a UI-created hub."""
        entry = self._get_reconfigure_entry()
        if entry.source == "import":
            return self.async_abort(reason="yaml_managed")
        errors: dict[str, str] = {}
        details = ""
        if user_input is not None:
            data = {**entry.data, **user_input}
            for key in (CONF_HUB_USERNAME, CONF_HUB_PASSWORD):
                if not user_input.get(key):
                    data.pop(key, None)
            error, details = await self._async_test_connection(data)
            if error is None:
                return self.async_update_reload_and_abort(
                    entry, data=without_unused_security(data)
                )
            errors["base"] = error
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=_connection_schema(user_input or entry.data, with_name=False),
            errors=errors,
            description_placeholders={
                "details": details,
                "certificate": self._cert_placeholder(dict(entry.data)),
            },
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:  # noqa: ARG004
        """Return the options flow."""
        return AsyncuaOptionsFlow()


class AsyncuaOptionsFlow(OptionsFlow):
    """Discovery options."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Edit discovery options."""
        errors: dict[str, str] = {}
        if user_input is not None:
            errors = validate_discovery(user_input)
            if not errors:
                return self.async_create_entry(data={**DEFAULT_OPTIONS, **user_input})
        return self.async_show_form(
            step_id="init",
            data_schema=_discovery_schema(user_input or self.config_entry.options),
            errors=errors,
            description_placeholders={"namespaces": await self._namespaces()},
        )

    async def _namespaces(self) -> str:
        """List the server namespaces to help choosing the filter."""
        runtime = getattr(self.config_entry, "runtime_data", None)
        if runtime is None or not runtime.hub.connected:
            return "?"
        try:
            names = await runtime.hub.client.get_namespace_array()
        except Exception:
            return "?"
        return ", ".join(f"{index}={name or '-'}" for index, name in enumerate(names))
