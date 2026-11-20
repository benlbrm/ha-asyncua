"""Constants for the asyncua integration."""

from typing import Final

DOMAIN: Final = "ha-asyncua"

# Hub (OPC UA server) configuration keys.
CONF_HUB_ID: Final = "name"
CONF_HUB_MANUFACTURER: Final = "manufacturer"
CONF_HUB_MODEL: Final = "model"
CONF_HUB_SCAN_INTERVAL: Final = "scan_interval"  # deprecated, ignored
CONF_HUB_URL: Final = "url"
CONF_HUB_USERNAME: Final = "username"
CONF_HUB_PASSWORD: Final = "password"
CONF_HUB_PUBLISHING_INTERVAL: Final = "publishing_interval"
CONF_HUB_SECURITY_POLICY: Final = "security_policy"
CONF_HUB_SECURITY_MODE: Final = "security_mode"
CONF_HUB_CERTIFICATE: Final = "certificate"
CONF_HUB_PRIVATE_KEY: Final = "private_key"

DEFAULT_PUBLISHING_INTERVAL: Final = 100  # ms

SECURITY_POLICY_NONE: Final = "None"
SECURITY_POLICIES: Final = (
    SECURITY_POLICY_NONE,
    "Basic256Sha256",
    "Aes128Sha256RsaOaep",
    "Aes256Sha256RsaPss",
)
SECURITY_MODES: Final = ("Sign", "SignAndEncrypt")
DEFAULT_SECURITY_MODE: Final = "SignAndEncrypt"

# Discovery options (config entry options).
CONF_DISCOVERY_ENABLED: Final = "discovery_enabled"
CONF_DISCOVERY_ROOT: Final = "discovery_root"
CONF_DISCOVERY_MAX_DEPTH: Final = "discovery_max_depth"
CONF_DISCOVERY_INCLUDE: Final = "discovery_include"
CONF_DISCOVERY_EXCLUDE: Final = "discovery_exclude"
CONF_DISCOVERY_NAMESPACES: Final = "discovery_namespaces"
CONF_DISCOVERY_WRITABLE: Final = "discovery_writable"

DEFAULT_DISCOVERY_ROOT: Final = "i=85"  # Objects folder
DEFAULT_DISCOVERY_MAX_DEPTH: Final = 10
# Hard cap so a huge address space never floods Home Assistant.
DISCOVERY_MAX_VARIABLES: Final = 2000
# Above this number of discovered entities, new ones are created disabled.
DISCOVERY_ENABLE_THRESHOLD: Final = 200

# Entity configuration keys (YAML platforms).
CONF_NODES: Final = "nodes"
CONF_NODE_DEVICE_CLASS: Final = "device_class"
CONF_NODE_HUB: Final = "hub"
CONF_NODE_ID: Final = "nodeid"
CONF_NODE_NAME: Final = "name"
CONF_NODE_STATE_CLASS: Final = "state_class"
CONF_NODE_UNIQUE_ID: Final = "unique_id"
CONF_NODE_UNIT_OF_MEASUREMENT: Final = "unit_of_measurement"
CONF_NODE_PRECISION: Final = "precision"
CONF_NODE_PULSE: Final = "pulse_ms"

# Light.
CONF_NODE_ID_LIGHT_ON: Final = "nodeid_light_on"
CONF_NODE_ID_LIGHT_OFF: Final = "nodeid_light_off"

# Switch.
CONF_NODE_SWITCH_DI: Final = "nodeid_switch_di"

# Cover.
CONF_NODE_ID_COVER_POSITION: Final = "nodeid_cover_position"
CONF_NODE_ID_COVER_OPEN: Final = "nodeid_cover_open"
CONF_NODE_ID_COVER_CLOSE: Final = "nodeid_cover_close"
CONF_NODE_ID_COVER_STOP: Final = "nodeid_cover_stop"
CONF_NODE_ID_COVER_SET_POSITION: Final = "nodeid_cover_set_position"

# Number (names kept for backward compatibility: these are values, not node ids).
CONF_NODE_ID_NUMBER_MIN: Final = "nodeid_number_min"
CONF_NODE_ID_NUMBER_MAX: Final = "nodeid_number_max"
CONF_NODE_ID_NUMBER_STEP: Final = "nodeid_number_step"
CONF_NODE_ID_NUMBER_MODE: Final = "nodeid_number_mode"

# Services.
ATTR_NODE_HUB: Final = "hub"
ATTR_NODE_ID: Final = "nodeid"
ATTR_VALUE: Final = "value"
SERVICE_SET_VALUE: Final = "set_value"
SERVICE_REDISCOVER: Final = "rediscover"

# Platforms.
YAML_PLATFORMS: Final = (
    "sensor",
    "binary_sensor",
    "switch",
    "light",
    "cover",
    "number",
)
ENTRY_PLATFORMS: Final = (
    "sensor",
    "binary_sensor",
    "switch",
    "number",
    "text",
    "button",
)
