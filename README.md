# OPC UA client for Home Assistant (asyncua)

> **Note**: fork of [KVSoong/asyncua](https://github.com/KVSoong/asyncua) with OPC UA subscriptions, automatic discovery of variables, robust reconnection and light / cover / number entities.

Connect industrial PLCs and any OPC UA server to Home Assistant. Values are **pushed** by OPC UA subscriptions (no polling), the connection is supervised and restored automatically, and the variables exposed by the server can be **discovered and added to Home Assistant automatically**.

## Table of contents

- [Installation](#installation)
- [Add a server from the UI (recommended)](#add-a-server-from-the-ui-recommended)
- [Automatic discovery](#automatic-discovery)
- [YAML configuration](#yaml-configuration)
  - [Hub](#hub)
  - [Sensor](#sensor) · [Binary sensor](#binary-sensor) · [Switch](#switch) · [Light](#light) · [Cover](#cover) · [Number](#number)
- [Services](#services)
- [Secure connections (certificates)](#secure-connections-certificates)
- [Diagnostics](#diagnostics)
- [Upgrading from 0.1.x](#upgrading-from-01x)
- [Development](#development)

## Installation

<details>
<summary>With HACS</summary>

1. In HACS, open the menu (three dots) → **Custom repositories**.
2. Add `https://github.com/benlbrm/ha-asyncua` with the category **Integration**.
3. Install **OPC UA (asyncua)** and restart Home Assistant.

</details>

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=benlbrm&repository=ha-asyncua&category=integration)

## Add a server from the UI (recommended)

**Settings → Devices & services → Add integration → OPC UA (asyncua)**

1. **Connection**: name (unique hub id, used by YAML entities and services), URL (`opc.tcp://host:4840`), optional username / password, security policy, publishing interval. The connection is tested before going further.
2. **Discovery**: choose what to browse and which variables may be controlled (see below).

The server becomes a device with a **Connected** diagnostic entity and, when discovery is enabled, a **Rediscover variables** button. Connection settings can be changed later with **Reconfigure**, discovery settings with **Configure**.

## Automatic discovery

When enabled, the integration browses the server address space and creates one entity per variable:

| OPC UA data type | Read-only (default) | Controllable (matches *Controllable variables*) |
| - | - | - |
| Boolean | `binary_sensor` | `switch` |
| Integer / Float / Double | `sensor` | `number` |
| String | `sensor` | `text` |
| LocalizedText, DateTime | `sensor` | `sensor` |

- **Read-only by default.** Many PLCs (B&R, Siemens…) report almost every variable as writable, inputs included. A variable only becomes a `switch` / `number` / `text` when it is writable **and** matches the *Controllable variables* regex. Empty regex → nothing can be written from Home Assistant.
- **Devices**: each OPC UA object (folder, task, program…) becomes a device linked to the hub. Each element of an array of structures (`Lights[0]`, `Lights[1]`…) gets its own device; its members are named relative to it (`TON_Publish.PT`).
- **Units and ranges**: the `EngineeringUnits` and `EURange` properties are used when present.
- **Quality**: a value with a bad OPC UA status (e.g. `BadResourceUnavailable`) makes the entity unavailable.
- **Offline start**: the last discovery is stored, so entities exist (unavailable) when the PLC is offline at startup; discovery runs again once it is reachable.
- **Rediscover** with the button or the `rediscover` service after changing the PLC program. Entities and devices that no longer exist are removed.
- Above 200 discovered variables, new entities are created **disabled**: enable those you need. Discovery stops at 2000 variables: use filters.
- Variables already used by YAML entities of the same hub are skipped.

### Options

| Option | Description |
| - | - |
| Root node | Where browsing starts. `i=85` (Objects folder) by default. |
| Maximum depth | Browse depth limit. |
| Include / Exclude | Regex matched against the browse path (`PLC/Modules/<Default>/Heaters/cmdHeater`) or the node id. |
| Namespaces | Comma separated namespace indexes to keep. The server namespace list is shown in the options dialog. |
| Controllable variables | Regex: writable variables matching it become controls. |

<details>
<summary>Example: B&R X20 / Automation Runtime</summary>

B&R exposes every process variable twice: namespace 6 (`urn:B&R/pv/`) and namespace 7 (PLCopen view under `DeviceSet`). Keep only one:

| Option | Value |
| - | - |
| Namespaces | `6` |
| Exclude | `TON_\|TP_\|IotMqtt\|_old$` (function block internals) |
| Controllable variables | `/cmd` (only `cmd*` variables can be written) |

</details>

## YAML configuration

YAML keeps working. Each YAML hub is also imported as an integration entry (so it gets a device, diagnostics and, if you enable it in **Configure**, discovery). YAML stays the source of truth for its connection settings.

### Hub

```yaml
ha-asyncua:
  - name: "plc-01"              # unique hub id
    url: "opc.tcp://192.168.0.10:4840/"
    username: admin             # optional (with password)
    password: !secret plc_password
    publishing_interval: 100    # optional, ms (default 100)
    manufacturer: "B&R"         # optional, shown on the device
    model: "X20CP1584"          # optional
    security_policy: None       # optional: None, Basic256Sha256, Aes128Sha256RsaOaep, Aes256Sha256RsaPss
    security_mode: SignAndEncrypt  # optional: Sign, SignAndEncrypt
    certificate: /config/opcua/client_cert.der  # optional (with private_key), else auto-generated
    private_key: /config/opcua/client_key.pem
```

`scan_interval` is still accepted but ignored: values are pushed by subscription.

Every entity below supports `name`, `hub`, and optional `unique_id` (default `ha-asyncua.<hub>.<nodeid>`). Node ids are validated at startup.

### Sensor

```yaml
sensor:
  - platform: ha-asyncua
    nodes:
      - name: "Boiler temperature"
        hub: "plc-01"
        nodeid: "ns=6;s=::AsGlobalPV:g_temp"
        device_class: temperature   # optional
        unit_of_measurement: "°C"   # optional
        state_class: measurement    # optional (default measurement when a unit or device class is set)
        precision: 1                # optional, displayed decimals (default 2)
```

### Binary sensor

```yaml
binary_sensor:
  - platform: ha-asyncua
    nodes:
      - name: "Garage detector"
        hub: "plc-01"
        nodeid: "ns=6;s=::AsGlobalPV:diDetecteurGarage"
        device_class: motion        # optional
```

### Switch

The command is written to `nodeid`; the state is read from `nodeid_switch_di` (e.g. a contactor feedback input) when set, otherwise from `nodeid`.

```yaml
switch:
  - platform: ha-asyncua
    nodes:
      - name: "Heater office"
        hub: "plc-01"
        nodeid: "ns=6;s=::Heaters:cmdDoRadiateurBureau"
        nodeid_switch_di: "ns=6;s=::AsGlobalPV:doRadiateurBureau"   # optional
```

### Light

State is read from `nodeid`. `nodeid_light_on` / `nodeid_light_off` are optional command nodes: when they differ from `nodeid`, `True` is written to them (a command bit for the PLC, reset after `pulse_ms` if set); otherwise the target state is written to `nodeid` directly.

```yaml
light:
  - platform: ha-asyncua
    nodes:
      - name: "Office"
        hub: "plc-01"
        nodeid: "ns=6;s=::Lights:Telerupteurs[0].Light"
        nodeid_light_on: "ns=6;s=::Lights:Telerupteurs[0].SwitchOn"
        nodeid_light_off: "ns=6;s=::Lights:Telerupteurs[0].SwitchOff"
        pulse_ms: 500    # optional: write False after 500 ms
```

### Cover

Position feedback (0–100) is required. Open / close / stop write `True` to their command node (reset after `pulse_ms` if set). Without open / close nodes, the cover moves through `nodeid_cover_set_position`. Only the features whose nodes are configured are offered.

```yaml
cover:
  - platform: ha-asyncua
    nodes:
      - name: "Bedroom shutter"
        hub: "plc-01"
        nodeid_cover_position: "ns=6;s=::Covers:Cover_Chambre.Position"
        nodeid_cover_open: "ns=6;s=::Covers:cmdCoverChambreOpen"          # optional
        nodeid_cover_close: "ns=6;s=::Covers:cmdCoverChambreClose"        # optional
        nodeid_cover_stop: "ns=6;s=::Covers:cmdCoverChambreStop"          # optional
        nodeid_cover_set_position: "ns=6;s=::Covers:Cover_Chambre.SetPos" # optional
        device_class: shutter   # optional
        pulse_ms: 500           # optional
```

### Number

The value is converted to the node data type before writing (e.g. `21.0` → `Int16 21`; `21.5` to an integer node is refused with an error).

```yaml
number:
  - platform: ha-asyncua
    nodes:
      - name: "Temperature setpoint"
        hub: "plc-01"
        nodeid: "ns=6;s=::Config:SetpointTemperature"
        unit_of_measurement: "°C"
        min_value: 15
        max_value: 30
        step: 0.5
        mode: slider   # auto, box or slider
```

<details>
<summary>Split YAML entities in several files</summary>

```yaml
binary_sensor ha-asyncua: !include_dir_merge_list opcua/binary_sensors/
sensor ha-asyncua: !include_dir_merge_list opcua/sensors/
switch ha-asyncua: !include_dir_merge_list opcua/switchs/
light ha-asyncua: !include_dir_merge_list opcua/lights/
cover ha-asyncua: !include_dir_merge_list opcua/covers/
number ha-asyncua: !include_dir_merge_list opcua/numbers/
```

Each file contains a list of `- platform: ha-asyncua` blocks.

</details>

## Services

### `ha-asyncua.set_value`

Write a value to any node. The value is converted to the node data type; errors (unknown hub, invalid value, write refused by the server, hub offline) are reported to the caller.

```yaml
action: ha-asyncua.set_value
data:
  hub: plc-01
  nodeid: "ns=6;s=::AsGlobalPV:setpoint"
  value: 21.5
```

### `ha-asyncua.rediscover`

Browse the server again and update discovered entities.

```yaml
action: ha-asyncua.rediscover
data:
  hub: plc-01
```

## Secure connections (certificates)

With a security policy other than `None`, the client needs a certificate trusted by the server:

- Without `certificate` / `private_key`, a self-signed certificate is generated in `/config/ha-asyncua/<hub>/client_cert.der` (renewed automatically when expired).
- The first connection is usually rejected (`security_error`): trust the certificate on the server (e.g. Automation Studio / TIA Portal certificate store, or move it from *rejected* to *trusted* on the PLC), then retry.

## Diagnostics

- **Connected** binary sensor on the hub device, with monitored / failed node counts and the last error.
- **Download diagnostics** on the integration entry: connection state, failed nodes, bad quality nodes, discovery summary (credentials redacted).
- Debug logs:

```yaml
logger:
  logs:
    custom_components.ha-asyncua: debug
    asyncua: warning
```

## Upgrading from 0.1.x

- YAML hubs now also appear in **Devices & services**. Discovery is **disabled** for them (nothing is browsed, an info log says so) until you enable it in **Configure** (for B&R, set *Namespaces* to `6`).
- Entity unique ids are unchanged, so existing entities, names and history are kept.
- `binary_sensor`: the configured `device_class` is now honoured (it was always *occupancy*).
- `cover`: only the features whose nodes are configured are offered.
- `light` / `cover`: optional `pulse_ms` to reset command bits.
- `number`: `min_value` / `max_value` / `step` / `mode` now work as documented (the `nodeid_number_*` keys are still accepted).
- Write errors are now reported in the UI instead of being only logged.
- Requires `asyncua==2.1.0` (reconnection and subscription recovery are handled by asyncua's supervisor).

## Development

```bash
scripts/setup     # install requirements (Home Assistant 2026.11.2, Python >= 3.14.2)
pip install -r requirements_test.txt  # test deps, in a separate venv: the HA test plugin pins its own HA version
scripts/test      # run the test-suite (embedded OPC UA server, no PLC needed)
scripts/lint      # ruff format + check
scripts/develop   # start Home Assistant with ./config
```

Optional read-only test against a real server (never writes):

```bash
ASYNCUA_LIVE_URL=opc.tcp://192.168.0.10:4840/ ASYNCUA_LIVE_USER=dev ASYNCUA_LIVE_PASSWORD=... \
ASYNCUA_LIVE_NAMESPACES=6 scripts/test tests/test_live_plc.py -s
```
