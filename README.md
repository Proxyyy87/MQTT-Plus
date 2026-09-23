![Logo](admin/mqtt-plus.png)
# ioBroker.mqtt-plus

[![NPM version](https://img.shields.io/npm/v/iobroker.mqtt-plus.svg)](https://www.npmjs.com/package/iobroker.mqtt-plus)
[![Downloads](https://img.shields.io/npm/dm/iobroker.mqtt-plus.svg)](https://www.npmjs.com/package/iobroker.mqtt-plus)
![Number of Installations](https://iobroker.live/badges/mqtt-plus-installed.svg)
![Current version in stable repository](https://iobroker.live/badges/mqtt-plus-stable.svg)

[![NPM](https://nodei.co/npm/iobroker.mqtt-plus.png?downloads=true)](https://nodei.co/npm/iobroker.mqtt-plus/)

**Tests:** ![Test and Release](https://github.com/Proxyyy87/ioBroker.mqtt-plus/workflows/Test%20and%20Release/badge.svg)

## mqtt-plus adapter for ioBroker

**mqtt-plus** mirrors ioBroker states into the namespace of an already installed MQTT client
adapter (e.g. `mqtt.0`) and back. It creates the required folder structure automatically,
converts values on the fly, provides a secured web dashboard for backup/restore and can
additionally push the data to an external webhook via HTTP(S).

`mqtt-plus` does not speak the [MQTT protocol](https://mqtt.org/) itself. Connection, QoS and
`retain` towards the broker are handled entirely by the separately installed MQTT adapter;
`mqtt-plus` only reads and writes states in its namespace. Protocol details and the
specification are documented at [mqtt.org](https://mqtt.org/getting-started/).

### Features

* **Bidirectional mirroring** – `out` (ioBroker → MQTT), `in` (MQTT → ioBroker) or `both`.
* **Topic mode per mapping** – `single` (one shared topic) or `dual` following the common MQTT
  convention (state on `<topic>`, commands on `<topic>/set`).
* **Type conversion** – rounding, boolean ↔ number or automatic detection of the target type,
  in both directions.
* **Unaltered timestamps** – the mirror takes over `ts`/`lc`/`q` of the source instead of
  setting a new "now" on every copy. Inactive sources (age or quality) are detected and not kept
  artificially fresh. Three sync modes per mapping allow exceptions for rarely reporting sensors
  or targets that should always look fresh.
* **Force sync** – compares source and target directly and only heals real deviations, e.g.
  after a restart or a lost connection.
* **Ack filter per mapping** – for states without a real device behind them
  (`0_userdata.0.*`, `alias.0.*`).
* **Secured web dashboard** – basic auth with brute-force lockout, optional HTTPS, CSRF/CORS
  protection. Live status, JSON structure preview, backup & restore.
* **Remote sync** – sends the data periodically or on demand via HTTP(S) POST to an external
  webhook. The payload is freely configurable via template; TLS verification is always active
  (an own CA can be configured for internal/self-signed targets).

### Requirements

* js-controller >= 6.0.11
* Admin >= 7.6.20
* Node.js >= 22
* An installed and configured MQTT client adapter (e.g.
  [ioBroker.mqtt](https://github.com/ioBroker/ioBroker.mqtt)) whose namespace is used as target
  prefix.

### Configuration

The complete configuration guide (topic mode, echo protection, sync mode, remote sync
templates) is available in German in [admin/readme.md](admin/readme.md) – this is the same text
that ioBroker Admin shows in the instance view.

The adapter is currently in the review process for the official ioBroker repository.

## Changelog
<!--
    Placeholder for the next version (at the beginning of the line):
    ### **WORK IN PROGRESS**
-->
### 1.6.2 (2026-09-22)
* (proxy) Web server retries several times if the port is still in use (e.g. during an update)
  instead of shutting the instance down permanently
* (proxy) Compact mode compatibility: removed `process.exit()`, adapter timers are used for delays
* (proxy) Node.js 22 is required as minimum, dependencies updated
* (proxy) Admin configuration validated against the official jsonConfig schema
* (proxy) Translations for all supported languages, GitHub Actions workflow for tests and release

### 1.6.1 (2026-09-22)
* (proxy) Per-mapping sync mode: "standard", "pass every update" (same value with a newer
  timestamp is mirrored, for rarely changing sensors) or "force as before 1.6.0"
* (proxy) Mapping settings are shown as accordion with two short rows per entry

## License
MIT License

Copyright (c) 2026 proxy <zumloeschen@ich.ms>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
