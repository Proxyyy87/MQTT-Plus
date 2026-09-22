<p align="center">
  <img src="admin/mqtt-plus.png" alt="MQTT Plus Logo" width="96">
</p>

<h1 align="center">ioBroker.mqtt-plus</h1>

<p align="center">
  <a href="LICENSE"><img alt="License" src="https://img.shields.io/badge/License-MIT-blue"></a>
  <img alt="Version" src="https://img.shields.io/badge/Version-1.6.1-informational">
  <img alt="js-controller" src="https://img.shields.io/badge/js--controller-%3E%3D6.0.11-informational">
  <img alt="Tests" src="https://img.shields.io/badge/Tests-not%20implemented-yellow">
</p>

**mqtt-plus** ist ein Adapter für [ioBroker](https://www.iobroker.net/), der ioBroker-Datenpunkte
mit dem Namespace eines bereits installierten MQTT-Client-Adapters (z. B. `mqtt.0`) spiegelt.
Er legt die nötige Ordnerstruktur automatisch an, konvertiert Werte live, bietet ein
abgesichertes Web-Dashboard für Backup/Restore und kann Daten zusätzlich per HTTP(S) an einen
externen Webhook senden.

`mqtt-plus` spricht selbst kein MQTT-Protokoll — Verbindung, QoS und `retain` zum eigentlichen
Broker liegen vollständig beim separat installierten MQTT-Adapter, auf dessen Namespace
`mqtt-plus` nur liest/schreibt.

## Funktionsumfang

* **Bidirektionale Spiegelung** — `out` (IOB → MQTT), `in` (MQTT → IOB) oder `both`.
* **Topic-Modus pro Mapping** — `single` (ein gemeinsames Topic) oder `dual` nach üblicher
  MQTT-Konvention (Status auf `<topic>`, Befehle auf `<topic>/set`).
* **Typ-Konvertierung** — Runden, Bool ↔ Zahl oder automatische Erkennung des Zieltyps, in
  beide Richtungen.
* **Unverfälschte Zeitstempel** — der Spiegel übernimmt `ts`/`lc`/`q` der Quelle statt bei
  jedem Kopiervorgang ein neues "jetzt" zu setzen. Inaktive Quellen (Alter oder Qualität)
  werden erkannt und nicht künstlich frisch gehalten; drei Sync-Modi pro Mapping erlauben
  Ausnahmen für selten meldende Sensoren oder bewusst immer frische Ziele.
* **Force-Sync** — vergleicht Quelle und Ziel direkt und heilt nur echte Abweichungen, z. B.
  nach einem Neustart oder Verbindungsabbruch.
* **Ack-Filter pro Mapping** — für Datenpunkte ohne echtes Gerät dahinter
  (`0_userdata.0.*`, `alias.0.*`).
* **Abgesichertes Web-Dashboard** — Basic-Auth mit Brute-Force-Sperre, optional HTTPS,
  CSRF-/CORS-Schutz. Live-Status, JSON-Struktur-Vorschau, Backup & Restore.
* **Remote Sync** — sendet die Daten zyklisch oder manuell per HTTP(S) POST an einen externen
  Webhook, Payload frei per Template konfigurierbar, TLS-Prüfung immer aktiv (optionale eigene
  CA für interne/selbstsignierte Ziele).
* **Admin 7 Ready** — modernes `jsonConfig` mit responsiven Elementen.

Die vollständige Konfigurationsanleitung inklusive aller Details zu Topic-Modus, Echo-Schutz,
Sync-Modus und Remote-Sync-Templates steht in **[admin/readme.md](admin/readme.md)** — das ist
derselbe Text, den ioBroker Admin auch in der Instanz-Ansicht anzeigt.

## Installation

`mqtt-plus` ist (noch) kein Teil des offiziellen ioBroker-Repositories. Installation direkt aus
diesem Repository:

**Über die Admin-Oberfläche:**
Instanzen → "+" → Reiter *Benutzerdefiniert* → GitHub-URL eintragen:

```
https://github.com/Proxyyy87/MQTT-Plus/
```

**Über die Kommandozeile:**

```bash
iobroker url https://github.com/Proxyyy87/MQTT-Plus/tarball/main mqtt-plus
```

Beim Installieren aus dem Git-Repository wird TypeScript automatisch kompiliert
(`npm`-`prepare`-Skript) — ein manueller Build-Schritt ist nicht nötig.

Vorausgesetzt wird ein bereits eingerichteter MQTT-Client-Adapter (z. B.
[ioBroker.mqtt](https://github.com/ioBroker/ioBroker.mqtt)), dessen Namespace `mqtt-plus` als
Ziel-Präfix verwendet.

## Entwicklung

```bash
git clone https://github.com/Proxyyy87/MQTT-Plus.git
cd MQTT-Plus
npm install
npm run build   # kompiliert src/*.ts nach build/
npm run watch   # kompiliert bei Änderungen automatisch neu
```

Technisch basiert der Adapter auf TypeScript, `@iobroker/adapter-core` und `axios`; das
Web-Dashboard und der HTTP(S)-Server sind mit Bordmitteln von Node.js (`http`/`https`)
umgesetzt, ohne zusätzliches Web-Framework.

## Changelog

Die vollständige Versionshistorie steht in [`io-package.json`](io-package.json) unter
`common.news`. Die letzten Versionen:

* **1.6.1** — Sync-Modus pro Mapping (`Standard`, `Jede Meldung weiterreichen`,
  `Force wie vor 1.6.0`); Mapping-Einstellungen als aufklappbare Liste mit zwei kurzen Zeilen
  pro Eintrag statt einer breiten Tabelle.
* **1.6.0** — Zeitstempel (`ts`/`lc`/`q`) werden beim Spiegeln durchgereicht statt überschrieben;
  Start, Update-Intervall und Force-Sync überspringen inaktive Quellen; Force-Sync schreibt nur
  tatsächlich abweichende Werte; neue Remote-Sync-Platzhalter `%LC%`, `%ACK%`, `%Q%`.
* **1.5.x** — Topic-Modus `dual` (Status/Befehl auf getrennten Topics) pro Mapping.

## Lizenz

MIT — siehe [LICENSE](LICENSE).
