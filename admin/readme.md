# ioBroker.mqtt-plus

![Logo](mqtt-plus.png)

**Tests:** ![Test Status](https://img.shields.io/badge/Tests-not%20implemented-yellow)
**Lizenz:** ![License](https://img.shields.io/badge/License-MIT-blue)

## Zusammenfassung

`mqtt-plus` ist ein Adapter für ioBroker, der als Brücke zwischen ioBroker-Datenpunkten und
einem MQTT-Broker fungiert (genauer: zwischen ioBroker-Datenpunkten und dem Namespace eines
bereits installierten MQTT-Client-Adapters, z.B. `mqtt.0`). Er legt automatisch die nötige
Ordnerstruktur auf der Zielseite an, konvertiert Werte live, bietet ein abgesichertes
Web-Dashboard für Backup/Restore und Struktur-Analyse und kann Daten zusätzlich aktiv an einen
externen Webhook (Remote Sync) senden.

`mqtt-plus` spricht selbst kein MQTT-Protokoll — Verbindung, QoS und `retain` zum eigentlichen
Broker liegen vollständig beim separat installierten MQTT-Adapter, auf dessen Namespace
`mqtt-plus` nur liest/schreibt.

## Hauptfunktionen

* **Bidirektionale Spiegelung:** Synchronisiert Werte von ioBroker zu MQTT (`out`), von MQTT zu
  ioBroker (`in`) oder in beide Richtungen (`both`).
* **Topic-Modus pro Mapping:** `single` (ein gemeinsames Topic) oder `dual` nach üblicher
  MQTT-Konvention — Status auf `<topic>`, Befehle auf `<topic>/set` (Suffix frei wählbar).
* **Automatische Strukturerstellung:** Erstellt rekursiv fehlende Ordner und Datenpunkte auf der
  Zielseite basierend auf den MQTT-Pfaden (z.B. wird `home/küche/licht` zu `home.küche.licht`).
* **Typ-Konvertierung:** Rundet Zahlen (mit konfigurierbaren Nachkommastellen), wandelt
  Boolean ↔ Zahl um oder erkennt bei `Auto` automatisch den tatsächlichen ioBroker-Datentyp des
  Zielobjekts — in beide Richtungen.
* **Deterministischer Echo-Schutz:** Verhindert Rückkopplungsschleifen bei `both`-Mappings, ohne
  echte Gerätebestätigungen zu verschlucken (siehe [Echo-Schutz](#echo-schutz-bei-both-mappings)).
* **Ack-Filter pro Mapping:** Datenpunkte ohne echtes Gerät dahinter (`0_userdata.0.*`,
  `alias.0.*`) können sofort statt erst zeitverzögert synchronisiert werden.
* **Force-Sync:** Zusätzliches, konfigurierbares Intervall, das den Cache umgeht, Quelle und
  Ziel direkt vergleicht und nur tatsächlich abweichende Werte neu schreibt — heilt Zustände,
  die durch einen Neustart oder Verbindungsabbruch auseinandergelaufen sind.
* **Unverfälschte Zeitstempel:** Der Spiegel übernimmt `ts` (letzte Aktualisierung), `lc`
  (letzte Änderung) und `q` (Qualität) der Quelle. Inaktive Geräte werden auf der Zielseite
  nicht wieder „frisch“ (siehe [Zeitstempel & Aktualität](#zeitstempel--aktualität)).
* **Abgesichertes Web-Dashboard:** Login-Pflicht (Basic-Auth), optional per HTTPS, mit
  Brute-Force-Sperre. Bietet Live-Status, JSON-Struktur-Vorschau, Backup & Restore und einen
  Template-Editor für den Remote-Sync.
* **Remote Sync Client:** Sendet die gesammelten Daten zyklisch oder manuell per HTTP(S) POST an
  einen externen Server. Payload-Format frei per Template konfigurierbar, TLS-Zertifikatsprüfung
  aktiv (mit optionaler eigener CA für interne/selbstsignierte Ziele).
* **Admin 7 Ready:** Nutzt modernes `jsonConfig` mit responsiven Elementen.

---

## Konfiguration

Die Konfiguration erfolgt über die ioBroker Admin-Oberfläche, verteilt auf drei Reiter.

### Reiter 1: Einstellungen & Routen

* **MQTT Ziel-Pfad (Prefix):** Der Basis-Namespace, unter dem die gespiegelten Objekte angelegt
  werden. Muss mit einem Punkt enden (Standard: `mqtt.0.`). Ist das Feld leer, wird automatisch
  auf `mqtt.0.` zurückgefallen.
* **Update Intervall (s):** Zyklischer Abgleich der `out`/`both`-Richtungen — schreibt nur
  Werte, die sich seit dem letzten Durchlauf tatsächlich geändert haben (kein Zwangs-Schreiben,
  das übernimmt der Force-Sync). Minimum 5s, auch wenn ein kleinerer Wert eingetragen wird.
* **Log Aktiv:** Schreibt jeden synchronisierten Wert als Info-Zeile ins ioBroker-Protokoll.
* **Force-Sync Intervall (Min, 0 = aus):** Ignoriert den Cache und vergleicht alle
  `out`/`both`-Werte sowie alle `in`-Werte (Modus `Single`) direkt mit dem aktuellen Wert auf der
  Zielseite. Geschrieben wird nur, wo beide tatsächlich voneinander abweichen und die Quelle
  aktiv ist — zur Heilung von Zuständen, die z.B. nach einem Neustart auseinandergelaufen sind.
  Die Schreibvorgänge werden einzeln um 75ms gestaffelt, um das Funkbudget (Zigbee/433MHz)
  nicht als Burst zu belasten.
* **Aktualitätsgrenze (Min, 0 = aus):** Standard `1440` (24 h). Quellen, deren letztes Update
  älter ist oder die eine Qualität `q ≠ 0` melden, werden beim Start, im Update-Intervall und
  beim Force-Sync nicht gespiegelt. Pro Mapping überschreibbar (Spalte „Aktualität“).
* **Mapping Tabelle:** Das Herzstück des Adapters.
    * **Quell-ID (ioBroker):** Der originale ioBroker-Datenpunkt
      (z.B. `shelly.0.SHSW-25#D8BFC01A#1.Relay0.Switch`).
    * **Pfad Suffix:** Der gewünschte MQTT-Pfad (z.B. `büro/licht/decke`). `/` wird automatisch
      zu `.`; Zeichen, die ioBroker in IDs verbietet, werden durch `_` ersetzt.
    * **Richtung:** `IOB -> MQTT`, `MQTT -> IOB` oder `Beide`.
    * **MQTT-Topic-Modus:** Betrifft **ausschließlich die MQTT-Seite**. Siehe
      [Topic-Modus](#topic-modus-single-vs-dual) weiter unten. `Single` (Standard) entspricht
      dem bisherigen Verhalten.
    * **Befehls-Suffix (MQTT):** Nur bei `Dual` relevant, Standard `/set`. Ein führender Slash
      ist optional, verschachtelte Suffixe (`/cmd/write`) sind möglich.
    * **Typ:**
        * `Auto`: Ermittelt den tatsächlichen ioBroker-Datentyp des Zielobjekts (`boolean`,
          `number`) und konvertiert passend dorthin — funktioniert in beide Richtungen.
        * `Runden`: Rundet numerische Werte auf die eingestellten Nachkommastellen.
        * `Bool->Num`: `true`/`false` → `1`/`0`.
        * `Num->Bool`: Erkennt `"0"`/`"false"`/`"off"`/leer als `false` und
          `"1"`/`"true"`/`"on"` als `true`, alles andere über normale JavaScript-Wahrheitswerte.
        * Bei `Beide` wird `Bool->Num`/`Num->Bool` in der Rückrichtung automatisch getauscht
          (nie eine Wert-Invertierung, nur die Darstellung passt sich der jeweiligen Seite an).
    * **Nachkommastellen:** Nur für `Runden` relevant, Standard 2.
    * **Einheit:** Optional, für Exporte/Remote-Sync und neu angelegte Zielobjekte.
    * **Ack-Filter:** Siehe [Ack-Filter](#ack-filter-für-datenpunkte-ohne-gerät) weiter unten.
    * **Aktualität (Min):** Leer = globale Aktualitätsgrenze, `0` = für diesen Eintrag keine
      Altersprüfung (sinnvoll bei Geräten, die nur bei Wertänderung melden, z.B. Fensterkontakte).
    * **Sync-Modus:** Siehe [Sync-Modus pro Mapping](#sync-modus-pro-mapping) weiter unten.

  Die Mappings erscheinen als aufklappbare Liste mit dem Pfad-Suffix als Titel. Jeder Eintrag
  hat zwei Zeilen: oben *was wohin* (Quelle, Pfad, Richtung, Topic-Modus), unten *wie* (Typ,
  Einheit, Ack-Filter, Aktualität, Sync-Modus). Befehls-Suffix und Nachkommastellen werden nur
  angezeigt, wenn sie wirken (Topic-Modus `Dual` bzw. Typ `Runden`).

### Reiter 2: Web Dashboard

* **Webserver Port:** Standard 8095.
* **Bind-Interface:** `0.0.0.0` (alle Interfaces, aus dem LAN erreichbar) oder `127.0.0.1`
  (nur lokal auf dem ioBroker-Host).
* **Dashboard Benutzername / Passwort:** Zugangsdaten fürs Dashboard (HTTP Basic-Auth). Bleibt
  das Passwort leer, ist der Webserver **ohne Zugangsschutz** erreichbar — der Adapter warnt
  dann beim Start explizit im Log.
* **TLS-Zertifikat / TLS-Key (PEM, optional):** Beide zusammen gesetzt → das Dashboard läuft
  über HTTPS statt HTTP, Zugangsdaten werden dann verschlüsselt übertragen. Ohne TLS werden
  Basic-Auth-Zugangsdaten im Klartext (Base64 ist keine Verschlüsselung) über das Netz
  übertragen — auf einem reinen LAN ein geringeres, aber reales Risiko.
* **Dashboard URL:** Schreibgeschütztes Feld mit dem fertigen Link zum Dashboard.

### Reiter 3: Remote Sync

* **Webhook URL:** Die vollständige Ziel-URL, inkl. eventueller API-Key-Parameter.
* **Intervall (Min):** Wie oft automatisch gesendet wird.
* **Inaktive Quellen weglassen:** Werte, die laut Aktualitätsgrenze/Qualität als inaktiv gelten,
  werden nicht an den Webhook gesendet (Standard: aus).
* **Vertrauenswürdiges Zertifikat / CA (PEM, optional):** Nur bei selbstsigniertem/internem
  Zertifikat der Sync-Gegenstelle nötig. Leer lassen bei öffentlichem Zertifikat — dann gilt der
  normale System-Vertrauensstore. **Die TLS-Zertifikatsprüfung ist immer aktiv**, es gibt keine
  Möglichkeit, sie abzuschalten; für interne Ziele wird stattdessen gezielt das eigene
  Zertifikat/CA vertraut, statt die Prüfung pauschal zu deaktivieren.
* **Verbindung Prüfen (Button):** Führt sofort eine Synchronisation aus und zeigt das Ergebnis
  direkt im Admin. Loggt bei diesem manuellen Test zusätzlich den Fingerprint der geladenen CA
  (falls konfiguriert) — nützlich zur Fehlersuche bei Zertifikatsproblemen.

---

## Nutzungskonzepte im Detail

### Das Web Dashboard (Backup & Restore)

Erreichbar über die im Reiter „Web Dashboard“ angezeigte URL (Login erforderlich, falls ein
Passwort gesetzt ist).

**Funktionen:**
1. **Status:** Watchdog, Mapping-Anzahl, Uptime.
2. **Backup & Restore:**
   * **Download Backup:** Lädt eine `.json`-Datei mit Baumstruktur und kompletter
     Mapping-Konfiguration herunter.
   * **Backup Wiederherstellen:** Lädt eine zuvor gesicherte Datei hoch, ersetzt die
     Mapping-Konfiguration und startet den Adapter neu. Der Upload muss von derselben Origin
     kommen wie das Dashboard selbst (CSRF-Schutz) — ein Upload aus einer fremden Webseite
     heraus wird abgelehnt.
3. **JSON Struktur:** Live-Vorschau der generierten MQTT-Baumstruktur.
4. **Remote Sync Konfiguration (Template):** Format des JSON-Objekts pro Datenpunkt, das an den
   Remote-Server gesendet wird.

### Sicherheit

* Ohne gesetztes Dashboard-Passwort ist der Webserver offen erreichbar — der Adapter weist beim
  Start ausdrücklich im Log darauf hin.
* Nach 10 fehlgeschlagenen Login-Versuchen wird die anfragende IP für 5 Minuten gesperrt. Diese
  Sperre übersteht auch einen Adapter-Neustart (wird persistiert).
* Zustandsändernde Requests (z.B. Backup-Upload) werden nur akzeptiert, wenn Origin/Referer zum
  eigenen Host passen — verhindert, dass eine fremde Webseite die Konfiguration über den Browser
  eines eingeloggten Nutzers überschreibt.
* Empfehlung: Passwort setzen und, falls das Dashboard außerhalb eines vertrauenswürdigen LANs
  erreichbar ist, zusätzlich TLS-Zertifikat/Key hinterlegen.

### Ack-Filter für Datenpunkte ohne Gerät

Bei der Richtung ioBroker → MQTT wird standardmäßig nur weitergeleitet, wenn eine
Wertänderung mit `ack: true` markiert ist — das signalisiert normalerweise, dass ein echtes
Gerät/eine Bridge den Wert bestätigt hat (z.B. ein Shelly-Relais). Bei `0_userdata.0.*` und
`alias.0.*` gibt es kein Gerät, das eine solche Bestätigung setzt — Werte, die dort z.B. über
die Admin-UI oder ein Skript geschrieben werden, kommen typischerweise mit `ack: false` an und
werden mit dem Standard-Filter gar nicht übertragen. Der Filter gilt für alle Wege
(Ereignis, Start, Update-Intervall, Force-Sync) — ein nie bestätigter Befehl an ein
Offline-Gerät erscheint so auch nicht nachträglich als Status auf MQTT.

Für genau solche Mappings auf den Ack-Filter **„Auch unbestätigt“** stellen: Jede Änderung wird
dann sofort weitergeleitet, unabhängig vom `ack`-Flag. Bei Mappings mit echtem Gerät dahinter
bleibt der Standard **„Nur bestätigt“** die richtige Wahl. Die Einstellung wirkt ausschließlich
auf die Richtung IOB→MQTT, die Rückrichtung MQTT→IOB verlangt immer eine echte
Broker-Bestätigung.

### Topic-Modus: `single` vs. `dual`

Der Modus wird **pro Mapping** eingestellt, nicht global — verschiedene Geräte können also
unterschiedliche Konventionen nutzen.

**Wichtig zur Abgrenzung:** Der Modus betrifft **nur die MQTT-Seite**. Der ioBroker-Datenpunkt
aus der Spalte „Quell-ID" wird immer direkt angesprochen, ohne jeden Suffix — egal welcher Modus
eingestellt ist. Ein Shelly-Relais oder eine selbst angelegte `0_userdata`-Variable braucht also
kein `/set` und bekommt auch keins; nur der Broker sieht die getrennten Topics.

| Seite | Modus `Single` | Modus `Dual` |
|---|---|---|
| ioBroker (Quell-ID) | `shelly.0.…Relay0.Switch` | `shelly.0.…Relay0.Switch` (identisch) |
| MQTT Status | `wohnzimmer/licht` | `wohnzimmer/licht` |
| MQTT Befehl | `wohnzimmer/licht` (dasselbe) | `wohnzimmer/licht/set` |

**`Single` (Standard, bisheriges Verhalten):** Befehl und Status teilen sich ein Topic. Bei
`Richtung = Beide` wird also auf dasselbe Topic geschrieben, von dem auch gelesen wird.

**`Dual`:** Folgt der üblichen MQTT-Konvention und trennt beide Aufgaben:

| | Topic | Wer schreibt |
|---|---|---|
| Befehl (Schreibzugriff) | `wohnzimmer/licht/set` | externer Client / Dashboard |
| Status (Zustandsmeldung) | `wohnzimmer/licht` | `mqtt-plus`, sobald das Gerät bestätigt |

Der Ablauf bei `Richtung = Beide` + `Dual`:

1. Ein Befehl trifft auf `…/licht/set` ein und wird auf den ioBroker-Datenpunkt geschrieben —
   mit `ack: false`, also als unbestätigter Steuerbefehl.
2. Das Gerät schaltet und meldet seinen neuen Zustand zurück (`ack: true`).
3. `mqtt-plus` spiegelt diese bestätigte Meldung auf das Basis-Topic `…/licht`.

Damit sind Schreib- und Leserichtung physisch getrennte Topics — eine Rückkopplung zwischen
beiden ist strukturell ausgeschlossen, nicht nur durch Schutzmechanismen abgefedert.

Die resultierende Zuordnung von Topic und `ack`-Flag:

| Funktion | MQTT-Topic | ioBroker `ack` | Zweck |
|---|---|---|---|
| Lesen / Status (Get) | `<prefix>/<pfad>` | `ack: true` (bestätigt) | Tatsächlicher Ist-Zustand des Geräts |
| Setzen / Befehl (Set) | `<prefix>/<pfad>/set` | `ack: false` (Befehl) | Schaltanforderung an das Gerät |

Konkret heißt das: Ein eingehender Befehl wird immer mit `ack: false` auf den ioBroker-Datenpunkt
geschrieben — also als unbestätigte Schaltanforderung, die der zuständige Geräte-Adapter ausführt.
Auf das Status-Topic wird umgekehrt erst dann etwas gemeldet, wenn das Gerät den neuen Zustand
mit `ack: true` bestätigt hat. Ein noch unbestätigter Befehl erscheint dort also nicht.

Auf dem Befehls-Topic selbst akzeptiert der Adapter im Modus `Dual` **jede** eingehende Nachricht,
unabhängig vom `ack`-Flag: Viele MQTT-Adapter reichen `/set`-Nachrichten bewusst als
unbestätigten Steuerbefehl (`ack: false`) weiter, und ein Echo kann dort nicht entstehen, weil
`mqtt-plus` auf das Befehls-Topic nie selbst schreibt. Im Modus `Single` bleibt die Prüfung auf
`ack: true` dagegen zwingend — dort wäre ein `ack: false`-Ereignis der eigene Schreibvorgang.

Der Adapter legt bei `Dual` beide Objekte im Zielbaum an, das Basis-Topic zuerst und das
Befehls-Topic darunter. In der JSON-Struktur-Vorschau und im Backup erscheinen beide Topics
(`full_topic` und `command_topic`) samt Modus.

**Wichtig — Befehle werden bei `Dual` nie wiederholt:** Ein Befehls-Topic ist ein Kommando-Kanal,
kein Zustandsspeicher. Deshalb überspringen sowohl der Initial-Abgleich beim Adapterstart als
auch der Force-Sync die Richtung MQTT → IOB, wenn `Dual` eingestellt ist. Andernfalls würde ein
dort liegender (womöglich retained) Befehl nach jedem Neustart bzw. alle paar Minuten erneut
ausgeführt — ein am Wandschalter ausgeschaltetes Licht ginge dann von selbst wieder an. Im Modus
`Single` bleibt dieser Abgleich aktiv, weil das Topic dort tatsächlich den Zustand führt.

### Echo-Schutz bei `both`-Mappings

Bei `dir: "both"` dient ein Topic gleichzeitig als Quelle und Ziel — ohne Schutzmechanismus
würde ein eigener Schreibvorgang eine Rückkopplungsschleife auslösen. `mqtt-plus` verhindert das
auf zwei Ebenen:

1. **MQTT → IOB** ist immer an `ack: true` gebunden. Der MQTT-Adapter markiert eingehende
   Broker-Nachrichten mit `ack: true`, eigene Schreibvorgänge laufen dagegen immer mit
   `ack: false` — ein `ack: false`-Ereignis kann also nie ein eigenes Echo *auf diesem Weg*
   verursachen und wird ignoriert, ohne dass eine echte Bestätigung riskiert wird.
2. **IOB → MQTT** verwendet standardmäßig ebenfalls die Ack-Prüfung (siehe Ack-Filter oben) und
   braucht deshalb keinen zusätzlichen Echo-Schutz. Nur bei `ackFilter: "any"` (wo auch eigene
   `ack: false`-Schreibvorgänge durchkommen können) merkt sich der Adapter kurzzeitig
   (max. 10 Sekunden) den zuletzt selbst geschriebenen Wert je Ziel-ID und verwirft ein exakt
   übereinstimmendes Echo — eine echte, abweichende Änderung wird davon nie blockiert.

Ergebnis: Echte Gerätebestätigungen kommen zuverlässig durch, auch bei `both`-Mappings, während
echte Selbst-Loops (v.a. bei `ackFilter: "any"`) weiterhin verhindert werden.

Im Topic-Modus `Dual` entfällt die Frage ohnehin: Dort wird auf ein anderes Topic geschrieben
als gelesen, ein Selbst-Echo kann also gar nicht erst entstehen.

### Zeitstempel & Aktualität

Jeder ioBroker-State hat `ts` (letzte Aktualisierung, auch bei gleichem Wert) und `lc` (letzte
Änderung). Früher bekam jeder Spiegel beim Kopieren `ts = jetzt` — nach jedem Neustart und bei
jedem Force-Sync. Ein seit Tagen totes Gerät sah dadurch auf der Zielseite dauerhaft aktiv aus.

Seit 1.6.0 gilt:

1. **Zeitstempel werden durchgereicht:** Beim Spiegeln eines Zustands werden `ts`, `lc` und `q`
   der Quelle übernommen (`c = "mqtt-plus"` kennzeichnet die Herkunft). Ausnahme: Befehle im
   Topic-Modus `Dual` (`/set`) bekommen den aktuellen Zeitpunkt — ein Befehl ist tatsächlich neu.
2. **Nur aktive Quellen:** Start, Update-Intervall und Force-Sync spiegeln eine Quelle nur, wenn
   ihr `ts` jünger als die Aktualitätsgrenze ist und `q = 0` gilt. Echte Änderungs-Ereignisse
   gehen immer durch — das Ereignis selbst belegt, dass das Gerät lebt. Wechsel zwischen
   „inaktiv“ und „wieder aktiv“ werden einmalig im Log gemeldet.
3. **Kein blindes Neuschreiben:** Start und Force-Sync lesen zuerst den Zielwert; stimmt er schon,
   wird nicht geschrieben (kein neues `ts`, keine MQTT-Nachricht, kein Funkverkehr).

**Hinweis:** Manche Adapter schreiben nur bei Wertänderung — dort bleibt `ts` auch bei aktivem
Gerät lange stehen. Für solche Mappings die Spalte „Aktualität“ auf `0` setzen oder großzügig
wählen.

### Sync-Modus pro Mapping

| Modus | Verhalten | Wofür |
|---|---|---|
| **Standard (nur Änderungen)** | Nur echte Wertänderungen, Original-Zeitstempel, inaktive Quellen ruhen. | Normalfall |
| **Jede Meldung weiterreichen** | Wie Standard, aber auch jede neue Meldung der Quelle mit *gleichem* Wert wird gespiegelt – `ts` wandert mit. Verstummt das Gerät, veraltet das Ziel ehrlich. | Sensoren, die korrekt messen, deren Wert sich aber kaum ändert und die auf der Zielseite sonst „ausgegraut“ würden |
| **Force wie vor 1.6.0** | Keine Aktualitätsprüfung, kein Zielvergleich; Start und Force-Sync schreiben immer mit `ts = jetzt`. | Wenn das Ziel unbedingt frisch wirken soll |

**Achtung bei „Force wie vor 1.6.0“:** Das Ziel wirkt dauerhaft aktiv, auch wenn das Gerät
längst nicht mehr meldet – genau das Verhalten, das 1.6.0 abgestellt hat. Wo das Gerät
regelmäßig meldet (auch mit gleichem Wert), ist „Jede Meldung weiterreichen“ die ehrlichere
Wahl. „Weiterreichen“ erzeugt dafür mehr MQTT-Nachrichten (eine pro Gerätemeldung).

### Remote Sync & Templates

Der Adapter sendet ein Array von Objekten an den konfigurierten Webhook. Bei sehr vielen
Mappings wird die Übertragung automatisch in Häppchen von je 200 Einträgen aufgeteilt, damit ein
einzelner Timeout nicht die komplette Payload verwirft; schlägt ein Häppchen fehl, zeigt die
Fehlermeldung, wie viele Werte bereits angekommen sind. Netzwerkfehler und 5xx-Antworten werden
mit steigender Wartezeit bis zu zweimal wiederholt, TLS-Zertifikatsfehler und 4xx-Antworten
dagegen nicht (die ändern sich durch einen Retry ohnehin nicht).

**Verfügbare Platzhalter:**
* `%ID%`: Die ioBroker-Quell-ID (z.B. `shelly.0.relay`).
* `%MQTT%`: Das definierte MQTT-Suffix (z.B. `licht/kueche`).
* `%PREFIX%`: Der in den Einstellungen definierte Basis-Pfad (z.B. `mqtt.0.`).
* `%VAL%`: Der aktuelle Wert (Zahl, String oder Boolean).
* `%TS%`: Zeitstempel der letzten Aktualisierung (`ts`, ms seit 1970).
* `%LC%`: Zeitstempel der letzten Wertänderung (`lc`).
* `%ACK%`: `true`, wenn der Wert vom Gerät bestätigt ist.
* `%Q%`: Qualität des Werts (`0` = gut).
* `%UNIT%`: Die definierte Einheit.
* `%DIR%`: Die konfigurierte Richtung (`in`, `out`, `both`).

Alle Platzhalter außer `%VAL%`/`%TS%`/`%LC%`/`%ACK%`/`%Q%` werden beim Einsetzen automatisch JSON-escaped — ein
Anführungszeichen in einer ID bricht das erzeugte JSON also nicht mehr.

**Standard-Template:**
```json
{
  "id": "%ID%",
  "topic": "%MQTT%",
  "value": %VAL%,
  "ts": %TS%,
  "unit": "%UNIT%",
  "prefix": "%PREFIX%",
  "dir": "%DIR%"
}
```

---

## Diagnose-States

Neben den Config-Reitern legt der Adapter folgende Status-Datenpunkte an:

| State | Bedeutung |
|---|---|
| `info.connection` | Webserver läuft/läuft nicht |
| `info.version` | Aktuell laufende Adapter-Version |
| `info.status` | Maschinenlesbarer Status (`Running`, `Cycle OK`, `Force-Sync OK`, …) |
| `info.lastCycle` | Unix-Zeitstempel des letzten abgeschlossenen Sync-Zyklus |
| `info.lastSyncStatus` | Ergebnis des letzten Remote-Sync-Laufs (Erfolg/Fehlertext) |
| `info.dashboardUrl` | Fertiger Link zum Web-Dashboard |
| `watchdog` | Freitext-Statuszeile (historisch, für Übersicht im Objektbaum) |
