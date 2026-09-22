"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/*
 * MQTT-Bridge-Manager
 * Feature: Force-Sync Interval (5 min) bypassing cache to heal split-brain/async states
 */
const utils = __importStar(require("@iobroker/adapter-core"));
const axios_1 = __importDefault(require("axios"));
const http = __importStar(require("node:http"));
const https = __importStar(require("node:https"));
const crypto = __importStar(require("node:crypto"));
const os = __importStar(require("node:os"));
// Einmalig aus package.json gelesen, statt an mehreren Stellen (User-Agent, info.version)
// manuell zu pflegen und bei jedem Versionssprung zu vergessen.
const ADAPTER_VERSION = require("../package.json").version;
class MqttPlus extends utils.Adapter {
    constructor(options = {}) {
        super({
            ...options,
            name: "mqtt-plus",
        });
        // Map statt Plain Object: verhindert Prototype Pollution über Datenpunkt-/Topic-Namen wie "__proto__"
        this.lastSyncValues = new Map();
        // Quell-ts des zuletzt geschriebenen Werts je Verbindung - Basis für syncMode "refresh".
        this.lastSyncTs = new Map();
        this.updateInterval = undefined;
        this.forceSyncInterval = undefined;
        this.syncInterval = undefined;
        this.httpServer = undefined;
        this.currentWatchdogStatus = "Init";
        this.unloaded = false;
        this.syncRunning = false;
        this.activeSockets = new Set();
        // Performance Lookup Maps
        this.sourceToMappings = new Map();
        this.targetToMappings = new Map();
        // common.type des jeweiligen Zielobjekts (aus ensureAdapterObject) - Basis für Auto-Konvertierung
        this.targetTypeCache = new Map();
        // common.type des jeweiligen Quellobjekts, einmalig beim Setup ermittelt (für /api/json)
        this.sourceTypeCache = new Map();
        // Konsumierbarer Echo-Schutz: merkt sich pro Ziel-ID den zuletzt selbst geschriebenen Wert,
        // bis entweder das passende Echo eintrifft (verbraucht) oder ein abweichender Wert eintrifft
        // (verworfen - echte Änderung). Nur wirksam, wenn syncValue() mit useEchoGuard=true läuft -
        // sonst hätte die Ack-Prüfung in onStateChange ein echtes eigenes Echo bereits ausgeschlossen,
        // und der Schutz würde nur eine spätere, echte Bestätigung fälschlich verschlucken.
        this.pendingWrites = new Map();
        // Quellen, die aktuell als inaktiv (veraltet/schlechte Qualität) gelten - nur für das
        // Logging beim Zustandswechsel, damit nicht jeder Cycle dieselbe Meldung wiederholt.
        this.staleSources = new Set();
        // Brute-Force-Schutz für das Dashboard-Login, pro Client-IP
        this.failedAuthAttempts = new Map();
        this.lastWatchdogStatus = "";
        this.lastWatchdogWriteTs = 0;
        this.on("ready", this.onReady.bind(this));
        this.on("stateChange", this.onStateChange.bind(this));
        this.on("unload", this.onUnload.bind(this));
        this.on("message", this.onMessage.bind(this));
    }
    // this.delay() statt eines eigenen setTimeout: wird beim Unload vom Adapter automatisch
    // aufgeräumt (Voraussetzung für Compact Mode).
    sleep(ms) {
        return this.delay(ms);
    }
    convertMqttPathToIobrokerId(mqttPath) {
        if (!mqttPath)
            return "unknown";
        let cleaned = mqttPath.replace(/^[\/\.]+|[\/\.]+$/g, "");
        cleaned = cleaned.replace(/\//g, ".");
        cleaned = cleaned.replace(/\s+/g, "_");
        // Zusätzlich zu / und . verbietet ioBroker weitere Zeichen in IDs (*?,;'"`<>[])
        cleaned = cleaned.replace(this.FORBIDDEN_CHARS, "_");
        return cleaned || "unknown";
    }
    // Ermittelt das Befehls-Topic eines Mappings.
    // "single" (Standard): Befehl und Status teilen sich ein Topic - wie bisher.
    // "dual": Befehle laufen über <topic>/set, der Status bleibt auf <topic> - dadurch sind
    // Schreib- und Leserichtung physisch getrennt und können sich nicht gegenseitig auslösen.
    resolveCommandPath(entry, statePath) {
        if (entry.topicMode !== "dual")
            return statePath;
        const raw = (entry.commandSuffix || "").trim() || "/set";
        const suffix = this.convertMqttPathToIobrokerId(raw);
        if (!suffix || suffix === "unknown") {
            this.log.warn(`[Setup] Ungültiger Befehls-Suffix "${raw}" für "${entry.id}" - verwende "/set".`);
            return `${statePath}.set`;
        }
        return `${statePath}.${suffix}`;
    }
    // Liest den konfigurierten Ziel-Präfix mit Absicherung gegen leere/fehlerhafte Config.
    getValidatedBasePath() {
        let base = (this.config.targetBasePath || "").trim();
        if (!base) {
            this.log.warn("[Config] Kein MQTT Ziel-Pfad konfiguriert - verwende Standard 'mqtt.0.'");
            base = "mqtt.0.";
        }
        return base.endsWith(".") ? base : base + ".";
    }
    async onReady() {
        var _a, _b;
        await this.initObjects();
        await this.loadAuthLockouts();
        await this.setStateAsync("info.version", ADAPTER_VERSION, true);
        this.log.info(`[Setup] MQTT-Bridge-Manager v${ADAPTER_VERSION} startet...`);
        const staleMin = (_a = this.parseMinutes(this.config.staleAfterMin)) !== null && _a !== void 0 ? _a : MqttPlus.DEFAULT_STALE_AFTER_MIN;
        this.log.info(staleMin > 0
            ? `[Setup] Aktualitätsprüfung: Quellen ohne Update seit ${staleMin} Min werden bei Start/Cycle/Force-Sync nicht gespiegelt.`
            : "[Setup] Aktualitätsprüfung (Alter) global deaktiviert - nur Qualität (q) wird geprüft.");
        await this.setupBridge();
        const port = this.config.serverPort || 8095;
        const hasTls = !!(this.config.dashboardTlsCert && this.config.dashboardTlsKey);
        if (!this.config.dashboardPassword) {
            this.log.warn("[Dashboard] Kein Passwort gesetzt - der Webserver ist ohne Zugangsschutz erreichbar! Bitte in den Adapter-Einstellungen (Tab 'Web Dashboard') ein Passwort vergeben.");
        }
        else if (!hasTls) {
            this.log.warn("[Dashboard] Passwort ist gesetzt, aber kein TLS-Zertifikat konfiguriert - die Zugangsdaten werden unverschlüsselt (HTTP) übertragen. Für Verschlüsselung im Tab 'Web Dashboard' Zertifikat/Key hinterlegen.");
        }
        this.startWebServer(port);
        await this.updateDashboardUrlState(port);
        // 1. Normales Fallback-Intervall (Mit Cache), mindestens 5s gegen Busy-Loop bei Fehlkonfiguration
        const updateIntervalSec = Math.max(5, this.config.updateIntervalSec || 60);
        this.log.info(`Starte Update-Intervall (Fallback): ${updateIntervalSec} Sekunden`);
        this.updateInterval = this.setInterval(() => {
            this.runCycleSync().catch(e => this.log.error(`Cycle-Sync Fehler: ${e.message}`));
        }, updateIntervalSec * 1000);
        // 2. Force-Sync Intervall (ohne Cache zur System-Heilung), konfigurierbar/abschaltbar
        const forceSyncMin = (_b = this.config.forceSyncIntervalMin) !== null && _b !== void 0 ? _b : 5;
        if (forceSyncMin > 0) {
            this.log.info(`Starte Force-Sync-Intervall: Alle ${forceSyncMin} Minuten (Heilung von Asynchronität)`);
            this.forceSyncInterval = this.setInterval(() => {
                this.runForceSync().catch(e => this.log.error(`Force-Sync Fehler: ${e.message}`));
            }, forceSyncMin * 60 * 1000);
        }
        else {
            this.log.info("Force-Sync-Intervall deaktiviert (Konfiguration).");
        }
        // 3. Remote Sync
        if (this.config.syncUrl) {
            const syncIntervalMin = this.config.syncIntervalMin || 60;
            this.log.info(`Starte Remote-Sync: alle ${syncIntervalMin} Minuten`);
            this.runRemoteSync(false).catch(e => this.log.error(`Remote-Sync Fehler: ${e.message}`));
            this.syncInterval = this.setInterval(() => {
                this.runRemoteSync(false).catch(e => this.log.error(`Remote-Sync Fehler: ${e.message}`));
            }, syncIntervalMin * 60 * 1000);
        }
        this.updateWatchdog("Running", true);
    }
    async updateDashboardUrlState(port) {
        let ip = "127.0.0.1";
        try {
            const ifaces = os.networkInterfaces();
            let found = false;
            for (const name in ifaces) {
                const iface = ifaces[name];
                if (!iface)
                    continue;
                for (const alias of iface) {
                    if (alias.family === "IPv4" && !alias.internal) {
                        ip = alias.address;
                        found = true;
                        break;
                    }
                }
                if (found)
                    break;
            }
        }
        catch (e) {
            this.log.debug(`[Setup] Netzwerk-Interfaces konnten nicht ermittelt werden: ${e.message}`);
        }
        const url = `http://${ip}:${port}`;
        await this.setStateAsync("info.dashboardUrl", url, true);
        this.log.info(`Dashboard erreichbar unter: ${url}`);
    }
    async onMessage(obj) {
        if (!obj || typeof obj !== "object")
            return;
        if (obj.command === "generateJson") {
            try {
                const tree = await this.generateJsonTree();
                if (obj.callback)
                    this.sendTo(obj.from, obj.command, tree, obj.callback);
            }
            catch (e) {
                if (obj.callback)
                    this.sendTo(obj.from, obj.command, { error: e.message }, obj.callback);
            }
        }
        else if (obj.command === "checkSync") {
            this.log.info("Manueller Sync-Test angefordert...");
            try {
                const result = await this.runRemoteSync(true);
                if (obj.callback) {
                    this.sendTo(obj.from, obj.command, {
                        success: result.success,
                        result: result.message
                    }, obj.callback);
                }
            }
            catch (e) {
                if (obj.callback)
                    this.sendTo(obj.from, obj.command, { error: e.message }, obj.callback);
            }
        }
        else if (obj.callback) {
            this.sendTo(obj.from, obj.command, { error: "unknown command" }, obj.callback);
        }
    }
    async initObjects() {
        await this.setObjectNotExistsAsync("watchdog", {
            type: "state",
            common: { name: "MQTT Bridge Watchdog", type: "string", role: "text", read: true, write: false },
            native: {},
        });
        await this.setObjectNotExistsAsync("config.syncTemplate", {
            type: "state",
            common: { name: "Remote Sync JSON Template", type: "string", role: "json", read: true, write: true, def: this.getDefaultSyncTemplate() },
            native: {},
        });
        await this.setObjectNotExistsAsync("info.dashboardUrl", {
            type: "state",
            common: { name: "Dashboard URL", type: "string", role: "url", read: true, write: false },
            native: {},
        });
        await this.setObjectNotExistsAsync("info.lastSyncStatus", {
            type: "state",
            common: { name: "Letzter Sync Status", type: "string", role: "text", read: true, write: false },
            native: {},
        });
        await this.setObjectNotExistsAsync("info.status", {
            type: "state",
            common: { name: "Status", type: "string", role: "text", read: true, write: false },
            native: {},
        });
        await this.setObjectNotExistsAsync("info.lastCycle", {
            type: "state",
            common: { name: "Letzter Sync-Zyklus", type: "number", role: "value.time", read: true, write: false },
            native: {},
        });
        await this.setObjectNotExistsAsync("info.connection", {
            type: "state",
            common: { name: "Connected", type: "boolean", role: "indicator.connected", read: true, write: false, def: false },
            native: {},
        });
        await this.setObjectNotExistsAsync("info.version", {
            type: "state",
            common: { name: "Adapter-Version", type: "string", role: "text", read: true, write: false },
            native: {},
        });
        await this.setObjectNotExistsAsync("info.authLockouts", {
            type: "state",
            common: { name: "Login-Sperrliste (intern)", type: "string", role: "json", read: true, write: false, def: "{}" },
            native: {},
        });
    }
    // Lädt aktive Login-Sperren aus der Persistenz, damit ein Adapter-Neustart eine laufende
    // Brute-Force-Sperre nicht zurücksetzt. Bereits abgelaufene Einträge werden verworfen.
    async loadAuthLockouts() {
        try {
            const state = await this.getStateAsync("info.authLockouts");
            if (!state || !state.val)
                return;
            const stored = JSON.parse(state.val);
            const now = Date.now();
            for (const [ip, entry] of Object.entries(stored)) {
                if (entry.lockedUntil > now)
                    this.failedAuthAttempts.set(ip, entry);
            }
        }
        catch (e) {
            this.log.debug(`[Dashboard] Sperrliste konnte nicht geladen werden: ${e.message}`);
        }
    }
    // Persistiert nur bei tatsächlicher Sperrung (nicht bei jedem Fehlversuch), um die
    // States-DB nicht unnötig zu belasten - und räumt dabei abgelaufene Einträge auf.
    persistAuthLockouts() {
        const now = Date.now();
        const active = {};
        for (const [ip, entry] of this.failedAuthAttempts) {
            if (entry.lockedUntil > now)
                active[ip] = entry;
        }
        this.setState("info.authLockouts", JSON.stringify(active), true);
    }
    async setupBridge() {
        this.log.info("[MQTT-Bridge] Starte Setup & Indexierung...");
        this.sourceToMappings.clear();
        this.targetToMappings.clear();
        this.targetTypeCache.clear();
        this.sourceTypeCache.clear();
        const mappings = this.config.mappings || [];
        if (!mappings || !Array.isArray(mappings))
            return;
        const basePath = this.getValidatedBasePath();
        const seenTargetPaths = new Map();
        const subscribeIds = [];
        for (const entry of mappings) {
            if (!entry.id || !entry.mqttName)
                continue;
            const cleanSuffix = this.convertMqttPathToIobrokerId(entry.mqttName);
            const fullTargetPath = `${basePath}${cleanSuffix}`;
            const commandPath = this.resolveCommandPath(entry, fullTargetPath);
            entry.fullTargetPath = fullTargetPath;
            entry.commandPath = commandPath;
            try {
                const sObj = await this.getForeignObjectAsync(entry.id);
                const sType = sObj && sObj.common && sObj.common.type;
                this.sourceTypeCache.set(entry.id, sType || "unknown");
                // Auch als Ziel-Typ cachen: entry.id ist bei dir "in"/"both" ein Schreibziel, und
                // convertType() kannte dessen echten Typ bisher nur über fullTargetPath (IOB->MQTT-
                // Richtung) - die Befehlsrichtung (MQTT->IOB) bekam bei "auto" nie eine Koerzion.
                if (sType) {
                    this.targetTypeCache.set(entry.id, sType);
                }
            }
            catch (e) {
                this.log.debug(`[Setup] Quellobjekt ${entry.id} nicht lesbar: ${e.message}`);
                this.sourceTypeCache.set(entry.id, "unknown");
            }
            // Source Index (IOB -> MQTT)
            if (entry.dir === "out" || entry.dir === "both") {
                const prevOwner = seenTargetPaths.get(fullTargetPath);
                if (prevOwner && prevOwner !== entry.id) {
                    this.log.warn(`[Setup] Doppeltes Ziel-Topic "${fullTargetPath}": wird sowohl von "${prevOwner}" als auch von "${entry.id}" beschrieben - die Werte überschreiben sich gegenseitig!`);
                }
                else {
                    seenTargetPaths.set(fullTargetPath, entry.id);
                }
                if (!this.sourceToMappings.has(entry.id)) {
                    this.sourceToMappings.set(entry.id, []);
                }
                this.sourceToMappings.get(entry.id).push(entry);
                subscribeIds.push(entry.id);
            }
            // Target Index (MQTT -> IOB): abonniert wird das Befehls-Topic. Bei "dual" ist das
            // <topic>/set, bei "single" das Basis-Topic selbst.
            if (entry.dir === "in" || entry.dir === "both") {
                if (!this.targetToMappings.has(commandPath)) {
                    this.targetToMappings.set(commandPath, []);
                }
                this.targetToMappings.get(commandPath).push(entry);
                subscribeIds.push(commandPath);
            }
            // Reihenfolge ist wichtig: erst das Status-Topic als State anlegen, danach das
            // darunterliegende Befehls-Topic. Andernfalls würde die Ordner-Anlage das Basis-Topic
            // als "folder" erzeugen, bevor es als State existiert.
            await this.ensureAdapterObject(entry.id, fullTargetPath, entry.unit);
            if (commandPath !== fullTargetPath) {
                await this.ensureAdapterObject(entry.id, commandPath, entry.unit);
            }
        }
        // Ein gebündelter Subscribe-Aufruf statt einem pro ID
        if (subscribeIds.length > 0) {
            await this.subscribeForeignStatesAsync(subscribeIds);
        }
        // Initial-Sync gebündelt: ein getForeignStatesAsync-Roundtrip statt N Einzelabfragen.
        // Zusätzlich werden die aktuellen Zielwerte gelesen: Ist der Wert dort schon korrekt,
        // wird nicht neu geschrieben - sonst bekäme jeder Spiegel nach jedem Neustart ein
        // frisches ts, obwohl sich nichts geändert hat (und ein totes Gerät sähe aktiv aus).
        const outIds = [...this.sourceToMappings.keys()];
        const outStates = outIds.length ? await this.getForeignStatesAsync(outIds) : {};
        const outTargetStates = await this.getTargetStates(outIds.flatMap(id => this.sourceToMappings.get(id).map(e => e.fullTargetPath)));
        for (const [id, entries] of this.sourceToMappings) {
            const state = outStates[id];
            for (const entry of entries) {
                if (entry.dir === "out" || entry.dir === "both") {
                    if (!state || !this.passesAckFilter(entry, state))
                        continue;
                    const label = entry.dir === "both" ? "IOB -> MQTT (START: both)" : "IOB -> MQTT (START)";
                    await this.syncValue(id, entry.fullTargetPath, label, "START-UP", entry.type, state, this.syncOptionsFor(entry, "start", outTargetStates[entry.fullTargetPath]));
                }
            }
        }
        // Achtung: Bei topicMode "dual" ist das Quell-Topic ein reiner BEFEHLS-Kanal, kein
        // Zustandsspeicher. Ein dort liegender (evtl. retained) Befehl darf beim Start nicht
        // erneut ausgeführt werden - sonst würde z.B. nach jedem ioBroker-Neustart das zuletzt
        // gesendete Kommando das Gerät erneut schalten. Deshalb nur "single" initial abgleichen.
        const inOnlyTargets = [];
        for (const [commandPath, entries] of this.targetToMappings) {
            if (entries.some(e => e.dir === "in" && e.topicMode !== "dual"))
                inOnlyTargets.push(commandPath);
        }
        const inStates = inOnlyTargets.length ? await this.getForeignStatesAsync(inOnlyTargets) : {};
        const inTargetStates = await this.getTargetStates(inOnlyTargets.flatMap(p => this.targetToMappings.get(p).map(e => e.id)));
        for (const [commandPath, entries] of this.targetToMappings) {
            const state = inStates[commandPath];
            for (const entry of entries) {
                if (entry.dir === "in" && entry.topicMode !== "dual") {
                    // Wie im Event-Pfad (onStateChange) nur echte Broker-Werte (ack=true) übernehmen.
                    if (!state || state.ack !== true)
                        continue;
                    await this.syncValue(commandPath, entry.id, "MQTT -> IOB (START)", "START-UP", entry.type, state, this.syncOptionsFor(entry, "start", inTargetStates[entry.id]));
                }
            }
        }
        this.log.info(`[Setup] Indiziert: ${this.sourceToMappings.size} Quellen, ${this.targetToMappings.size} Ziele.`);
    }
    async onStateChange(id, state) {
        if (!state)
            return;
        // 1. Ist es eine Quelle? (IOB -> MQTT)
        const sourceMatches = this.sourceToMappings.get(id);
        if (sourceMatches) {
            for (const entry of sourceMatches) {
                // Standard: nur bestätigte Werte (ack=true) weiterleiten. Für Datenpunkte ohne
                // echtes Gerät dahinter (0_userdata.0.*, alias.0.*) kann pro Mapping "any" gewählt
                // werden, damit auch ack=false (Admin-UI/Skripte) sofort statt erst beim nächsten
                // Cycle-Sync durchkommt.
                if (this.passesAckFilter(entry, state) && entry.fullTargetPath) {
                    const dirLabel = entry.dir === "both" ? "IOB -> MQTT (Config: both)" : "IOB -> MQTT";
                    // Echo-Schutz nur bei "any" aktivieren: nur dort können ack=false-Ereignisse
                    // (also potenziell unser eigener Schreibvorgang) diesen Zweig überhaupt erreichen.
                    // Keine Aktualitätsprüfung: das Ereignis selbst belegt, dass die Quelle lebt.
                    await this.syncValue(id, entry.fullTargetPath, dirLabel, "LOCAL-CHANGE", entry.type, state, {
                        ...this.syncOptionsFor(entry, "event"),
                        useEchoGuard: entry.ackFilter === "any"
                    });
                }
            }
        }
        // 2. Ist es ein Ziel? (MQTT -> IOB)
        const targetMatches = this.targetToMappings.get(id);
        if (targetMatches) {
            for (const entry of targetMatches) {
                if (!entry.commandPath)
                    continue;
                // Die Ack-Prüfung ist nur im Modus "single" nötig - dort teilen sich Status und
                // Befehl ein Topic, und ein ack=false-Ereignis wäre unser eigener Schreibvorgang
                // (Echo). Bei "dual" schreiben wir auf das Befehls-Topic grundsätzlich nie, ein
                // Echo kann dort also gar nicht entstehen. Die Prüfung würde dort im Gegenteil
                // schaden: Übliche MQTT-Adapter reichen eingehende /set-Nachrichten bewusst als
                // unbestätigten Steuerbefehl (ack=false) weiter - der Schaltbefehl würde dann
                // stillschweigend verworfen.
                const isDual = entry.topicMode === "dual";
                if (!isDual && state.ack !== true)
                    continue;
                const modeLabel = isDual ? " [dual]" : "";
                const dirLabel = entry.dir === "both" ? `MQTT -> IOB (Config: both${modeLabel})` : `MQTT -> IOB${modeLabel}`;
                // Bei "both" ist dies die Rückrichtung des konfigurierten Typs (z.B. boolToNum <-> numToBool).
                // Der Wert selbst wird dabei nie invertiert (true bleibt true), nur die Darstellung angepasst.
                const effectiveType = entry.dir === "both" ? this.invertConversionType(entry.type) : entry.type;
                // Bei "dual" ist das ein neuer Befehl - der bekommt bewusst den aktuellen Zeitpunkt,
                // nicht den der MQTT-Nachricht, und wird nie als "Refresh" wiederholt. Bei "single"
                // wird ein Zustand gespiegelt.
                const eventOpts = isDual
                    ? { decimals: entry.decimals, preserveTimestamp: false }
                    : this.syncOptionsFor(entry, "event");
                await this.syncValue(entry.commandPath, entry.id, dirLabel, "MQTT-EVENT", effectiveType, state, eventOpts);
            }
        }
    }
    /**
     * Sync Funktion. Gibt true zurück, wenn tatsächlich geschrieben wurde.
     */
    async syncValue(sourceId, targetId, dirLabel, triggerSource, convType, stateObj, opts = {}) {
        var _a, _b;
        const force = opts.force === true;
        try {
            let srcState = stateObj;
            if (!srcState) {
                srcState = await this.getForeignStateAsync(sourceId);
            }
            if (!srcState)
                return false;
            const val = srcState.val;
            if (val === null || val === undefined)
                return false;
            // --- 0. AKTUALITÄT DER QUELLE ---
            // Nur bei Start/Cycle/Force-Sync: dort liegt kein frisches Ereignis vor, der Wert kann
            // beliebig alt sein. Ein totes Gerät soll auf der Zielseite nicht "neu" werden.
            if (opts.requireAlive) {
                const staleReason = this.getStaleReason(srcState, (_a = opts.staleLimitMs) !== null && _a !== void 0 ? _a : 0);
                if (staleReason) {
                    if (!this.staleSources.has(sourceId)) {
                        this.staleSources.add(sourceId);
                        this.log.info(`[Aktualität] Quelle ${sourceId} gilt als inaktiv (${staleReason}) - wird bis zum nächsten echten Update nicht mehr nach ${targetId} gespiegelt.`);
                    }
                    else {
                        this.log.debug(`[Aktualität] (${triggerSource}) Überspringe ${sourceId} -> ${targetId}: ${staleReason}`);
                    }
                    return false;
                }
            }
            if (this.staleSources.delete(sourceId)) {
                this.log.info(`[Aktualität] Quelle ${sourceId} ist wieder aktiv.`);
            }
            // --- 1. ECHO-SCHUTZ (verbrauchbar, mit kurzer Verfallszeit) ---
            // Ein evtl. vorhandener Merker wird immer konsumiert (aufgeräumt), damit er nicht
            // später ein unabhängiges Ereignis mit zufällig demselben Wert blockiert - reagiert
            // wird darauf aber nur, wenn useEchoGuard aktiv ist.
            const pending = this.pendingWrites.get(sourceId);
            if (pending) {
                this.pendingWrites.delete(sourceId);
                const stillFresh = Date.now() - pending.ts < MqttPlus.PENDING_WRITE_TTL_MS;
                if (opts.useEchoGuard && stillFresh && this.sameValue(pending.value, val)) {
                    this.log.debug(`[Echo-Schild] (${triggerSource}) Ignoriere Echo von ${sourceId} -> ${targetId} (Wert '${val}' identisch zum eigenen Schreibvorgang)`);
                    return false;
                }
            }
            // ----------------------------------------------
            const processedValue = this.convertType(val, convType, targetId, (_b = opts.decimals) !== null && _b !== void 0 ? _b : 2);
            // "::" statt "_to_": vermeidet Key-Kollisionen, falls eine ID selbst "_to_" enthält.
            const cacheKey = `${sourceId}::${targetId}`;
            // syncMode "refresh": eine neue Meldung der Quelle (neueres ts) mit gleichem Wert
            // zählt trotzdem als weiterzureichen - das Ziel bleibt so aktuell, solange das Gerät
            // tatsächlich meldet, und veraltet ehrlich, sobald es verstummt.
            const srcTs = typeof srcState.ts === "number" ? srcState.ts : 0;
            const isRefresh = (knownTs) => opts.passRefresh === true && srcTs > (knownTs !== null && knownTs !== void 0 ? knownTs : 0);
            // --- 2. VALUE CACHE (Ping-Pong Schutz für langsame Echos) ---
            // WICHTIG: Wenn force = true ist, ignorieren wir den Cache komplett!
            if (!force && this.sameValue(this.lastSyncValues.get(cacheKey), processedValue) && !isRefresh(this.lastSyncTs.get(cacheKey))) {
                this.log.debug(`[Cache-Schild] Blockiere redundanten Wert für ${targetId} (Wert '${processedValue}' ist identisch zum letzten Sendevorgang)`);
                return false;
            }
            // --- 2b. ZIELVERGLEICH (Start/Force-Sync) ---
            // Steht auf der Zielseite bereits der richtige Wert, gibt es nichts zu heilen. Ein
            // erneutes Schreiben würde nur ts/lc des Ziels auffrischen und eine MQTT-Nachricht
            // auslösen - und damit ein inaktives Gerät als aktiv erscheinen lassen.
            if (opts.targetState && this.sameValue(opts.targetState.val, processedValue) && !isRefresh(opts.targetState.ts)) {
                this.lastSyncValues.set(cacheKey, processedValue);
                this.lastSyncTs.set(cacheKey, srcTs);
                this.log.debug(`[Ziel-Schild] (${triggerSource}) ${targetId} hat bereits den Wert '${processedValue}' - kein Schreiben nötig`);
                return false;
            }
            this.lastSyncValues.set(cacheKey, processedValue);
            this.lastSyncTs.set(cacheKey, srcTs);
            // -----------------------------------------------------------
            // --- 3. SCHREIBEN & ECHO-MERKER SETZEN ---
            this.pendingWrites.set(targetId, { value: processedValue, ts: Date.now() });
            // Spiegelung eines Zustands: Zeitstempel (ts = letzte Aktualisierung, lc = letzte
            // Änderung) und Qualität der Quelle übernehmen, damit das Ziel den echten
            // Messzeitpunkt zeigt statt des Kopier-Zeitpunkts. Befehle (dual /set) bekommen
            // dagegen den aktuellen Zeitpunkt - ein Befehl ist tatsächlich neu.
            const newState = { val: processedValue, ack: false, c: "mqtt-plus" };
            if (opts.preserveTimestamp) {
                if (typeof srcState.ts === "number")
                    newState.ts = srcState.ts;
                if (typeof srcState.lc === "number")
                    newState.lc = srcState.lc;
                if (typeof srcState.q === "number")
                    newState.q = srcState.q;
            }
            await this.setForeignStateAsync(targetId, newState);
            if (this.config.logTransfers) {
                const forceLabel = force ? "[FORCED] " : "";
                this.log.info(`${forceLabel}[${dirLabel}] (${triggerSource}) ${val} -> ${processedValue} (${targetId})`);
            }
            this.updateWatchdog("Running");
            return true;
        }
        catch (e) {
            this.log.error(`Sync-Fehler ${sourceId}: ${e.message}`);
            return false;
        }
    }
    // Standard: nur bestätigte Werte (ack=true) weiterleiten. Für Datenpunkte ohne echtes Gerät
    // dahinter (0_userdata.0.*, alias.0.*) kann pro Mapping "any" gewählt werden. Gilt für alle
    // Wege IOB -> MQTT (Event, Start, Cycle, Force) - sonst würde z.B. der Cycle einen nie
    // bestätigten Befehl an ein Offline-Gerät nach MQTT als Status melden.
    passesAckFilter(entry, state) {
        return entry.ackFilter === "any" || state.ack === true;
    }
    // Sync-Optionen eines Mappings je Auslöser, abhängig vom Sync-Modus des Eintrags.
    syncOptionsFor(entry, phase, targetState) {
        if (entry.syncMode === "force") {
            // Verhalten vor 1.6.0: keine Aktualitätsprüfung, kein Zielvergleich, ts = jetzt.
            // Start und Force-Sync schreiben dadurch immer - das Ziel wirkt dauerhaft frisch.
            return { force: phase === "force", decimals: entry.decimals, preserveTimestamp: false };
        }
        const opts = {
            decimals: entry.decimals,
            preserveTimestamp: true,
            passRefresh: entry.syncMode === "refresh"
        };
        if (phase === "event")
            return opts;
        opts.requireAlive = true;
        opts.staleLimitMs = this.getStaleLimitMs(entry);
        if (phase === "force")
            opts.force = true;
        if (phase === "start" || phase === "force")
            opts.targetState = targetState !== null && targetState !== void 0 ? targetState : null;
        return opts;
    }
    // Liest eine Minuten-Angabe aus der Config tolerant ein: leer/ungültig -> undefined.
    parseMinutes(raw) {
        if (raw === undefined || raw === null || String(raw).trim() === "")
            return undefined;
        const n = Number(raw);
        return isNaN(n) || n < 0 ? undefined : n;
    }
    // Aktualitätsgrenze in ms für ein Mapping: Eintrag im Mapping vor globalem Wert, 0 = aus.
    getStaleLimitMs(entry) {
        var _a;
        const perMapping = this.parseMinutes(entry.staleAfterMin);
        const global = (_a = this.parseMinutes(this.config.staleAfterMin)) !== null && _a !== void 0 ? _a : MqttPlus.DEFAULT_STALE_AFTER_MIN;
        return (perMapping !== null && perMapping !== void 0 ? perMapping : global) * 60000;
    }
    // Liefert den Grund, warum eine Quelle als inaktiv gilt, oder null wenn sie aktiv ist.
    // - q != 0: der Geräte-Adapter meldet selbst ein Problem (z.B. keine Verbindung).
    // - ts älter als die Grenze: das Gerät hat sich seitdem nicht mehr gemeldet.
    getStaleReason(state, staleLimitMs) {
        if (typeof state.q === "number" && state.q !== 0) {
            return `Qualität q=0x${state.q.toString(16).padStart(2, "0")}`;
        }
        if (staleLimitMs > 0 && typeof state.ts === "number") {
            const age = Date.now() - state.ts;
            if (age > staleLimitMs)
                return `letzte Aktualisierung vor ${this.formatAge(age)}`;
        }
        return null;
    }
    formatAge(ms) {
        const min = Math.round(ms / 60000);
        if (min < 120)
            return `${min} Min`;
        const h = Math.round(min / 60);
        if (h < 48)
            return `${h} Std`;
        return `${Math.round(h / 24)} Tagen`;
    }
    // Liest die aktuellen Werte der Zielobjekte gebündelt (für den Zielvergleich bei Start/Force).
    async getTargetStates(ids) {
        const unique = [...new Set(ids.filter(Boolean))];
        if (!unique.length)
            return {};
        try {
            return await this.getForeignStatesAsync(unique);
        }
        catch (e) {
            this.log.debug(`[Sync] Zielwerte nicht lesbar, schreibe ohne Vergleich: ${e.message}`);
            return {};
        }
    }
    // Kehrt boolToNum/numToBool für die Rückrichtung eines "both"-Mappings um.
    // round/auto sind richtungsunabhängig symmetrisch und bleiben unverändert.
    invertConversionType(type) {
        switch (type) {
            case "boolToNum": return "numToBool";
            case "numToBool": return "boolToNum";
            default: return type;
        }
    }
    // Wandelt einen Wert korrekt in boolean um - Strings wie "0"/"false"/"off" zählen als false,
    // statt (wie !!value) fälschlich als true.
    toBoolean(value) {
        if (typeof value === "string") {
            const normalized = value.trim().toLowerCase();
            if (normalized === "0" || normalized === "false" || normalized === "off" || normalized === "")
                return false;
            if (normalized === "1" || normalized === "true" || normalized === "on")
                return true;
        }
        return !!value;
    }
    // Typtoleranter Vergleich für Echo-/Cache-Erkennung: true/"true", 53/"53" gelten als gleich.
    // Ein reiner === schlägt fehl, wenn derselbe Wert einmal als String, einmal als Zahl/Boolean
    // vorliegt, obwohl er inhaltlich identisch ist - mit Folgen in beide Richtungen (verpasste
    // Echo-Erkennung oder verpasste Cache-Treffer).
    sameValue(a, b) {
        if (a === b)
            return true;
        if (a === null || a === undefined || b === null || b === undefined)
            return false;
        const numA = typeof a === "number" ? a : (typeof a === "string" && a.trim() !== "" && !isNaN(Number(a)) ? Number(a) : null);
        const numB = typeof b === "number" ? b : (typeof b === "string" && b.trim() !== "" && !isNaN(Number(b)) ? Number(b) : null);
        if (numA !== null && numB !== null)
            return numA === numB;
        const isBoolLike = (v) => typeof v === "boolean" || (typeof v === "string" && ["true", "false"].includes(v.trim().toLowerCase()));
        if (isBoolLike(a) && isBoolLike(b))
            return this.toBoolean(a) === this.toBoolean(b);
        return String(a) === String(b);
    }
    convertType(value, targetType, targetId, decimals = 2) {
        if (value === null || value === undefined)
            return value;
        switch (targetType) {
            case "boolToNum":
                return value ? 1 : 0;
            case "numToBool":
                return this.toBoolean(value);
            case "round": {
                const factor = Math.pow(10, decimals);
                if (typeof value === "number")
                    return Math.round(value * factor) / factor;
                const num = parseFloat(value);
                return isNaN(num) ? value : Math.round(num * factor) / factor;
            }
            default: {
                // Ohne explizit konfigurierten Typ: anhand des tatsächlichen Zielobjekt-Typs
                // (aus ensureAdapterObject bekannt) automatisch konvertieren statt anhand einer
                // fragilen Namensheuristik.
                const knownType = this.targetTypeCache.get(targetId);
                if (knownType === "boolean")
                    return this.toBoolean(value);
                if (knownType === "number") {
                    const num = typeof value === "number" ? value : parseFloat(value);
                    return isNaN(num) ? value : num;
                }
                return value;
            }
        }
    }
    // Wartet aktiv (statt eines pauschalen Sleeps) darauf, dass ein neu angelegtes Objekt
    // in der Objekt-DB sichtbar ist - beendet sich sobald es existiert, spätestens nach timeoutMs.
    async waitForObject(id, timeoutMs = 2000) {
        const start = Date.now();
        while (Date.now() - start < timeoutMs) {
            const obj = await this.getForeignObjectAsync(id);
            if (obj)
                return;
            await this.sleep(100);
        }
    }
    async ensureAdapterObject(sourceId, targetPath, mappingUnit) {
        const parts = targetPath.split(".");
        if (parts.length < 2) {
            this.log.warn(`[Setup] Ungültiger Zielpfad "${targetPath}" (Basispfad zu kurz) - übersprungen.`);
            return;
        }
        let currentPath = parts[0] + "." + parts[1];
        for (let i = 2; i < parts.length - 1; i++) {
            currentPath += "." + parts[i];
            try {
                const exists = await this.getForeignObjectAsync(currentPath);
                if (!exists) {
                    await this.setForeignObjectAsync(currentPath, {
                        _id: currentPath,
                        type: "folder",
                        common: { name: parts[i] },
                        native: {}
                    });
                }
            }
            catch (e) {
                this.log.debug(`[Setup] Ordner-Anlage ${currentPath} übersprungen: ${e.message}`);
            }
        }
        try {
            const finalExists = await this.getForeignObjectAsync(targetPath);
            if (!finalExists) {
                const sObj = await this.getForeignObjectAsync(sourceId);
                let type = "mixed";
                let role = "variable";
                let unit = mappingUnit;
                if (sObj && sObj.common) {
                    type = sObj.common.type || "mixed";
                    role = sObj.common.role || "variable";
                    if (!unit)
                        unit = sObj.common.unit;
                }
                if (targetPath.includes("Switch") || targetPath.includes("Schalter")) {
                    type = "boolean";
                    role = "switch";
                }
                await this.setForeignObjectAsync(targetPath, {
                    _id: targetPath,
                    type: "state",
                    common: {
                        name: `Export von ${sourceId}`,
                        type: type,
                        role: role,
                        unit: unit,
                        read: true,
                        write: true
                    },
                    native: {}
                });
                this.targetTypeCache.set(targetPath, type);
                await this.waitForObject(targetPath);
            }
            else if (finalExists.common) {
                this.targetTypeCache.set(targetPath, finalExists.common.type || "mixed");
            }
        }
        catch (e) {
            this.log.debug(`[Setup] Zielobjekt ${targetPath} übersprungen: ${e.message}`);
        }
    }
    /**
     * Regulärer Cycle Sync: Prüft mit Cache (produziert keinen massiven Funkverkehr)
     */
    async runCycleSync() {
        if (this.syncRunning || this.unloaded)
            return;
        this.syncRunning = true;
        try {
            const ids = [...this.sourceToMappings.keys()];
            const states = ids.length ? await this.getForeignStatesAsync(ids) : {};
            for (const [id, entries] of this.sourceToMappings) {
                const state = states[id];
                if (!state)
                    continue;
                for (const entry of entries) {
                    if (entry.fullTargetPath && (entry.dir === "out" || entry.dir === "both")) {
                        if (!this.passesAckFilter(entry, state))
                            continue;
                        const label = entry.dir === "both" ? "IOB -> MQTT (CYCLE: both)" : "IOB -> MQTT (CYCLE)";
                        await this.syncValue(id, entry.fullTargetPath, label, "CYCLE", entry.type, state, this.syncOptionsFor(entry, "cycle"));
                    }
                }
            }
            this.updateWatchdog("Cycle OK", true);
        }
        catch (e) {
            this.log.error(`Cycle-Sync-Fehler: ${e.message}`);
        }
        finally {
            this.syncRunning = false;
        }
    }
    /**
     * Force Sync (Heilung): Ignoriert den Cache und vergleicht direkt mit dem tatsächlichen
     * Zielwert - geschrieben wird nur, wo Quelle und Ziel wirklich auseinanderlaufen, und nur
     * von aktiven Quellen. Läuft nie parallel zum Cycle-Sync (gemeinsame Sperre) und staffelt
     * die Schreibvorgänge, um Funkbudget (Zigbee/433MHz) nicht als Burst zu belasten.
     */
    async runForceSync() {
        if (this.syncRunning || this.unloaded)
            return;
        this.syncRunning = true;
        try {
            this.log.info("[Force-Sync] Starte zyklische Zwangssynchronisation (Heilung von Asynchronität)...");
            let healed = 0;
            let forced = 0;
            // 1. IOB -> MQTT (für 'out' und 'both' - IOB ist die Quelle der Wahrheit für Aktoren)
            const outIds = [...this.sourceToMappings.keys()];
            const outStates = outIds.length ? await this.getForeignStatesAsync(outIds) : {};
            const outTargetStates = await this.getTargetStates(outIds.flatMap(id => this.sourceToMappings.get(id).map(e => e.fullTargetPath)));
            for (const [id, entries] of this.sourceToMappings) {
                const state = outStates[id];
                if (!state)
                    continue;
                for (const entry of entries) {
                    if (entry.fullTargetPath && (entry.dir === "out" || entry.dir === "both")) {
                        if (!this.passesAckFilter(entry, state))
                            continue;
                        const label = entry.dir === "both" ? "IOB -> MQTT (FORCE: both)" : "IOB -> MQTT (FORCE)";
                        const written = await this.syncValue(id, entry.fullTargetPath, label, "FORCE-SYNC", entry.type, state, this.syncOptionsFor(entry, "force", outTargetStates[entry.fullTargetPath]));
                        if (written) {
                            if (entry.syncMode === "force")
                                forced++;
                            else
                                healed++;
                            await this.sleep(75);
                        }
                    }
                }
                if (this.unloaded)
                    return;
            }
            // 2. MQTT -> IOB (für 'in' - MQTT ist die Quelle der Wahrheit für externe Sensoren)
            // Nur "single": bei "dual" ist die Quelle ein Befehls-Topic - ein Force-Sync würde
            // dort den zuletzt gesendeten Befehl periodisch wiederholen und damit z.B. ein am
            // Wandschalter ausgeschaltetes Licht von selbst wieder einschalten.
            const inIds = [];
            for (const [commandPath, entries] of this.targetToMappings) {
                if (entries.some(e => e.dir === "in" && e.topicMode !== "dual"))
                    inIds.push(commandPath);
            }
            const inStates = inIds.length ? await this.getForeignStatesAsync(inIds) : {};
            const inTargetStates = await this.getTargetStates(inIds.flatMap(p => this.targetToMappings.get(p).map(e => e.id)));
            for (const [commandPath, entries] of this.targetToMappings) {
                const state = inStates[commandPath];
                // Wie im Event-Pfad nur echte Broker-Werte (ack=true) übernehmen.
                if (!state || state.ack !== true)
                    continue;
                for (const entry of entries) {
                    if (entry.dir === "in" && entry.topicMode !== "dual") {
                        const written = await this.syncValue(commandPath, entry.id, "MQTT -> IOB (FORCE: in)", "FORCE-SYNC", entry.type, state, this.syncOptionsFor(entry, "force", inTargetStates[entry.id]));
                        if (written) {
                            if (entry.syncMode === "force")
                                forced++;
                            else
                                healed++;
                            await this.sleep(75);
                        }
                    }
                }
                if (this.unloaded)
                    return;
            }
            const forcedInfo = forced ? `, ${forced} im Modus "Force" neu geschrieben` : "";
            this.log.info(`[Force-Sync] Abgeschlossen: ${healed} abweichende Werte geheilt${forcedInfo}.`);
            this.updateWatchdog("Force-Sync OK", true);
        }
        catch (e) {
            this.log.error(`Force-Sync-Fehler: ${e.message}`);
        }
        finally {
            this.syncRunning = false;
        }
    }
    async generateJsonTree() {
        const mappings = this.config.mappings || [];
        const tree = {};
        const basePath = this.getValidatedBasePath();
        for (const entry of mappings) {
            if (!entry.mqttName)
                continue;
            const cleanSuffix = this.convertMqttPathToIobrokerId(entry.mqttName);
            const pathParts = cleanSuffix.split(".");
            let current = tree;
            for (let i = 0; i < pathParts.length; i++) {
                const part = pathParts[i];
                const isLast = i === pathParts.length - 1;
                if (isLast) {
                    if (current[part] && typeof current[part] === "object" && !current[part].full_topic) {
                        this.log.warn(`[JSON-Tree] Topic-Präfix-Kollision: "${cleanSuffix}" überschreibt eine bestehende Unterstruktur.`);
                    }
                    const statePath = `${basePath}${cleanSuffix}`;
                    const commandPath = this.resolveCommandPath(entry, statePath);
                    current[part] = {
                        full_topic: statePath,
                        // Bei "dual" laufen Befehle über ein eigenes Topic - im Export sichtbar,
                        // damit Backup und Vorschau die tatsächliche Topic-Struktur abbilden.
                        command_topic: commandPath,
                        topic_mode: entry.topicMode === "dual" ? "dual" : "single",
                        iobroker_id: entry.id,
                        type: this.sourceTypeCache.get(entry.id) || "unknown",
                        unit: entry.unit || ""
                    };
                }
                else {
                    if (current[part] && current[part].full_topic) {
                        this.log.warn(`[JSON-Tree] Topic-Präfix-Kollision: "${cleanSuffix}" kollidiert mit dem bestehenden Topic "${current[part].full_topic}".`);
                        current[part] = {};
                    }
                    else if (!current[part]) {
                        current[part] = {};
                    }
                    current = current[part];
                }
            }
        }
        return tree;
    }
    chunkArray(arr, size) {
        const out = [];
        for (let i = 0; i < arr.length; i += size)
            out.push(arr.slice(i, i + size));
        return out;
    }
    // Fehler, bei denen ein Retry mit identischer Payload nie zu einem anderen Ergebnis führt:
    // 4xx-Antworten (Client-/Konfigurationsfehler) und TLS-Zertifikatsfehler. Nur bei Netzwerk-
    // problemen (keine Antwort) oder 5xx-Serverfehlern lohnt sich ein erneuter Versuch.
    isRetryableError(e) {
        if (e.response) {
            return e.response.status >= 500;
        }
        const nonRetryableCodes = [
            "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
            "DEPTH_ZERO_SELF_SIGNED_CERT",
            "SELF_SIGNED_CERT_IN_CHAIN",
            "ERR_TLS_CERT_ALTNAME_INVALID",
            "CERT_HAS_EXPIRED"
        ];
        if (e.code && nonRetryableCodes.includes(e.code))
            return false;
        return true;
    }
    async postWithRetry(url, data, httpsAgent, retries = 2) {
        for (let attempt = 0; attempt <= retries; attempt++) {
            try {
                await axios_1.default.post(url, data, {
                    timeout: 10000,
                    httpsAgent,
                    // Verhindert, dass eine gesetzte HTTPS_PROXY-Umgebungsvariable den eigenen
                    // httpsAgent (und damit die konfigurierte CA) stillschweigend umgeht.
                    proxy: false,
                    headers: {
                        "Content-Type": "application/json",
                        "User-Agent": `ioBroker.mqtt-plus/${ADAPTER_VERSION}`
                    }
                });
                return;
            }
            catch (e) {
                if (attempt === retries || !this.isRetryableError(e))
                    throw e;
                await this.sleep(500 * Math.pow(2, attempt));
            }
        }
    }
    // Repariert häufige Copy-Paste-Beschädigungen in eingefügten PEM-Zertifikaten:
    // 1) Editoren/Autokorrektur ersetzen normale Bindestriche in "-----BEGIN...-----" durch
    //    optisch identische Unicode-Striche (En-/Em-Dash, Minuszeichen, U+2010-U+2015, U+2212)
    //    oder normale Leerzeichen durch geschützte Leerzeichen (U+00A0).
    // 2) Manche einzeiligen Formularfelder wandeln echte Zeilenumbrüche in Leerzeichen um -
    //    dann fehlt der Zeilenumbruch nach BEGIN/vor END, den OpenSSLs PEM-Parser zwingend
    //    braucht ("no start line"), obwohl Zeichenzahl und Kopfzeile unauffällig aussehen.
    normalizePem(pemInput) {
        let pem = pemInput
            .replace(/[\u2010-\u2015\u2212]/g, "-")
            .replace(/\u00a0/g, " ")
            .replace(/\r\n/g, "\n");
        pem = pem.replace(/-----BEGIN ([A-Z ]+)-----\s*/g, "-----BEGIN $1-----\n");
        pem = pem.replace(/\s*-----END ([A-Z ]+)-----/g, "\n-----END $1-----");
        return pem;
    }
    async runRemoteSync(verbose = false) {
        if (!this.config.syncUrl) {
            const msg = "Keine Sync-URL konfiguriert";
            await this.setStateAsync("info.lastSyncStatus", msg, true);
            return { success: false, message: msg };
        }
        let parsedUrl;
        try {
            parsedUrl = new URL(this.config.syncUrl);
        }
        catch (_a) {
            const msg = "Ungültige Sync-URL";
            this.log.error(`Remote Sync Error: ${msg} (${this.config.syncUrl})`);
            await this.setStateAsync("info.lastSyncStatus", msg, true);
            return { success: false, message: msg };
        }
        const encodedUrl = parsedUrl.toString();
        let templateStr = this.getDefaultSyncTemplate();
        try {
            const tplState = await this.getStateAsync("config.syncTemplate");
            if (tplState && tplState.val)
                templateStr = tplState.val;
        }
        catch (e) {
            this.log.debug(`[Remote Sync] Template-State nicht lesbar, verwende Standard: ${e.message}`);
        }
        // Platzhalter, die roh in einen JSON-String eingesetzt werden, müssen JSON-escaped werden -
        // sonst bricht ein Anführungszeichen/Backslash in einer ID das gesamte JSON.
        const esc = (s) => JSON.stringify(String(s)).slice(1, -1);
        const mappings = this.config.mappings || [];
        const payload = [];
        let skippedStale = 0;
        for (const entry of mappings) {
            try {
                const state = await this.getForeignStateAsync(entry.id);
                if (state) {
                    if (this.config.remoteSyncSkipStale && this.getStaleReason(state, this.getStaleLimitMs(entry))) {
                        skippedStale++;
                        continue;
                    }
                    // Funktions-Ersetzer statt String: ein "$" im Wert würde sonst als
                    // Ersetzungsmuster ($&, $1 ...) interpretiert.
                    let itemStr = templateStr
                        .replace(/%ID%/g, () => esc(entry.id))
                        .replace(/%MQTT%/g, () => esc(entry.mqttName))
                        .replace(/%PREFIX%/g, () => esc(this.config.targetBasePath))
                        .replace(/%DIR%/g, () => esc(entry.dir))
                        .replace(/%VAL%/g, () => JSON.stringify(state.val))
                        .replace(/%TS%/g, () => String(state.ts))
                        .replace(/%LC%/g, () => { var _a; return String((_a = state.lc) !== null && _a !== void 0 ? _a : state.ts); })
                        .replace(/%ACK%/g, () => String(state.ack === true))
                        .replace(/%Q%/g, () => { var _a; return String((_a = state.q) !== null && _a !== void 0 ? _a : 0); })
                        .replace(/%UNIT%/g, () => esc(entry.unit || ""));
                    try {
                        payload.push(JSON.parse(itemStr));
                    }
                    catch (_b) {
                        this.log.warn(`Remote-Sync Template Fehler bei ${entry.id}`);
                    }
                }
            }
            catch (e) {
                this.log.debug(`[Remote Sync] Zustand für ${entry.id} nicht lesbar: ${e.message}`);
            }
        }
        this.log.debug(`[Remote Sync] Payload to send: ${JSON.stringify(payload)}`);
        // Für interne/selbstsignierte Ziele: das eigene Zertifikat/CA gezielt vertrauen,
        // statt die Prüfung komplett abzuschalten. Ohne Angabe gilt der normale
        // System-Vertrauensstore (öffentliche CAs).
        let httpsAgent;
        if (this.config.syncCaCert && this.config.syncCaCert.trim()) {
            const normalizedCaCert = this.normalizePem(this.config.syncCaCert);
            try {
                httpsAgent = new https.Agent({ ca: normalizedCaCert });
                // Nur beim manuellen Verbindungstest (info-Level), um den Log bei regulären
                // Intervall-Läufen nicht zuzumüllen - beweist, dass die konfigurierte CA den
                // Adapter-Prozess tatsächlich erreicht hat (Diagnose für Config-Save/Restart-Probleme).
                if (verbose) {
                    let fingerprintInfo = "Fingerabdruck nicht ermittelbar";
                    try {
                        const cert = new crypto.X509Certificate(normalizedCaCert);
                        fingerprintInfo = `Subject="${cert.subject.replace(/\n/g, ", ")}" Fingerprint(SHA256)=${cert.fingerprint256}`;
                    }
                    catch (certErr) {
                        const trimmed = normalizedCaCert.trim();
                        const headCodes = [...trimmed.slice(0, 12)].map(c => c.charCodeAt(0)).join(",");
                        fingerprintInfo = `Zertifikat konnte auch nach Normalisierung nicht geparst werden: ${certErr.message} | erste 12 Zeichencodes: ${headCodes} (erwartet für "-----BEGIN": 45,45,45,45,45,66,69,71,73,78,32,67)`;
                    }
                    this.log.info(`[Remote Sync] Benutzerdefinierte CA geladen (${normalizedCaCert.trim().length} Zeichen). ${fingerprintInfo}`);
                }
            }
            catch (e) {
                const msg = `Ungültiges CA-Zertifikat in den Einstellungen: ${e.message}`;
                this.log.error(`Remote Sync Error: ${msg}`);
                await this.setStateAsync("info.lastSyncStatus", msg, true);
                return { success: false, message: msg };
            }
        }
        else if (verbose) {
            this.log.info("[Remote Sync] Kein CA-Zertifikat konfiguriert - Standard-Systemvertrauensstore wird verwendet.");
        }
        let sentCount = 0;
        try {
            // Sehr große Konfigurationen in Häppchen senden statt als einen Riesen-POST,
            // dessen einzelner Timeout sonst die komplette Payload verwirft.
            const chunks = this.chunkArray(payload, MqttPlus.REMOTE_SYNC_CHUNK_SIZE);
            for (const chunk of chunks) {
                await this.postWithRetry(encodedUrl, chunk, httpsAgent);
                sentCount += chunk.length;
            }
            const staleInfo = skippedStale ? `, ${skippedStale} inaktive übersprungen` : "";
            const msg = `OK: ${payload.length} Werte gesendet${staleInfo} (${new Date().toLocaleTimeString()})`;
            await this.setStateAsync("info.lastSyncStatus", msg, true);
            this.log.debug(`Remote Sync (${payload.length}) OK.`);
            return { success: true, message: msg };
        }
        catch (e) {
            let errorMsg = e.message;
            if (e.response) {
                errorMsg = `HTTP ${e.response.status}: ${e.response.statusText}`;
            }
            if (e.code) {
                errorMsg += ` (${e.code})`;
            }
            // Bei Chunking zeigt der Teilerfolg, ob nur ein Bruchteil oder praktisch nichts
            // angekommen ist - relevant, weil ein einzelner gescheiterter Chunk sonst wie ein
            // Totalausfall aussieht, obwohl der Großteil der Werte bereits übertragen wurde.
            const progress = payload.length > MqttPlus.REMOTE_SYNC_CHUNK_SIZE ? ` (${sentCount}/${payload.length} Werte übertragen)` : "";
            const fullMsg = `Fehler${progress}: ${errorMsg} (${new Date().toLocaleTimeString()})`;
            this.log.error(`Remote Sync Error: ${fullMsg} | URL: ${encodedUrl}`);
            await this.setStateAsync("info.lastSyncStatus", fullMsg, true);
            return { success: false, message: fullMsg };
        }
    }
    // Konstante-Zeit-Vergleich gegen Timing-Angriffe auf den Passwortvergleich.
    timingSafeStringEqual(a, b) {
        const bufA = Buffer.from(a);
        const bufB = Buffer.from(b);
        if (bufA.length !== bufB.length) {
            // Trotzdem konstante Zeit vergleichen (gegen Längen-Rückschlüsse per Timing);
            // Ergebnis ist ohnehin false.
            crypto.timingSafeEqual(bufA, bufA);
            return false;
        }
        return crypto.timingSafeEqual(bufA, bufB);
    }
    // Vergleicht die "Authorization: Basic ..."-Kopfzeile gegen die konfigurierten Zugangsdaten.
    // Ist kein Passwort konfiguriert, bleibt der Server bewusst offen (Warnung erfolgt in onReady).
    // Zusätzlich: Brute-Force-Sperre nach zu vielen Fehlversuchen pro Client-IP.
    checkAuth(req) {
        const password = this.config.dashboardPassword;
        if (!password)
            return true;
        const ip = req.socket.remoteAddress || "unknown";
        const entry = this.failedAuthAttempts.get(ip);
        if (entry && entry.lockedUntil > Date.now())
            return false;
        const user = this.config.dashboardUser || "admin";
        const header = req.headers["authorization"];
        let ok = false;
        if (header && header.startsWith("Basic ")) {
            try {
                const decoded = Buffer.from(header.slice(6), "base64").toString("utf8");
                const sep = decoded.indexOf(":");
                if (sep !== -1) {
                    const reqUser = decoded.slice(0, sep);
                    const reqPass = decoded.slice(sep + 1);
                    ok = this.timingSafeStringEqual(reqUser, user) && this.timingSafeStringEqual(reqPass, password);
                }
            }
            catch (_a) {
                ok = false;
            }
        }
        if (ok) {
            this.failedAuthAttempts.delete(ip);
            return true;
        }
        const attempts = ((entry === null || entry === void 0 ? void 0 : entry.count) || 0) + 1;
        if (attempts >= MqttPlus.MAX_AUTH_ATTEMPTS) {
            this.failedAuthAttempts.set(ip, { count: 0, lockedUntil: Date.now() + MqttPlus.AUTH_LOCKOUT_MS });
            this.log.warn(`[Dashboard] Zu viele fehlgeschlagene Login-Versuche von ${ip} - für 5 Minuten gesperrt.`);
            this.persistAuthLockouts();
        }
        else {
            this.failedAuthAttempts.set(ip, { count: attempts, lockedUntil: 0 });
        }
        return false;
    }
    // Vergleicht Origin/Referer-Header gegen den eigenen Host - Basis für CORS-Beschränkung
    // und CSRF-Schutz bei zustandsändernden Requests. Ohne Origin/Referer (z.B. curl/Skript,
    // kein Browser) wird nicht blockiert - dort besteht kein CSRF-Risiko über den Browser.
    isSameOrigin(req) {
        const host = req.headers.host;
        if (!host)
            return false;
        const check = (req.headers.origin || req.headers.referer);
        if (!check)
            return true;
        try {
            return new URL(check).host === host;
        }
        catch (_a) {
            return false;
        }
    }
    // Liest den Request-Body byte-genau ein (Buffer statt String-Konkatenation, damit ein über
    // zwei Chunks gesplittetes Multibyte-Zeichen nicht zu korruptem JSON führt), bricht mit 413
    // ab, sobald maxBytes überschritten wird, und hängt bei Verbindungsabbruch/Timeout nicht.
    readBodyLimited(req, res, maxBytes) {
        return new Promise(resolve => {
            const chunks = [];
            let size = 0;
            let settled = false;
            const finish = (result) => {
                if (settled)
                    return;
                settled = true;
                resolve(result);
            };
            req.setTimeout(30000, () => {
                if (!settled) {
                    res.writeHead(408, { "Content-Type": "application/json" });
                    res.end(JSON.stringify({ success: false, error: "Zeitüberschreitung beim Empfang" }));
                    req.destroy();
                    finish(null);
                }
            });
            req.on("data", (chunk) => {
                if (settled)
                    return;
                size += chunk.length;
                if (size > maxBytes) {
                    res.writeHead(413, { "Content-Type": "application/json" });
                    res.end(JSON.stringify({ success: false, error: "Anfrage zu groß" }));
                    req.destroy();
                    finish(null);
                    return;
                }
                chunks.push(chunk);
            });
            req.on("error", () => finish(null));
            req.on("end", () => {
                if (!settled)
                    finish(Buffer.concat(chunks).toString("utf8"));
            });
        });
    }
    startWebServer(port) {
        try {
            const requestListener = async (req, res) => {
                try {
                    res.setHeader("Access-Control-Allow-Methods", "GET, POST");
                    // CORS gezielt statt "*": nur die eigene Origin darf die Antwort lesen.
                    const sameOrigin = this.isSameOrigin(req);
                    if (sameOrigin && req.headers.origin) {
                        res.setHeader("Access-Control-Allow-Origin", req.headers.origin);
                    }
                    if (!this.checkAuth(req)) {
                        res.setHeader("WWW-Authenticate", 'Basic realm="mqtt-plus"');
                        res.writeHead(401, { "Content-Type": "text/plain; charset=utf-8" });
                        res.end("Unauthorized");
                        return;
                    }
                    // CSRF-Schutz: zustandsändernde Requests nur, wenn Origin/Referer zum eigenen
                    // Host passt. Basic-Auth wird vom Browser automatisch mitgeschickt - ohne
                    // diese Prüfung könnte jede fremde Webseite die Mapping-Konfiguration
                    // überschreiben.
                    if (req.method === "POST" && !sameOrigin) {
                        res.writeHead(403, { "Content-Type": "application/json" });
                        res.end(JSON.stringify({ success: false, error: "Cross-Origin-Request abgelehnt" }));
                        return;
                    }
                    // Pfad statt rohem req.url vergleichen - sonst bricht jeder Query-String (?t=123)
                    const pathname = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`).pathname;
                    if (pathname === "/" || pathname === "/index.html") {
                        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
                        res.end(this.getDashboardHtml());
                    }
                    else if (pathname === "/api/json") {
                        const tree = await this.generateJsonTree();
                        const exportData = {
                            prefix: this.config.targetBasePath,
                            mappings: this.config.mappings || [],
                            structure: tree
                        };
                        res.writeHead(200, { "Content-Type": "application/json" });
                        res.end(JSON.stringify(exportData, null, 2));
                    }
                    else if (pathname === "/api/status") {
                        const syncTemplateState = await this.getStateAsync("config.syncTemplate");
                        const status = {
                            watchdog: this.currentWatchdogStatus,
                            uptime: process.uptime(),
                            mappings: this.config.mappings ? this.config.mappings.length : 0,
                            syncTemplate: syncTemplateState ? syncTemplateState.val : this.getDefaultSyncTemplate()
                        };
                        res.writeHead(200, { "Content-Type": "application/json" });
                        res.end(JSON.stringify(status));
                    }
                    else if (pathname === "/api/save-template" && req.method === "POST") {
                        const body = await this.readBodyLimited(req, res, MqttPlus.MAX_BODY_BYTES);
                        if (body === null)
                            return; // Fehlerantwort wurde bereits gesendet
                        try {
                            const data = JSON.parse(body);
                            if (data.template) {
                                await this.setStateAsync("config.syncTemplate", data.template, true);
                                this.log.info("Neues Remote-Sync Template gespeichert.");
                                res.writeHead(200, { "Content-Type": "application/json" });
                                res.end(JSON.stringify({ success: true }));
                            }
                            else {
                                throw new Error("Kein Template empfangen");
                            }
                        }
                        catch (e) {
                            res.writeHead(500, { "Content-Type": "application/json" });
                            res.end(JSON.stringify({ success: false, error: e.message }));
                        }
                    }
                    else if (pathname === "/api/upload-backup" && req.method === "POST") {
                        const body = await this.readBodyLimited(req, res, MqttPlus.MAX_BODY_BYTES);
                        if (body === null)
                            return; // Fehlerantwort wurde bereits gesendet
                        try {
                            const uploaded = JSON.parse(body);
                            if (uploaded && Array.isArray(uploaded.mappings)) {
                                this.log.info(`Restore gestartet! ${uploaded.mappings.length} Mappings gefunden.`);
                                const adapterObj = await this.getForeignObjectAsync(`system.adapter.${this.namespace}`);
                                if (adapterObj) {
                                    adapterObj.native.mappings = uploaded.mappings;
                                    if (uploaded.prefix) {
                                        adapterObj.native.targetBasePath = uploaded.prefix;
                                        this.log.info(`Prefix auf ${uploaded.prefix} gesetzt.`);
                                    }
                                    await this.setForeignObjectAsync(`system.adapter.${this.namespace}`, adapterObj);
                                    res.writeHead(200, { "Content-Type": "application/json" });
                                    res.end(JSON.stringify({ success: true, message: "Konfiguration wiederhergestellt. Adapter startet neu..." }));
                                }
                                else {
                                    throw new Error("Adapter-Objekt nicht gefunden!");
                                }
                            }
                            else {
                                throw new Error("Ungültiges Dateiformat. 'mappings' Array fehlt.");
                            }
                        }
                        catch (e) {
                            this.log.error(`Restore Fehler: ${e.message}`);
                            res.writeHead(500, { "Content-Type": "application/json" });
                            res.end(JSON.stringify({ success: false, error: e.message }));
                        }
                    }
                    else {
                        res.writeHead(404);
                        res.end("Not found");
                    }
                }
                catch (e) {
                    // Ohne dieses catch würde ein Fehler in generateJsonTree() o.ä. nie eine
                    // Antwort senden - der Client hinge bis zum eigenen Timeout.
                    this.log.error(`[Dashboard] Unerwarteter Fehler: ${e.message}`);
                    if (!res.headersSent) {
                        res.writeHead(500, { "Content-Type": "application/json" });
                        res.end(JSON.stringify({ success: false, error: "Interner Fehler" }));
                    }
                }
            };
            // Optionales HTTPS: ohne Zertifikat/Key läuft der Server wie bisher über HTTP -
            // dann werden Basic-Auth-Zugangsdaten aber unverschlüsselt übertragen (siehe Warnung
            // in onReady). Mit beiden Feldern gesetzt wird verschlüsselt.
            let usesTls = false;
            if (this.config.dashboardTlsCert && this.config.dashboardTlsKey) {
                try {
                    const cert = this.normalizePem(this.config.dashboardTlsCert);
                    const key = this.normalizePem(this.config.dashboardTlsKey);
                    this.httpServer = https.createServer({ cert, key }, requestListener);
                    usesTls = true;
                }
                catch (e) {
                    this.log.error(`[Dashboard] TLS-Zertifikat/Key ungültig, falle auf HTTP zurück: ${e.message}`);
                }
            }
            if (!this.httpServer) {
                this.httpServer = http.createServer(requestListener);
            }
            this.httpServer.on("connection", (socket) => {
                this.activeSockets.add(socket);
                socket.on("close", () => this.activeSockets.delete(socket));
            });
            const bindHost = this.config.bindHost || "0.0.0.0";
            // Bei einem Update/Neustart hält der alte Prozess den Port oft noch einige Sekunden.
            // Deshalb erst mehrfach neu versuchen, statt sofort (und dauerhaft) aufzugeben.
            let listenAttempts = 0;
            const tryListen = () => {
                listenAttempts++;
                this.httpServer.listen(port, bindHost);
            };
            this.httpServer.on("listening", () => {
                this.log.info(`Dashboard Webserver läuft auf ${usesTls ? "https" : "http"}://${bindHost}:${port}`);
                this.setState("info.connection", true, true);
            });
            this.httpServer.on("error", (e) => {
                if (e.code === "EADDRINUSE" && listenAttempts < MqttPlus.LISTEN_ATTEMPTS && !this.unloaded) {
                    this.log.warn(`Port ${port} ist noch belegt - neuer Versuch ${listenAttempts + 1}/${MqttPlus.LISTEN_ATTEMPTS} in ${MqttPlus.LISTEN_RETRY_MS / 1000} s.`);
                    this.setTimeout(tryListen, MqttPlus.LISTEN_RETRY_MS);
                    return;
                }
                this.log.error(`Webserver Fehler: ${e.message}`);
                this.setState("info.connection", false, true);
                if (e.code === "EADDRINUSE") {
                    this.log.error(`Port ${port} ist dauerhaft belegt - Adapter wird beendet, damit er nicht "grün" ohne Dashboard weiterläuft.`);
                    // terminate() statt eines harten Prozess-Endes: beendet im Compact Mode nur
                    // diese Instanz, nicht den gesamten Host-Prozess.
                    this.terminate("EADDRINUSE", utils.EXIT_CODES.ADAPTER_REQUESTED_TERMINATION);
                }
            });
            tryListen();
        }
        catch (e) {
            this.log.error(`Konnte Webserver nicht starten: ${e.message}`);
            this.setState("info.connection", false, true);
        }
    }
    getDashboardHtml() {
        return `
<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MQTT Plus Dashboard</title>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background: #f0f2f5; color: #333; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        h1 { color: #0078d4; border-bottom: 2px solid #0078d4; padding-bottom: 10px; }
        h2 { margin-top: 30px; color: #444; }
        .status-bar { display: flex; gap: 20px; margin-bottom: 20px; background: #e6f0ff; padding: 15px; border-radius: 4px; }
        .status-item { font-weight: bold; }
        .status-value { color: #0078d4; }
        textarea { width: 100%; height: 150px; font-family: monospace; padding: 10px; border: 1px solid #ccc; border-radius: 4px; }
        pre { background: #282c34; color: #abb2bf; padding: 15px; border-radius: 4px; overflow: auto; max-height: 500px; }
        button { background: #0078d4; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-size: 14px; margin-right: 10px; }
        button:hover { background: #005a9e; }
        button.secondary { background: #6c757d; }
        button.secondary:hover { background: #5a6268; }
        .help { font-size: 0.9em; color: #666; margin-top: 5px; }
        .upload-area { border: 2px dashed #ccc; padding: 20px; text-align: center; margin-bottom: 20px; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>MQTT Plus Dashboard</h1>

        <div class="status-bar">
            <div class="status-item">Watchdog: <span id="wd" class="status-value">Lade...</span></div>
            <div class="status-item">Mappings: <span id="map" class="status-value">0</span></div>
            <div class="status-item">Uptime: <span id="up" class="status-value">0s</span></div>
        </div>

        <h2>Backup & Restore</h2>
        <div class="upload-area">
            <button onclick="downloadJson()">Download Backup (.json)</button>
            <span style="margin: 0 15px;">|</span>
            <input type="file" id="restoreFile" accept=".json" />
            <button class="secondary" onclick="uploadBackup()">Backup Wiederherstellen</button>
        </div>

        <h2>JSON Struktur Vorschau</h2>
        <div style="margin-bottom: 10px;">
            <button class="secondary" onclick="loadJson()">Vorschau Aktualisieren</button>
        </div>
        <pre id="jsonViewer">Lade Daten...</pre>

        <h2>Remote Sync Konfiguration</h2>
        <p>Hier können Sie definieren, wie das JSON für den Remote-Sync (POST Request) aussieht.</p>
        <div class="help">Platzhalter: %ID%, %MQTT%, %VAL%, %TS% (letzte Aktualisierung), %LC% (letzte Änderung), %ACK%, %Q% (Qualität), %UNIT%, %PREFIX%, %DIR%</div>
        <textarea id="templateEditor"></textarea>
        <div style="margin-top: 10px;">
            <button onclick="saveTemplate()">Template Speichern</button>
            <button class="secondary" onclick="resetTemplate()">Standard wiederherstellen</button>
        </div>
    </div>

    <script>
        const defaultTemplate = '{"id": "%ID%", "topic": "%MQTT%", "value": %VAL%, "ts": %TS%, "unit": "%UNIT%", "prefix": "%PREFIX%", "dir": "%DIR%"}';

        async function loadStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                document.getElementById('wd').innerText = data.watchdog;
                document.getElementById('map').innerText = data.mappings;
                document.getElementById('up').innerText = Math.round(data.uptime) + 's';
                if (!document.getElementById('templateEditor').value) {
                    document.getElementById('templateEditor').value = data.syncTemplate || defaultTemplate;
                }
            } catch(e) { console.error(e); }
        }

        async function loadJson() {
            try {
                document.getElementById('jsonViewer').innerText = "Lade...";
                const res = await fetch('/api/json');
                const data = await res.json();
                document.getElementById('jsonViewer').innerText = JSON.stringify(data.structure, null, 4);
                window.lastJson = data;
            } catch(e) {
                document.getElementById('jsonViewer').innerText = "Fehler beim Laden: " + e;
            }
        }

        function downloadJson() {
            if(!window.lastJson) {
                fetch('/api/json').then(r => r.json()).then(data => {
                    window.lastJson = data;
                    executeDownload();
                });
            } else {
                executeDownload();
            }
        }

        function executeDownload() {
            const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(window.lastJson, null, 4));
            const downloadAnchorNode = document.createElement('a');
            downloadAnchorNode.setAttribute("href",     dataStr);
            downloadAnchorNode.setAttribute("download", "mqtt_plus_backup.json");
            document.body.appendChild(downloadAnchorNode);
            downloadAnchorNode.click();
            downloadAnchorNode.remove();
        }

        async function uploadBackup() {
            const fileInput = document.getElementById('restoreFile');
            if(fileInput.files.length === 0) return alert("Bitte erst eine Datei auswählen!");

            const file = fileInput.files[0];
            const reader = new FileReader();

            reader.onload = async function(e) {
                try {
                    const jsonContent = e.target.result;
                    const parsed = JSON.parse(jsonContent);
                    if(!parsed.mappings) throw new Error("Keine Mappings in Datei gefunden!");

                    if(!confirm("ACHTUNG: Dies überschreibt die aktuelle Konfiguration und startet den Adapter neu. Fortfahren?")) return;

                    const res = await fetch('/api/upload-backup', {
                        method: 'POST',
                        body: jsonContent
                    });
                    const ret = await res.json();

                    if(ret.success) {
                        alert(ret.message);
                        location.reload();
                    } else {
                        alert("Fehler beim Restore: " + ret.error);
                    }
                } catch(err) {
                    alert("Dateifehler: " + err);
                }
            };
            reader.readAsText(file);
        }

        async function saveTemplate() {
            const tpl = document.getElementById('templateEditor').value;
            try {
                const res = await fetch('/api/save-template', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({template: tpl})
                });
                const ret = await res.json();
                if(ret.success) alert("Gespeichert!");
                else alert("Fehler: " + ret.error);
            } catch(e) { alert("Sende-Fehler: " + e); }
        }

        function resetTemplate() {
            document.getElementById('templateEditor').value = defaultTemplate;
        }

        loadStatus();
        loadJson();
        window.setInterval(loadStatus, 5000);
    </script>
</body>
</html>
        `;
    }
    getDefaultSyncTemplate() {
        return '{"id": "%ID%", "topic": "%MQTT%", "value": %VAL%, "ts": %TS%, "unit": "%UNIT%", "prefix": "%PREFIX%", "dir": "%DIR%"}';
    }
    // Schreibt den Watchdog-Status nur bei Änderung oder maximal alle 30s - verhindert
    // hunderte States-DB-Schreibvorgänge pro Minute bei aktivem Datenverkehr (z.B. auf SD-Karte).
    // Ausnahme: info.lastCycle bei isCycleEnd=true (echter Zyklusabschluss) wird immer sofort
    // geschrieben, sonst könnte der Zeitstempel bei kurzen Intervallen ohne Statuswechsel
    // zwischen zwei Zyklen bis zu 30s hinter dem tatsächlichen letzten Zyklus zurückbleiben.
    updateWatchdog(status, isCycleEnd = false) {
        const now = Date.now();
        if (isCycleEnd) {
            this.setState("info.lastCycle", now, true);
        }
        const changed = status !== this.lastWatchdogStatus;
        if (!changed && now - this.lastWatchdogWriteTs < 30000)
            return;
        this.lastWatchdogStatus = status;
        this.lastWatchdogWriteTs = now;
        this.currentWatchdogStatus = `${status} (${new Date().toLocaleTimeString()})`;
        this.setState("watchdog", this.currentWatchdogStatus, true);
        // Maschinenlesbare Variante zusätzlich zum lokalisierten Text-State: reiner Status-String
        // plus Unix-Timestamp statt serverlokalem toLocaleTimeString().
        this.setState("info.status", status, true);
        if (!isCycleEnd) {
            this.setState("info.lastCycle", now, true);
        }
    }
    async onUnload(callback) {
        this.unloaded = true;
        try {
            [this.updateInterval, this.forceSyncInterval, this.syncInterval].forEach(i => {
                if (i)
                    this.clearInterval(i);
            });
            if (this.httpServer) {
                for (const socket of this.activeSockets)
                    socket.destroy();
                this.activeSockets.clear();
                await new Promise(resolve => this.httpServer.close(() => resolve()));
            }
            try {
                const allIds = new Set([...this.sourceToMappings.keys(), ...this.targetToMappings.keys()]);
                if (allIds.size > 0) {
                    await this.unsubscribeForeignStatesAsync([...allIds]);
                }
            }
            catch (e) {
                this.log.debug(`Unsubscribe-Fehler: ${e.message}`);
            }
            await this.setStateAsync("info.connection", false, true);
            this.log.info("cleaned everything up...");
            callback();
        }
        catch (e) {
            this.log.error(`Fehler beim Herunterfahren: ${e.message}`);
            callback();
        }
    }
}
// Obergrenze für POST-Bodies (Backup-Upload / Sync-Template) gegen Memory-Exhaustion
MqttPlus.MAX_BODY_BYTES = 5 * 1024 * 1024; // 5 MB
MqttPlus.MAX_AUTH_ATTEMPTS = 10;
MqttPlus.AUTH_LOCKOUT_MS = 5 * 60 * 1000; // 5 Minuten
MqttPlus.REMOTE_SYNC_CHUNK_SIZE = 200;
// Sicherheitsnetz für pendingWrites: falls auf einen eigenen Schreibvorgang nie ein
// (echtes oder Echo-)Ereignis folgt, verfällt der Merker statt für immer liegenzubleiben.
MqttPlus.PENDING_WRITE_TTL_MS = 10000;
// Port-Konflikt beim Start: so oft neu versuchen, bevor der Adapter aufgibt.
MqttPlus.LISTEN_ATTEMPTS = 6;
MqttPlus.LISTEN_RETRY_MS = 5000;
// Standard-Aktualitätsgrenze, falls in der Instanz (z.B. nach Update von <1.6.0) nichts gesetzt ist.
MqttPlus.DEFAULT_STALE_AFTER_MIN = 1440; // 24 h
if (require.main !== module) {
    module.exports = (options) => new MqttPlus(options);
}
else {
    (() => new MqttPlus())();
}
//# sourceMappingURL=main.js.map