const path = require("node:path");
const { tests } = require("@iobroker/testing");

// Startet den Adapter in einer frischen js-controller-Testinstanz und prüft, dass er
// ohne Fehler hochfährt und sauber wieder beendet werden kann.
tests.integration(path.join(__dirname, ".."));
