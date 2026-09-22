const path = require("node:path");
const { tests } = require("@iobroker/testing");

// Prüft package.json und io-package.json auf Konsistenz (Version, Pflichtfelder, ...)
tests.packageFiles(path.join(__dirname, ".."));
