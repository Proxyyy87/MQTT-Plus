# Contributing to ioBroker.mqtt-plus

## Development setup

```bash
git clone https://github.com/Proxyyy87/ioBroker.mqtt-plus.git
cd ioBroker.mqtt-plus
npm install
npm run build          # compiles src/*.ts to build/
npm run watch          # recompiles on every change
npm run check          # type checking only
npm test               # package file tests
npm run test:integration   # starts the adapter in a temporary js-controller instance
```

## Please note

* The `build/` folder is committed on purpose. The official ioBroker adapter checker loads the
  `main` file (`build/main.js`) directly from the repository. After every change in `src/*.ts`
  run `npm run build` and commit the updated `build/` folder as well.
* Document every change in the `## Changelog` section of `README.md` below the
  `### **WORK IN PROGRESS**` placeholder.

## Releasing

Releases are published by the GitHub Actions workflow `test-and-release.yml` when a version tag
(`v1.2.3`) is pushed. Publishing uses npm Trusted Publishing, so no npm token is stored in the
repository and every release is published with provenance.

### One-time setup: trusted publisher on npmjs.com

**This must be configured before the next release, otherwise the deploy job fails with
`ENEEDAUTH` / `OIDC token exchange error - package not found`.**

On npmjs.com go to **Packages → `iobroker.mqtt-plus` → Settings → Trusted publishing**, add a
publisher of type **GitHub Actions** and enter exactly (all fields are case-sensitive):

| Field | Value |
|---|---|
| Organization or user | `Proxyyy87` |
| Repository | `ioBroker.mqtt-plus` |
| Workflow filename | `test-and-release.yml` |
| Environment | *(leave empty)* |

Under **Allowed actions**, make sure **`npm publish`** is permitted. Entries created after
2026-09-03 only allow `npm stage publish` by default, which is *not* what the release workflow
uses.

Notes:

* npm does not validate the configuration when saving - mistakes only surface during a release.
* Existing entries cannot be edited. To correct them, delete and recreate.
* After saving, the entry must be visible in the list. An empty list means nothing was stored.

### Publishing a release

Before tagging:

1. Bump the version in `package.json` and `io-package.json` (`common.version`).
2. Add a `common.news` entry for the new version in `io-package.json` (all languages).
3. Replace `### **WORK IN PROGRESS**` in `README.md` with `### <version> (<date>)`.
4. Run `npm run build` and commit everything.
5. Push the tag: `git tag v<version> && git push origin v<version>`.
