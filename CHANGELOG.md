# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2](https://github.com/andvari-labs/skyblock-collector/compare/v0.2.1...v0.2.2) - 2026-11-23

### Fixed

- *(deploy)* say the pre-trellis image is gone when restore.sh is asked for its revision ([#14](https://github.com/andvari-labs/skyblock-collector/pull/14))

### Other

- *(core)* word the fixtures note as the scoped Test data rule does ([#12](https://github.com/andvari-labs/skyblock-collector/pull/12))
- *(core)* say what the test fixtures are and where they came from ([#10](https://github.com/andvari-labs/skyblock-collector/pull/10))
- use plainly invented identifiers in the unit tests ([#7](https://github.com/andvari-labs/skyblock-collector/pull/7))

## [0.2.1](https://github.com/andvari-labs/skyblock-collector/compare/v0.2.0...v0.2.1) - 2026-11-22

### Fixed

- *(deploy)* copy the systemd units instead of linking them ([#4](https://github.com/andvari-labs/skyblock-collector/pull/4))

## [0.2.0](https://github.com/andvari-labs/skyblock-collector/releases/tag/v0.2.0) - 2026-11-22

The first release. It polls Hypixel's auction house, ended auctions, bazaar, election and slower
resources, keeps every response in a raw archive and writes the parsed data to Postgres, with the
same database schema, archive, spool, settings, command line and status server as the collector it
replaces. It is released as a container image for x86-64 and ARM64, and the deploy kit in
`crates/skyblock_collector/deploy/` deploys released images.

### Added

- *(deploy)* the deploy kit for released images
- port the collector from skyblock-collector-old

### Other

- generate from trellis v0.17.3
