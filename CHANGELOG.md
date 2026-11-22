# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
