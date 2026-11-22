# Design

How skyblock-collector works and why. Update this page in the same change as the code it
describes.

## Goals

skyblock-collector records the Hypixel SkyBlock market so that mispriced auctions can be studied
later. It polls Hypixel's public API, which needs no key and is its only data source. Its one
user is its owner, who queries the data in Postgres.

- **Lose nothing that can't be fetched again.** `auctions_ended` covers only the last 60
  seconds, and nothing else has the full history, so a missed sale is gone for good. The same goes
  for the bazaar's prices and the election's vote counts. Every response is kept, and a second
  collector on another network covers outages.
- **Keep the raw data.** Every response is archived byte for byte. The database can be rebuilt
  from the archive alone, so a parsing bug or a new column never means lost history.
- **Don't push Hypixel.** Poll each feed once per published snapshot, just after Cloudflare's
  copy changes, and never in a way that makes Cloudflare cache an old snapshot.

## Architecture

| Crate | Owns |
| --- | --- |
| `skyblock_collector` | The binary: the command line, the async runtime, the supervisor that restarts a failed task, signal handling and the shutdown order. |
| `skyblock_collector_core` | Everything else: the pollers and their cadence, the HTTP client, the archive, the spool, the ingest worker, the database writes, replay and backfill, and the status server. |
| `skyblock_collector_testdb` | Test support only: a throwaway Postgres container for the integration tests. |

A response moves through three stores, each written before the next:

1. A poller fetches a feed and appends the response to the **archive**, one zstd stream per feed
   and UTC day (hourly on a secondary collector), synced after every line.
2. A new snapshot then goes to the **spool**, one file per snapshot, until Postgres commits it.
3. The **ingest** worker writes spooled snapshots to **Postgres** in idempotent transactions and
   deletes each file only once its rows are committed.

`replay` reads archive files back through the same writes, and `backfill` writes only the
snapshots the database lacks, from the secondary collector's files. The schema in
`crates/skyblock_collector_core/schema/0001_schema.sql` is applied once, and its checksum is
recorded in `_sqlx_migrations`. The collector refuses a database created from another version of
the file, so a schema change is a rebuild from the archive.

## Decisions

- **The schema migration is built at runtime.** sqlx's `migrate!` macro is a proc macro, which
  the custom lints can't check. `db::ensure_schema` builds the same migration with
  `Migrator::with_migrations`: version 1, description `schema`, and the checksum of the unchanged
  file. Databases created by earlier versions accept it unchanged.
- **The deployment runs released images.** The deploy kit used to build the image from `main` on
  the host. It now deploys the newest release tag and pulls that release's image, which CI built
  and ran, with the kit's files taken from the same tag. The secondary collector runs the ARM64
  image of the same release, copied from the primary host, so its host pulls nothing.
  Health checks, rollback and the secondary's known-good image work as before.
- **The tests start their own Postgres.** Trellis's CI has no service containers, and `just ci`
  does locally what CI does. So each test process starts a throwaway Postgres container through
  `docker`, unless `TEST_DATABASE_URL` names a server to use.
- **The image runs as distroless's nonroot user, and the kit runs it as uid 999.** The deployment's
  data directories belong to the host user, whom the pod maps to uid 999. The units pass
  `User=999:999`, so the files stay readable by the host user. The image has no shell, so the
  health checks call the binary directly.
