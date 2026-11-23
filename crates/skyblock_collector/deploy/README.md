# The skyblock-collector deployment

This directory is the deployment of [andvari-labs/skyblock-collector](https://github.com/andvari-labs/skyblock-collector). The source of these files is `crates/skyblock_collector/deploy/` in that repository; `update.sh` replaces them on every deploy, so change them there. The host's own settings are in `deploy.env` here, which no deploy touches; `deploy.env.example` in the repository lists them.

## What runs

- The pod `skyblock-collector` (`skyblock-collector-pod.service`) publishes the status server on `BIND_ADDRESS:STATUS_PORT` and Postgres on `BIND_ADDRESS:PG_PORT` from `deploy.env`, by default `127.0.0.1:3004` and `127.0.0.1:5432`. `install.sh` and `update.sh` restart the pod when these change.
- `skyblock-collector-db` runs Postgres 18. It accepts the collector over the Unix socket in `data/socket`, and only the read-only `reader` login over TCP (`postgres/pg_hba.conf`).
- `skyblock-collector` runs `localhost/skyblock-collector:current`. Its healthcheck restarts it when a task stops reporting.
- `skyblock-collector-update.timer` runs `update.sh` every 5 minutes.
- `skyblock-collector-backup.timer` runs `backup.sh daily` at 03:20 in the host's time zone.
- With a secondary collector, `skyblock-collector-pull.timer` runs `pull-secondary.sh` every 5 minutes, and the secondary runs on `SECONDARY_HOST`, on another internet connection, as `skyblock-collector-secondary.service`. See [The secondary collector](#the-secondary-collector).

## Layout

| Path | Holds |
| --- | --- |
| `data/archive/` | Every raw API response, never deleted. Treat it as the primary record. |
| `data/postgres/` | The Postgres data directory. |
| `data/spool/` | Snapshots waiting for Postgres. `data/spool/failed/` holds jobs that can never be written; the archive still has them. |
| `data/secondary/` | Files pulled from the secondary collector: `inbox/` until they are backfilled, then `archive/`, never deleted. Once pulled, these are the only copy of those responses; `backup.sh`'s archive checksums do not cover them. |
| `backups/` | Restore points, closed partition dumps, and `archive.sha256`. |
| `secrets/` | `db-password` (the `collector` superuser) and `reader-password`. |
| `collector.env` | The collector's settings, including `DATABASE_URL`. |
| `deploy.env` | The host's settings for these scripts: address, ports, secondary host, and the path of a registry credential for a private image. Restore points keep a copy. |
| `repo/` | A clone of the repository at the deployed release. |
| `installed-rev`, `failed-rev` | The deployed release's commit, and the commit of a release whose deploy failed. |
| `pre-trellis-rev` | The revision that ran before the first release; its image is kept as `localhost/skyblock-collector:pre-trellis` until someone removes it. |
| `secondary-rev`, `secondary-failed`, `secondary-bad-rev` | The revision the secondary collector runs, when its last deploy failed, and a revision it was unhealthy on, which is not retried until the next release. |
| `logs/` | `collector.log` and `postgres.log` (container output, 10 MB each) and `operations.log` (these scripts). |

## Check the service

```sh
~/services/skyblock-collector/verify.sh
curl -s http://<BIND_ADDRESS>:<STATUS_PORT>/status | jq
```

`verify.sh` prints `OK` or lists each problem: a stopped unit, a failing healthcheck, a stale feed, failing database writes, a full spool, archive write errors, less than 60 GB free, a failed pull from the secondary collector, or no new file from it for over two hours.

## How backups work

Each run of `backup.sh` does four things:

1. It closes the daily partitions of `auction_items`, `sales`, `bazaar_quick`, `bazaar_books`, and `fetch_log` for finished UTC days. A day stays open while the spool holds a job from it, and until the secondary collector's files from it are backfilled, for at most 3 days. A closed partition is renamed `<table>_closed_<day>` and refuses writes.
2. It dumps each newly closed partition once to `backups/partitions/`.
3. It writes a restore point, `backups/<kind>-<date>-<time>/`. The point holds `database.dump` (the schema, the small tables, and the open partitions), hard links to every closed partition dump, the config and secrets, and `SHA256SUMS`. A day's data is therefore stored once, however many restore points include it.
4. It records a checksum for each new archive file and re-checks every recorded checksum.

Retention keeps the newest daily restore point from each of the last 7 days and the 3 newest `manual` points. Restore points contain secrets.

To make a restore point now:

```sh
~/services/skyblock-collector/backup.sh manual
```

## Restore the database

To restore, pass a restore point:

```sh
~/services/skyblock-collector/restore.sh ~/services/skyblock-collector/backups/daily-YYYY-MM-DD-YYYYMMDDTHHMMSSZ
```

`restore.sh` verifies the checksums, then builds the restored copy in a new database while the current one keeps running. Then it replays the archive from the day before the restore point into that copy, so data collected after the backup returns too, while the collector keeps writing to the current database. History therefore goes in oldest first. Replay runs in passes: each restarts the collector, which completes its open archive files in a few seconds, and replays the files completed since the last pass. Once a pass takes under a minute, the script stops the collector, replays the last files, swaps the database names, and starts it again, even when a step fails. The replaced database stays as `skyblock_before_restore_<time>`. The script prints the command that drops it.

## Rebuild the database from the archive

The archive rebuilds the database on its own, for example when every restore point is lost:

```sh
~/services/skyblock-collector/restore.sh --rebuild
```

It creates an empty database, replays every archive file into it in the same passes as a restore, and swaps it in. A restore or rebuild also backfills from the secondary collector's files in `data/secondary/` before the swap.

The schema in `schema/0001_schema.sql` is fixed once a database is created from it: the collector refuses to write to a database built from another version of the file, and `update.sh` then rolls the update back. To deploy a schema change, release it, then pass the release:

```sh
~/services/skyblock-collector/restore.sh --rebuild v<version>
```

It pulls that release's image before it stops anything, and the swapped-in database starts on it. A release commit works too. The one unreleased revision it takes is the one in `pre-trellis-rev`, and only while its image `localhost/skyblock-collector:pre-trellis` is kept. Once that image is removed, `restore.sh` refuses the revision before it locks, creates or stops anything. Any other unreleased revision has no image, and `restore.sh` refuses it. The next `update.sh` run then installs the release's deploy files and records it as installed, even if the update timer had already tried that revision against the old database and marked it failed. Restore points taken before a schema change hold the old schema, so `restore.sh` refuses them; restore a newer point or rebuild.

## Write to a closed day

A closed partition refuses writes, so replaying archive files of a closed day rejects those rows. To write them anyway, reopen the partition first. The next backup closes and dumps it again:

```sql
SELECT reopen_partition('sales_closed_20261113');
```

## The secondary collector

`auctions_ended` covers only the last 60 seconds, so when this host loses its internet connection, sales from that time are gone. The secondary collector covers that: it runs `skyblock-collector secondary` on `SECONDARY_HOST`, a host on another internet connection. It polls sales, the bazaar, page 0 of the auctions, and the election on the same schedule as the main collector. During an election, Hypixel updates the vote counts every 5 minutes, and a missed update is gone too. It has no database and writes an hourly archive in `~/services/skyblock-collector-secondary/data/archive/` there, using about 100 MB of memory.

Every 5 minutes, `pull-secondary.sh` here copies the finished hourly files over SSH. Each file is deleted on the secondary host only once it is safely copied here, so while this host is down, the secondary keeps everything; its disk holds weeks. Then it runs `skyblock-collector backfill` on the copied files, which writes only the sales, bazaar, page-0, and election snapshots the database does not have, and moves the files to `data/secondary/archive/`. A gap is therefore filled within about an hour of the outage ending.

Both collectors read the same Cloudflare copy, so the secondary cannot recover snapshots Cloudflare itself skipped, only those this host failed to fetch.

To check it:

```sh
ssh <SECONDARY_HOST> systemctl --user status skyblock-collector-secondary
ssh <SECONDARY_HOST> ls -R services/skyblock-collector-secondary/data/archive
journalctl --user -u skyblock-collector-pull.service -n 20
```

`update.sh` deploys it after each main deploy: `secondary.sh` pulls the release's image for `SECONDARY_ARCH` here, copies it over, and restarts the service there. Each deploy that passes its health check becomes the known-good one (the image tag `:good` and the unit saved as `skyblock-collector-secondary.container.good` there), and an unhealthy deploy puts the known-good image and unit back. A failed secondary deploy leaves the main collector running and is retried within the hour, except a revision the secondary was unhealthy on, which waits for the next release.

A day closed before its secondary files arrived refuses them; the backfill counts them as rejected and the files stay in `data/secondary/archive/`. To write them, reopen the day's partitions (see [Write to a closed day](#write-to-a-closed-day)), then run:

```sh
source ~/services/skyblock-collector/common.sh
collector_cmd backfill /data/secondary/archive/<feed>/<day>/<file>...
```

## Updates

`update.sh` deploys the newest release, the highest `v*` tag, when its commit differs from `installed-rev`:

1. It pulls the release's x86-64 image from `ghcr.io/andvari-labs/skyblock-collector`. The release workflow built and pushed that image; CI built and ran the same source. If the image isn't pushed yet, the next run tries again. Once 6 hours have passed since this host first missed it, the release is marked failed in `failed-rev`.
2. It installs the new deploy files, tags the image `current`, and restarts the collector. Postgres restarts only when its unit file changed.
3. If the collector is not healthy within 5 minutes, it puts back the previous image and files and records the revision in `failed-rev`. That release is skipped until a newer one.
4. It deploys the secondary collector, as described above.

To deploy again after a failure, run `update.sh --force`. To turn updates off, and back on, see [Go back to the image from before the first release](#go-back-to-the-image-from-before-the-first-release); the timers are ordinary unit files under `~/.config/systemd/user/`, so `systemctl --user disable` and `enable` work on them.

The image is public, so the pulls need no credential, and they use an empty auth file so a credential another service keeps in podman's default auth file can't get in the way. For a private image, set `REGISTRY_AUTHFILE` in `deploy.env` to a podman auth file with read access to it, which the pulls read in place:

```sh
podman login --authfile <file> ghcr.io
```

When the registry refuses a pull, `update.sh` and `install.sh` fail before they change anything that runs and say what to do; the running collector is left alone. Dropping the `REGISTRY_AUTHFILE` line goes back to pulls without a credential at the next run, with no restart.

## Set up from scratch

To set up the deployment, or to repair it, write `~/services/skyblock-collector/deploy.env` from `deploy.env.example`, then run the installer from a checkout of a release tag:

```sh
~/src/skyblock-collector/crates/skyblock_collector/deploy/install.sh
```

The installer keeps existing data, secrets, and settings. When it replaces a collector that runs, it keeps the files and image that ran and puts them back if the new release isn't healthy within 5 minutes, marking the release in `failed-rev`. It creates the directories and secrets, installs these files, pulls the release's image, starts the pod, sets the `reader` password and its idle-transaction timeout, enables the timers, and deploys the secondary collector. It logs the settings it read from `deploy.env` first. The secondary host needs Podman, rsync, lingering for its user, and SSH access from this host without a password.

## Test a change to the kit

Test a change in an install of its own, never in this one. Write a settings file that gives the
test install other names, ports and folders, and point the installer at it:

```sh
cat > /tmp/kittest.env <<'ENV'
NAME=skc-kittest
STATUS_PORT=43004
PG_PORT=45432
ENABLE_TIMERS=0
ENV
mkdir -p ~/services/skc-kittest/secrets
head -c 32 /dev/urandom | sha256sum | cut -c1-48 > ~/services/skc-kittest/secrets/db-password
printf 'DATABASE_URL=postgres://collector:%s@localhost/skyblock?host=/run/postgresql\nHYPIXEL_API_BASE=http://127.0.0.1:9/v2\n' \
    "$(cat ~/services/skc-kittest/secrets/db-password)" > ~/services/skc-kittest/collector.env
SKC_DEPLOY_ENV=/tmp/kittest.env ~/src/skyblock-collector/crates/skyblock_collector/deploy/install.sh
```

Writing `collector.env` before the installer runs keeps the test collector from asking Hypixel for
anything, since the installer starts it. The installer keeps the settings file as
`~/services/skc-kittest/deploy.env`, and every script there reads it. A `SKC_DEPLOY_ENV` that
names a missing file stops every script rather than falling back to the deployment. The units are
written with the test install's names and ports. `REMOTE`, `REGISTRY_IMAGE`, `SECONDARY_HOST` and
`SECONDARY_ARCH` can point it at a test repository, a local image source such as an `oci:` layout,
and a secondary host. Give its `collector.env` a `HYPIXEL_API_BASE` that refuses connections or
replays recorded responses. The secondary's unit takes no settings file, so for a test install with
a secondary, test releases whose `secondary` unit adds that `Environment=` line; otherwise the test
secondary polls Hypixel.
Remove a test install by its names: stop its units, delete its quadlets, units and folder, and
untag its images, each by an explicit `skc-kittest` name.

## Go back to the image from before the first release

The first release that replaces the build from before releases keeps that image as
`localhost/skyblock-collector:pre-trellis`. It runs under the units of any release. To run it again,
for example when a release misbehaves in a way the health check misses:

```sh
podman tag localhost/skyblock-collector:pre-trellis localhost/skyblock-collector:current
systemctl --user restart skyblock-collector.service
```

The update timer leaves it running until the next release, which it then deploys as usual. To stay on it, turn updates off: `systemctl --user disable --now skyblock-collector-update.timer`. Turn them back on with `systemctl --user enable --now skyblock-collector-update.timer`. `install.sh` turns the timers on again unless `deploy.env` sets `ENABLE_TIMERS=0`.

All of this works only while `:pre-trellis` is kept. Once it is removed, neither these commands
nor `restore.sh --rebuild` with the revision in `pre-trellis-rev` can bring that build back. The
release before the current one stays as `localhost/skyblock-collector:previous`, and runs the same
way in its place.
