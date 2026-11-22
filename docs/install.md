# Install skyblock-collector

Read this page to deploy skyblock-collector, or to run its image by hand. It covers the deploy kit,
which runs it on one host with an optional secondary collector on another, the image and its tags,
and every setting and command.

## Deploy with the kit

The kit in [`crates/skyblock_collector/deploy/`](../crates/skyblock_collector/deploy) runs the
collector and its Postgres as a Podman pod under `~/services/skyblock-collector`, the secondary
collector on a second host if you name one, and timers that deploy each new release, pull the
secondary's files, and make a daily restore point.
[Its README](../crates/skyblock_collector/deploy/README.md) covers operations, backups and restores.
The installer copies it into the deployment.

1. Write the host's settings. Copy
   [`deploy.env.example`](../crates/skyblock_collector/deploy/deploy.env.example) to
   `~/services/skyblock-collector/deploy.env` and set the lines you need: the address the status
   server and Postgres listen on, and the SSH host of the secondary collector, if any. Without the
   file, both listen on 127.0.0.1 and no secondary runs.

2. Check out the release to install in a clone of the repository, then run the installer:

   ```sh
   git -C ~/src/skyblock-collector fetch --tags
   git -C ~/src/skyblock-collector checkout v<version>
   ~/src/skyblock-collector/crates/skyblock_collector/deploy/install.sh
   ```

   It logs the settings it read, and keeps existing data, secrets and settings, so it also repairs
   a deployment. When it replaces a collector that runs, it puts back what ran if the release
   doesn't start healthy. The first time it replaces a build from before releases, it keeps that
   image as `localhost/skyblock-collector:pre-trellis`. The secondary host needs Podman, rsync,
   lingering for its user, and SSH access from the first host without a password.

After that, `skyblock-collector-update.timer` deploys each new release within 5 minutes and rolls
back if the new version isn't healthy.

## The image

The image is `ghcr.io/andvari-labs/skyblock-collector`, for Linux on x86-64 and Linux on ARM64.
Docker and Podman pull the one for your machine.

Each release pushes three tags, such as these for 1.4.2:

| Tag | Follows |
| --- | --- |
| `v1.4.2`, the version | That release only |
| `1.4`, the minor version | The latest patch release of 1.4 |
| `latest` | The latest release |

## Run the image by hand

The collector needs a Postgres database, a directory for the archive and one for the spool. Its
database login creates the schema, including a `reader` role, so it must be a superuser, or own
the database and have `CREATEROLE`:

```sh
docker run -d --name skyblock-collector --restart unless-stopped --stop-timeout 30 \
  -e DATABASE_URL=postgres://collector:<password>@<host>/skyblock \
  -v /srv/skyblock/archive:/data/archive -v /srv/skyblock/spool:/data/spool \
  -p 127.0.0.1:8080:8080 \
  ghcr.io/andvari-labs/skyblock-collector:<minor> run
```

A secondary collector needs no database and only the archive directory:

```sh
docker run -d --name skyblock-collector-secondary --restart unless-stopped --stop-timeout 30 \
  -v /srv/skyblock/archive:/data/archive \
  ghcr.io/andvari-labs/skyblock-collector:<minor> secondary
```

- Replace `<minor>` with the latest release's minor version, such as `1.4`, so updates within it
  arrive when you pull. To run one release exactly, use its digest, which
  [Verify a release](verifying-releases.md) shows how to find.
- The collector takes up to 20 seconds to stop cleanly, so `--stop-timeout 30` keeps Docker from
  killing it first, which would leave archive files for the next start to recover.
- The image runs as a non-root user, so the directories must be writable by the user you run it
  as. Pass `--user` to match the directories' owner.
- The image has no health check of its own. The kit's units add one that runs `health`.

Podman takes the same arguments. Check the image first as [Verify a release](verifying-releases.md)
shows.

## Commands

| Command | Does |
| --- | --- |
| `run` | Collects continuously into the archive, the spool and Postgres. The default when no command is given. |
| `secondary` | Collects sales, bazaar, new listings and the election into an hourly archive only, with no database. |
| `replay [files]` | Writes archived responses to the database. With no files, it replays the whole archive. |
| `backfill <files>` | Writes only the sales, bazaar, new-listing and election snapshots in these archive files that the database lacks. |
| `recover-archive` | Finishes archive files that an unclean stop left open, then exits. |
| `health` | Exits 0 when the running collector reports healthy. The kit's health checks run it. |

The collector serves its state as JSON on `/status`, and `/health` answers `ok` while every task
reports.

## Settings

Each setting is an environment variable.

| Variable | Default | Sets |
| --- | --- | --- |
| `DATABASE_URL` | none | The Postgres database. `run`, `replay` and `backfill` need it; `secondary` doesn't use it. |
| `ARCHIVE_DIR` | `/data/archive` | The raw archive. The collector never deletes from it. |
| `SPOOL_DIR` | `/data/spool` | Snapshots waiting for Postgres. |
| `SPOOL_MAX_GB` | `50` | The most the spool holds, in GiB, before it refuses the bazaar, listing, sweep and resource snapshots, which then stay only in the archive. Sales and the election are always spooled. |
| `LISTEN_ADDR` | `0.0.0.0:8080` | The address of the status server. |
| `HYPIXEL_API_BASE` | `https://api.hypixel.net/v2` | The API it polls. |
| `SWEEP_INTERVAL_SECS` | `300` | How often it reads every auction page. |
| `ARCHIVE_ZSTD_LEVEL` | `9` | The archive's zstd level. |
| `USER_AGENT` | `skyblock-collector/<version> (andvari-labs)` | The `User-Agent` of its requests. |
| `RUST_LOG` | `info` | The log filter. |

## Update

The kit updates itself. By hand, pull the tag again and recreate the container:

```sh
docker pull ghcr.io/andvari-labs/skyblock-collector:<minor>
docker rm -f skyblock-collector
```

Then run the image again as above. To roll back, run the previous version's tag, such as
`v1.4.1`.
