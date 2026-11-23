# skyblock-collector

[![CI](https://github.com/andvari-labs/skyblock-collector/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/andvari-labs/skyblock-collector/actions/workflows/ci.yml?query=branch%3Amain)
[![Latest release](https://img.shields.io/github/v/release/andvari-labs/skyblock-collector)](https://github.com/andvari-labs/skyblock-collector/releases/latest)

skyblock-collector is a background service that records Hypixel SkyBlock's economy as it happens:
every auction sale it sees, the auction listings, the bazaar every 20 seconds, and the mayor
elections. It is for someone who wants to study prices over months, back a tool of their own with
that history, or answer "what did this sell for" long after the game forgot. It has no user
interface: you read the data with SQL.

Hypixel's API only shows the present. Its ended-auctions feed covers the last 60 seconds, so a sale
nobody fetched in that minute is gone for good, and price-history websites give you their
aggregates, not the sales. A script that polls the API and keeps a running average loses every
detail it didn't think to keep. This collector fetches each feed just after Hypixel publishes it,
keeps every response byte for byte in a compressed archive it never deletes from, and writes the
parsed data, each item's attributes decoded, to Postgres. A question you think of next year can be
answered from today's data, and the database can be rebuilt from the archive. An optional second
copy on another internet connection covers outages at the first.

Its limits: history starts when you start it, since nothing earlier can be fetched. It reads only
SkyBlock's public endpoints, which need no API key, so it has no player profiles, stats or
inventories; the sales and listings it stores carry the player IDs the API publishes with them.
It keeps everything, so its disk use only grows: the archive by about 1 GB a day, and Postgres
by more, its bazaar prices alone by about 1 GB a day.

- **Every feed, on Hypixel's schedule.** Sales and changed listings every 60 seconds, every listing
  page every 5 minutes, the bazaar every 20 seconds, the election every minute, fire sales every
  5 minutes, and item and collection data hourly. The `feed_gaps` view lists any stretch it missed.
  See [What it collects](docs/collecting.md).
- **A raw archive.** Every response, error pages included, in daily zstd files.
  `skyblock-collector replay` rebuilds the database from it. See
  [How the data is kept](docs/collecting.md#how-the-data-is-kept).
- **Postgres with views to start from.** Sales with their item's stars, reforge, enchantments and
  pet details and the mayor in office; listings and how each ended; bazaar prices and order books;
  terms and vote counts. See [Query the data](docs/queries.md).
- **An optional secondary collector.** A second copy on another connection archives the sales, the
  bazaar, new listings and the election, and the deploy kit writes what the main database missed.
  See [The secondary collector](crates/skyblock_collector/deploy/README.md#the-secondary-collector).
- **A status server.** `/health` for checks, and `/status` with each feed's last snapshot, the
  database, the spool and archive errors. See [Commands](docs/install.md#commands).
- **A deploy kit.** Runs it in a rootless Podman pod with its Postgres, deploys each new release
  and rolls back if it isn't healthy, and makes a daily restore point. See
  [Run it on a server](#run-it-on-a-server).

What half an hour of sales looks like: the median price paid for each item at each star level,
from a test instance that a stand-in for the API fed invented sales. [Query the data](docs/queries.md#examples)
has more queries to start from.

```console
$ psql -d skyblock -c "SELECT item_id, stars, count(*) AS sales,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY price)::bigint AS median_price
    FROM sales_enriched WHERE bin GROUP BY 1, 2 ORDER BY 1, 2"
      item_id      | stars | sales | median_price
-------------------+-------+-------+--------------
 ASPECT_OF_THE_END |       |    30 |       162500
 GRAPPLING_HOOK    |       |    37 |      1188000
 HYPERION          |     3 |     9 |    982230000
 HYPERION          |     5 |     7 |   1047351000
 HYPERION          |       |     2 |    915402500
 JUJU_SHORTBOW     |     3 |     5 |     10001000
 JUJU_SHORTBOW     |     5 |    11 |     11910000
 JUJU_SHORTBOW     |       |     5 |      9351000
 NECRON_HELMET     |     3 |     8 |     47486000
 NECRON_HELMET     |     5 |    15 |     49425000
 NECRON_HELMET     |       |     8 |     40700000
 TERMINATOR        |     3 |     7 |    962005000
 TERMINATOR        |     5 |     9 |    925416000
 TERMINATOR        |       |    10 |    819669000
(14 rows)
```

## Quick start

This runs the collector and Postgres in containers on one machine and takes a few minutes. You need
Docker or Podman on Linux or macOS, and `curl` and `jq`. The image is public, built for x86-64 and
ARM64, and needs no Hypixel API key.

1. Start Postgres on a network of its own:

   ```sh
   docker network create skyblock
   docker run -d --name skyblock-db --network skyblock --restart unless-stopped \
     -e POSTGRES_USER=collector -e POSTGRES_PASSWORD=change-me -e POSTGRES_DB=skyblock \
     -v skyblock-db:/var/lib/postgresql docker.io/library/postgres:18
   ```

2. Make folders for the archive and the spool, and start the collector. It runs as your user so it
   can write to them:

   ```sh
   mkdir -p ~/skyblock/archive ~/skyblock/spool
   docker run -d --name skyblock-collector --network skyblock --restart unless-stopped \
     --stop-timeout 30 --user "$(id -u):$(id -g)" \
     -e DATABASE_URL=postgres://collector:change-me@skyblock-db/skyblock \
     -v ~/skyblock/archive:/data/archive -v ~/skyblock/spool:/data/spool \
     -p 127.0.0.1:8080:8080 \
     ghcr.io/andvari-labs/skyblock-collector:0.2 run
   ```

   With rootless Docker, use `--user 0:0` instead: root in a rootless container is your own user.
   With Podman, add `--userns=keep-id` so your user owns what it writes. Container names such as
   `skyblock-db` resolve only with Podman's `aardvark-dns`, which most Podman packages install.

3. Check it. `/health` answers `ok` while every task reports, and after a minute or two `/status`
   shows each feed's newest snapshot:

   ```sh
   curl -s http://127.0.0.1:8080/health
   curl -s http://127.0.0.1:8080/status | jq '.ingest.database_ok, (.feeds | map_values(.last_new_snapshot_at))'
   ```

4. Query it. Sales arrive every minute, and the other examples in
   [Query the data](docs/queries.md#examples) run the same way:

   ```sh
   docker exec skyblock-db psql -U collector -d skyblock -c 'SELECT count(*) FROM sales'
   ```

Each poll is a few requests to Hypixel's public API, and every 5 minutes it reads every listing
page, about 40 on a typical day, 8 at a time. Podman doesn't start these containers again after a
reboot unless `podman-restart.service` is enabled. To remove them, run
`docker rm -f skyblock-collector skyblock-db` and `docker network rm skyblock`; the data stays in
the `skyblock-db` volume and `~/skyblock`. [Install](docs/install.md) has every setting and command,
the secondary collector, and updates.

## Run it on a server

The deploy kit in [`crates/skyblock_collector/deploy/`](crates/skyblock_collector/deploy) runs the
collector and Postgres 18 as a rootless Podman pod under systemd. It checks for a new release every
5 minutes and rolls back if it isn't healthy, makes a daily restore point, and runs the optional
secondary collector on a second host over SSH. It needs:

- An x86-64 Linux host with rootless Podman 5, systemd lingering for your user, `git`, `jq`,
  `curl`, `zstd`, and `rsync` for a secondary collector, with memory to spare for Postgres, whose
  unit sets `shared_buffers` to 4 GB.
- For a secondary collector, a second host with rootless Podman, lingering, `rsync`, and SSH from
  the first without a password. It may be x86-64 or ARM64.

```sh
git clone https://github.com/andvari-labs/skyblock-collector.git ~/src/skyblock-collector
git -C ~/src/skyblock-collector checkout v<version>
mkdir -p ~/services/skyblock-collector
cp ~/src/skyblock-collector/crates/skyblock_collector/deploy/deploy.env.example ~/services/skyblock-collector/deploy.env
~/src/skyblock-collector/crates/skyblock_collector/deploy/install.sh
```

Edit `deploy.env` before the last command to publish the status server and Postgres on another
address than 127.0.0.1, where they listen on ports 3004 and 5432, or to name the secondary
collector's host. The kit's [README](crates/skyblock_collector/deploy/README.md) covers checks,
backups, restores and the secondary collector.

## Documentation

- [Install](docs/install.md): the image's tags, running it by hand, every command and setting, and the deploy kit.
- [What it collects](docs/collecting.md): the feeds, how often each is polled, and how every response is kept.
- [Query the data](docs/queries.md): connect to Postgres, the views to start from, and worked examples.
- [The deploy kit](crates/skyblock_collector/deploy/README.md): what runs, backups, restores, the secondary collector, and updates.
- [Verify a release](docs/verifying-releases.md): check that an image came from this repository.
- [Design](docs/design.md): the goals, the architecture, and the decisions behind them.
- [Stability](docs/stability.md): what a release can change, and what to read before you upgrade.
- [Code style](docs/style.md): the style rules a tool checks, and the review checklist for the rest.
- [Contributing](CONTRIBUTING.md): set up, run the checks, and send a change.
- [Maintain](docs/maintaining.md): the owner's repository settings, releases and trellis updates.
- [Security policy](SECURITY.md): how to report a vulnerability.
- [Changelog](CHANGELOG.md): what changed in each release.

## License

Released under either the [MIT License](LICENSE-MIT) or the [Apache License 2.0](LICENSE-APACHE), at
your option.

The image holds the licenses of the crates it is built with in
`/usr/share/doc/skyblock-collector/THIRD-PARTY-LICENSES.html`.

skyblock-collector isn't affiliated with or endorsed by Hypixel.
