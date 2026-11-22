# What it collects

Read this page to know which data skyblock-collector records, how often, and how it keeps every response so that no single failure loses data.

## Feeds

| Feed | Endpoint | How often | Stored as |
| --- | --- | --- | --- |
| Ended auctions (sales) | `skyblock/auctions_ended` | Every snapshot (60 s) | `sales` |
| Changed listings | `skyblock/auctions` page 0, and more pages when page 0 is all new | Every snapshot (60 s) | `auctions`, `auction_items`, `bids` |
| Every listing | `skyblock/auctions`, all pages | Every 5 minutes | `auctions`, `auction_sweeps` |
| Bazaar | `skyblock/bazaar` | Every snapshot (20 s) | `bazaar_quick`, `bazaar_books` |
| Mayor and election | `resources/skyblock/election` | Every minute, kept when it changes | `mayor_terms`, `election_votes`, `election_snapshots` |
| Item metadata, collections, fire sales | `resources/skyblock/items`, `resources/skyblock/collections`, `skyblock/firesales` | Hourly, or 5 minutes for fire sales | `resource_snapshots` |

`auctions_ended` only covers the last 60 seconds, and no public source has the full history, so a sale the collector misses is gone. The collector polls each feed just after Hypixel publishes it, and `feed_gaps` lists every snapshot it missed. Hypixel sometimes answers with an empty body, and Cloudflare then serves that answer for the rest of the snapshot. When that happens to the sales, bazaar, or new-listing feeds, the collector asks the unversioned API (`api.hypixel.net/skyblock/...`) once, which Cloudflare caches separately. `fetch_log` records the URL of every request, so you can see when the fallback was used. A second copy of the collector on another network archives the sales, bazaar, page 0, and the election too, and the main collector fills its own gaps from that copy, so an outage at one site does not lose those feeds.

## How the data is kept

The collector stores each response three times over, so one failure cannot lose data:

1. **The raw archive** keeps every HTTP response byte for byte, error pages included, in `data/archive/<feed>/<day>/*.ndjson.zst`. The collector never deletes from it. Each line is one response with its time, URL, status, headers, and `run`, which names the collector process that fetched it so a replay can tell restarts apart. Consecutive snapshots are almost identical, so zstd's long-distance matching stores a day in about 1 GB. A full auction sweep is about 90 MB, so sweep files use a 1 GiB window to reach the previous sweep; read them with `zstd -dc --long=30`. `skyblock-collector replay` rebuilds the database from it.
2. **The spool** holds each new snapshot on disk until Postgres commits it. A crash or a database outage delays rows but does not lose them.
3. **Postgres** holds the parsed data. Every item's NBT is decoded three ways: the exact gzipped bytes (`item_bytes`), the whole NBT as JSON (`item_nbt`), and `ExtraAttributes` with JSON-in-string values such as `petInfo` parsed (`item_attributes`).

The bazaar refreshes every 20 seconds. `bazaar_quick` gets a price row for every product in every snapshot, about 1 GB a day. Order books are larger and only about 15% of them change in a 20-second step, so `bazaar_books` gets a row for a product only when its book changed, plus a row for every product every 30 minutes. The book at any moment is the newest row at or before it, which `bazaar_book_at` looks up.

Cloudflare caches each auction page separately, so a full read often mixes two snapshots and can miss a listing that moved between pages. A listing is marked gone (`absent_since`) only after two complete sweeps in a row miss it, and is unmarked if it shows up again.

