# Query the data

Read this page to connect to the collected data and find the views to start from. Worked examples follow.

## Connect

With the deploy kit, Postgres listens on the address and port the deployment's `deploy.env` sets,
`BIND_ADDRESS` and `PG_PORT`, by default 127.0.0.1:5432. Connect with the read-only `reader` login:

```sh
psql "postgres://reader:$(cat ~/services/skyblock-collector/secrets/reader-password)@<BIND_ADDRESS>:<PG_PORT>/skyblock"
```

In the README's Quick start, Postgres isn't published to the host and `reader` can't log in, so run
queries in its container as `collector`: `docker exec skyblock-db psql -U collector -d skyblock`.

Start from these views:

| View or function | One row per | Use it for |
| --- | --- | --- |
| `sales_enriched` | Sale | Prices paid, with item attributes, the listing, and the mayor in office |
| `listings` | Listing | Asking prices, with reforge, stars, enchantments, pet details, and `item_tag` |
| `auction_outcomes` | Listing | Whether a listing sold, expired, was cancelled, or is still active, or `unknown` when it vanished during a gap in ended auctions |
| `bazaar_prices` | Product per snapshot (20 s) | Bazaar price and volume history |
| `bazaar_orders` | Order-book change | The top 30 orders on each side; `bazaar_book_at(product, at)` gives the book at a moment |
| `mayor_terms` | Election year | Who was in office and when (`term_start`, `term_end`), with perks, minister, and final results |
| `mayor_perks` | Perk per term | Each mayor and minister perk, with the description as plain text |
| `election_candidates` | Candidate per election | Who could be voted for, their perks and minister perk, final votes, rank, and whether they won or became minister |
| `election_standings` | Candidate per vote count change | Votes, share, and rank over time in an open election |
| `mayor_snapshots` | Change of mayor, perks, or minister | The mayor and minister exactly as the API showed them, including perk changes within a term |
| `mayor_context(at)` | Moment | The term at a moment, the previous and next mayor, whether voting is open, and the leading candidate then |
| `feed_gaps` | Missed stretch | Times the collector has no data for |

## Examples

Each query runs as the read-only `reader` login. Times are `timestamptz`; add `AT TIME ZONE '<zone>'`, such as `'Europe/Berlin'`, to read them in local time.

### Prices paid for an item

Median and count of BIN sales per day for Hyperion, by star count:

```sql
SELECT date_trunc('day', sold_at) AS day, stars,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
       count(*) AS sales
FROM sales_enriched
WHERE item_id = 'HYPERION' AND bin
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Listings far below recent sales

BIN listings that are still active and priced under 60% of the item tag's median sale over the last 3 days:

```sql
WITH recent AS (
    SELECT item_tag, percentile_cont(0.5) WITHIN GROUP (ORDER BY price_each) AS median, count(*) AS n
    FROM sales_enriched
    WHERE bin AND sold_at > now() - interval '3 days'
    GROUP BY item_tag
    HAVING count(*) >= 10
)
SELECT l.uuid, l.item_tag, l.item_name, l.starting_bid, r.median, r.n
FROM listings l
JOIN recent r USING (item_tag)
JOIN auction_outcomes o USING (uuid)
WHERE l.bin AND o.outcome = 'active' AND l.starting_bid < 0.6 * r.median
ORDER BY l.starting_bid / r.median;
```

### Prices by mayor

Average sale price of an item under each mayor, and how far into the term it sold:

```sql
SELECT mayor_name, mayor_election_year,
       avg(price) AS average_price, count(*) AS sales,
       avg(extract(epoch FROM into_term) / 3600) AS average_hours_into_term
FROM sales_enriched
WHERE item_tag = 'PET_ENDER_DRAGON' AND bin
GROUP BY 1, 2
ORDER BY 2;
```

Prices in the last 12 hours before a term ends compared with the rest of the term:

```sql
SELECT item_tag, until_term_end < interval '12 hours' AS final_hours,
       avg(price_each) AS average_price, count(*) AS sales
FROM sales_enriched
WHERE item_tag = 'SUMMONING_EYE' AND bin
GROUP BY 1, 2;
```

### Prices across the mayor cycle

The hourly median of an item, with the mayor in office, the vote leader, and whether voting was open. Prices often move once a mayor starts to lead the vote, again when they take office, and again after they leave:

```sql
WITH hourly AS (
    SELECT date_trunc('hour', sold_at) AS hour,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY price_each) AS median_price, count(*) AS sales
    FROM sales_enriched
    WHERE item_tag = 'SUMMONING_EYE' AND bin
    GROUP BY 1
)
SELECT d.hour, d.median_price, d.sales, c.mayor_name, c.previous_mayor_name,
       c.voting_open, c.leading_candidate_name, round(c.leading_share, 3) AS leading_share, c.next_mayor_name
FROM hourly d
CROSS JOIN LATERAL mayor_context(d.hour) c
ORDER BY d.hour;
```

The same works for bazaar prices. Hourly instant-buy price of a product with the mayor cycle:

```sql
SELECT h.hour, h.buy_price, c.mayor_name, c.voting_open, c.leading_candidate_name
FROM (
    SELECT date_trunc('hour', api_last_updated) AS hour, avg(buy_price) AS buy_price
    FROM bazaar_prices
    WHERE product_id = 'ENCHANTED_DIAMOND'
    GROUP BY 1
) h
CROSS JOIN LATERAL mayor_context(h.hour) c
ORDER BY h.hour;
```

Average price by hours around each term start, to see an announcement effect. Negative hours are the end of the previous term, counted toward the incoming mayor:

```sql
SELECT CASE WHEN into_term < until_term_end THEN floor(extract(epoch FROM into_term) / 3600)
            ELSE -ceil(extract(epoch FROM until_term_end) / 3600) END AS hours_from_term_start,
       CASE WHEN into_term < until_term_end THEN mayor_name ELSE next_mayor_name END AS incoming_mayor,
       avg(price_each) AS average_price, count(*) AS sales
FROM sales_enriched
WHERE item_tag = 'SUMMONING_EYE' AND bin
GROUP BY 1, 2
ORDER BY 2, 1;
```

Perks of each term, as plain text:

```sql
SELECT election_year, role, holder_name, perk_name, description FROM mayor_perks ORDER BY election_year DESC, role;
```

### The running election

Vote share and rank of each candidate over time:

```sql
SELECT observed_at, candidate_name, votes, round(share, 3) AS share, rank
FROM election_standings
WHERE election_year = (SELECT max(election_year) FROM election_votes)
ORDER BY observed_at, rank;
```

Every candidate of an election, with the perk each would bring as minister and the outcome:

```sql
SELECT candidate_name, minister_perk->>'name' AS minister_perk, final_votes, round(final_share, 3) AS final_share,
       final_rank, won, became_minister, voting_opens_at, voting_closes_at
FROM election_candidates
WHERE election_year = 524
ORDER BY final_rank;
```

### Bazaar history and manipulation

Every 20-second snapshot of Enchanted Diamond's price over the last day:

```sql
SELECT api_last_updated, buy_price, sell_price, buy_volume, sell_volume
FROM bazaar_prices
WHERE product_id = 'ENCHANTED_DIAMOND' AND api_last_updated > now() - interval '1 day'
ORDER BY api_last_updated;
```

Products whose instant-buy price moved more than 20% between two consecutive snapshots:

```sql
SELECT product_id, api_last_updated, buy_price, previous
FROM (
    SELECT product_id, api_last_updated, buy_price,
           lag(buy_price) OVER (PARTITION BY product_id ORDER BY api_last_updated) AS previous
    FROM bazaar_prices
    WHERE api_last_updated > now() - interval '1 day'
) changes
WHERE previous > 0 AND abs(buy_price / previous - 1) > 0.2
ORDER BY api_last_updated DESC;
```

The cheapest sell offer and its size at each order-book change, to spot walls:

```sql
SELECT api_last_updated,
       (buy_summary->0->>'pricePerUnit')::numeric AS cheapest_offer,
       (buy_summary->0->>'amount')::bigint AS amount
FROM bazaar_orders
WHERE product_id = 'BOOSTER_COOKIE'
ORDER BY api_last_updated DESC
LIMIT 100;
```

### Item details

Every attribute of one listed item, and the fields of a pet:

```sql
SELECT jsonb_pretty(item_attributes) FROM listings WHERE uuid = '<listing uuid>';
SELECT pet_type, pet_tier, pet_exp, pet_held_item, pet_candy_used FROM listings WHERE pet_type IS NOT NULL LIMIT 20;
```

The archive keeps the full raw responses. DuckDB reads them directly, except `auctions_sweep` files, whose 1 GiB window needs `zstd -dc --long=30 <file>` first:

```sql
SELECT fetched_at, status, body.lastUpdated
FROM read_json('~/services/skyblock-collector/data/archive/auctions_ended/2026-11-13/*.ndjson.zst');
```

### Coverage

Snapshots the collector missed, and the requests that failed:

```sql
SELECT * FROM feed_gaps ORDER BY gap_after DESC;
SELECT feed, http_status, error, count(*) FROM fetch_log WHERE started_at > now() - interval '1 day' GROUP BY 1, 2, 3;
```
