-- Coin amounts in the auction API are whole numbers (bigint). Bazaar prices are fractional.
-- Every timestamp is timestamptz. Hypixel sends milliseconds, which timestamptz holds exactly.
--
-- The large append-only tables are partitioned by UTC day. After a day ends, backup.sh closes its
-- partitions (a trigger then refuses writes) and dumps each closed partition once. See
-- deploy/README.md.

-- Read-only login for analysis. install.sh sets its password.
DO $$
BEGIN
    CREATE ROLE reader NOLOGIN;
-- Roles are cluster-wide; two databases migrating at once race to create it.
EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL;
END
$$;
GRANT USAGE ON SCHEMA public TO reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO reader;

-- Daily partitions ---------------------------------------------------------------------------

CREATE TABLE partition_days (
    parent         text        NOT NULL,
    day            date        NOT NULL,
    partition_name text        NOT NULL UNIQUE,
    created_at     timestamptz NOT NULL DEFAULT now(),
    closed_at      timestamptz,
    PRIMARY KEY (parent, day)
);
COMMENT ON TABLE partition_days IS 'Every daily partition. A closed partition refuses writes and has been, or will be, dumped once by backup.sh.';

CREATE FUNCTION partitioned_tables() RETURNS text[]
LANGUAGE sql IMMUTABLE
RETURN ARRAY['auction_items', 'sales', 'bazaar_quick', 'bazaar_books', 'fetch_log'];

CREATE FUNCTION deny_write_to_closed_partition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'partition % is closed; run SELECT reopen_partition(''%'') first', TG_TABLE_NAME, TG_TABLE_NAME
        USING ERRCODE = 'read_only_sql_transaction';
END
$$;

-- Creates any missing daily partitions from first_day through last_day. A day whose rows already
-- landed in the default partition is skipped, because attaching it would fail.
CREATE FUNCTION ensure_partitions(first_day date, last_day date) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    table_name text;
    d date;
    name text;
    created integer := 0;
    lower_bound timestamptz;
    upper_bound timestamptz;
    blocked boolean;
BEGIN
    FOREACH table_name IN ARRAY partitioned_tables() LOOP
        d := first_day;
        WHILE d <= last_day LOOP
            IF NOT EXISTS (SELECT 1 FROM partition_days p WHERE p.parent = table_name AND p.day = d) THEN
                name := format('%s_p%s', table_name, to_char(d, 'YYYYMMDD'));
                lower_bound := d::timestamp AT TIME ZONE 'UTC';
                upper_bound := (d + 1)::timestamp AT TIME ZONE 'UTC';
                EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I WHERE %I >= $1 AND %I < $2)',
                               table_name || '_default', partition_column(table_name), partition_column(table_name))
                    INTO blocked USING lower_bound, upper_bound;
                IF blocked THEN
                    RAISE WARNING 'rows for % on % are in %_default; not creating its partition', table_name, d, table_name;
                ELSE
                    EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                                   name, table_name, lower_bound, upper_bound);
                    INSERT INTO partition_days (parent, day, partition_name) VALUES (table_name, d, name);
                    created := created + 1;
                END IF;
            END IF;
            d := d + 1;
        END LOOP;
    END LOOP;
    RETURN created;
END
$$;

CREATE FUNCTION partition_column(parent text) RETURNS text
LANGUAGE sql IMMUTABLE
RETURN CASE parent
    WHEN 'auction_items' THEN 'first_seen_at'
    WHEN 'sales' THEN 'sold_at'
    WHEN 'fetch_log' THEN 'started_at'
    ELSE 'api_last_updated'
END;

-- Closes every open partition for days before before_day: renames it to <parent>_closed_<day> so
-- dumps can exclude it by name, and adds the write-refusing trigger. Returns the closed names.
CREATE FUNCTION close_partitions(before_day date) RETURNS SETOF text
LANGUAGE plpgsql AS $$
DECLARE
    p record;
    closed_name text;
BEGIN
    FOR p IN SELECT * FROM partition_days WHERE closed_at IS NULL AND day < before_day ORDER BY day, parent FOR UPDATE LOOP
        closed_name := format('%s_closed_%s', p.parent, to_char(p.day, 'YYYYMMDD'));
        EXECUTE format('ALTER TABLE %I RENAME TO %I', p.partition_name, closed_name);
        EXECUTE format('CREATE TRIGGER deny_writes BEFORE INSERT OR UPDATE OR DELETE ON %I
                        FOR EACH ROW EXECUTE FUNCTION deny_write_to_closed_partition()', closed_name);
        EXECUTE format('CREATE TRIGGER deny_truncate BEFORE TRUNCATE ON %I
                        FOR EACH STATEMENT EXECUTE FUNCTION deny_write_to_closed_partition()', closed_name);
        UPDATE partition_days SET partition_name = closed_name, closed_at = now()
        WHERE parent = p.parent AND day = p.day;
        RETURN NEXT closed_name;
    END LOOP;
END
$$;

-- Makes a closed partition writable again, for example to replay archived data into it. The next
-- backup closes and dumps it again.
CREATE FUNCTION reopen_partition(closed_name text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    p record;
    open_name text;
BEGIN
    SELECT * INTO p FROM partition_days WHERE partition_name = closed_name AND closed_at IS NOT NULL FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no closed partition named %', closed_name;
    END IF;
    open_name := format('%s_p%s', p.parent, to_char(p.day, 'YYYYMMDD'));
    EXECUTE format('DROP TRIGGER deny_writes ON %I', closed_name);
    EXECUTE format('DROP TRIGGER deny_truncate ON %I', closed_name);
    EXECUTE format('ALTER TABLE %I RENAME TO %I', closed_name, open_name);
    UPDATE partition_days SET partition_name = open_name, closed_at = NULL
    WHERE parent = p.parent AND day = p.day;
    RETURN open_name;
END
$$;

-- Collection bookkeeping --------------------------------------------------------------------

CREATE TABLE fetch_log (
    started_at       timestamptz NOT NULL,
    feed             text        NOT NULL,
    url              text        NOT NULL,
    duration_ms      integer     NOT NULL,
    http_status      smallint,
    body_bytes       integer,
    api_last_updated timestamptz,
    cache_age_s      integer,
    error            text
) PARTITION BY RANGE (started_at);
CREATE TABLE fetch_log_default PARTITION OF fetch_log DEFAULT;
CREATE INDEX fetch_log_feed_started ON fetch_log (feed, started_at);
COMMENT ON TABLE fetch_log IS 'Every HTTP request the collector made. http_status is null when no response arrived.';

CREATE TABLE feed_snapshots (
    feed             text        NOT NULL,
    api_last_updated timestamptz NOT NULL,
    fetched_at       timestamptz NOT NULL,
    row_count        integer     NOT NULL,
    PRIMARY KEY (feed, api_last_updated)
);
COMMENT ON TABLE feed_snapshots IS 'One row per distinct API snapshot written to the database. Gaps between rows are snapshots the collector missed; see feed_gaps.';

-- Auction house -----------------------------------------------------------------------------

CREATE TABLE auctions (
    uuid                 uuid PRIMARY KEY,
    seller               uuid,
    seller_profile       uuid,
    coop                 uuid[]      NOT NULL DEFAULT '{}',
    started_at           timestamptz,
    ends_at              timestamptz,
    item_name            text,
    search_text          text,
    category             text,
    categories           text[]      NOT NULL DEFAULT '{}',
    tier                 text,
    bin                  boolean     NOT NULL,
    starting_bid         bigint,
    highest_bid          bigint,
    bid_count            integer     NOT NULL DEFAULT 0,
    claimed              boolean,
    claimed_bidders      uuid[]      NOT NULL DEFAULT '{}',
    auction_last_updated timestamptz,
    item_id              text,
    item_uuid            text,
    item_count           integer,
    api_unknown          jsonb,
    first_seen_at        timestamptz NOT NULL,
    last_changed_at      timestamptz NOT NULL,
    missing_since        timestamptz,
    absent_since         timestamptz
);
CREATE INDEX auctions_item_started ON auctions (item_id, started_at);
CREATE INDEX auctions_started ON auctions (started_at);
CREATE INDEX auctions_seller ON auctions (seller);
CREATE INDEX auctions_item_uuid ON auctions (item_uuid);
CREATE INDEX auctions_present ON auctions (uuid) WHERE absent_since IS NULL;
COMMENT ON TABLE auctions IS 'One row per auction listing, with the fields that change while it is listed. The item itself is in auction_items.';
COMMENT ON COLUMN auctions.search_text IS 'The API''s "extra" field: item name, lore keywords, and enchantments as one search string.';
COMMENT ON COLUMN auctions.starting_bid IS 'For BIN listings this is the buy-it-now price.';
COMMENT ON COLUMN auctions.first_seen_at IS 'The API snapshot time when the collector first saw this listing.';
COMMENT ON COLUMN auctions.api_unknown IS 'API fields this schema does not have a column for yet.';
COMMENT ON COLUMN auctions.missing_since IS 'Set when one complete sweep did not list this auction. A sweep can miss a listing that moved between separately cached pages, so this alone does not mean it is gone.';
COMMENT ON COLUMN auctions.absent_since IS 'The auction is gone (sold, expired, or cancelled): two complete sweeps in a row did not list it. This is the time of the first of them.';

CREATE TABLE auction_items (
    uuid              uuid        NOT NULL,
    first_seen_at     timestamptz NOT NULL,
    item_lore         text,
    item_attributes   jsonb,
    item_nbt          jsonb,
    item_bytes        bytea,
    item_decode_error text,
    PRIMARY KEY (uuid, first_seen_at)
) PARTITION BY RANGE (first_seen_at);
CREATE TABLE auction_items_default PARTITION OF auction_items DEFAULT;
COMMENT ON TABLE auction_items IS 'The listed item for each auction, written once when the listing is first seen. Join to auctions on uuid.';
COMMENT ON COLUMN auction_items.item_attributes IS 'ExtraAttributes from the item NBT, with JSON-in-string values such as petInfo parsed.';
COMMENT ON COLUMN auction_items.item_nbt IS 'The whole decoded item NBT as JSON.';
COMMENT ON COLUMN auction_items.item_bytes IS 'The gzipped NBT exactly as received (the API sends it base64-encoded).';

CREATE TABLE bids (
    auction_uuid   uuid        NOT NULL,
    bidder         uuid,
    bidder_profile uuid,
    amount         bigint      NOT NULL,
    bid_at         timestamptz NOT NULL,
    PRIMARY KEY (auction_uuid, bid_at, amount)
);
COMMENT ON TABLE bids IS 'Bids on regular (non-BIN) auctions.';

CREATE TABLE sales (
    auction_uuid      uuid        NOT NULL,
    sold_at           timestamptz NOT NULL,
    seller            uuid,
    seller_profile    uuid,
    buyer             uuid,
    buyer_profile     uuid,
    price             bigint      NOT NULL,
    bin               boolean     NOT NULL,
    item_id           text,
    item_uuid         text,
    item_count        integer,
    item_attributes   jsonb,
    item_nbt          jsonb,
    item_bytes        bytea,
    item_decode_error text,
    api_unknown       jsonb,
    api_last_updated  timestamptz NOT NULL,
    collected_at      timestamptz NOT NULL,
    PRIMARY KEY (auction_uuid, sold_at)
) PARTITION BY RANGE (sold_at);
CREATE TABLE sales_default PARTITION OF sales DEFAULT;
CREATE INDEX sales_item_sold ON sales (item_id, sold_at);
CREATE INDEX sales_sold ON sales (sold_at);
CREATE INDEX sales_auction ON sales (auction_uuid);
CREATE INDEX sales_buyer ON sales (buyer);
CREATE INDEX sales_seller ON sales (seller);
COMMENT ON TABLE sales IS 'Every ended auction from auctions_ended: BIN purchases and won regular auctions.';

CREATE TABLE auction_sweeps (
    api_last_updated timestamptz PRIMARY KEY,
    oldest_page_at   timestamptz NOT NULL,
    fetched_at       timestamptz NOT NULL,
    pages            integer     NOT NULL,
    auction_count    integer     NOT NULL,
    complete         boolean     NOT NULL,
    consistent       boolean     NOT NULL,
    marked_missing   integer     NOT NULL,
    marked_absent    integer     NOT NULL
);
COMMENT ON TABLE auction_sweeps IS 'Full reads of every auction page. complete: every page was read. consistent: every page came from one snapshot, which Cloudflare''s per-page caching often prevents. Only complete sweeps flag listings.';

-- Bazaar ------------------------------------------------------------------------------------
-- The bazaar refreshes about every 20 seconds. Every snapshot stores every product's price. An
-- order book gets a row only when it differs from the product's previous one, and every product
-- gets one in a keyframe snapshot every 30 minutes, so the book at any time is the newest row at
-- or before it.

CREATE TABLE bazaar_products (
    product_key   integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id    text        NOT NULL UNIQUE,
    first_seen_at timestamptz NOT NULL
);

CREATE TABLE bazaar_snapshots (
    api_last_updated timestamptz PRIMARY KEY,
    fetched_at       timestamptz NOT NULL,
    product_count    integer     NOT NULL,
    book_rows        integer     NOT NULL,
    book_keyframe    boolean     NOT NULL
);
COMMENT ON TABLE bazaar_snapshots IS 'Every bazaar snapshot written. In a book keyframe every product has a bazaar_books row.';

CREATE TABLE bazaar_quick (
    product_key      integer     NOT NULL,
    api_last_updated timestamptz NOT NULL,
    buy_price        double precision,
    sell_price       double precision,
    buy_volume       bigint,
    sell_volume      bigint,
    buy_moving_week  bigint,
    sell_moving_week bigint,
    buy_orders       integer,
    sell_orders      integer,
    PRIMARY KEY (product_key, api_last_updated)
) PARTITION BY RANGE (api_last_updated);
CREATE TABLE bazaar_quick_default PARTITION OF bazaar_quick DEFAULT;
COMMENT ON TABLE bazaar_quick IS 'quick_status for every product in every bazaar snapshot (about every 20 seconds).';
COMMENT ON COLUMN bazaar_quick.buy_price IS 'Hypixel''s buyPrice: the cost of an instant buy, from the cheapest sell offers.';
COMMENT ON COLUMN bazaar_quick.sell_price IS 'Hypixel''s sellPrice: the return of an instant sell, from the highest buy orders.';

CREATE TABLE bazaar_books (
    product_key      integer     NOT NULL,
    api_last_updated timestamptz NOT NULL,
    buy_summary      jsonb,
    sell_summary     jsonb,
    PRIMARY KEY (product_key, api_last_updated)
) PARTITION BY RANGE (api_last_updated);
CREATE TABLE bazaar_books_default PARTITION OF bazaar_books DEFAULT;
COMMENT ON TABLE bazaar_books IS 'The top 30 orders on each side for a product whenever they changed, plus every product in keyframe snapshots every 30 minutes.';
COMMENT ON COLUMN bazaar_books.buy_summary IS 'Hypixel''s buy_summary: sell offers, cheapest first, that an instant buy fills from. [{amount, pricePerUnit, orders}].';
COMMENT ON COLUMN bazaar_books.sell_summary IS 'Hypixel''s sell_summary: buy orders, highest first, that an instant sell fills into. [{amount, pricePerUnit, orders}].';

-- Mayors and elections ----------------------------------------------------------------------

CREATE TABLE election_snapshots (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    api_last_updated timestamptz,
    fetched_at       timestamptz NOT NULL,
    payload_sha256   bytea       NOT NULL,
    payload          jsonb       NOT NULL
);
CREATE INDEX election_snapshots_fetched ON election_snapshots (fetched_at);
COMMENT ON TABLE election_snapshots IS 'The election resource each time its content changed.';

CREATE TABLE mayor_terms (
    election_year     integer PRIMARY KEY,
    term_start        timestamptz NOT NULL,
    term_end          timestamptz NOT NULL,
    mayor_key         text,
    mayor_name        text,
    mayor_perks       jsonb,
    minister_key      text,
    minister_name     text,
    minister_perk     jsonb,
    mayor_votes       bigint,
    election_results  jsonb,
    first_observed_at timestamptz NOT NULL,
    last_observed_at  timestamptz NOT NULL
);
COMMENT ON TABLE mayor_terms IS 'Who held office for each election year the collector observed. term_start and term_end come from the SkyBlock calendar.';
COMMENT ON COLUMN mayor_terms.mayor_votes IS 'Votes the mayor won with.';

CREATE TABLE election_votes (
    election_year  integer     NOT NULL,
    candidate_key  text        NOT NULL,
    observed_at    timestamptz NOT NULL,
    candidate_name text,
    votes          bigint,
    perks          jsonb,
    PRIMARY KEY (election_year, candidate_key, observed_at)
);
COMMENT ON TABLE election_votes IS 'Vote counts in the running election each time they changed, to see an upcoming mayor before they take office.';

-- Other resources ---------------------------------------------------------------------------

CREATE TABLE resource_snapshots (
    resource         text        NOT NULL,
    payload_sha256   bytea       NOT NULL,
    api_last_updated timestamptz,
    fetched_at       timestamptz NOT NULL,
    payload          jsonb       NOT NULL,
    PRIMARY KEY (resource, payload_sha256)
);
COMMENT ON TABLE resource_snapshots IS 'Item metadata, collections, and fire sales, each time their content changed.';

-- Views and helpers -------------------------------------------------------------------------

CREATE FUNCTION try_bigint(value text) RETURNS bigint
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
    RETURN value::numeric::bigint;
EXCEPTION WHEN others THEN
    RETURN NULL;
END
$$;

CREATE FUNCTION try_numeric(value text) RETURNS numeric
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
    RETURN value::numeric;
EXCEPTION WHEN others THEN
    RETURN NULL;
END
$$;

-- SkyBlock year 1 began at 2019-06-11 17:55 UTC. A year is 124 real hours (12 months of 31
-- twenty-minute days). Votes are counted on Late Spring 27, 88 days into the year, and the winner
-- of election year Y holds office from then in year Y + 1 until the same day in year Y + 2.
CREATE FUNCTION skyblock_year(at timestamptz) RETURNS integer
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN floor((extract(epoch FROM at) * 1000 - 1560275700000) / 446400000)::integer + 1;

CREATE FUNCTION mayor_term_start(election_year integer) RETURNS timestamptz
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN to_timestamp((1560275700000 + election_year::bigint * 446400000 + 105600000) / 1000.0);

CREATE FUNCTION mayor_election_year(at timestamptz) RETURNS integer
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN floor((extract(epoch FROM at) * 1000 - 1560275700000 - 105600000) / 446400000)::integer;

COMMENT ON FUNCTION mayor_election_year(timestamptz) IS 'The election year whose winner was in office at the given time. Join it to mayor_terms.election_year.';

-- An item tag that tells apart items sharing an ID, such as pets and enchanted books.
CREATE FUNCTION item_tag(item_id text, attributes jsonb) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN item_id = 'PET' AND attributes->'petInfo'->>'type' IS NOT NULL
            THEN 'PET_' || (attributes->'petInfo'->>'type')
        WHEN item_id = 'ENCHANTED_BOOK' AND jsonb_typeof(attributes->'enchantments') = 'object'
             AND (SELECT count(*) FROM jsonb_object_keys(attributes->'enchantments')) = 1
            THEN (SELECT 'ENCHANTMENT_' || upper(key) || '_' || value
                  FROM jsonb_each_text(attributes->'enchantments'))
        WHEN item_id IN ('RUNE', 'UNIQUE_RUNE') AND jsonb_typeof(attributes->'runes') = 'object'
             AND (SELECT count(*) FROM jsonb_object_keys(attributes->'runes')) = 1
            THEN (SELECT 'RUNE_' || upper(key) || '_' || value FROM jsonb_each_text(attributes->'runes'))
        WHEN item_id = 'POTION' AND attributes->>'potion' IS NOT NULL
            THEN 'POTION_' || upper(attributes->>'potion')
        ELSE item_id
    END
$$;

-- Listings with the attributes most pricing questions need as plain columns.
CREATE VIEW listings AS
SELECT
    a.uuid,
    a.seller,
    a.started_at,
    a.ends_at,
    a.bin,
    a.starting_bid,
    a.highest_bid,
    a.bid_count,
    a.item_name,
    a.tier,
    a.category,
    a.item_id,
    item_tag(a.item_id, i.item_attributes) AS item_tag,
    a.item_count,
    i.item_attributes->>'modifier' AS reforge,
    coalesce(try_bigint(i.item_attributes->>'upgrade_level'),
             try_bigint(i.item_attributes->>'dungeon_item_level')) AS stars,
    coalesce(try_bigint(i.item_attributes->>'rarity_upgrades'), 0) > 0 AS recombobulated,
    try_bigint(i.item_attributes->>'hot_potato_count') AS hot_potato_count,
    try_bigint(i.item_attributes->>'art_of_war_count') AS art_of_war_count,
    i.item_attributes->'enchantments' AS enchantments,
    i.item_attributes->'gems' AS gems,
    i.item_attributes->'attributes' AS kuudra_attributes,
    i.item_attributes->'ability_scroll' AS ability_scrolls,
    i.item_attributes->>'skin' AS skin,
    i.item_attributes->>'dye_item' AS dye,
    i.item_attributes->'petInfo'->>'type' AS pet_type,
    i.item_attributes->'petInfo'->>'tier' AS pet_tier,
    try_numeric(i.item_attributes->'petInfo'->>'exp') AS pet_exp,
    i.item_attributes->'petInfo'->>'heldItem' AS pet_held_item,
    try_bigint(i.item_attributes->'petInfo'->>'candyUsed') AS pet_candy_used,
    i.item_attributes->'petInfo'->>'skin' AS pet_skin,
    i.item_attributes,
    i.item_lore,
    mayor_election_year(a.started_at) AS mayor_year_at_listing,
    a.first_seen_at,
    a.absent_since
FROM auctions a
LEFT JOIN auction_items i ON i.uuid = a.uuid AND i.first_seen_at = a.first_seen_at;
COMMENT ON VIEW listings IS 'auctions with item attributes as columns. stars counts dungeon and master stars together (upgrade_level).';

-- Snapshots the collector missed. Each snapshot normally follows the previous one by the feed's
-- refresh interval; a longer step means snapshots were lost in between.
CREATE VIEW feed_gaps AS
SELECT feed, previous AS gap_after, api_last_updated AS gap_before, api_last_updated - previous AS gap
FROM (
    SELECT feed, api_last_updated,
           lag(api_last_updated) OVER (PARTITION BY feed ORDER BY api_last_updated) AS previous
    FROM feed_snapshots
) steps
-- One missed bazaar snapshot is a 40-second step, so its threshold sits between 20 and 40 seconds.
WHERE api_last_updated - previous > CASE feed WHEN 'bazaar' THEN interval '30 seconds' ELSE interval '90 seconds' END;

-- What happened to each listing.
CREATE VIEW auction_outcomes AS
-- A listing that vanishes is flagged with the oldest page time of the next complete sweep, so a gap
-- in auctions_ended can hide the sale of any listing flagged by the first complete sweep still
-- reading pages after the gap's start, through the first complete sweep after the gap.
WITH ended_gaps AS MATERIALIZED (
    SELECT g.gap_before,
           (SELECT min(s.oldest_page_at) FROM auction_sweeps s
            WHERE s.complete AND s.api_last_updated > g.gap_after) AS flagged_from,
           coalesce((SELECT min(s.oldest_page_at) FROM auction_sweeps s
                     WHERE s.complete AND s.oldest_page_at >= g.gap_before), 'infinity') AS flagged_by
    FROM feed_gaps g
    WHERE g.feed = 'auctions_ended'
),
-- Checked once per row before the per-gap test, which alone is too slow with many gaps.
ended_windows AS MATERIALIZED (
    -- A gap no complete sweep has followed yet cannot have flagged anything.
    SELECT range_agg(tstzrange(flagged_from, flagged_by, '[]')) AS flagged FROM ended_gaps
    WHERE flagged_from IS NOT NULL
)
SELECT
    a.uuid,
    a.item_id,
    item_tag(a.item_id, i.item_attributes) AS item_tag,
    a.bin,
    a.starting_bid,
    a.started_at,
    a.ends_at,
    s.sold_at,
    s.price AS sold_price,
    s.buyer,
    CASE
        WHEN s.auction_uuid IS NOT NULL THEN 'sold'
        WHEN a.absent_since IS NULL AND a.ends_at > now() THEN 'active'
        WHEN a.absent_since IS NULL THEN 'ended_unseen'
        WHEN (a.absent_since < a.ends_at OR a.bid_count > 0)
            AND a.absent_since <@ (SELECT flagged FROM ended_windows)
            AND EXISTS (
                SELECT 1 FROM ended_gaps g
                WHERE a.absent_since BETWEEN g.flagged_from AND g.flagged_by
                  AND g.gap_before > a.last_changed_at
            ) THEN 'unknown'
        WHEN a.absent_since < a.ends_at THEN 'cancelled'
        ELSE 'expired'
    END AS outcome,
    s.sold_at - a.started_at AS time_to_sell,
    a.absent_since
FROM auctions a
LEFT JOIN auction_items i ON i.uuid = a.uuid AND i.first_seen_at = a.first_seen_at
LEFT JOIN sales s ON s.auction_uuid = a.uuid;
COMMENT ON VIEW auction_outcomes IS 'outcome: sold, active, cancelled (gone before its end time without a sale), expired (gone after its end time), ended_unseen (past its end time but no complete sweep has run since), or unknown (gone while auctions_ended had a gap, so a sale may be missing; see feed_gaps). A gap counts once a later snapshot closes it, so during an outage these show as cancelled or expired. A BIN listing sold during a gap in its last minutes shows as expired.';

CREATE VIEW bazaar_prices AS
SELECT p.product_id, q.*
FROM bazaar_quick q
JOIN bazaar_products p USING (product_key);

CREATE VIEW bazaar_orders AS
SELECT p.product_id, b.*
FROM bazaar_books b
JOIN bazaar_products p USING (product_key);

CREATE VIEW skyblock_items AS
SELECT i->>'id' AS item_id, i->>'name' AS name, i->>'tier' AS tier, i->>'category' AS category,
       try_numeric(i->>'npc_sell_price') AS npc_sell_price, i AS details
FROM (
    SELECT payload FROM resource_snapshots
    WHERE resource = 'items'
    ORDER BY api_last_updated DESC NULLS LAST, fetched_at DESC
    LIMIT 1
) latest, jsonb_array_elements(latest.payload->'items') AS i;
COMMENT ON VIEW skyblock_items IS 'The newest item metadata snapshot, one row per item.';

-- The order book at a moment: the newest book row at or before it, looking back at most two hours
-- (a keyframe comes every 30 minutes).
CREATE FUNCTION bazaar_book_at(product text, at timestamptz) RETURNS SETOF bazaar_orders
LANGUAGE sql STABLE AS $$
    SELECT * FROM bazaar_orders
    WHERE product_id = product AND api_last_updated <= at AND api_last_updated > at - interval '2 hours'
    ORDER BY api_last_updated DESC
    LIMIT 1
$$;

-- The election calendar. Voting for election year Y opens on Late Summer 27 of SkyBlock year Y,
-- 181 days into the year, and closes when its winner takes office at mayor_term_start(Y), on
-- Late Spring 27 of year Y + 1. Both come from the SkyBlock calendar; election_votes records
-- when the collector actually saw voting.
CREATE FUNCTION election_opens(election_year integer) RETURNS timestamptz
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN to_timestamp((1560275700000 + (election_year - 1)::bigint * 446400000 + 217200000) / 1000.0);

CREATE FUNCTION election_closes(election_year integer) RETURNS timestamptz
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN mayor_term_start(election_year);

-- Hypixel's text carries Minecraft formatting codes such as §a.
CREATE FUNCTION strip_formatting(value text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN regexp_replace(value, '§.', '', 'g');

-- Every candidate of every election: what the collector saw while voting was open, and the final
-- result once the election closed.
CREATE VIEW election_candidates AS
WITH observed AS (
    SELECT election_year, candidate_key,
           (array_agg(candidate_name ORDER BY observed_at DESC))[1] AS candidate_name,
           (array_agg(perks ORDER BY observed_at DESC))[1] AS perks,
           (array_agg(votes ORDER BY observed_at DESC))[1] AS last_observed_votes,
           min(observed_at) AS first_observed_at,
           max(observed_at) AS last_observed_at
    FROM election_votes
    GROUP BY election_year, candidate_key
),
final AS (
    SELECT m.election_year, c->>'key' AS candidate_key, c->>'name' AS candidate_name,
           c->'perks' AS perks, try_bigint(c->>'votes') AS final_votes, m.mayor_key, m.minister_key
    FROM mayor_terms m
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(m.election_results) = 'array' THEN m.election_results ELSE '[]' END) AS c
),
merged AS (
    SELECT coalesce(f.election_year, o.election_year) AS election_year,
           coalesce(f.candidate_key, o.candidate_key) AS candidate_key,
           coalesce(f.candidate_name, o.candidate_name) AS candidate_name,
           coalesce(f.perks, o.perks) AS perks,
           f.final_votes, f.mayor_key, f.minister_key,
           o.last_observed_votes, o.first_observed_at, o.last_observed_at
    FROM final f
    FULL JOIN observed o ON o.election_year = f.election_year AND o.candidate_key = f.candidate_key
)
SELECT election_year, candidate_key, candidate_name, perks,
       (SELECT p FROM jsonb_array_elements(CASE WHEN jsonb_typeof(perks) = 'array' THEN perks ELSE '[]' END) AS p
        WHERE p->>'minister' = 'true' LIMIT 1) AS minister_perk,
       final_votes,
       final_votes::numeric / nullif(sum(final_votes) OVER (PARTITION BY election_year), 0) AS final_share,
       CASE WHEN final_votes IS NOT NULL
            THEN rank() OVER (PARTITION BY election_year ORDER BY final_votes DESC NULLS LAST) END AS final_rank,
       candidate_key = mayor_key AS won,
       candidate_key = minister_key AS became_minister,
       last_observed_votes, first_observed_at, last_observed_at,
       election_opens(election_year) AS voting_opens_at,
       election_closes(election_year) AS voting_closes_at
FROM merged;
COMMENT ON VIEW election_candidates IS 'One row per candidate per election the collector observed. minister_perk is the perk the candidate keeps if they become minister. won and became_minister are null until the election closes.';

-- Vote standings at each moment the counts changed.
CREATE VIEW election_standings AS
SELECT election_year, observed_at, candidate_key, candidate_name, votes,
       votes::numeric / nullif(sum(votes) OVER moment, 0) AS share,
       rank() OVER (PARTITION BY election_year, observed_at ORDER BY votes DESC NULLS LAST) AS rank
FROM election_votes
WINDOW moment AS (PARTITION BY election_year, observed_at);

-- Every perk of every term, one row each, for the mayor and the minister.
CREATE VIEW mayor_perks AS
SELECT m.election_year, m.term_start, m.term_end, 'mayor' AS role,
       m.mayor_key AS holder_key, m.mayor_name AS holder_name,
       p->>'name' AS perk_name, strip_formatting(p->>'description') AS description, p->>'description' AS description_raw
FROM mayor_terms m
CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(m.mayor_perks) = 'array' THEN m.mayor_perks ELSE '[]' END) AS p
UNION ALL
SELECT m.election_year, m.term_start, m.term_end, 'minister',
       m.minister_key, m.minister_name,
       m.minister_perk->>'name', strip_formatting(m.minister_perk->>'description'), m.minister_perk->>'description'
FROM mayor_terms m
WHERE m.minister_perk IS NOT NULL;

-- One row each time the mayor, their perks, or the minister changed.
CREATE VIEW mayor_snapshots AS
SELECT fetched_at, api_last_updated, mayor_key, mayor_name, mayor_perks, minister_key,
       minister_name, minister_perk, election_year, open_election_year
FROM (
    SELECT fetched_at, api_last_updated,
           payload->'mayor'->>'key' AS mayor_key, payload->'mayor'->>'name' AS mayor_name,
           payload->'mayor'->'perks' AS mayor_perks,
           payload->'mayor'->'minister'->>'key' AS minister_key,
           payload->'mayor'->'minister'->>'name' AS minister_name,
           payload->'mayor'->'minister'->'perk' AS minister_perk,
           try_bigint(payload->'mayor'->'election'->>'year') AS election_year,
           try_bigint(payload->'current'->>'year') AS open_election_year,
           jsonb_build_array(payload->'mayor'->'key', payload->'mayor'->'perks', payload->'mayor'->'minister') AS state,
           lag(jsonb_build_array(payload->'mayor'->'key', payload->'mayor'->'perks', payload->'mayor'->'minister'))
               OVER (ORDER BY fetched_at) AS previous_state
    FROM election_snapshots
) snapshots
WHERE previous_state IS DISTINCT FROM state;
COMMENT ON VIEW mayor_snapshots IS 'The mayor and minister as the API showed them, one row each time the mayor, their perks, or the minister changed. Perks can change within a term, for example under Jerry''s Perkpocalypse.';

-- Where a moment falls in the mayor cycle: who is in office, who held office before, who wins
-- the next election (known once it closes), and how the open vote stands at that moment.
CREATE FUNCTION mayor_context(at timestamptz)
RETURNS TABLE (
    election_year integer,
    mayor_key text,
    mayor_name text,
    minister_key text,
    minister_name text,
    term_start timestamptz,
    term_end timestamptz,
    into_term interval,
    until_term_end interval,
    previous_mayor_key text,
    previous_mayor_name text,
    next_mayor_key text,
    next_mayor_name text,
    voting_open boolean,
    leading_candidate_key text,
    leading_candidate_name text,
    leading_share numeric
)
LANGUAGE sql STABLE AS $$
    SELECT y.year, cur.mayor_key, cur.mayor_name, cur.minister_key, cur.minister_name,
           mayor_term_start(y.year), mayor_term_start(y.year + 1),
           at - mayor_term_start(y.year), mayor_term_start(y.year + 1) - at,
           prev.mayor_key, prev.mayor_name, nxt.mayor_key, nxt.mayor_name,
           at >= election_opens(y.year + 1),
           lead.candidate_key, lead.candidate_name, lead.share
    FROM (SELECT mayor_election_year(at) AS year) y
    LEFT JOIN mayor_terms cur ON cur.election_year = y.year
    LEFT JOIN mayor_terms prev ON prev.election_year = y.year - 1
    LEFT JOIN mayor_terms nxt ON nxt.election_year = y.year + 1
    LEFT JOIN LATERAL (
        SELECT v.candidate_key, v.candidate_name,
               v.votes::numeric / nullif(sum(v.votes) OVER (), 0) AS share
        FROM (
            SELECT DISTINCT ON (candidate_key) candidate_key, candidate_name, votes
            FROM election_votes
            WHERE election_votes.election_year = y.year + 1 AND observed_at <= at
            ORDER BY candidate_key, observed_at DESC
        ) v
        ORDER BY v.votes DESC NULLS LAST
        LIMIT 1
    ) lead ON true
$$;
COMMENT ON FUNCTION mayor_context(timestamptz) IS 'The mayor cycle at a moment. Join it laterally to time buckets, for example hourly prices: FROM buckets b CROSS JOIN LATERAL mayor_context(b.hour).';

-- Sales with listing details and the mayor cycle.
CREATE VIEW sales_enriched AS
SELECT
    s.auction_uuid,
    s.sold_at,
    s.price,
    s.bin,
    s.item_id,
    item_tag(s.item_id, s.item_attributes) AS item_tag,
    s.item_count,
    s.price / nullif(s.item_count, 0) AS price_each,
    s.item_attributes->>'modifier' AS reforge,
    coalesce(try_bigint(s.item_attributes->>'upgrade_level'),
             try_bigint(s.item_attributes->>'dungeon_item_level')) AS stars,
    coalesce(try_bigint(s.item_attributes->>'rarity_upgrades'), 0) > 0 AS recombobulated,
    try_bigint(s.item_attributes->>'hot_potato_count') AS hot_potato_count,
    s.item_attributes->'enchantments' AS enchantments,
    s.item_attributes->'petInfo'->>'type' AS pet_type,
    s.item_attributes->'petInfo'->>'tier' AS pet_tier,
    try_numeric(s.item_attributes->'petInfo'->>'exp') AS pet_exp,
    s.item_attributes->'petInfo'->>'heldItem' AS pet_held_item,
    s.item_attributes,
    s.seller,
    s.buyer,
    a.item_name,
    a.tier,
    a.category,
    a.starting_bid,
    a.started_at AS listed_at,
    s.sold_at - a.started_at AS time_to_sell,
    m.election_year AS mayor_election_year,
    m.mayor_name,
    m.minister_name,
    m.mayor_perks,
    s.sold_at - m.term_start AS into_term,
    m.term_end - s.sold_at AS until_term_end,
    prev.mayor_name AS previous_mayor_name,
    nxt.mayor_name AS next_mayor_name,
    s.sold_at >= election_opens(mayor_election_year(s.sold_at) + 1) AS voting_open
FROM sales s
LEFT JOIN auctions a ON a.uuid = s.auction_uuid
LEFT JOIN mayor_terms m ON m.election_year = mayor_election_year(s.sold_at)
LEFT JOIN mayor_terms prev ON prev.election_year = mayor_election_year(s.sold_at) - 1
LEFT JOIN mayor_terms nxt ON nxt.election_year = mayor_election_year(s.sold_at) + 1;
COMMENT ON VIEW sales_enriched IS 'sales joined to their listing and the mayor cycle. into_term and until_term_end place the sale within the term; previous_mayor_name and next_mayor_name are the terms on either side (next is known once that election closed); voting_open says the next election was open.';

SELECT ensure_partitions((now() AT TIME ZONE 'UTC')::date - 1, (now() AT TIME ZONE 'UTC')::date + 3);
