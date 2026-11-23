//! Hypixel API response shapes. Every field the collector does not model is kept in `unknown` so a
//! new API field still reaches the database.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Map, Value as Json};
use time::OffsetDateTime;
use uuid::Uuid;

/// The time `ms` milliseconds after the Unix epoch, or None when it is out of range.
#[must_use]
pub fn ms_to_time(ms: i64) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000).ok()
}

/// Reads a UUID in any of the forms Hypixel sends, with or without dashes, ignoring surrounding
/// whitespace. None when `s` is None or isn't a UUID.
#[must_use]
pub fn parse_uuid(s: Option<&str>) -> Option<Uuid> {
    s.and_then(|s| Uuid::try_parse(s.trim()).ok())
}

/// How far past the time it was fetched a snapshot's stamp may be, at least: well beyond any drift
/// between a host kept on time and Hypixel's clock, so only a broken or hostile stamp is past it.
/// The pollers ignore such a snapshot, and replay skips it.
pub const MAX_STAMP_LEAD_MS: i64 = 10 * 60 * 1_000;

/// The `lastUpdated` field, read without parsing the rest of a large body.
#[derive(Deserialize, Debug)]
pub struct Envelope {
    /// Hypixel's `success`, false on an error answer.
    pub success: Option<bool>,
    /// The snapshot's stamp in Unix milliseconds.
    #[serde(rename = "lastUpdated")]
    pub last_updated: Option<i64>,
}

/// `item_bytes` is a bare base64 string on the live API but documented as `{type, data}`.
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum ItemBytes {
    /// The bare base64 string the live API sends.
    Text(String),
    /// The documented object, whose `data` holds the base64 string.
    Object {
        /// The base64 string.
        data: String,
    },
}

impl ItemBytes {
    /// The base64 text, whichever shape it came in.
    #[must_use]
    pub fn base64(&self) -> &str {
        match self {
            Self::Text(s) | Self::Object { data: s } => s,
        }
    }
}

/// One page of `skyblock/auctions`.
#[derive(Deserialize, Debug)]
pub struct AuctionsPage {
    /// Hypixel's `success`, false on an error answer.
    pub success: Option<bool>,
    /// The page number, from 0.
    pub page: Option<i64>,
    /// How many pages the snapshot has.
    #[serde(rename = "totalPages")]
    pub total_pages: Option<i64>,
    /// The snapshot's stamp in Unix milliseconds.
    #[serde(rename = "lastUpdated")]
    pub last_updated: Option<i64>,
    /// The listings on the page.
    #[serde(default)]
    pub auctions: Vec<Auction>,
}

/// An active listing.
#[derive(Deserialize, Debug, Clone)]
pub struct Auction {
    /// The listing's UUID.
    pub uuid: String,
    /// The seller's player UUID.
    pub auctioneer: Option<String>,
    /// The seller's profile UUID.
    pub profile_id: Option<String>,
    /// The player UUIDs of the seller's co-op members.
    #[serde(default)]
    pub coop: Vec<String>,
    /// When the listing started, in Unix milliseconds.
    pub start: Option<i64>,
    /// When the listing ends, in Unix milliseconds.
    pub end: Option<i64>,
    /// The item's display name.
    pub item_name: Option<String>,
    /// The item's lore, with formatting codes.
    pub item_lore: Option<String>,
    /// Hypixel's search text for the item.
    pub extra: Option<String>,
    /// The listing's categories.
    #[serde(default)]
    pub categories: Vec<String>,
    /// The listing's main category.
    pub category: Option<String>,
    /// The item's rarity.
    pub tier: Option<String>,
    /// The starting bid, or the price of a buy-it-now listing.
    pub starting_bid: Option<i64>,
    /// The item, as base64 of gzipped NBT.
    pub item_bytes: Option<ItemBytes>,
    /// The seller collected the listing.
    pub claimed: Option<bool>,
    /// The bidders who collected what they were owed.
    #[serde(default)]
    pub claimed_bidders: Vec<String>,
    /// The highest bid so far.
    pub highest_bid_amount: Option<i64>,
    /// When the listing last changed, in Unix milliseconds.
    pub last_updated: Option<i64>,
    /// The listing is buy-it-now.
    pub bin: Option<bool>,
    /// The bids so far.
    #[serde(default)]
    pub bids: Vec<Bid>,
    /// Every field this struct doesn't model.
    #[serde(flatten)]
    pub unknown: Map<String, Json>,
}

/// A bid on a listing.
#[derive(Deserialize, Debug, Clone)]
pub struct Bid {
    /// The bidder's player UUID.
    pub bidder: Option<String>,
    /// The bidder's profile UUID.
    pub profile_id: Option<String>,
    /// The bid in coins.
    pub amount: Option<i64>,
    /// When the bid was made, in Unix milliseconds.
    pub timestamp: Option<i64>,
}

/// A snapshot of `skyblock/auctions_ended`.
#[derive(Deserialize, Debug)]
pub struct EndedPage {
    /// Hypixel's `success`, false on an error answer.
    pub success: Option<bool>,
    /// The snapshot's stamp in Unix milliseconds.
    #[serde(rename = "lastUpdated")]
    pub last_updated: Option<i64>,
    /// The sales in the snapshot.
    #[serde(default)]
    pub auctions: Vec<EndedAuction>,
}

/// A sale.
#[derive(Deserialize, Debug, Clone)]
pub struct EndedAuction {
    /// The listing's UUID.
    pub auction_id: String,
    /// The seller's player UUID.
    pub seller: Option<String>,
    /// The seller's profile UUID.
    pub seller_profile: Option<String>,
    /// The buyer's player UUID.
    pub buyer: Option<String>,
    /// The buyer's profile UUID.
    pub buyer_profile: Option<String>,
    /// When the sale happened, in Unix milliseconds.
    pub timestamp: Option<i64>,
    /// The price in coins.
    pub price: Option<i64>,
    /// The listing was buy-it-now.
    pub bin: Option<bool>,
    /// The item, as base64 of gzipped NBT.
    pub item_bytes: Option<ItemBytes>,
    /// Every field this struct doesn't model.
    #[serde(flatten)]
    pub unknown: Map<String, Json>,
}

/// A snapshot of `skyblock/bazaar`, borrowing the order books from the body.
#[derive(Deserialize, Debug)]
pub struct Bazaar<'body> {
    /// Hypixel's `success`, false on an error answer.
    pub success: Option<bool>,
    /// The snapshot's stamp in Unix milliseconds.
    #[serde(rename = "lastUpdated")]
    pub last_updated: Option<i64>,
    /// Each product by its ID.
    #[serde(borrow, default)]
    pub products: BTreeMap<String, BazaarProduct<'body>>,
}

/// One bazaar product's order books and prices.
#[derive(Deserialize, Debug)]
pub struct BazaarProduct<'body> {
    /// The buy orders, as the JSON Hypixel sent.
    #[serde(borrow)]
    pub buy_summary: Option<&'body RawValue>,
    /// The sell offers, as the JSON Hypixel sent.
    #[serde(borrow)]
    pub sell_summary: Option<&'body RawValue>,
    /// The product's prices and volumes.
    pub quick_status: Option<QuickStatus>,
}

/// Hypixel's names. `buyPrice` is what an instant buy costs (the cheapest sell offers) and
/// `sellPrice` is what an instant sell earns (the highest buy orders).
#[derive(Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QuickStatus {
    /// Hypixel's `buyPrice`.
    pub buy_price: Option<f64>,
    /// Hypixel's `sellPrice`.
    pub sell_price: Option<f64>,
    /// Hypixel's `buyVolume`.
    pub buy_volume: Option<f64>,
    /// Hypixel's `sellVolume`.
    pub sell_volume: Option<f64>,
    /// Hypixel's `buyMovingWeek`.
    pub buy_moving_week: Option<f64>,
    /// Hypixel's `sellMovingWeek`.
    pub sell_moving_week: Option<f64>,
    /// Hypixel's `buyOrders`.
    pub buy_orders: Option<f64>,
    /// Hypixel's `sellOrders`.
    pub sell_orders: Option<f64>,
}

/// Converts a count that Hypixel sends as a JSON number to an integer column, tolerating floats.
#[must_use]
#[expect(
    clippy::cast_possible_truncation,
    reason = "the cast saturates at the bounds of i64, the clamp a count far out of range wants"
)]
pub fn whole(v: Option<f64>) -> Option<i64> {
    v.filter(|v| v.is_finite()).map(|v| v.round() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auction_keeps_unknown_fields_and_both_item_bytes_shapes() {
        let page: AuctionsPage = serde_json::from_str(
            r#"{"success":true,"page":0,"totalPages":2,"lastUpdated":1794551932072,"auctions":[
              {"uuid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1","auctioneer":"cccccccccccccccccccccccccccccccc",
               "start":1,"end":2,"item_name":"X","starting_bid":5,"item_bytes":"H4sI","bin":true,
               "bids":[],"brand_new_field":{"a":1}},
              {"uuid":"0000000000000000000000000000000a","item_bytes":{"type":0,"data":"H4sJ"}}]}"#,
        )
        .unwrap();
        assert_eq!(page.auctions.len(), 2);
        assert_eq!(page.auctions[0].unknown["brand_new_field"]["a"], 1);
        assert_eq!(
            page.auctions[0].item_bytes.as_ref().unwrap().base64(),
            "H4sI"
        );
        assert_eq!(
            page.auctions[1].item_bytes.as_ref().unwrap().base64(),
            "H4sJ"
        );
        assert_eq!(page.auctions[1].bin, None);
        assert!(parse_uuid(page.auctions[0].auctioneer.as_deref()).is_some());
    }

    #[test]
    fn whole_rounds_and_rejects_non_finite() {
        assert_eq!(whole(Some(3.0)), Some(3));
        assert_eq!(whole(Some(2.6)), Some(3));
        assert_eq!(whole(Some(f64::NAN)), None);
        assert_eq!(whole(None), None);
    }

    #[test]
    fn ms_converts_exactly() {
        let t = ms_to_time(1_794_551_932_072).unwrap();
        assert_eq!(t.unix_timestamp(), 1_794_551_932);
        assert_eq!(t.millisecond(), 72);
    }
}
