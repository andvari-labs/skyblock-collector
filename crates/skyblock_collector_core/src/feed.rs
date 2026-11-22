//! Feed names. They name archive directories, `fetch_log` rows, and status entries.

/// Sales: the auctions that ended in the last minute.
pub const ENDED: &str = "auctions_ended";
/// Page 0 of the active auctions, with the pages after it while every listing on them changed.
pub const AUCTIONS: &str = "auctions";
/// Every page of the active auctions, read on an interval.
pub const SWEEP: &str = "auctions_sweep";
/// The bazaar's prices and order books.
pub const BAZAAR: &str = "bazaar";
/// The mayor election and the current term.
pub const ELECTION: &str = "election";
/// The item resource.
pub const ITEMS: &str = "items";
/// The collections resource.
pub const COLLECTIONS: &str = "collections";
/// The fire sales resource.
pub const FIRESALES: &str = "firesales";
