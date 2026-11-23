# Test fixtures

These five files are responses recorded from Hypixel's public SkyBlock API, which needs no key:
`auctions_page.json` (a page of active auctions), `ended.json` (ended auctions), `bazaar_a.json`
and `bazaar_b.json` (two bazaar snapshots), and `election.json` (the mayor and the election).

Every player, profile and auction identifier in them was replaced with a synthetic one, and the
encoded item data was rebuilt to match. The prices, item
names and descriptions, timestamps, bazaar order books, and mayors and vote counts are public game
data, kept as recorded, because the parser tests depend on their real shapes and quirks.

They are an exception to the Test data rule in [CONTRIBUTING.md](../../../../CONTRIBUTING.md), and
the only one. A new fixture follows that rule: every value invented, or replaced if recorded.
