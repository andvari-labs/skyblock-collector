# skyblock-collector

[![CI](https://github.com/andvari-labs/skyblock-collector/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/andvari-labs/skyblock-collector/actions/workflows/ci.yml?query=branch%3Amain)
[![Latest release](https://img.shields.io/github/v/release/andvari-labs/skyblock-collector)](https://github.com/andvari-labs/skyblock-collector/releases/latest)

Records Hypixel's in-game market and election data from its public API into a raw archive and
Postgres.

It polls the auction house, ended auctions, the bazaar, the election and a few slower resources,
keeps every response byte for byte, and writes the parsed data to Postgres for querying. A second
copy on another network covers outages.

## Run it

The deploy kit runs it and its Postgres in a Podman pod, with an optional secondary collector on a
second host. On a host the kit deployed:

```sh
~/services/skyblock-collector/verify.sh
curl -s http://<BIND_ADDRESS>:3004/status | jq
```

To deploy it, or to run the image by hand with your own Postgres, see [Install](docs/install.md).
To query the data, see [Query the data](docs/queries.md).

## Documentation

- [Install](docs/install.md): deploy it with the kit, or run the image by hand, with every setting and command.
- [What it collects](docs/collecting.md): the feeds, how often each is polled, and how every response is kept.
- [Query the data](docs/queries.md): connect to Postgres, find the views to start from, and copy worked examples.
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
