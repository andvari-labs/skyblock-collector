# The build stage reads rust-toolchain.toml, so the image builds with the same Rust as CI. trellis's
# Renovate moves the pinned digests, and they arrive with trellis updates. `just notices` writes
# the license notices under target/ first; `just image` and the workflows run it.
#
# The build stages run on the builder's own platform and cross-compile for the image's, so one
# runner builds every architecture without emulating one; the final stage only copies files.
FROM --platform=$BUILDPLATFORM docker.io/library/rust:slim-trixie@sha256:6f14b2b4b278ec0d50aa1d69541c304ce0f95e0f6268487aa9924af1c2e9aebe AS build
ARG BUILDARCH
ARG TARGETARCH
WORKDIR /src
# Another architecture needs Debian's cross gcc, which links the program and which cc finds on its
# own to build the C code some crates have.
RUN if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      case "$TARGETARCH" in \
        amd64) gcc=gcc-x86-64-linux-gnu ;; \
        arm64) gcc=gcc-aarch64-linux-gnu ;; \
        *) echo "error: no cross gcc for $TARGETARCH" >&2; exit 1 ;; \
      esac; \
      apt-get update && apt-get install --yes --no-install-recommends "$gcc" "libc6-dev-$TARGETARCH-cross" \
        && rm -rf /var/lib/apt/lists/*; \
    fi
COPY . .
RUN case "$TARGETARCH" in \
      amd64) arch=x86_64 ;; \
      arm64) arch=aarch64 ;; \
      *) echo "error: no Rust target for $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    target="$arch-unknown-linux-gnu"; \
    if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      export "CARGO_TARGET_$(echo "$target" | tr 'a-z-' 'A-Z_')_LINKER=$arch-linux-gnu-gcc"; \
    fi; \
    rustup toolchain install --no-self-update && rustup target add "$target" \
      && cargo build --locked --release --package skyblock_collector --target "$target" \
      && cp "target/$target/release/skyblock-collector" /skyblock-collector
# The image runs as a non-root user, so its license files must be readable by all, whatever modes
# the checkout and `just notices` gave them. COPY --chmod would set the directory BuildKit creates
# to the same mode, which the user can't enter.
RUN install -D --mode=644 --target-directory=/licenses target/THIRD-PARTY-LICENSES.html LICENSE-MIT LICENSE-APACHE

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:532c7a94f9e96ba9db75eb9388f4227751131c7cb0a411d1a8521af4096135de
LABEL org.opencontainers.image.source="https://github.com/andvari-labs/skyblock-collector" \
      org.opencontainers.image.description="Records Hypixel's in-game market and election data from its public API into a raw archive and Postgres." \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"
COPY --from=build /licenses/ /usr/share/doc/skyblock-collector/
COPY --from=build /skyblock-collector /usr/local/bin/skyblock-collector
ENTRYPOINT ["/usr/local/bin/skyblock-collector"]
