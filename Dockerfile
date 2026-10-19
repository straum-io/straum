# straum-server image. Built and published by .github/workflows/release.yml
# for version tags only.
#
# Build:  docker build -t straum .
# Run:    see "Run with Docker" in README.md; a node needs the STRAUM_CLUSTER_*
#         settings of docs/configuration.md and a volume for its data.

FROM rust:1.85-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p server --bin straum-server \
    && cp target/release/straum-server /straum-server

# glibc and libgcc, no shell, no package manager; runs as uid 65532.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /straum-server /usr/local/bin/straum-server
# The node refuses any address beyond loopback unless the operator sets
# STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1 (D-107); the image does not set it.
ENV STRAUM_CLUSTER_DATA_DIR=/var/lib/straum
VOLUME ["/var/lib/straum"]
# MQTT, node links, metrics (when configured to listen on them).
EXPOSE 1883 7000 9100
ENTRYPOINT ["/usr/local/bin/straum-server"]
