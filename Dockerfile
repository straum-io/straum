# straum-server image. Built and published by .github/workflows/release.yml
# for version tags only; a pull request that touches the build builds it
# without publishing.
#
# Build:  docker build -t straum .
# Run:    docker run --network host \
#             --security-opt seccomp=scripts/container/seccomp-io-uring.json \
#             -v straum-data:/var/lib/straum straum --cluster-id dev --node-id 1
#
# The broker runs on io_uring, which the default seccomp profiles of Docker
# and Podman block: the profile above is Podman's default with the three
# io_uring syscalls allowed (scripts/container/README.md, which also covers
# SELinux). It listens on loopback only (127.0.0.1:7171 by default) and
# refuses any other address until authentication exists. In a container,
# loopback is the container's own, so the broker is reachable from the host
# only with --network host; a published port (-p) does not reach it. Any
# setting of crates/straum-broker/README.md can follow as a flag, or come
# from a file mounted into the container and passed with --config.

# Both images are pinned by the digest of their multi-platform index, so a
# rebuild of the same commit starts from the same bases. To move a base:
# resolve the tag's index digest (docs/release.md, "Pins") and change
# it here and, for rust, in .github/workflows/release.yml.
#
# Rust 1.95.0, the workspace's rust-version: compio-executor 0.1.4 uses
# cfg_select! (stable in 1.95).
FROM rust:1.95-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1 AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p straum-broker --bin straum-server \
    && cp target/release/straum-server /straum-server \
    && mkdir /straum-data

# glibc and libgcc, no shell, no package manager; runs as uid 65532, stated
# again with USER so the image stays non-root whatever the base sets.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=build /straum-server /usr/local/bin/straum-server
# The data directory, owned by the runtime user, so a new named volume is too.
COPY --from=build --chown=65532:65532 /straum-data /var/lib/straum
USER 65532:65532
VOLUME ["/var/lib/straum"]
EXPOSE 7171
ENTRYPOINT ["/usr/local/bin/straum-server", "--data-dir", "/var/lib/straum"]
