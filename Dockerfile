# syntax=docker/dockerfile:1.6

FROM gitea.contc/controlplane/rust-builder:0.2.0 as source
ARG GITVERSION=
WORKDIR /root/source/dbdaemon
COPY --link Source/Rust /root/source/dbdaemon

FROM source as build-dev
RUN --mount=type=ssh \
    --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/source/dbdaemon/target \
    /root/.cargo/bin/cargo build \
    && cp target/debug/dbdaemon /root \
    && cp target/debug/client /root/dbdaemon-client

FROM source as build-release
RUN --mount=type=ssh \
    --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/source/dbdaemon/target \
    /root/.cargo/bin/cargo build --release \
    && cp target/release/dbdaemon /root \
    && cp target/release/client /root/dbdaemon-client

FROM ubuntu:24.04 as dbdaemon-dev
RUN apt update && apt install -y curl && apt clean
COPY --from=build-dev /root/dbdaemon /usr/bin/
COPY --from=build-dev /root/dbdaemon-client /usr/bin/
EXPOSE 9999
CMD /usr/bin/dbdaemon

FROM ubuntu:24.04 as dbdaemon
RUN apt update && apt install -y curl && apt clean
COPY --from=build-release /root/dbdaemon /usr/bin/
COPY --from=build-release /root/dbdaemon-client /usr/bin/
EXPOSE 9999
CMD /usr/bin/dbdaemon
