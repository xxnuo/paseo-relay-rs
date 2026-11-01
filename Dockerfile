FROM rust:1.97.1-alpine AS toolchain
RUN apk add --no-cache bash docker-cli && rustup component add rustfmt clippy
WORKDIR /app
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src

FROM toolchain AS test
COPY rust/tests ./tests
COPY rust/examples ./examples
RUN cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings \
    && cargo test --locked --all-targets \
    && cargo build --locked --release --example relay_bench
CMD ["cargo", "test", "--locked", "--test", "binary", "--", "--ignored"]

FROM toolchain AS build
RUN cargo build --locked --release

FROM scratch AS binary
COPY --from=build /app/target/release/paseo-relay /paseo-relay
USER 65532:65532
ENV PASEO_RELAY_HOST=0.0.0.0 PASEO_RELAY_PORT=4000
EXPOSE 4000 4400
ENTRYPOINT ["/paseo-relay"]

# Nginx 1.30.6; keep the runtime base reproducible for comparative loads.
FROM nginx:stable-alpine@sha256:0e52bd054126cdc040b1276c0af347aeb02a41b9cfe305d6094767ac30e97949 AS nginx
COPY --from=build /app/target/release/paseo-relay /paseo-relay
COPY deploy/nginx.conf.template /etc/nginx/paseo.conf.template
COPY --chmod=755 deploy/docker-entrypoint.sh /entrypoint.sh
USER 65532:65532
ENV PASEO_RELAY_HOST=0.0.0.0 PASEO_RELAY_PORT=4000
EXPOSE 4000 4400
STOPSIGNAL SIGTERM
ENTRYPOINT ["/entrypoint.sh"]

FROM binary AS runtime
