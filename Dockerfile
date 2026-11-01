FROM rust:1.97.1-alpine AS toolchain
RUN apk add --no-cache bash docker-cli gcc musl-dev && rustup component add rustfmt clippy
WORKDIR /app
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src

FROM toolchain AS test
COPY rust/tests ./tests
COPY rust/examples ./examples
RUN cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings \
    && cargo test --locked --all-targets \
    && cargo build --locked --release --example relay_bench
CMD ["cargo", "test", "--locked", "--tests", "--", "--ignored"]

FROM toolchain AS build
RUN cargo build --locked --release

FROM scratch AS binary
COPY --from=build /app/target/release/paseo-relay /paseo-relay
USER 65532:65532
ENV PASEO_RELAY_HOST=0.0.0.0 PASEO_RELAY_PORT=4000
EXPOSE 4000 4400
ENTRYPOINT ["/paseo-relay"]

FROM binary AS runtime
