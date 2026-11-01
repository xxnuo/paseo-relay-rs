FROM rust:1.97.1-alpine AS build
WORKDIR /app
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
RUN cargo build --locked --release

FROM scratch
COPY --from=build /app/target/release/paseo-relay /paseo-relay
USER 65532:65532
ENV PASEO_RELAY_HOST=0.0.0.0 PASEO_RELAY_PORT=4000
EXPOSE 4000
ENTRYPOINT ["/paseo-relay"]
