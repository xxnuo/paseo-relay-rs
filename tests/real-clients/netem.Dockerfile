FROM rust:1.97.1-alpine
RUN apk add --no-cache iproute2
ENTRYPOINT ["tc"]
