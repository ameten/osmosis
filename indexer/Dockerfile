FROM rust:1.69.0-bullseye as builder

RUN mkdir /tmp/osmosis
COPY . /tmp/osmosis/.
WORKDIR /tmp/osmosis
RUN cargo build --release

FROM debian:11.7-slim

RUN apt-get -y update
RUN apt-get -y install ca-certificates
COPY --from=builder /tmp/osmosis/target/release/indexer /opt/indexer
ENTRYPOINT ["/opt/indexer"]
