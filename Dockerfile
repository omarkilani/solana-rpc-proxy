FROM rust as builder
WORKDIR /usr/src/solana-rpc-proxy
COPY . .
RUN cargo install --path .

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
ARG LISTEN_PORT=8899
EXPOSE $LISTEN_PORT
ENV LISTEN_ADDRESS=0.0.0.0:$LISTEN_PORT
COPY --from=builder /usr/local/cargo/bin/solana-rpc-proxy /usr/local/bin/solana-rpc-proxy
CMD ["solana-rpc-proxy"]
