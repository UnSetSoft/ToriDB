# Build Stage
FROM rust:bookworm AS builder

WORKDIR /usr/src/toridb
COPY . .

RUN cargo build --release

# Runtime Stage
FROM debian:bookworm-slim

WORKDIR /app

# Copy binary from builder
COPY --from=builder /usr/src/toridb/target/release/toridb /app/toridb

# Create data directory
RUN mkdir -p /app/data

EXPOSE 8569

CMD ["./toridb"]
