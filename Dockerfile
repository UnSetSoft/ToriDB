# Build Stage
FROM rust:bookworm AS builder

WORKDIR /usr/src/undb
COPY . .

RUN cargo build --release

# Runtime Stage
FROM debian:bookworm-slim

WORKDIR /app

# Copy binary from builder
COPY --from=builder /usr/src/undb/target/release/db-engine /app/undb

# Create data directory
RUN mkdir -p /app/data

EXPOSE 8569

CMD ["./undb"]
