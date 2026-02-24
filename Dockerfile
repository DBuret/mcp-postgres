# --- Étape 1 : Build ---
FROM rust:1.88-slim AS builder

RUN apt-get update && apt-get install -y \
    musl-tools \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN rustup target add x86_64-unknown-linux-musl
RUN cargo build --release --target x86_64-unknown-linux-musl

# --- Étape 2 : Runtime final ---
FROM scratch

# --- LABEL OCI STANDARDS ---
LABEL org.opencontainers.image.title="MCP PostgreSQL Rust Bridge"
LABEL org.opencontainers.image.description="High-performance MCP server bridge connecting AI agents to PostgreSQL via SSE. Read-only access with schema introspection."
LABEL org.opencontainers.image.vendor="DBuret"
LABEL org.opencontainers.image.authors="DBuret"

LABEL org.opencontainers.image.url="https://github.com/DBuret/mcp-postgres"
LABEL org.opencontainers.image.source="https://github.com/DBuret/mcp-postgres"
LABEL org.opencontainers.image.documentation="https://github.com/DBuret/mcp-postgres/blob/main/README.adoc"

LABEL org.opencontainers.image.version="0.1.0"
LABEL org.opencontainers.image.revision="HEAD"
LABEL org.opencontainers.image.licenses="MIT"

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/mcp-postgres /app/mcp-bridge

# Variables d'environnement par défaut
ENV MCP_PG_DATABASE_URL="postgresql://postgres:postgres@172.17.0.1:5432/paitrimony"
ENV MCP_PG_PORT="3005"
ENV MCP_PG_LOG="info"

WORKDIR /app
EXPOSE 3001
USER 1000

ENTRYPOINT ["./mcp-bridge"]
