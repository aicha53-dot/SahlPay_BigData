FROM debian:bookworm-slim

ARG RELEASE=RELEASE.2025-09-07T16-13-09Z
ARG TARGETARCH

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN set -eu; \
    URL="https://github.com/minio/minio/releases/download/${RELEASE}/minio.linux-${TARGETARCH}.${RELEASE}"; \
    curl -fsSL "$URL" -o /usr/local/bin/minio; \
    curl -fsSL "$URL.sha256sum" -o /tmp/minio.sha256; \
    HASH="$(awk '{print $1}' /tmp/minio.sha256)"; \
    echo "$HASH  /usr/local/bin/minio" | sha256sum -c -; \
    chmod +x /usr/local/bin/minio; \
    rm /tmp/minio.sha256

EXPOSE 9000 9001

ENTRYPOINT ["/usr/local/bin/minio"]