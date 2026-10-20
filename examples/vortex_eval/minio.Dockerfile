# Disposable local S3 acceptance service, built from the upstream release.
# Build with an empty context:
# docker build -t daft-vortex-minio:phase3 - < examples/vortex_eval/minio.Dockerfile
FROM golang:1.24.8-bookworm AS build
ARG MINIO_VERSION=RELEASE.2025-10-15T17-29-55Z
RUN CGO_ENABLED=0 GOBIN=/out go install github.com/minio/minio@${MINIO_VERSION}

FROM debian:bookworm-slim
COPY --from=build /out/minio /usr/local/bin/minio
ENTRYPOINT ["/usr/local/bin/minio"]
