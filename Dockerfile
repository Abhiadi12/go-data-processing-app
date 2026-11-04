# syntax=docker/dockerfile:1


FROM golang:1.27.3-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/datapipe ./cmd/server

RUN mkdir -p /out/state/output

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/datapipe /app/datapipe
COPY testdata /app/data/testdata
COPY --from=build --chown=nonroot:nonroot /out/state /var/lib/datapipe

ENV PORT=9090 \
    DATA_DIR=/app/data \
    OUTPUT_DIR=/var/lib/datapipe/output \
    SQLITE_PATH=/var/lib/datapipe/datapipe.db

EXPOSE 9090
VOLUME ["/var/lib/datapipe"]
USER nonroot:nonroot
ENTRYPOINT ["/app/datapipe"]
