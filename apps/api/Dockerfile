# syntax=docker/dockerfile:1
# Build the API and migrate binaries, then ship each on a minimal distroless image.
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/api ./cmd/api \
    && CGO_ENABLED=0 go build -trimpath -o /out/migrate ./cmd/migrate

FROM gcr.io/distroless/static-debian12:nonroot AS api
COPY --from=build /out/api /api
EXPOSE 8080
ENTRYPOINT ["/api"]

# Migrations are mounted at /migrations (MIGRATIONS_DIR); cmd/migrate tracks
# applied versions in schema_migrations and is safe to re-run.
FROM gcr.io/distroless/static-debian12:nonroot AS migrate
COPY --from=build /out/migrate /migrate
ENV MIGRATIONS_DIR=/migrations
ENTRYPOINT ["/migrate"]
