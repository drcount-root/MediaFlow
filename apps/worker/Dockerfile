# syntax=docker/dockerfile:1
# Build the worker binary, then ship it on a slim runtime that carries ffmpeg/
# ffprobe (the transcoder shells out to them).
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/worker /usr/local/bin/worker
ENV WORK_DIR=/tmp/mediaflow-worker
ENTRYPOINT ["worker"]
