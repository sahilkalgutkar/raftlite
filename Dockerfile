# Build both binaries with the toolchain, ship neither the toolchain nor a
# shell. CGO is off so the result is a static binary that runs on a base image
# with no libc at all.
FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/raftlited ./cmd/raftlited && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/raftctl   ./cmd/raftctl && \
    go vet ./... && \
    mkdir -p /out/data

# distroless static: no shell, no package manager, nothing to exploit that
# isn't raftlite itself. The health check runs raftctl, so the image carries
# its own probe rather than needing curl installed alongside it.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/raftlited /raftlited
COPY --from=build /out/raftctl   /raftctl
COPY --from=build --chown=nonroot:nonroot /out/data /data

USER nonroot:nonroot
WORKDIR /data
VOLUME ["/data"]

# 9001 is peer traffic, 8001 is the client API.
EXPOSE 9001 8001

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/raftctl", "--endpoints", "127.0.0.1:8001", "--timeout", "2s", "status"]

ENTRYPOINT ["/raftlited"]
CMD ["--help"]
