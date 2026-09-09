# Static doorbell-pm binary in a distroless image. The image has no shell and
# no worker runtime: add the worker on top (FROM doorbell-pm) or bind-mount it.
#
# Working directory is /app, owned by nonroot. The config is expected at
# /app/doorbell.yaml; mount or copy it there. Relative paths in the config
# (pool `dir`, future log dirs) resolve against /app.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /doorbell-pm ./cmd/doorbell-pm \
    && mkdir -p /app

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/lezhnev74/doorbell-pm" \
      org.opencontainers.image.description="On-demand worker process spawner" \
      org.opencontainers.image.licenses="MIT"
COPY --from=build /doorbell-pm /usr/local/bin/doorbell-pm
COPY --from=build --chown=nonroot:nonroot /app /app
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/doorbell-pm"]
CMD ["run", "--config", "/app/doorbell.yaml"]
