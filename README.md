# 🔔 `doorbell-pm` Process Manager

[![CI](https://github.com/lezhnev74/doorbell-pm/actions/workflows/ci.yml/badge.svg)](https://github.com/lezhnev74/doorbell-pm/actions/workflows/ci.yml)

> Ring the bell, workers show up. Doorbell spawns worker processes on demand and gets out of the way.

Most job runners want to own your queue, your retries and your worker code. Doorbell wants none of it. It is a small,
predictable process spawner: your app publishes "there is work" and doorbell starts up to N copies of the command you
configured for that pool. Workers claim jobs themselves and quit when the queue is empty, so the process count follows
demand but never exceeds your cap. One YAML file, one hint endpoint, zero framework lock-in - pair it with any PHP or
other app and any queue backend you already have.

A deliberately dumb process spawner. It listens for work notifications (doorbells) over Redis pub/sub or HTTP and
spawns worker processes up to a per-pool concurrency. Workers claim their own jobs from the database and exit whenever
they decide to. Doorbell does not know what a job is, does not retry, and never restarts a worker on its own.

## How it works

```
  [your app]  ---> hint via Redis or HTTP --->  doorbell  ---+---> worker
                                                (cap: 2)     +---> worker
```

One pool per queue, one command per pool, a cap on how many run at once:

```yaml
pools:
  encoding:                                   # PUBLISH jobs:encoding <n>
    command: [ php, worker.php, --queue=encoding ]
    concurrency: 4                            # never more than 4 at once
    ttl: 10m                                  # kill a worker that runs longer
    poke: 1m                                  # re-hint every minute in case a message was lost

  mail:                                       # PUBLISH jobs:mail <n>
    command: [ php, worker.php, --queue=mail ]
    concurrency: 1
```

Spawn rule per hint: `spawn = min(hint, concurrency - running)`. The hint is approximate; over- or under-spawning is
fine because idle workers find no jobs and exit. A pool that keeps crashing is paused by a [breaker](#failure-breaker),
and on shutdown every worker gets `term_signal`, then `grace_shutdown`, then SIGKILL.
Every key with a one-line explanation of what it changes is in
[`test/testdata/config/full.yaml`](test/testdata/config/full.yaml).

## How to hint

A hint is "about N jobs are waiting on this pool". It is the only input doorbell reacts to, and it can come from
four places:

- **Your app over Redis**: `PUBLISH jobs:encoding 100` after enqueuing. Fire-and-forget; lost while doorbell is
  disconnected.
- **Your app over HTTP**: `POST /hint` with `{"jobs:encoding": 100}`. Same shape, synchronous, gets a status code.
- **Doorbell itself (`poke`)**: with `poke: 1m` the pool hints itself `poke_count` every minute. This is the safety net
  for lost messages and for a backlog left behind after a [breaker](#failure-breaker) pause.
- **The worker on exit**: a worker that exits ok and prints a number N > 0 as its last line hints the pool for N more.
  A worker that quit on a job quota keeps a busy queue draining without the app's help.

Each hint spawns `min(hint, concurrency - running)` workers. Details: [Hint sources](#hint-sources) and
[Worker contract](#worker-contract).

## Quick start

```sh
make build
./bin/doorbell-pm check --config doorbell.yaml     # validate and print the resolved config
./bin/doorbell-pm run --config doorbell.yaml
```

Commands: `run --config <path>`, `check --config <path>` (validate, print resolved config, exit), `version`;
`-c` is short for `--config`, `--help` works on every command. Exit code 0 on a clean stop, 1 on config or runtime
error, 2 on bad flags or usage. SIGINT or SIGTERM starts a graceful shutdown.

Minimal config:

```yaml
redis:
  addr: "127.0.0.1:6379"
  channel_prefix: "jobs:"        # subscribes to jobs:*

pools:
  _defaults:                     # reserved: shared settings for every pool
    grace_shutdown: 30s

  encoding:
    concurrency: 4
    ttl: 10m
    poke: 1m
    command: [ php, worker.php, --queue=encoding, --drain ]
```

Then `redis-cli PUBLISH jobs:encoding 100` or:

```sh
curl -X POST 127.0.0.1:8080/hint -d '{"jobs:encoding": 100}'
```

`${ENV_VAR}` in the yaml is expanded before parsing, so secrets such as `redis.password` stay out of the file.

## Worker contract

- Doorbell starts the command; the worker claims its own jobs and exits when it wants to.
- Exit code in `ok_exit_codes` (default `[0]`) is `ok`. Any other code or death by a signal is a `failure` and counts
  towards the [breaker](#failure-breaker). Killed by doorbell (`ttl`, shutdown) is neutral.
- Last non-empty line on `result_stream` (default `stdout`) may be the number of tasks processed: a non-negative
  integer, at most 64 bytes. Anything else is ignored.
- Exit `ok` with N > 0: doorbell hints the pool for N more workers (`source=worker`). A worker that quit on `--max-jobs`
  or a quota keeps a busy queue draining.
- Exit `ok` with `0` (or no number): "no work", nothing spawns.
- A trailing log line on that stream silently disables the hint (`result_ignored` at debug is the only trace).
- Everything else the worker prints goes to `/dev/null`; workers log to their own files.
- Pitfall: a worker that always exits 0 with N > 0 while doing nothing chains forever and the breaker never fires.
  `result_stream: none` is the kill switch; `ttl` and `concurrency` bound the damage.

## Hint sources

**Redis.** `PSUBSCRIBE <channel_prefix>*`; the channel suffix is the pool `channel` (defaults to the pool name), the
payload is the count as an integer. Unparsable payloads are dropped and logged. The client reconnects with exponential
backoff between `reconnect_min` and `reconnect_max`. Pub/sub is fire-and-forget: a message published while doorbell is
disconnected is lost, which is what `poke` is for.

```sh
redis-cli PUBLISH jobs:encoding 100      # pool "encoding", about 100 jobs waiting
```

**HTTP.** `POST <hint_path>` with a JSON object of the same shape as the Redis messages. Keys must carry the
`redis.channel_prefix`; several pools may be hinted in one request. The whole body is validated first, so a request is
either fully accepted or fully rejected.

```sh
curl -X POST 127.0.0.1:8080/hint \
  -H 'Content-Type: application/json' \
  -d '{"jobs:encoding": 100, "jobs:mail": 3}'
```

| Status | Meaning                                                 |
|--------|---------------------------------------------------------|
| 202    | accepted                                                |
| 400    | bad json, empty body or negative count                  |
| 404    | unknown pool, disabled pool or key without the prefix   |
| 503    | doorbell is shutting down                               |

At least one source must be enabled.

## Endpoints

All on `http.addr` (default `127.0.0.1:8080`):

- `POST /hint` - hint source, see above.
- `GET /healthz` - `200` with a snapshot per enabled pool:

  ```json
  {
    "status": "ok",
    "pools": {
      "encoding": {
        "running": 1,
        "concurrency": 4,
        "last_hint": "2026-09-08T10:00:00Z",
        "last_exit": "2026-09-08T09:59:50Z",
        "last_exit_code": 0,
        "last_tasks": 12,
        "breaker": { "open_until": null, "level": 0 }
      }
    }
  }
  ```

- `GET /metrics` - Prometheus text format. Served on the main listener, or on `metrics.addr` when set (then the main
  listener answers 404 there). Nothing is registered when `metrics.enabled: false`.

Paths are configurable via `http.hint_path`, `http.health_path`, `http.metrics_path`.

## Config reference

Every key with its built-in default and a one-line explanation is in
[`test/testdata/config/full.yaml`](test/testdata/config/full.yaml).

Precedence: built-in default, then `pools._defaults`, then the pool block. `env` merges across layers, lists replace.
Unknown keys are an error. Durations are strings such as `500ms`, `30s`, `5m`; a bare `0` is allowed.

Validation rules: at least one enabled pool; `concurrency >= 1`; `command` non-empty; `poke_count <= concurrency`;
`exit_failure_threshold >= 0`; `exit_failure_window > 0`; `exit_cooldown_multiplier >= 1`;
`exit_cooldown_max >= exit_cooldown`; `ok_exit_codes` non-empty with each code in 0..255; known signal name;
`result_stream` is `stdout`, `stderr` or `none`; `http.addr` required when http is enabled, `redis.addr` when redis is
enabled; http paths start with `/` and are distinct; pool `channel` unique; `command` and `channel` are rejected in
`_defaults`.

## Failure breaker

The breaker rate-limits process churn. It never judges pool health and never touches a running worker.

Every exit is classified under the pool lock:

- `ok`: exit code in `ok_exit_codes` (default `[0]`).
- `killed`: terminated by doorbell (ttl or shutdown). Neutral: neither trips nor resets the breaker.
- `failure`: any other exit code, death by an external signal, or a spawn error (the command could not be started;
  counts as an instant exit).

Doorbell keeps the timestamps of the last `exit_failure_threshold` failures. When all of them fall inside
`exit_failure_window` the pool trips:

```
open_until = now + min(exit_cooldown * exit_cooldown_multiplier ^ level, exit_cooldown_max)
level      = level + 1
```

While open, hints and pokes spawn nothing and hints are dropped (`reason=exit_cooldown`). One error line is logged per
trip, `cooldown_trips_total` is incremented and `cooldown_active` is 1.

`level` resets to 0 on an `ok` exit that happens while the pool is not tripped and no failure lies inside the window,
and after `exit_cooldown_max` has passed untripped. `exit_failure_threshold: 0` or `exit_cooldown: 0` disables the
breaker.

With the defaults (threshold 3, window 10s, cooldown 5s x2 up to 5m):

- **Bad command** (exec fails instantly): three failures trip at once. Steady state is three failed execs and one error
  line per 5 minutes.
- **Starts then dies** (database down): the pool trips within the first hint; worst case four short processes per 5
  minutes. The first ok exit after recovery resets the level.
- **One poisoned job** next to good workers: the fast failures trip the pool, the long-running good worker is left
  alone. Hints for good jobs wait up to `exit_cooldown_max`. Tune `exit_cooldown_max` or the threshold per pool; the real
  fix is the worker marking the poisoned job as failed.

### Use `poke` with the breaker

Dropped hints are gone. Once the cooldown expires nothing spawns until the next notification arrives, and if the
producer only publishes when it enqueues, a pool with a backlog can sit idle. Set `poke` (for example `1m`) on any pool
that relies on the breaker: doorbell then hints itself `poke_count` at that rate and work resumes after the pause without
an external hint.

`poke` is also the answer to lost Redis messages. There is deliberately no idle floor: a worker that exits 0 on an empty
queue would otherwise be respawned in a hot loop.

## Shutdown

On SIGINT or SIGTERM doorbell stops the hint sources, closes the HTTP server (`http.shutdown_timeout`), then for every
running process sends `term_signal`, waits `grace_shutdown`, and SIGKILLs the process group. The whole sequence is bounded
by the top-level `shutdown_timeout`. A worker that ignores the signal is still gone when doorbell exits.

## Logging

One slog line per event, `msg` is the event name: `spawn`, `spawn_error`, `exit`, `hint`, `drop`, `ttl_kill`,
`shutdown_kill`, `shutdown`, `breaker_trip`, `hint_rejected`, `result_ignored` (debug). Fields: `pool`, `pid`, `code`, `source`,
`channel`, `count`, `reason`, `result`, `tasks`, `duration`, `signal`, `breaker_level`.

`log.format` is `text` or `json`; `log.level` is `debug`, `info`, `warn` or `error`; `log.timestamp_format` is a Go
layout or `unix`. Worker output is not forwarded; workers log to their own files.

## Metrics

Prefix `<ns>` is `metrics.namespace` (default `doorbell`). Everything is in memory and resets on restart. Every per-pool
series is pre-initialised at startup so zeros are visible before the first event.

| Metric                                            | Labels                | Notes                                      |
|---------------------------------------------------|-----------------------|--------------------------------------------|
| `<ns>_build_info`                                 | `version`             |                                            |
| `<ns>_concurrency`                                | `pool`                | from config                                |
| `<ns>_running`                                    | `pool`                |                                            |
| `<ns>_last_hint_timestamp`                        | `pool`                | unix seconds                               |
| `<ns>_last_exit_timestamp`                        | `pool`                | unix seconds                               |
| `<ns>_spawns_total`                               | `pool`                |                                            |
| `<ns>_spawn_errors_total`                         | `pool`                |                                            |
| `<ns>_exits_total`                                | `pool,result,code`    | result ok/failure/killed; code, signal, or ttl/shutdown |
| `<ns>_kills_total`                                | `pool,reason`         | reason ttl/shutdown                        |
| `<ns>_process_duration_seconds`                   | `pool,result`         | histogram                                  |
| `<ns>_tasks_processed_total`                      | `pool`                | sum of worker-reported counts              |
| `<ns>_cooldown_active`                            | `pool`                | 0/1                                        |
| `<ns>_cooldown_level`                             | `pool`                |                                            |
| `<ns>_cooldown_open_until_timestamp`              | `pool`                | 0 when closed                              |
| `<ns>_cooldown_trips_total`                       | `pool`                |                                            |
| `<ns>_hints_total`                                | `source,pool`         | source redis/http/poke/worker              |
| `<ns>_dropped_hints_total`                        | `source,pool,reason`  | reason exit_cooldown/unknown_pool/disabled/bad_payload/shutdown |
| `<ns>_redis_connected`                            |                       | 0/1                                        |
| `<ns>_redis_reconnects_total`                     |                       |                                            |

Default `go_*` and `process_*` collectors stay on.

## Deployment

Run doorbell itself under a supervisor such as systemd or Docker so that it is restarted.

### Binaries

Static binaries (linux/darwin, amd64/arm64) are attached to every
[GitHub Release](https://github.com/lezhnev74/doorbell-pm/releases) with a `checksums.txt`:

```sh
curl -fsSLO https://github.com/lezhnev74/doorbell-pm/releases/download/v1.2.3/doorbell-pm_v1.2.3_linux_amd64.tar.gz
tar -xzf doorbell-pm_v1.2.3_linux_amd64.tar.gz && install doorbell-pm /usr/local/bin/
doorbell-pm run --config doorbell.yaml
```

`make dist` builds the same tarballs locally into `dist/`.

### Docker

The [`Dockerfile`](Dockerfile) builds a static binary into a distroless image. The image carries no shell and no PHP,
so it is only useful when the worker command is available in the image; copy or bind-mount the worker into it or use the
image as a base.

The working directory is `/app`, owned by `nonroot`, and the config is expected at `/app/doorbell.yaml`. Relative
paths in the config resolve against `/app`.

Prebuilt images (linux/amd64, linux/arm64) are published to GHCR on every release tag:

```sh
docker pull ghcr.io/lezhnev74/doorbell-pm:latest      # or :1, :1.2, :1.2.3
docker run --rm -v $PWD/doorbell.yaml:/app/doorbell.yaml:ro -p 8080:8080 ghcr.io/lezhnev74/doorbell-pm
```

Or build locally:

```sh
make image    # ghcr.io/lezhnev74/doorbell-pm:<git describe>, also tagged :latest
docker run --rm -v $PWD/doorbell.yaml:/app/doorbell.yaml:ro -p 8080:8080 ghcr.io/lezhnev74/doorbell-pm
```

Set `http.addr` to `0.0.0.0:8080` in a container. Stop with `docker stop -t <shutdown_timeout+5>` so the grace period
is honoured.

### docker compose

Workers need a runtime, so the usual layout is a worker image with the doorbell binary copied in from the GHCR image:

```dockerfile
# Dockerfile.worker
FROM ghcr.io/lezhnev74/doorbell-pm:1 AS doorbell
FROM php:8.4-cli
COPY --from=doorbell /usr/local/bin/doorbell-pm /usr/local/bin/doorbell-pm
WORKDIR /app
COPY . .
ENTRYPOINT ["doorbell-pm"]
CMD ["run", "--config", "/app/doorbell.yaml"]
```

```yaml
# compose.yaml
services:
  redis:
    image: redis:7-alpine

  doorbell:
    build:
      context: .
      dockerfile: Dockerfile.worker
    depends_on: [ redis ]
    restart: unless-stopped
    stop_grace_period: 65s              # shutdown_timeout + 5s
    environment:
      REDIS_PASSWORD: ${REDIS_PASSWORD:-}
    ports:
      - "8080:8080"                     # /hint, /healthz, /metrics
```

```yaml
# doorbell.yaml
http:
  addr: 0.0.0.0:8080
redis:
  addr: redis:6379
  password: "${REDIS_PASSWORD}"
shutdown_timeout: 60s
pools:
  encoding:
    command: [ php, worker.php, --queue=encoding ]
    concurrency: 4
```

`docker compose up -d --build`, then `redis-cli -h <host> PUBLISH jobs:encoding 10` or `curl -XPOST localhost:8080/hint
-d '{"jobs:encoding":10}'` to see workers appear.

## Development

```sh
make test     # go test -race ./...
make e2e      # needs redis-server or docker on PATH
make lint     # golangci-lint, if installed
make release VERSION=v1.2.3   # tag + push; CI publishes the image
make qa       # CRAP <= 6 per function, total coverage >= 85%
```

CI runs all four on every push and pull request. `make qa` is the quality gate: every function must score
[CRAP](https://testing.googleblog.com/2011/02/this-code-is-crap.html) (Change Risk Anti-Patterns,
`cyclo^2 * (1 - coverage)^3 + cyclo`) at most 6, and total statement coverage must stay at or above 85%. Since CRAP is
never below cyclomatic complexity, the bound means no function branches more than six ways, and a function with five
branches needs at least two thirds of its statements covered. The point is not the number: small functions with one
job are easy to read, easy to test and easy to change, and coverage tells you which one you forgot. Unit tests use fake
clocks and fake spawners so they run in milliseconds and never flake; e2e tests drive the real binary against a real
redis and cover the wiring the unit tests cannot. When a change trips the gate, split the function or write the test,
do not raise the threshold.

## License

[MIT](LICENSE)
