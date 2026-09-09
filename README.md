# Doorbell Process Manager (`doorbell-pm`)

Most job runners want to own your queue, your retries and your worker code. Doorbell wants none of it. It is a small,
predictable process spawner: your app publishes "there is work" and doorbell starts up to N copies of the command you
configured for that pool. Workers claim jobs themselves and quit when the queue is empty, so the process count follows
demand but never exceeds your cap. One YAML file, one hint endpoint, zero framework lock-in - pair it with any PHP or
other app and any queue backend you already have.

A deliberately dumb process spawner. It listens for work notifications (doorbells) over Redis pub/sub or HTTP and
spawns worker processes up to a per-pool concurrency. Workers claim their own jobs from the database and exit whenever
they decide to. Doorbell does not know what a job is, does not retry, and never restarts a worker on its own.

Design notes live in [`orchestrator.md`](orchestrator.md); the step-by-step build log in
[`dev_docs/implementation-plan.md`](dev_docs/implementation-plan.md).

## How it works

```
Redis PUBLISH jobs:encoding 100      or      POST /hint {"jobs:encoding": 100}
        |
        v
doorbell: running(encoding) = 1, concurrency = 4
        -> spawn min(100, 4 - 1) = 3 processes with the pool's command
        |
        v
workers claim jobs themselves and exit when done
```

Spawn rule per hint: `spawn = min(hint, concurrency - running)`. The hint is approximate; over- or under-spawning is
fine because idle workers find no jobs and exit.

### Worker contract

A worker may print the number of tasks it processed as its last non-empty line on the pool's `result_stream` (default
`stdout`): a non-negative integer of at most 64 bytes, anything else is ignored. On an `ok` exit with N > 0 doorbell
hints the pool for N more workers (`source=worker`), so a worker that quit voluntarily (`--max-jobs`, quota) keeps a busy
queue draining; exit 0 with `0` means "no work". A trailing log line on that stream silently disables the hint for that
exit (`result_ignored` at debug is the only trace). Both child streams otherwise go to `/dev/null`. A worker that always
exits 0 with N > 0 while doing nothing chains forever and the breaker never sees a failure; `result_stream: none` is the
kill switch, `ttl` and `concurrency` bound the damage.

Per pool doorbell also:

- classifies every exit as `ok`, `failure` or `killed` and rate-limits churn with a breaker (below);
- kills a worker after `ttl` (default forever);
- optionally sends itself a hint every `poke` interval so a pool recovers even if a notification was lost;
- on shutdown sends `term_signal`, waits `grace_shutdown`, then SIGKILLs the process group.

## Quick start

```sh
make build
./bin/doorbell-pm -check -config doorbell.yaml     # validate and print the resolved config
./bin/doorbell-pm -config doorbell.yaml
```

Flags: `-config <path>` (required), `-check` (validate, print resolved config, exit), `-version`. Exit code 0 on a clean
stop, 1 on config or runtime error, 2 on bad flags. SIGINT or SIGTERM starts a graceful shutdown.

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

## Hint sources

**Redis.** `PSUBSCRIBE <channel_prefix>*`; the channel suffix is the pool `channel` (defaults to the pool name), the
payload is the count as an integer. Unparsable payloads are dropped and logged. The client reconnects with exponential
backoff between `reconnect_min` and `reconnect_max`. Pub/sub is fire-and-forget: a message published while doorbell is
disconnected is lost, which is what `poke` is for.

**HTTP.** `POST <hint_path>` with a JSON object of the same shape as the Redis messages. Keys must carry the
`redis.channel_prefix`; several pools may be hinted in one request. The whole body is validated first, so a request is
either fully accepted or fully rejected.

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

## Config reference

Built-in default applies when a key is absent; `pools._defaults` overrides it for every pool; a pool block overrides
`_defaults`. `env` merges across layers, lists replace. Unknown keys are an error. Pool names must match
`^[A-Za-z0-9][A-Za-z0-9_.-]*$`; names starting with `_` are reserved. Durations are strings such as `500ms`, `30s`,
`5m`; a bare `0` is allowed.

| Key                                    | Default                         |
|----------------------------------------|---------------------------------|
| `log.format`                           | `text`                          |
| `log.level`                            | `info`                          |
| `log.timestamp_format`                 | `2006-01-02T15:04:05.000Z07:00` |
| `http.enabled`                         | `true`                          |
| `http.addr`                            | `127.0.0.1:8080`                |
| `http.hint_path`                       | `/hint`                         |
| `http.health_path`                     | `/healthz`                      |
| `http.metrics_path`                    | `/metrics`                      |
| `http.read_timeout`                    | `5s`                            |
| `http.write_timeout`                   | `5s`                            |
| `http.shutdown_timeout`                | `5s`                            |
| `redis.enabled`                        | `true`                          |
| `redis.addr`                           | `127.0.0.1:6379`                |
| `redis.username`                       | `""`                            |
| `redis.password`                       | `""`                            |
| `redis.db`                             | `0`                             |
| `redis.tls`                            | `false`                         |
| `redis.channel_prefix`                 | `jobs:`                         |
| `redis.dial_timeout`                   | `5s`                            |
| `redis.reconnect_min`                  | `500ms`                         |
| `redis.reconnect_max`                  | `30s`                           |
| `metrics.enabled`                      | `true`                          |
| `metrics.namespace`                    | `doorbell`                      |
| `metrics.addr`                         | `""` (main listener)            |
| `shutdown_timeout`                     | `60s`                           |
| `pools._defaults.*` / `pools.<name>.*` |                                 |
| `enabled`                              | `true`                          |
| `channel`                              | pool name                       |
| `concurrency`                          | `1`                             |
| `command`                              | required (pool only)            |
| `ok_exit_codes`                        | `[0]`                           |
| `exit_failure_threshold`               | `3`                             |
| `exit_failure_window`                  | `10s`                           |
| `exit_cooldown`                        | `5s`                            |
| `exit_cooldown_max`                    | `5m`                            |
| `exit_cooldown_multiplier`             | `2`                             |
| `ttl`                                  | `0` (forever)                   |
| `poke`                                 | `0` (off)                       |
| `poke_count`                           | `1`                             |
| `grace_shutdown`                       | `30s`                           |
| `term_signal`                          | `SIGTERM`                       |
| `inherit_env`                          | `true`                          |
| `env`                                  | `{}`                            |
| `dir`                                  | `""` (inherit)                  |
| `result_stream`                        | `stdout`                        |

Validation rules: at least one enabled pool; `concurrency >= 1`; `command` non-empty; `poke_count <= concurrency`;
`exit_failure_threshold >= 0`; `exit_failure_window > 0`; `exit_cooldown_multiplier >= 1`;
`exit_cooldown_max >= exit_cooldown`; `ok_exit_codes` non-empty with each code in 0..255; known signal name;
`result_stream` is `stdout`, `stderr` or `none`; `log.child_output` is rejected with a message naming `result_stream`;
`http.addr` required when http is enabled, `redis.addr` when redis is enabled; http paths start with `/` and are
distinct; pool `channel` unique; `command` and `channel` are rejected in `_defaults`.

A full example with every key is in [`test/testdata/config/full.yaml`](test/testdata/config/full.yaml).

## Deployment

Run doorbell itself under systemd or Docker so that it is supervised.

### systemd

[`deploy/doorbell-pm.service`](deploy/doorbell-pm.service):

```sh
useradd -r -s /usr/sbin/nologin doorbell-pm
install -m 0755 bin/doorbell-pm /usr/local/bin/doorbell-pm
install -d -m 0750 -o root -g doorbell-pm /etc/doorbell-pm
install -m 0640 -o root -g doorbell-pm doorbell.yaml /etc/doorbell-pm/doorbell.yaml
install -m 0644 deploy/doorbell-pm.service /etc/systemd/system/doorbell-pm.service
systemctl daemon-reload && systemctl enable --now doorbell-pm
```

The unit sends SIGTERM and waits `TimeoutStopSec`, which must exceed the config `shutdown_timeout`. Doorbell logs to
stderr, so journald collects it. `KillMode=mixed` lets doorbell terminate its own children first.

### Docker

The [`Dockerfile`](Dockerfile) builds a static binary into a distroless image. The image carries no shell and no PHP,
so it is only useful when the worker command is available in the image; copy or bind-mount the worker into it or use the
image as a base.

The working directory is `/app`, owned by `nonroot`, and the config is expected at `/app/doorbell.yaml`. Relative
paths in the config resolve against `/app`.

```sh
docker build -t doorbell-pm .
docker run --rm -v $PWD/doorbell.yaml:/app/doorbell.yaml:ro -p 8080:8080 doorbell-pm
```

Set `http.addr` to `0.0.0.0:8080` in a container. Stop with `docker stop -t <shutdown_timeout+5>` so the grace period
is honoured.

## Development

```sh
make test     # go test -race ./...
make e2e      # needs redis-server or docker on PATH
make lint     # golangci-lint, if installed
```
