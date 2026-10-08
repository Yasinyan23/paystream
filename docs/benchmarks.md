# Benchmarks

Load-test results for the compose stack, produced with `scripts/loadtest.py`. The stack, the load generator and Docker Desktop all run on one laptop, so read the numbers as an order of magnitude, not as a capacity limit.

## Results

| Scenario | Accept p50 / p95 | End-to-end p50 / p95 | Throughput |
|---|---|---|---|
| Default gateway (2–5 s), 1 consumer, 500 payments, 50 concurrent | 70.0 ms / 197.7 ms | 90.02 s / 168.11 s | 2.8 payments/s |
| Fast gateway (50–100 ms), 3 consumers, 1000 payments, 100 concurrent | 211.5 ms / 2416.6 ms | 0.20 s / 0.63 s | 160.5 payments/s |

- **Accept** is the client's clock from sending `POST /api/v1/payments` until the `202` arrives.
- **End-to-end** is `processed_at - created_at` of each payment as `GET /api/v1/payments/{id}` returns it: from the API inserting the payment to the consumer storing the gateway result. Postgres stamps both at transaction start, so neither polling nor the network is part of it. **Throughput** is the number of payments divided by the time from the first `created_at` to the last `processed_at`.
- In the first scenario the end-to-end time is queue wait: one consumer is the bottleneck. In the second the pipeline keeps up, and the accept rate is capped by the macOS host's path into Docker Desktop, not by the service; see [Where the host path saturates](#where-the-host-path-saturates).
- All 23,500 payments created for this document (repeats and one aborted attempt included) reached a final status, the outbox was fully published, `payments.dead` stayed empty, and the API answered every request with a success status (`202` for creates, `200` for polls).

## Conditions

| | |
|---|---|
| Date | 2026-10-08, runs between 00:19 and 00:30 UTC |
| Code | `main` at `c0a1105` plus the load-test fix that ships with this file |
| Machine | MacBook Pro (Mac17,8), Apple M5 Pro, 18 cores (6 Super + 12 Performance), 24 GB RAM, macOS 26.4 |
| Power | on battery: 26 % at the first run, 19 % after the last; no thermal warning recorded |
| Docker | Docker Desktop 4.84.0 (Engine 29.6.2, Compose 5.3.1); Linux VM with 18 CPUs and 7.75 GiB RAM; no per-container limits |
| Stack | the shipped `docker-compose.yml`: PostgreSQL 18.6 and RabbitMQ 4.3.6 with default settings (`payments.new` is a quorum queue), one `uvicorn` process for the API, one outbox relay, `CONSUMER_PREFETCH=10`, tracing off |
| Load generator | `scripts/loadtest.py` on the same machine: on the macOS host, reaching `localhost:8000` through Docker Desktop's port forwarding, or where stated inside the `api` container |
| State | `make up` on an empty database; the stack stays up between runs, so a run sees the payments of the runs before it |

## Run 1: default gateway

```bash
make load    # 500 payments, 50 concurrent; gateway 2-5 s, one consumer
```

```
payments: 500, concurrency: 50
accept  p50    70.0 ms   p95   197.7 ms   p99   286.1 ms   rate   496.0 req/s
e2e     p50   90.02 s    p95  168.11 s    mean 89.58 s   throughput    2.8 payments/s
```

One consumer with prefetch 10 runs at most 10 gateway calls at a time. With a mean call of 3.5 s that is a ceiling of about 2.9 payments/s, and the run measured 2.8. All 500 payments were accepted within a second, so a payment's end-to-end time is its wait behind the earlier ones plus its own gateway call; the last one finished after about three minutes. No container used a full core (`docker stats` every ~2.5 s during this run, % of one core, mean / peak): API 27 / 60, PostgreSQL 7 / 21, RabbitMQ 17 / 79, consumer 1 / 3. While it waits, the script polls about 200 `GET`s per second (36,696 in this run); that is read load on the API and the database, but no figure depends on when a poll happens.

## Run 2: fast gateway, 3 consumers

```bash
GATEWAY_MIN_DELAY=0.05 GATEWAY_MAX_DELAY=0.1 docker compose up -d --scale consumer=3
uv run python scripts/loadtest.py --payments 1000 --concurrency 100
```

Same stack and database as run 1, containers recreated with the new environment; the load started once `/ready` answered and all three consumers were attached to `payments.new`.

```
payments: 1000, concurrency: 100
accept  p50   211.5 ms   p95  2416.6 ms   p99  3892.7 ms   rate   161.6 req/s
e2e     p50    0.20 s    p95    0.63 s    mean  0.27 s   throughput  160.5 payments/s
```

End-to-end p50 is 0.20 s against a gateway call of 0.05–0.1 s: the pipeline keeps up with the 161.6 req/s that the API accepts, so the throughput of this run is the accept rate. Four runs of this exact command (this one, one inside the sweep below and two repeats) gave accept p50 211.5–254.0 ms, p95 2416.6–2476.0 ms and 158.9–161.8 req/s; end-to-end p50 0.13–0.20 s and p95 0.32–0.63 s; 152.0–160.5 payments/s. The accept tail is not the API's: see the next section.

## Where the host path saturates

Fast gateway, 3 consumers, 1000 payments per run, on the same stack as run 2. Each row is one run.

| Load generator | Concurrent | Accept p50 / p95 | Accept rate | End-to-end p50 / p95 | Throughput |
|---|---|---|---|---|---|
| macOS host | 25 | 48.9 ms / 184.2 ms | 345.1 req/s | 0.23 s / 0.45 s | 338.6 payments/s |
| macOS host | 50 | 79.4 ms / 291.7 ms | 450.8 req/s | 0.63 s / 1.02 s | 354.2 payments/s |
| macOS host | 65 | 101.1 ms / 189.1 ms | 592.3 req/s | 0.65 s / 1.10 s | 355.7 payments/s |
| macOS host | 70 | 192.0 ms / 1413.6 ms | 173.1 req/s | 0.10 s / 0.32 s | 168.1 payments/s |
| macOS host | 100 (run 2) | 211.5 ms / 2416.6 ms | 161.6 req/s | 0.20 s / 0.63 s | 160.5 payments/s |
| macOS host | 150 | 445.1 ms / 4626.0 ms | 139.3 req/s | 0.11 s / 0.41 s | 138.9 payments/s |
| macOS host | 200 | 398.3 ms / 4234.1 ms | 155.0 req/s | 0.34 s / 0.93 s | 154.5 payments/s |
| `api` container | 25 | 34.1 ms / 44.9 ms | 689.2 req/s | 0.78 s / 1.31 s | 354.8 payments/s |
| `api` container | 50 | 68.4 ms / 129.3 ms | 707.8 req/s | 0.74 s / 1.33 s | 354.5 payments/s |
| `api` container | 100 | 138.1 ms / 329.0 ms | 690.0 req/s | 0.73 s / 1.35 s | 355.9 payments/s |
| `api` container | 150 | 213.3 ms / 521.0 ms | 624.6 req/s | 0.69 s / 1.30 s | 355.3 payments/s |
| `api` container | 200 | 285.1 ms / 586.6 ms | 608.6 req/s | 0.72 s / 1.34 s | 351.1 payments/s |

- From the macOS host the API accepts 345.1–636.5 req/s with an accept p95 of 126.4–291.7 ms up to 65 concurrent connections, and 139.3–173.1 req/s with a p95 of 1.4–4.6 s from 70 on. Runs at 60 and 75 (not in the table) fall on the same sides of the cliff, and two runs at 50 gave 450.8 and 636.5 req/s. Inside the network the same script accepts 608.6–707.8 req/s with a p95 of 44.9–586.6 ms at every level up to 200. Only the place of the load generator differs, so the cliff comes from the host side (Docker Desktop's port forwarding or the macOS client), not from the API. I did not look further.
- Where accepts outpace about 355 payments/s, the processing side is the limit: in every in-network run and in the host runs at 50–65, throughput is 351.1–359.7 payments/s (over a burst of about 3 s, ramp-up and drain included) while accepts run at 450.8–707.8 req/s. A backlog forms and end-to-end latency rises to p50 0.59–0.78 s, p95 1.02–1.35 s. Once the host path caps accepts (70 and up) the pipeline keeps up: throughput equals the accept rate (138.9–168.1 payments/s) and end-to-end is p50 0.10–0.34 s, p95 0.17–0.93 s.
- One attempt at 150 concurrent inside the network aborted while polling (`httpx.RemoteProtocolError`: a connection was closed without a response; the API stayed up and logged no error), and the script has no retry. Four repeats completed with the same numbers within a few percent; the table shows the first of them.

## Reproduce

```bash
make up                                  # fresh stack, empty database
make load                                # run 1

GATEWAY_MIN_DELAY=0.05 GATEWAY_MAX_DELAY=0.1 docker compose up -d --scale consumer=3
until curl -sf localhost:8000/ready >/dev/null; do sleep 1; done
uv run python scripts/loadtest.py --payments 1000 --concurrency 100    # run 2

# concurrency sweep from the host
for c in 25 50 65 70 100 150 200; do uv run python scripts/loadtest.py --payments 1000 --concurrency $c; done

# the same from inside the network (httpx is already in the image; recreating the api container removes the copy)
docker compose cp scripts/loadtest.py api:/tmp/loadtest.py
docker compose exec -T api python /tmp/loadtest.py --base-url http://localhost:8000 --payments 1000 --concurrency 100

make down
```

## Cross-check against the database

The script's figures can be re-derived from Postgres. Run this before `make down`, which deletes the database volume. `FROM` and `TO` are UTC timestamps taken before and after the run (`date -u +%FT%TZ`):

```bash
docker compose exec -T postgres psql -U paystream -d paystream -v from="$FROM" -v to="$TO" <<'SQL'
select count(*) as payments,
       round(percentile_cont(0.5)  within group (order by extract(epoch from processed_at - created_at))::numeric, 2) as e2e_p50_s,
       round(percentile_cont(0.95) within group (order by extract(epoch from processed_at - created_at))::numeric, 2) as e2e_p95_s,
       round(avg(extract(epoch from processed_at - created_at))::numeric, 2) as e2e_mean_s,
       round((count(*) / extract(epoch from max(processed_at) - min(created_at)))::numeric, 1) as payments_per_s,
       count(*) filter (where status = 'succeeded') as succeeded,
       count(*) filter (where status = 'failed') as failed
from payments
where created_at between :'from' and :'to' and status <> 'pending';
SQL
```

Across the 19 runs checked while writing this file the mean and the throughput were identical to the script's. The script's percentiles are nearest-rank and the query's are interpolated, which moved p50 by at most 0.07 s and p95 by at most 0.02 s.
