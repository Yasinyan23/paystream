# Benchmarks

First load-test results for the compose stack, produced with `scripts/loadtest.py`. One laptop runs the stack, the load generator and Docker Desktop at the same time, so read the numbers as an order of magnitude, not as a capacity limit.

## Environment

| | |
|---|---|
| Date | 2026-10-08 |
| Code | commit `c09a6ef` (`main`) |
| Machine | MacBook Pro (Mac17,8), Apple M5 Pro, 18 cores (6 Super + 12 Performance), 24 GB RAM, macOS 26.4, on battery power |
| Docker | Docker Desktop 4.84.0 (Engine 29.6.2, Compose 5.3.1); Linux VM with 18 CPUs and 7.75 GiB RAM; no per-container limits |
| Stack | the shipped `docker-compose.yml`: PostgreSQL 18.6 and RabbitMQ 4.3.6 with default settings (`payments.new` is a quorum queue), one `uvicorn` process for the API, one outbox relay, `CONSUMER_PREFETCH=10`, tracing off |
| Load generator | `scripts/loadtest.py` on the macOS host, reaching the API at `localhost:8000` through Docker Desktop's port forwarding |
| Start state | stack started with `make up`, empty database |

## Results

| Scenario | Accept p50 / p95 | End-to-end p50 / p95 | Throughput |
|---|---|---|---|
| Default gateway (2–5 s), 1 consumer, 500 payments, 50 concurrent | 76.0 ms / 154.3 ms | 90.9 s / 169.2 s | 2.8 payments/s |
| Fast gateway (50–100 ms), 3 consumers, 1000 payments, 100 concurrent | 308.7 ms / 2476.1 ms | 0.11 s / 0.24 s | 148 payments/s |

- **Accept** is what the script prints: the time from sending `POST /api/v1/payments` until the `202`, on the client clock, through the host's port forwarding.
- **End-to-end** is `processed_at - created_at` read from the database: from the moment the API inserted the payment until the gateway result was stored. **Throughput** is the payments divided by the time from the first insert to the last result. Postgres stamps both, so they contain no polling or network time. The script prints its own figures for these two (below); they include polling delay and are not comparable, see [Reading the script's end-to-end figures](#reading-the-scripts-end-to-end-figures).
- Every payment reached a final status (run 1: 445 succeeded, 55 declined; run 2: 907 and 93), the outbox was fully published and `payments.dead` stayed empty. The API answered every request with a success status: 500 and 1000 `202`s, 39,055 and 1,000 polling `GET`s with `200` (its own `paystream_http_requests_total`).

## Run 1: default gateway

```bash
make load    # 500 payments, 50 concurrent; gateway 2-5 s, one consumer
```

```
payments: 500, concurrency: 50
accept  p50    76.0 ms   p95   154.3 ms   p99   188.8 ms   rate   571.7 req/s
e2e     p50   92.53 s    p95  170.06 s    mean 90.84 s   throughput    2.8 payments/s
```

Server side: end-to-end p50 90.90 s, p95 169.16 s; 2.8 payments/s. All 500 payments were accepted within 0.8 s and the last result arrived 179.6 s after the first insert.

One consumer with prefetch 10 runs at most 10 gateway calls at a time. With a mean call of 3.5 s that is a ceiling of about 2.9 payments/s, and the run measured 2.8, so end-to-end time here is queue wait: a payment waits for the ones accepted before it. No container used a full core (`docker stats` every ~2.5 s, % of one core, mean / peak): API 31 / 63, PostgreSQL 8 / 27, RabbitMQ 19 / 82, consumer 1 / 2.

## Run 2: fast gateway, 3 consumers

```bash
GATEWAY_MIN_DELAY=0.05 GATEWAY_MAX_DELAY=0.1 docker compose up -d --scale consumer=3
uv run python scripts/loadtest.py --payments 1000 --concurrency 100
```

Same stack and database as run 1, containers recreated with the new environment. The load started once `/ready` answered and all three consumers were attached to `payments.new`.

```
payments: 1000, concurrency: 100
accept  p50   308.7 ms   p95  2476.1 ms   p99  3930.0 ms   rate   149.5 req/s
e2e     p50    7.24 s    p95   10.10 s    mean  7.59 s   throughput   77.3 payments/s
```

Server side: end-to-end p50 0.11 s, p95 0.24 s; 148 payments/s. The pipeline kept up with the API: the last result was stored 0.27 s after the last payment was accepted (6.49 s accept phase, 6.76 s in total), so the throughput of this run is the accept rate. The accept rate and its 2.5 s p95 tail are what the host path limits: the same script run inside the `api` container accepts 642 req/s with a p95 of 326 ms (see below). The host-side accept numbers at 100 connections say more about this laptop's way into Docker Desktop than about the API.

## Reading the script's end-to-end figures

The script cannot see when a payment finishes; it finds out by polling `GET /api/v1/payments/{id}`. Its end-to-end time for a payment runs from the `202` until the first poll that sees a final status, so it includes:

- the 0.25 s sleep between polling passes **plus the duration of a pass**. A pass sends one `GET` per pending payment, at most `--concurrency` at a time. Measured on the idle stack with the same loop: 500 payments at 50 concurrent took 0.6–2.3 s from the host (8 passes) and 0.5–0.9 s from inside the `api` container (4 passes); 1000 payments at 100 concurrent took 1.4–5.8 s from the host (4 passes) and 0.9–4.9 s inside (4 passes). While hundreds of payments are pending, a polling cycle therefore takes roughly 0.7–6 s, not 0.25 s.
- in run 2, the time the other payments were still being accepted. Polling starts only after the last `POST` returned (6.5 s), so a payment accepted in the first second and finished within 0.1 s is credited with about 6 s. All 1000 payments were already final at the first pass: the API served exactly 1000 polling `GET`s.

In run 1 this is noise: the script's figures are 0.6 s above the server-side mean (90.84 s against 90.28 s) and at most 1.8 % above either percentile. In run 2 it is the whole number: the script's p50 was 7.24 s, 5.46 s and 4.10 s in three identical runs while the server-side p50 stayed at 0.10–0.11 s. The script's throughput also counts the polling tail after the last payment finished: 77.3 payments/s in run 2 against 148 server side.

## Supporting runs

Fast profile (3 consumers) on the same stack, between 00:03 and 00:08 UTC. "Script" columns are the script's own output, "server" columns come from the database.

| Run | Accept p50 / p95 / rate (script) | End-to-end p50 / p95 (script) | End-to-end p50 / p95 (server) | Throughput (server) |
|---|---|---|---|---|
| Run 2 again | 308.4 ms / 2476.9 ms / 148.9 req/s | 5.46 s / 7.60 s | 0.10 s / 0.31 s | 148.2 payments/s |
| Run 2 again, without the `docker stats` sampler | 280.6 ms / 2362.6 ms / 154.0 req/s | 4.10 s / 6.95 s | 0.11 s / 0.31 s | 152.3 payments/s |
| 1000 payments, 50 concurrent, from the host | 97.5 ms / 171.2 ms / 475.6 req/s | 3.79 s / 5.05 s | 0.37 s / 0.77 s | 347.3 payments/s |
| 1000 payments, 100 concurrent, script run inside the `api` container | 148.3 ms / 326.1 ms / 641.8 req/s | 3.56 s / 5.04 s | 0.70 s / 1.23 s | 357.2 payments/s |

- The accept numbers of run 2 repeat to within about 10 %, so the ceiling at 100 connections from the host is a property of the setup, not warm-up noise. A `docker stats` sampler ran during every run except the last repeat, which gave the same accept numbers, so the sampler does not distort them.
- At 50 concurrent the host reaches 476 req/s with a p95 of 171 ms, and the stack completes 1000 payments in under 3 s.
- Past roughly 350 payments/s the processing side no longer keeps up: end-to-end throughput stays at 347–357 payments/s in the last two rows while the server-side end-to-end time grows with the accept rate (p50 0.37 s at 476 req/s, 0.70 s at 642 req/s).

## Reproduce

```bash
make up                                  # fresh stack, empty database
FROM=$(date -u +%FT%TZ); make load; TO=$(date -u +%FT%TZ)

# Server-side figures. Run the query before `make down`, which deletes the database volume.
docker compose exec -T postgres psql -U paystream -d paystream -v from="$FROM" -v to="$TO" <<'SQL'
select count(*) as payments,
       round(percentile_cont(0.5)  within group (order by extract(epoch from processed_at - created_at))::numeric, 2) as e2e_p50_s,
       round(percentile_cont(0.95) within group (order by extract(epoch from processed_at - created_at))::numeric, 2) as e2e_p95_s,
       round((count(*) / extract(epoch from max(processed_at) - min(created_at)))::numeric, 1) as payments_per_s
from payments
where created_at between :'from' and :'to' and status <> 'pending';
SQL

GATEWAY_MIN_DELAY=0.05 GATEWAY_MAX_DELAY=0.1 docker compose up -d --scale consumer=3
until curl -sf localhost:8000/ready >/dev/null; do sleep 1; done
FROM=$(date -u +%FT%TZ); uv run python scripts/loadtest.py --payments 1000 --concurrency 100; TO=$(date -u +%FT%TZ)
# run the same query again
make down
```
