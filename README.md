# soup — training-job queue

## Run

```bash
docker compose up --build          # postgres + api (:8000) + 4 workers
```

```bash
curl -s -XPOST localhost:8000/jobs -H 'Idempotency-Key: demo-1' -H 'content-type: application/json' -d '{"payload":{"lr":0.1}}'
curl -s localhost:8000/jobs/<id>
curl -s 'localhost:8000/jobs?limit=20&status=succeeded'      # follow next_cursor for the next page
curl -s -XPOST localhost:8000/jobs/<id>/cancel
```

## Chaos harness

Runs against its own database (`soup_chaos`) with its own API process and 8 workers:

```bash
docker compose --profile chaos run --rm chaos                       # spec run: 10k jobs, 8 workers, kill -9 ~every 2s
docker compose --profile chaos run --rm chaos --runs 5              # 5 runs, different seeds
docker compose --profile chaos run --rm chaos --jobs 400 --freeze-every 2 --freeze-for 7 \
    --work-ms-min 500 --work-ms-max 2500                            # zombies (SIGSTOP past the lease)
docker compose --profile chaos run --rm chaos --mutation no_fencing --jobs 400 --freeze-every 2 \
    --freeze-for 7 --work-ms-min 500 --work-ms-max 2500             # deliberately broken: must FAIL
```

Postgres crashes (run the harness on the host, since it needs the docker CLI):

```bash
.venv/bin/python -m chaos.run --jobs 6000 --db-crash-every 6 --concurrency 32
```

Raw claim throughput (preloads jobs with SQL, no API):

```bash
.venv/bin/python -m chaos.claim_ceiling --jobs 30000 --workers 8 16 32
```

```bash
.venv/bin/python -m chaos.run --jobs 1000
```

## Load test (100 RPS question)

```bash
docker compose --profile load run --rm locust --headless -u 100 -r 20 -t 2m --csv runs/load_100rps
```

## Layout

```
app/queue.py        every SQL statement that matters: create, claim, heartbeat, finish, reap, cancel
app/api.py          FastAPI endpoints (thin)
worker/worker.py    one worker process; worker/pool.py spawns / kill -9s / freezes them
chaos/run.py        harness; chaos/invariants.py the checks
tests/              deterministic race tests (each forces one interleaving)
migrations/         schema
```

Worker logs over 5 MB are stored gzipped (`workers.log.gz`, read with `zcat`). The content is unchanged.
