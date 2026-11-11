"""Load profile for the SHIP / DON'T SHIP question.

~100 RPS:  locust -f loadtest/locustfile.py --host http://localhost:8000 \
               --headless -u 100 -r 20 -t 2m --csv runs/load_100rps
~10x:      ... -u 1000 -r 100 ...

Each user waits ~1s between tasks, so N users ≈ N requests/second.
Mix: 50% create (some with a reused key, like a client retry), 30% status,
10% list, 10% cancel.
"""

import random
import uuid

from locust import HttpUser, between, task


class JobClient(HttpUser):
    wait_time = between(0.5, 1.5)

    def on_start(self):
        self.ids: list[str] = []
        self.keys: list[str] = []

    @task(5)
    def create(self):
        if self.keys and random.random() < 0.2:
            key = random.choice(self.keys)  # client retry of an earlier request
        else:
            key = str(uuid.uuid4())
            self.keys.append(key)
        with self.client.post("/jobs", json={"payload": {"k": key}}, headers={"Idempotency-Key": key},
                              name="POST /jobs", catch_response=True) as r:
            if r.status_code in (200, 201):
                self.ids.append(r.json()["id"])
                r.success()

    @task(3)
    def status(self):
        if self.ids:
            self.client.get(f"/jobs/{random.choice(self.ids)}", name="GET /jobs/:id")

    @task(1)
    def list_jobs(self):
        self.client.get("/jobs", params={"limit": 50}, name="GET /jobs")

    @task(1)
    def cancel(self):
        if self.ids:
            with self.client.post(f"/jobs/{random.choice(self.ids)}/cancel", name="POST /jobs/:id/cancel",
                                  catch_response=True) as r:
                if r.status_code in (200, 202, 409):
                    r.success()
