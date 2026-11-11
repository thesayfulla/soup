import pytest
from fastapi.testclient import TestClient

from app import db
from app.api import app
from tests.conftest import TEST_URL


@pytest.fixture
def client():
    with db.connect(TEST_URL) as c:
        db.reset_schema(c)
    with TestClient(app) as tc:
        yield tc


def test_create_is_idempotent(client):
    r1 = client.post("/jobs", json={"payload": {"a": 1}}, headers={"Idempotency-Key": "k1"})
    r2 = client.post("/jobs", json={"payload": {"a": 1}}, headers={"Idempotency-Key": "k1"})
    assert (r1.status_code, r2.status_code) == (201, 200)
    assert r1.json()["id"] == r2.json()["id"]
    r3 = client.post("/jobs", json={"payload": {"a": 2}}, headers={"Idempotency-Key": "k1"})
    assert r3.status_code == 409


def test_missing_idempotency_key_is_rejected(client):
    assert client.post("/jobs", json={}).status_code == 422


def test_get_list_cancel(client):
    ids = [client.post("/jobs", json={}, headers={"Idempotency-Key": f"k{i}"}).json()["id"] for i in range(5)]
    assert client.get(f"/jobs/{ids[0]}").json()["status"] == "queued"
    page = client.get("/jobs", params={"limit": 2}).json()
    assert len(page["items"]) == 2 and page["next_cursor"]
    page2 = client.get("/jobs", params={"limit": 2, "cursor": page["next_cursor"]}).json()
    assert {j["id"] for j in page["items"]}.isdisjoint({j["id"] for j in page2["items"]})

    r = client.post(f"/jobs/{ids[0]}/cancel")
    assert r.status_code == 200 and r.json()["job"]["status"] == "cancelled"
    assert client.post(f"/jobs/{ids[0]}/cancel").status_code == 409
    assert client.get("/jobs/00000000-0000-0000-0000-000000000000").status_code == 404
