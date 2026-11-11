import os

TEST_URL = os.environ.get("TEST_DATABASE_URL", "postgresql://soup:soup@localhost:55432/soup_test")
# Must be set before app.config is imported anywhere.
os.environ["DATABASE_URL"] = TEST_URL
os.environ.setdefault("SOUP_LEASE_SECONDS", "5")

import pytest

from app import db


@pytest.fixture(scope="session", autouse=True)
def _database():
    db.ensure_database(TEST_URL)


@pytest.fixture
def conn():
    c = db.connect(TEST_URL)
    db.reset_schema(c)
    yield c
    c.close()


@pytest.fixture
def connect():
    """Factory for extra connections (one per simulated worker/client)."""
    opened = []

    def _make():
        c = db.connect(TEST_URL)
        opened.append(c)
        return c

    yield _make
    for c in opened:
        c.close()
