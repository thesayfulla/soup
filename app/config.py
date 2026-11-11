"""All tunables come from the environment so the harness can vary them per run."""

import os


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://soup:soup@localhost:55432/soup")

# Lease / timing (seconds). Heartbeat must be well below the lease.
LEASE_SECONDS = _float("SOUP_LEASE_SECONDS", 5.0)
HEARTBEAT_SECONDS = _float("SOUP_HEARTBEAT_SECONDS", 1.0)
POLL_SECONDS = _float("SOUP_POLL_SECONDS", 0.05)
REAP_SECONDS = _float("SOUP_REAP_SECONDS", 1.0)

# Emulated work duration when the payload does not specify `work_ms`.
WORK_MS_MIN = _float("SOUP_WORK_MS_MIN", 20)
WORK_MS_MAX = _float("SOUP_WORK_MS_MAX", 120)

# Postgres kills a session idle inside an open transaction after this long.
# Our transactions are single statements (milliseconds), so 10 s is generous.
IDLE_TX_TIMEOUT_MS = int(os.environ.get("SOUP_IDLE_TX_TIMEOUT_MS", "10000"))

API_POOL_MAX =int(os.environ.get("SOUP_API_POOL_MAX", 20))

# --- Fault injection -------------------------------------------------------
# Comma-separated list of deliberate bugs. Used ONLY to prove the harness can
# detect them. Never set in production.
#   no_fencing      worker writes ignore the fencing token (zombies can write)
#   racy_claim      claim = plain SELECT then UPDATE, no row lock
#   naive_complete  effect inserted unconditionally, status set without guards
#   local_clock     leases computed from the worker's clock (+ SOUP_CLOCK_SKEW_S)
KNOWN_MUTATIONS = {"no_fencing", "racy_claim", "naive_complete", "local_clock"}
MUTATIONS = {m.strip() for m in os.environ.get("SOUP_MUTATION", "").split(",") if m.strip() and m.strip() != "none"}
_unknown = MUTATIONS - KNOWN_MUTATIONS
if _unknown:
    raise SystemExit(f"unknown SOUP_MUTATION values: {sorted(_unknown)}")

# Only used with the local_clock mutation: offset added to this process's clock.
CLOCK_SKEW_S = _float("SOUP_CLOCK_SKEW_S", 0.0)
