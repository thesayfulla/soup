import json
import sys
import time


def log(event: str, **fields) -> None:
    """One JSON object per line. Lines are short, so O_APPEND writes from many
    processes into one file don't interleave."""
    rec = {"ts": round(time.time(), 4), "event": event, **fields}
    sys.stdout.write(json.dumps(rec, default=str) + "\n")
    sys.stdout.flush()
