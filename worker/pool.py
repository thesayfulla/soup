"""Spawns worker processes, kills them for real (SIGKILL), freezes them
(SIGSTOP/SIGCONT) and respawns them. Shared by the supervisor and the chaos harness."""

from __future__ import annotations

import os
import random
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import IO

ROOT = Path(__file__).resolve().parent.parent


class WorkerPool:
    def __init__(
        self,
        n: int,
        env: dict[str, str] | None = None,
        slot_env: dict[int, dict[str, str]] | None = None,
        log_file: IO | None = None,
    ):
        self.n = n
        self.env = env or {}
        self.slot_env = slot_env or {}
        self.log_file = log_file
        self.procs: dict[int, subprocess.Popen] = {}
        self.gen: dict[int, int] = {}
        self.frozen: set[int] = set()
        self.kills = 0
        self.freezes = 0
        self.spawns = 0
        self._lock = threading.Lock()

    def worker_id(self, slot: int) -> str:
        return f"w{slot}.{self.gen[slot]}"

    def _spawn(self, slot: int) -> None:
        self.gen[slot] = self.gen.get(slot, -1) + 1
        env = {**os.environ, **self.env, **self.slot_env.get(slot, {}), "SOUP_WORKER_ID": self.worker_id(slot)}
        self.procs[slot] = subprocess.Popen(
            [sys.executable, "-m", "worker.worker"],
            cwd=ROOT,
            env=env,
            stdout=self.log_file or None,
            stderr=subprocess.STDOUT if self.log_file else None,
        )
        self.spawns += 1

    def start(self) -> None:
        with self._lock:
            for slot in range(self.n):
                self._spawn(slot)

    def respawn_dead(self) -> list[int]:
        """Respawn any worker that exited on its own (crash)."""
        with self._lock:
            dead = [s for s, p in self.procs.items() if p.poll() is not None]
            for s in dead:
                self._spawn(s)
            return dead

    def kill_random(self, rng: random.Random) -> tuple[int, int, str] | None:
        """kill -9 a random live, non-frozen worker and immediately respawn the slot."""
        with self._lock:
            alive = [s for s, p in self.procs.items() if p.poll() is None and s not in self.frozen]
            if not alive:
                return None
            slot = rng.choice(alive)
            p, wid = self.procs[slot], self.worker_id(slot)
            os.kill(p.pid, signal.SIGKILL)
            p.wait()
            self.kills += 1
            self._spawn(slot)
            return slot, p.pid, wid

    def freeze(self, rng: random.Random, seconds: float) -> tuple[int, str] | None:
        """SIGSTOP a random worker for `seconds`, then SIGCONT it: a zombie that
        comes back after its lease expired."""
        with self._lock:
            alive = [s for s, p in self.procs.items() if p.poll() is None and s not in self.frozen]
            if not alive:
                return None
            slot = rng.choice(alive)
            p = self.procs[slot]
            os.kill(p.pid, signal.SIGSTOP)
            self.frozen.add(slot)
            self.freezes += 1
            wid = self.worker_id(slot)

        def thaw():
            time.sleep(seconds)
            with self._lock:
                if p.poll() is None:
                    try:
                        os.kill(p.pid, signal.SIGCONT)
                    except ProcessLookupError:
                        pass
                self.frozen.discard(slot)

        threading.Thread(target=thaw, daemon=True).start()
        return slot, wid

    def stop(self, timeout: float = 10.0) -> None:
        with self._lock:
            for p in self.procs.values():
                if p.poll() is None:
                    try:
                        os.kill(p.pid, signal.SIGCONT)
                        p.terminate()
                    except ProcessLookupError:
                        pass
            deadline = time.monotonic() + timeout
            for p in self.procs.values():
                try:
                    p.wait(max(0.0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait()
