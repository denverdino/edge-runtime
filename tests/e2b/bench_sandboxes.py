#!/usr/bin/env python3
"""Measures how fast the adapter creates sandboxes, under real SDK traffic.

Each iteration creates a sandbox through the official E2B SDK, runs `1 + 2` in
it, checks the answer, and kills it. Sandboxes are killed as they go, so
`--count` can far exceed `MAX_CONCURRENT_SANDBOXES` (128) while at most
`--concurrency` are alive at once.

The snippet is deliberately trivial so that the timings describe sandbox
lifecycle cost, not the guest's work. The answer is still checked, so an
iteration that failed to compute is reported as an error rather than as a fast
iteration.

Compute goes through `POST /execute` because no E2B SDK method reaches it — see
the module docstring in `test_sdk.py`.

Requires the SDK and a running adapter:

    pip install e2b
    E2B_API_KEY=e2b_0000000000000000000000000000000000000000 \
      ./target/debug/edge-runtime start --main-service ./examples/e2b-adapter \
      -p 9998
    python3 tests/e2b/bench_sandboxes.py --count 1000 --concurrency 16
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from dataclasses import dataclass
from dataclasses import field

try:
    from e2b import Sandbox
except ImportError:  # pragma: no cover - reported by main()
    Sandbox = None  # type: ignore[assignment]

BASE_URL = os.environ.get("E2B_BASE_URL", "http://localhost:9998")
# Older SDK versions reject anything not shaped like `e2b_` + hex client-side.
API_KEY = os.environ.get("E2B_API_KEY", "e2b_" + "0" * 40)


@dataclass
class Sample:
    index: int
    create_ms: float | None = None
    execute_ms: float | None = None
    kill_ms: float | None = None
    error: str | None = None


@dataclass
class Progress:
    total: int
    started: float
    lock: threading.Lock = field(default_factory=threading.Lock)
    done: int = 0
    failed: int = 0

    def record(self, sample: Sample) -> None:
        with self.lock:
            self.done += 1
            if sample.error is not None:
                self.failed += 1
            if self.done % 100 and self.done != self.total:
                return
            elapsed = time.perf_counter() - self.started
            rate = self.done / elapsed if elapsed else 0.0
            print(
                f"  {self.done}/{self.total}"
                f"  failed={self.failed}"
                f"  {elapsed:.1f}s"
                f"  {rate:.1f}/s",
                flush=True,
            )


CODE = "1 + 2"
EXPECTED = 3


def execute(sandbox_id: str, code: str, timeout: float) -> tuple[int, dict]:
    payload = {"code": code, "context_id": sandbox_id, "language": "javascript"}
    request = urllib.request.Request(
        f"{BASE_URL}/execute",
        data=json.dumps(payload).encode(),
        method="POST",
    )
    request.add_header("content-type", "application/json")
    request.add_header("x-api-key", API_KEY)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        raw = error.read()
        return error.code, json.loads(raw) if raw else {}


METRICS_FIELDS = (
    "rollingLifecycleWindow",
    "lifetimeFinalWorkerTotals",
    "capacity",
    "creationGate",
    "globalExecutionQueue",
    "activeWorkerHeap",
)


def fetch_metrics(timeout: float) -> dict:
    request = urllib.request.Request(f"{BASE_URL}/internal/metrics", method="GET")
    request.add_header("x-api-key", API_KEY)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        metrics = json.loads(response.read())
    if not isinstance(metrics, dict):
        raise ValueError("metrics response is not a JSON object")
    return {field: metrics[field] for field in METRICS_FIELDS if field in metrics}


def report_metrics(phase: str, timeout: float) -> None:
    try:
        metrics = fetch_metrics(timeout)
    except (OSError, ValueError) as error:
        print(f"\nmetrics {phase}: unavailable ({describe_error(error)})")
        return
    print(f"\nmetrics {phase} (outside measured wall time):")
    print(json.dumps(metrics, sort_keys=True))


def describe_error(error: Exception) -> str:
    """Groups failures by cause, so the histogram stays readable at 1000 runs."""
    text = str(error).strip().splitlines()
    head = text[0] if text else ""
    return f"{type(error).__name__}: {head[:120]}"


def run_one(index: int, options: argparse.Namespace) -> Sample:
    sample = Sample(index=index)

    box = None
    try:
        started = time.perf_counter()
        box = Sandbox.create(
            template="base",
            api_key=API_KEY,
            api_url=BASE_URL,
            timeout=options.timeout,
        )
        sample.create_ms = (time.perf_counter() - started) * 1000

        started = time.perf_counter()
        status, body = execute(box.sandbox_id, CODE, options.request_timeout)
        sample.execute_ms = (time.perf_counter() - started) * 1000

        if status != 200:
            raise AssertionError(f"execute returned {status}: {body}")
        if body.get("error") is not None:
            raise AssertionError(f"execute failed: {body['error']}")
        if body.get("result") != EXPECTED:
            raise AssertionError(f"wrong answer: {body.get('result')} != {EXPECTED}")
    except Exception as error:  # noqa: BLE001 - a benchmark counts failures
        sample.error = describe_error(error)
    finally:
        if box is not None:
            started = time.perf_counter()
            try:
                box.kill()
                sample.kill_ms = (time.perf_counter() - started) * 1000
            except Exception as error:  # noqa: BLE001 - a leak is worth reporting
                if sample.error is None:
                    sample.error = describe_error(error)
    return sample


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank, so every reported number is one an iteration really took."""
    rank = max(0, min(len(values) - 1, round(fraction * len(values) + 0.5) - 1))
    return values[rank]


def report_phase(name: str, samples: list[float]) -> None:
    if not samples:
        print(f"{name:<10}{'no successful samples':>40}")
        return
    ordered = sorted(samples)
    mean = sum(ordered) / len(ordered)
    print(
        f"{name:<10}"
        f"{percentile(ordered, 0.50):>9.1f}"
        f"{percentile(ordered, 0.90):>9.1f}"
        f"{percentile(ordered, 0.99):>9.1f}"
        f"{ordered[-1]:>9.1f}"
        f"{mean:>9.1f}"
    )


def report(samples: list[Sample], wall: float, options: argparse.Namespace) -> None:
    ok = [s for s in samples if s.error is None]
    print(
        f"\ncount={len(samples)} concurrency={options.concurrency}"
        f" ok={len(ok)} failed={len(samples) - len(ok)}"
    )
    rate = len(samples) / wall if wall else 0.0
    print(f"wall={wall:.1f}s  throughput={rate:.1f} sandboxes/s")

    print(f"\n{'phase':<10}{'p50':>9}{'p90':>9}{'p99':>9}{'max':>9}{'mean':>9}   (ms)")
    # Only successful iterations are timed: a create that raised has no duration,
    # and an execute after a failed create never ran.
    report_phase("create", [s.create_ms for s in ok if s.create_ms is not None])
    report_phase("execute", [s.execute_ms for s in ok if s.execute_ms is not None])
    report_phase("kill", [s.kill_ms for s in ok if s.kill_ms is not None])

    failures: dict[str, int] = {}
    for sample in samples:
        if sample.error is not None:
            failures[sample.error] = failures.get(sample.error, 0) + 1
    if failures:
        print("\nerrors:")
        for message, hits in sorted(failures.items(), key=lambda i: -i[1]):
            print(f"  {hits:>5}  {message}")


def run_phase(
    total: int,
    options: argparse.Namespace,
    *,
    stop_on_error: bool = False,
) -> list[Sample]:
    progress = Progress(total=total, started=time.perf_counter())
    samples: list[Sample] = []
    next_index = 0
    pending = set()

    with ThreadPoolExecutor(max_workers=options.concurrency) as pool:
        while next_index < total and len(pending) < options.concurrency:
            pending.add(pool.submit(run_one, next_index, options))
            next_index += 1

        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            completed = [future.result() for future in done]
            for sample in completed:
                progress.record(sample)
                samples.append(sample)

            if stop_on_error and any(sample.error is not None for sample in completed):
                for future in pending:
                    future.cancel()
                return samples

            while next_index < total and len(pending) < options.concurrency:
                pending.add(pool.submit(run_one, next_index, options))
                next_index += 1

    return samples


def main() -> int:
    global BASE_URL

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="sandbox TTL in seconds, long enough to outlive one iteration",
    )
    parser.add_argument("--request-timeout", type=float, default=60.0)
    parser.add_argument("--base-url", default=BASE_URL)
    options = parser.parse_args()
    BASE_URL = options.base_url

    if options.warmup < 0:
        print("--warmup must be non-negative")
        return 2

    if Sandbox is None:
        print("the official E2B SDK is missing: pip install e2b")
        return 2

    probe = urllib.request.Request(f"{BASE_URL}/sandboxes", method="GET")
    probe.add_header("x-api-key", API_KEY)
    try:
        urllib.request.urlopen(probe, timeout=5)
    except urllib.error.HTTPError as error:
        if error.code == 401:
            print(f"adapter rejected E2B_API_KEY={API_KEY!r}; set it to match")
            return 2
    except OSError as error:
        print(f"cannot reach adapter at {BASE_URL}: {error}")
        print("start it first - see tests/e2b/README.md")
        return 2

    if options.warmup:
        print(
            f"warming {options.warmup} sandboxes against {BASE_URL}"
            f" at concurrency {options.concurrency}"
        )
        warmup_samples = run_phase(options.warmup, options, stop_on_error=True)
        if any(sample.error is not None for sample in warmup_samples):
            print("warmup failed; skipping measurement")
            return 1
        report_metrics("after warmup", options.request_timeout)

    print(
        f"creating {options.count} sandboxes against {BASE_URL}"
        f" at concurrency {options.concurrency}"
    )
    started = time.perf_counter()
    samples = run_phase(options.count, options)
    wall = time.perf_counter() - started

    report(samples, wall, options)
    report_metrics("after measurement", options.request_timeout)
    return 0 if all(s.error is None for s in samples) else 1


if __name__ == "__main__":
    sys.exit(main())
