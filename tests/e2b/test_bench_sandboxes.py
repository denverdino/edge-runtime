#!/usr/bin/env python3
"""Unit tests for the sandbox lifecycle benchmark."""

from __future__ import annotations

import argparse
import importlib.util
import io
import sys
import unittest
from pathlib import Path
from unittest.mock import call
from unittest.mock import patch


BENCHMARK_PATH = Path(__file__).with_name("bench_sandboxes.py")
SPEC = importlib.util.spec_from_file_location("bench_sandboxes", BENCHMARK_PATH)
assert SPEC is not None
assert SPEC.loader is not None
benchmark = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = benchmark
SPEC.loader.exec_module(benchmark)


class BenchmarkWarmupTest(unittest.TestCase):
    def test_main_excludes_warmup_samples_from_report(self) -> None:
        options = argparse.Namespace(
            count=2,
            concurrency=1,
            timeout=60,
            request_timeout=60.0,
            base_url="http://localhost:9998",
            warmup=3,
        )
        reported_samples = []

        def run_one(index: int, _options: argparse.Namespace) -> benchmark.Sample:
            return benchmark.Sample(index=index)

        def report(
            samples: list[benchmark.Sample],
            _wall: float,
            _options: argparse.Namespace,
        ) -> None:
            reported_samples.extend(samples)

        with patch.object(benchmark.argparse.ArgumentParser, "parse_args", return_value=options):
            with patch.object(benchmark.urllib.request, "urlopen"):
                with patch.object(benchmark, "run_one", side_effect=run_one) as mocked_run:
                    with patch.object(benchmark, "report", side_effect=report):
                        with patch.object(benchmark, "report_metrics") as mocked_metrics:
                            self.assertEqual(benchmark.main(), 0)

        self.assertEqual(mocked_run.call_count, 5)
        self.assertEqual(len(reported_samples), 2)
        self.assertEqual(
            mocked_metrics.call_args_list,
            [call("after warmup", 60.0), call("after measurement", 60.0)],
        )

    def test_main_rejects_negative_warmup(self) -> None:
        options = argparse.Namespace(
            count=2,
            concurrency=1,
            timeout=60,
            request_timeout=60.0,
            base_url="http://localhost:9998",
            warmup=-1,
        )

        with patch.object(benchmark.argparse.ArgumentParser, "parse_args", return_value=options):
            with patch.object(benchmark.urllib.request, "urlopen"):
                with patch.object(
                    benchmark,
                    "run_one",
                    return_value=benchmark.Sample(index=0),
                ):
                    with patch.object(benchmark, "report"):
                        self.assertEqual(benchmark.main(), 2)

    def test_main_skips_measurement_after_warmup_failure(self) -> None:
        options = argparse.Namespace(
            count=2,
            concurrency=1,
            timeout=60,
            request_timeout=60.0,
            base_url="http://localhost:9998",
            warmup=3,
        )

        def run_one(index: int, _options: argparse.Namespace) -> benchmark.Sample:
            return benchmark.Sample(index=index, error="warmup failure")

        with patch.object(benchmark.argparse.ArgumentParser, "parse_args", return_value=options):
            with patch.object(benchmark.urllib.request, "urlopen"):
                with patch.object(benchmark, "run_one", side_effect=run_one) as mocked_run:
                    with patch.object(benchmark, "report") as mocked_report:
                        self.assertEqual(benchmark.main(), 1)

        self.assertEqual(mocked_run.call_count, 1)
        mocked_report.assert_not_called()


class BenchmarkMetricsTest(unittest.TestCase):
    def test_fetch_metrics_authenticates_and_sanitizes(self) -> None:
        class Response:
            def __enter__(self) -> Response:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                return (
                    b'{"capacity":{"active":2},'
                    b'"rollingLifecycleWindow":{"seconds":60},'
                    b'"unexpected":"not benchmark telemetry"}'
                )

        with patch.object(benchmark.urllib.request, "urlopen", return_value=Response()) as urlopen:
            metrics = benchmark.fetch_metrics(7.5)

        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "http://localhost:9998/internal/metrics")
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(request.headers["X-api-key"], benchmark.API_KEY)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 7.5)
        self.assertEqual(
            metrics,
            {
                "capacity": {"active": 2},
                "rollingLifecycleWindow": {"seconds": 60},
            },
        )

    def test_report_metrics_prints_sanitized_snapshot(self) -> None:
        metrics = {
            "capacity": {"active": 2},
            "rollingLifecycleWindow": {"seconds": 60},
        }
        with patch.object(benchmark, "fetch_metrics", return_value=metrics):
            with patch("sys.stdout", new_callable=io.StringIO) as stdout:
                benchmark.report_metrics("after warmup", 1.0)

        self.assertEqual(
            stdout.getvalue(),
            "\nmetrics after warmup (outside measured wall time):\n"
            '{"capacity": {"active": 2}, "rollingLifecycleWindow": {"seconds": 60}}\n',
        )


if __name__ == "__main__":
    unittest.main()
