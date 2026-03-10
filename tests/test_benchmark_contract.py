from __future__ import annotations

import pytest


@pytest.mark.contract
def test_benchmark_runner_exists():
    """
    Contract: benchmark runner script exists once benchmarking starts.
    Suggested: scripts/benchmark_runner.py
    """
    pytest.skip("Enable once benchmark runner is implemented.")


@pytest.mark.contract
def test_benchmark_runs_10x():
    """
    Contract: each query is executed 10 times and mean/std are computed.
    """
    pytest.skip("Enable once benchmark runner produces results.")
