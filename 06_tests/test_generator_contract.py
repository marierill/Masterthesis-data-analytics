from __future__ import annotations

import pytest


@pytest.mark.contract
def test_generator_module_exists():
    """
    Contract: a generator entrypoint should exist once Phase B starts.
    Suggested location: 02 data generation/generator.py
    """
    # This test is intentionally skipped until generator exists.
    pytest.skip("Enable once the synthetic data generator is implemented.")


@pytest.mark.contract
def test_generator_reproducibility(random_seed: int):
    """
    Contract: same seed -> identical output (row count + key aggregates).
    """
    pytest.skip("Enable once generator supports seed-based reproducibility.")


@pytest.mark.contract
def test_generator_volume_levels():
    """
    Contract: generator can produce the 3 required volume levels:
    500k / 5M / 20M rows.
    """
    pytest.skip("Enable once generator supports multiple volume levels.")