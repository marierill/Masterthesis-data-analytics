from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """
    Returns the repository root path assuming pytest is executed from repo root.
    """
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def data_dir(repo_root: Path) -> Path:
    """
    Location where generated datasets may be stored locally.
    Should be gitignored.
    """
    return repo_root / "data"


@pytest.fixture(scope="session")
def results_dir(repo_root: Path) -> Path:
    """
    Location where benchmark results are stored.
    """
    return repo_root / "05 benchmark results"


@pytest.fixture(scope="session")
def random_seed() -> int:
    """
    Default seed for reproducibility tests.
    """
    return int(os.getenv("RANDOM_SEED", "42"))
