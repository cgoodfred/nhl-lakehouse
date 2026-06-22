"""Pytest fixtures for silver job tests.

Adds silver/jobs to sys.path so test modules can `from plays import ...`
the same way the in-container jobs do (where /opt/jobs is on the path).
"""

import sys
from pathlib import Path

import pytest

_SILVER_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_SILVER_DIR / "jobs"))
sys.path.insert(0, str(_SILVER_DIR))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("silver-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def fixtures_dir():
    return Path(__file__).resolve().parent / "fixtures"
