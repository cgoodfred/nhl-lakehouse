"""Pytest fixtures for Spark job tests.

Mirrors the in-container PYTHONPATH so test modules can `from common
import ...` and `from games import ...` the same way the jobs do at
runtime (where /opt/jobs is on PYTHONPATH and tier subdirs sit beneath).
"""

import sys
from pathlib import Path

import pytest

_SPARK_DIR = Path(__file__).resolve().parent.parent
_JOBS_DIR = _SPARK_DIR / "jobs"
# `from common import ...` (common.py lives at jobs/common.py)
sys.path.insert(0, str(_JOBS_DIR))
# `from games import ...`, `from plays import ...`, etc. — tier subdirs
for tier in ("silver", "gold"):
    tier_dir = _JOBS_DIR / tier
    if tier_dir.is_dir():
        sys.path.insert(0, str(tier_dir))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("spark-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def fixtures_dir():
    return Path(__file__).resolve().parent / "fixtures"
