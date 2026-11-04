import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (SparkSession.builder
               .master("local[2]")
               .appName("delivery-pipeline-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.default.parallelism", "2")
               .config("spark.sql.session.timeZone", "Africa/Nairobi")
               .config("spark.ui.enabled", "false")
               .config("spark.ui.showConsoleProgress", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


BBOX = {"min_lat": -1.45, "max_lat": -1.15, "min_lon": 36.60, "max_lon": 37.05}
