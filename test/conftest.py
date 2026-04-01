import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("pytest-f1-project")
        .getOrCreate()
    )

    yield spark
    spark.stop()