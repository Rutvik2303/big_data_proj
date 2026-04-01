import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-full-load") \
        .master("local[*]") \
        .getOrCreate()

def test_full_load_users_schema(spark):
    df = spark.createDataFrame(
        [
            ("u1", "John", "john@example.com"),
            ("u2", "Jane", "jane@example.com")
        ],
        ["user_id", "name", "email"]
    )

    assert "user_id" in df.columns
    assert "email" in df.columns
    assert df.count() == 2



def test_remove_null_emails(spark):
    df = spark.createDataFrame(
        [
            ("u1", "John", "john@example.com"),
            ("u2", "Jane", None),
        ],
        ["user_id", "name", "email"]
    )

    cleaned_df = df.dropna(subset=["email"])

    assert cleaned_df.count() == 1
    assert cleaned_df.collect()[0]["user_id"] == "u1"


def test_trim_whitespace(spark):
    df = spark.createDataFrame(
        [
            ("u1", " John ", " john@example.com "),
        ],
        ["user_id", "name", "email"]
    )

    from pyspark.sql.functions import trim

    cleaned_df = df.select(
        "user_id",
        trim("name").alias("name"),
        trim("email").alias("email")
    )

    row = cleaned_df.collect()[0]
    assert row["name"] == "John"
    assert row["email"] == "john@example.com"


def test_remove_duplicates(spark):
    df = spark.createDataFrame(
        [
            ("u1", "John", "john@example.com"),
            ("u1", "John", "john@example.com"),
        ],
        ["user_id", "name", "email"]
    )

    dedup_df = df.dropDuplicates(["user_id"])

    assert dedup_df.count() == 1


def test_email_format_validation(spark):
    df = spark.createDataFrame(
        [
            ("u1", "John", "john@example.com"),
            ("u2", "Jane", "invalid-email"),
        ],
        ["user_id", "name", "email"]
    )

    from pyspark.sql.functions import col

    valid_df = df.filter(col("email").contains("@"))

    assert valid_df.count() == 1
    assert valid_df.collect()[0]["user_id"] == "u1"


def test_lowercase_emails(spark):
    df = spark.createDataFrame(
        [
            ("u1", "John", "JOHN@EXAMPLE.COM"),
        ],
        ["user_id", "name", "email"]
    )

    from pyspark.sql.functions import lower

    cleaned_df = df.withColumn("email", lower("email"))

    assert cleaned_df.collect()[0]["email"] == "john@example.com"