import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test-gold")
        .master("local")
        .getOrCreate()
    )


@pytest.fixture
def silver_df(spark):
    """Crée un DataFrame Silver de test directement en mémoire."""
    from datetime import date

    schema = StructType([
        StructField("accident_id", StringType()),
        StructField("day", IntegerType()),
        StructField("month", IntegerType()),
        StructField("year", IntegerType()),
        StructField("department", StringType()),
        StructField("weather_condition", StringType()),
    ])

    data = [
        ("2023001", 1, 1, 2023, "75", "1"),
        ("2023002", 2, 1, 2023, "75", "1"),
        ("2023003", 3, 2, 2023, "69", "2"),
        ("2023004", 4, 2, 2023, "69", "1"),
        ("2023005", 5, 3, 2023, "13", "2"),
    ]

    return spark.createDataFrame(data, schema)


def test_aggregate_by_month_columns(spark, silver_df, tmp_path):
    """Vérifie que l'agrégation par mois produit les bonnes colonnes."""
    result = (
        silver_df
        .groupBy("year", "month")
        .agg(F.count("accident_id").alias("total_accidents"))
    )
    assert "year" in result.columns
    assert "month" in result.columns
    assert "total_accidents" in result.columns


def test_aggregate_by_department_columns(spark, silver_df, tmp_path):
    """Vérifie que l'agrégation par département produit les bonnes colonnes."""
    result = (
        silver_df
        .groupBy("department")
        .agg(F.count("accident_id").alias("total_accidents"))
    )
    assert "department" in result.columns
    assert "total_accidents" in result.columns


def test_aggregate_by_weather_columns(spark, silver_df, tmp_path):
    """Vérifie que l'agrégation par météo produit les bonnes colonnes."""
    result = (
        silver_df
        .groupBy("weather_condition")
        .agg(F.count("accident_id").alias("total_accidents"))
    )
    assert "weather_condition" in result.columns
    assert "total_accidents" in result.columns


def test_aggregate_schema_is_not_null(spark, silver_df, tmp_path):
    """Vérifie que les DataFrames Gold ne sont pas null."""
    result_month = silver_df.groupBy("year", "month").agg(
        F.count("accident_id").alias("total_accidents")
    )
    result_dept = silver_df.groupBy("department").agg(
        F.count("accident_id").alias("total_accidents")
    )
    result_weather = silver_df.groupBy("weather_condition").agg(
        F.count("accident_id").alias("total_accidents")
    )
    assert result_month is not None
    assert result_dept is not None
    assert result_weather is not None