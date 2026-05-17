from pyspark.sql import SparkSession
from medallion.bronze.ingest import ingest_raw
from medallion.silver.transform import transform
from medallion.gold.aggregate import aggregate


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("medallion-pipeline")
        .master("local[*]")
        .getOrCreate()
    )

if __name__ == "__main__":
    spark = create_spark_session()

    print("\n🟤 Bronze : ingestion...")
    ingest_raw(
        spark,
        input_path="data/raw/caracteristiques_2023.csv",
        output_path="output/bronze"
    )

    print("\n⚪ Silver : transformation...")
    transform(
        spark,
        input_path="output/bronze",
        output_path="output/silver"
    )

    print("\n🟡 Gold : agrégation...")
    aggregate(
        spark,
        input_path="output/silver",
        output_path="output/gold"
    )

    print("\n✅ Pipeline complet Bronze → Silver → Gold terminé !")
    spark.stop()