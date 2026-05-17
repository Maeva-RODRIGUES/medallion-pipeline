from pyspark.sql import SparkSession
from medallion.bronze.ingest import ingest_raw


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

    print("\n✅ Pipeline Bronze terminé !")
    spark.stop()