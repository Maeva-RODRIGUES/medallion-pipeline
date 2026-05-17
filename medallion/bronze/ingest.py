from pyspark.sql import SparkSession, DataFrame


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("medallion-bronze")
        .master("local[*]")
        .getOrCreate()
    )

def ingest_raw(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """
    Couche Bronze : lecture du CSV brut et stockage en Parquet.
    Aucune transformation — on garde la donnée d'origine intacte.
    """
    df = spark.read.csv(input_path, header=True, inferSchema=True, sep=";")

    print(f"✅ Bronze : {df.count()} lignes ingérées")
    print(f"📋 Colonnes : {df.columns}")

    df.write.mode("overwrite").parquet(output_path)

    return df