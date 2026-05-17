from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

def transform(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """
    Couche Silver : nettoyage et typage des données brutes.
    - Suppression des lignes avec des valeurs critiques manquantes
    - Typage explicite des colonnes
    - Renommage pour plus de lisibilité
    """
    df = spark.read.parquet(input_path)

    df_silver = (
        df
        # Suppression des nulls sur les colonnes critiques
        .dropna(subset=["Num_Acc", "an", "mois", "jour"])

        # Renommage des colonnes
        .withColumnRenamed("Num_Acc", "accident_id")
        .withColumnRenamed("an", "year")
        .withColumnRenamed("mois", "month")
        .withColumnRenamed("jour", "day")
        .withColumnRenamed("hrmn", "time")
        .withColumnRenamed("lum", "light_condition")
        .withColumnRenamed("dep", "department")
        .withColumnRenamed("agg", "in_urban_area")
        .withColumnRenamed("atm", "weather_condition")
        .withColumnRenamed("col", "collision_type")

        # Typage explicite
        .withColumn("year", F.col("year").cast("int"))
        .withColumn("month", F.col("month").cast("int"))
        .withColumn("day", F.col("day").cast("int"))
        .withColumn("lat", F.regexp_replace(F.col("lat"), ",", ".").cast("double"))
        .withColumn("long", F.regexp_replace(F.col("long"), ",", ".").cast("double"))

        # Colonne calculée : date complète
        .withColumn(
            "accident_date",
            F.to_date(
                F.concat_ws("-", F.col("year"), F.col("month"), F.col("day")),
                "yyyy-M-d"
            )
        )
    )

    print(f"✅ Silver : {df_silver.count()} lignes après nettoyage")
    print(f"📋 Colonnes : {df_silver.columns}")

    df_silver.write.mode("overwrite").parquet(output_path)

    return df_silver