from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def aggregate(spark: SparkSession, input_path: str, output_path: str) -> DataFrame:
    """
    Couche Gold : agrégations métier prêtes à consommer.
    - Nombre d'accidents par mois et par an
    - Nombre d'accidents par département
    - Nombre d'accidents par condition météo
    """
    df = spark.read.parquet(input_path)

    # Agrégation 1 : accidents par mois
    df_by_month = (
        df.groupBy("year", "month")
        .agg(F.count("accident_id").alias("total_accidents"))
        .orderBy("year", "month")
    )

    # Agrégation 2 : accidents par département
    df_by_department = (
        df.groupBy("department")
        .agg(F.count("accident_id").alias("total_accidents"))
        .orderBy(F.desc("total_accidents"))
    )

    # Agrégation 3 : accidents par condition météo
    df_by_weather = (
        df.groupBy("weather_condition")
        .agg(F.count("accident_id").alias("total_accidents"))
        .orderBy(F.desc("total_accidents"))
    )

    print(f"📅 Gold — accidents par mois : {df_by_month.count()} lignes")
    print(f"🗺️ Gold — accidents par département : {df_by_department.count()} lignes")
    print(f"🌦️ Gold — accidents par météo : {df_by_weather.count()} lignes")

    # On écrit chaque agrégation dans un sous-dossier séparé
    df_by_month.write.mode("overwrite").parquet(f"{output_path}/by_month")
    df_by_department.write.mode("overwrite").parquet(f"{output_path}/by_department")
    df_by_weather.write.mode("overwrite").parquet(f"{output_path}/by_weather")

    return df_by_month