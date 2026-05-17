import pytest
from pyspark.sql import SparkSession
from medallion.bronze.ingest import ingest_raw


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test-bronze")
        .master("local")
        .getOrCreate()
    )


def test_ingest_raw_returns_dataframe(spark, tmp_path):
    # Arrange : créer un CSV de test
    csv_content = "Num_Acc;jour;mois;an;hrmn;lum;dep;com;agg;int;atm;col;adr;lat;long\n"
    csv_content += "2023001;1;1;2023;800;1;75;056;1;1;1;2;Rue de Rivoli;48,86;2,34\n"
    csv_content += "2023002;2;1;2023;900;2;69;123;2;2;2;3;Rue de la Paix;45,75;4,83\n"

    input_file = tmp_path / "test.csv"
    input_file.write_text(csv_content, encoding="utf-8")
    output_path = str(tmp_path / "bronze")

    # Act
    df = ingest_raw(spark, str(input_file), output_path)

    # Assert
    assert df.count() == 2
    assert "Num_Acc" in df.columns


def test_ingest_raw_preserves_all_columns(spark, tmp_path):
    # Arrange
    csv_content = "Num_Acc;jour;mois;an;hrmn;lum;dep;com;agg;int;atm;col;adr;lat;long\n"
    csv_content += "2023001;1;1;2023;800;1;75;056;1;1;1;2;Rue de Rivoli;48,86;2,34\n"

    input_file = tmp_path / "test.csv"
    input_file.write_text(csv_content, encoding="utf-8")
    output_path = str(tmp_path / "bronze")

    # Act
    df = ingest_raw(spark, str(input_file), output_path)

    # Assert : Bronze ne doit rien transformer
    expected_columns = ["Num_Acc", "jour", "mois", "an", "hrmn", "lum",
                        "dep", "com", "agg", "int", "atm", "col", "adr", "lat", "long"]
    assert df.columns == expected_columns