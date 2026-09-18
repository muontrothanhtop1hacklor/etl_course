"""Ingest the QTTG CSV files into the Bronze lake without business filtering."""

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


MASTER_FILE = "RAW_QTTG_BHXH.csv"
DETAIL_FILE = "RAW_QTTG_BHXH_DETAIL.csv"
EXPECTED_MASTER_ROWS = 142_857
EXPECTED_DETAIL_ROWS = 1_000_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest QTTG CSV files into Bronze")
    parser.add_argument("--input-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--expected-master-rows", type=int, default=EXPECTED_MASTER_ROWS)
    parser.add_argument("--expected-detail-rows", type=int, default=EXPECTED_DETAIL_ROWS)
    args = parser.parse_args()
    workspace_dir = Path(__file__).resolve().parents[3]
    if not args.input_dir or args.input_dir == "{input_dir}":
        args.input_dir = workspace_dir.as_uri()
    if not args.output_dir or args.output_dir == "{output_dir}":
        args.output_dir = (workspace_dir / "output" / "spark_lake" / "bronze").as_uri()
    return args


def read_raw_csv(spark: SparkSession, path: str):
    """Read every source column as text so Bronze preserves source values."""
    header = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(path)
        .limit(0)
        .schema
    )
    raw_schema = StructType(
        [StructField(field.name, StringType(), True) for field in header.fields]
    )
    return (
        spark.read.schema(raw_schema)
        .option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(path)
    )


def require_columns(dataframe, required_columns, dataset_name: str) -> None:
    missing = sorted(set(required_columns) - set(dataframe.columns))
    if missing:
        raise ValueError(f"{dataset_name} is missing columns: {', '.join(missing)}")


def validate_bronze(master, detail, expected_master_rows: int, expected_detail_rows: int) -> None:
    master_rows = master.count()
    detail_rows = detail.count()
    if master_rows != expected_master_rows:
        raise ValueError(f"Expected {expected_master_rows} master rows, got {master_rows}")
    if detail_rows != expected_detail_rows:
        raise ValueError(f"Expected {expected_detail_rows} detail rows, got {detail_rows}")

    invalid_detail = (
        detail.alias("d")
        .join(master.select("ID", "NLD_ID").alias("m"), F.col("d.MASTER_ID") == F.col("m.ID"), "left")
        .where(F.col("m.ID").isNull() | (F.col("m.NLD_ID") != F.col("d.NLD_ID")))
        .count()
    )
    if invalid_detail:
        raise ValueError(f"Invalid detail relationships: {invalid_detail}")

    invalid_month = detail.where(
        ~F.col("TU_THANG").rlike(r"^[0-9]{6}$")
        | ~F.col("DEN_THANG").rlike(r"^[0-9]{6}$")
        | ~F.substring("TU_THANG", 5, 2).between("01", "12")
        | ~F.substring("DEN_THANG", 5, 2).between("01", "12")
        | (F.col("TU_THANG") > F.col("DEN_THANG"))
    ).count()
    if invalid_month:
        raise ValueError(f"Invalid detail month ranges: {invalid_month}")

    print(
        "BRONZE_VALIDATION "
        f"MASTER_ROWS={master_rows} DETAIL_ROWS={detail_rows} "
        f"INVALID_DETAIL=0 INVALID_MONTH=0"
    )


def add_bronze_metadata(dataframe, source_file: str, load_time):
    return dataframe.withColumn("source_file", F.lit(source_file)) \
        .withColumn("load_time", F.lit(load_time)) \
        .withColumn("layer", F.lit("BRONZE"))


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("qttg-bronze-ingest").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        master = read_raw_csv(spark, f"{args.input_dir.rstrip('/')}/{MASTER_FILE}")
        detail = read_raw_csv(spark, f"{args.input_dir.rstrip('/')}/{DETAIL_FILE}")
        require_columns(master, ["ID", "NLD_ID", "CREATED_AT"], "master CSV")
        require_columns(detail, ["ID", "MASTER_ID", "NLD_ID", "TU_THANG", "DEN_THANG", "CREATED_AT"], "detail CSV")

        validate_bronze(master, detail, args.expected_master_rows, args.expected_detail_rows)

        load_time = datetime.now(timezone.utc).replace(tzinfo=None)
        master = add_bronze_metadata(master, MASTER_FILE, load_time)
        detail = add_bronze_metadata(detail, DETAIL_FILE, load_time)
        master_output = f"{args.output_dir.rstrip('/')}/raw_qttg_bhxh"
        detail_output = f"{args.output_dir.rstrip('/')}/raw_qttg_bhxh_detail"
        master.write.mode("overwrite").parquet(master_output)
        detail.write.mode("overwrite").parquet(detail_output)

        bronze_master_rows = spark.read.parquet(master_output).count()
        bronze_detail_rows = spark.read.parquet(detail_output).count()
        if bronze_master_rows != args.expected_master_rows:
            raise ValueError(f"Bronze master count mismatch: {bronze_master_rows}")
        if bronze_detail_rows != args.expected_detail_rows:
            raise ValueError(f"Bronze detail count mismatch: {bronze_detail_rows}")
        print(
            "BRONZE_OUTPUT "
            f"MASTER_PATH={master_output} DETAIL_PATH={detail_output} "
            f"MASTER_ROWS={bronze_master_rows} DETAIL_ROWS={bronze_detail_rows}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
