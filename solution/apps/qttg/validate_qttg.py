"""Validate the QTTG Silver and Gold contracts for an Airflow task."""

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LAKE_DIR = PROJECT_ROOT / "output" / "spark_lake"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate QTTG Silver and Gold")
    parser.add_argument("--lake-dir", default=str(DEFAULT_LAKE_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    lake_dir = args.lake_dir.rstrip("/")
    silver_master_path = f"{lake_dir}/silver/qttg_bhxh"
    silver_detail_path = f"{lake_dir}/silver/qttg_bhxh_detail"
    gold_path = f"{lake_dir}/gold/bao_cao_bhxh_thang"

    spark = SparkSession.builder.appName("qttg-validate").getOrCreate()
    try:
        master_df = spark.read.parquet(silver_master_path)
        detail_df = spark.read.parquet(silver_detail_path)
        gold_df = spark.read.parquet(gold_path)

        master_rows = master_df.count()
        detail_rows = detail_df.count()
        gold_rows = gold_df.count()
        duplicate_people = (
            master_df.groupBy("SO_SO_BHXH")
            .count()
            .filter(col("count") > 1)
            .count()
        )
        orphan_detail = (
            detail_df.select("MASTER_ID")
            .join(master_df.select(col("ID").alias("MASTER_ID")), "MASTER_ID", "left_anti")
            .count()
        )
        invalid_period = detail_df.filter(col("TU_THANG") > col("DEN_THANG")).count()
        duplicate_month = gold_df.groupBy("THANG_ID").count().filter(col("count") > 1).count()

        error_rows = duplicate_people + orphan_detail + invalid_period + duplicate_month
        if error_rows:
            raise ValueError(
                "QTTG validation failed: "
                f"duplicate_people={duplicate_people}, "
                f"orphan_detail={orphan_detail}, "
                f"invalid_period={invalid_period}, "
                f"duplicate_month={duplicate_month}"
            )

        print(
            "LAYER=VALIDATE STATUS=SUCCESS "
            f"MASTER_ROWS={master_rows} DETAIL_ROWS={detail_rows} "
            f"REPORT_ROWS={gold_rows} ERROR_ROWS={error_rows}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
