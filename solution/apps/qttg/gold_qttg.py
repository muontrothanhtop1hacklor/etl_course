"""Build the monthly Gold report from the cleaned Silver layer."""

import argparse
import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    countDistinct,
    date_format,
    explode,
    expr,
    lit,
    max as spark_max,
    min as spark_min,
    sequence,
    sum as spark_sum,
    to_date,
    when,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_PATH = PROJECT_ROOT / "output" / "spark_lake"

SILVER_MASTER_PATH = str(BASE_PATH / "silver" / "qttg_bhxh")
SILVER_DETAIL_PATH = str(BASE_PATH / "silver" / "qttg_bhxh_detail")
GOLD_PATH = str(BASE_PATH / "gold")
DIM_THANG_PATH = str(Path(GOLD_PATH) / "dim_thang")
REPORT_PATH = str(Path(GOLD_PATH) / "bao_cao_bhxh_thang")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the QTTG monthly Gold report")
    parser.add_argument("--input-dir")
    parser.add_argument("--output-dir")
    args = parser.parse_args()
    if not args.input_dir:
        args.input_dir = str(BASE_PATH / "silver")
    if not args.output_dir:
        args.output_dir = str(BASE_PATH / "gold")
    return args


def create_dim_thang(detail_df):
    """Create one row for every YYYYMM covered by Silver intervals."""
    period_dates = detail_df.select(
        to_date(col("TU_THANG"), "yyyyMM").alias("TU_THANG_DATE"),
        to_date(col("DEN_THANG"), "yyyyMM").alias("DEN_THANG_DATE"),
    )

    bounds = period_dates.agg(
        spark_min("TU_THANG_DATE").alias("MIN_MONTH"),
        spark_max("DEN_THANG_DATE").alias("MAX_MONTH"),
    )

    return (
        bounds.select(
            explode(
                sequence(
                    col("MIN_MONTH"),
                    col("MAX_MONTH"),
                    expr("INTERVAL 1 MONTH"),
                )
            ).alias("THANG_DATE")
        )
        .select(date_format("THANG_DATE", "yyyyMM").alias("THANG_ID"))
        .orderBy("THANG_ID")
    )


def build_report(detail_df, master_df, dim_thang_df):
    """Join report months to intervals and calculate the required KPIs."""
    detail = detail_df.select(
        "MASTER_ID",
        "MA_DON_VI",
        "TU_THANG",
        "DEN_THANG",
        "MUC_LUONG",
    ).withColumn(
        "TU_THANG_DATE", to_date(col("TU_THANG"), "yyyyMM")
    ).withColumn(
        "DEN_THANG_DATE", to_date(col("DEN_THANG"), "yyyyMM")
    )

    people = master_df.select("ID", "SO_SO_BHXH").withColumnRenamed(
        "ID", "MASTER_ID"
    )

    joined = (
        dim_thang_df.withColumn(
            "THANG_DATE", to_date(col("THANG_ID"), "yyyyMM")
        )
        .join(
            detail,
            col("THANG_DATE").between(
                col("TU_THANG_DATE"), col("DEN_THANG_DATE")
            ),
            "inner",
        )
        .join(people, "MASTER_ID", "inner")
    )

    return joined.groupBy("THANG_ID").agg(
        countDistinct("SO_SO_BHXH").alias("SO_NGUOI_THAM_GIA"),
        countDistinct("MA_DON_VI").alias("SO_DON_VI"),
        coalesce(
            spark_sum("MUC_LUONG"), lit(0).cast("decimal(38,2)")
        ).alias("TONG_QUY_LUONG"),
        avg(when(col("MUC_LUONG") > 0, col("MUC_LUONG"))).alias(
            "LUONG_BINH_QUAN"
        ),
        countDistinct(
            when(col("MUC_LUONG") == 0, col("SO_SO_BHXH"))
        ).alias("SO_NGUOI_LUONG_0"),
    ).orderBy("THANG_ID")


def main():
    args = parse_args()
    silver_path = args.input_dir.rstrip("/")
    gold_path = args.output_dir.rstrip("/")
    spark = SparkSession.builder.appName("Gold QTTG Monthly Report").getOrCreate()

    try:
        master_df = spark.read.parquet(f"{silver_path}/qttg_bhxh")
        detail_df = spark.read.parquet(f"{silver_path}/qttg_bhxh_detail")

        invalid_period_count = detail_df.filter(
            col("TU_THANG") > col("DEN_THANG")
        ).limit(1).count()
        if invalid_period_count:
            raise ValueError("Silver contains TU_THANG greater than DEN_THANG")

        dim_thang_df = create_dim_thang(detail_df).cache()
        report_df = build_report(detail_df, master_df, dim_thang_df).cache()

        report_count = report_df.count()
        distinct_month_count = report_df.select("THANG_ID").distinct().count()
        if report_count != distinct_month_count:
            raise ValueError("Gold report contains duplicate THANG_ID values")

        dim_thang_df.write.mode("overwrite").parquet(
            f"{gold_path}/dim_thang"
        )
        report_df.write.mode("overwrite").parquet(
            f"{gold_path}/bao_cao_bhxh_thang"
        )

        print(
            f"LAYER=GOLD STATUS=SUCCESS REPORT_ROWS={report_count} "
            f"DIM_MONTH_ROWS={dim_thang_df.count()}"
        )
        report_df.show(20, truncate=False)

        report_df.unpersist()
        dim_thang_df.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
