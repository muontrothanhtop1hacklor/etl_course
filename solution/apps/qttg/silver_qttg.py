import argparse
import os
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    current_date,
    regexp_replace,
    row_number,
    substring,
    trim,
    coalesce,
    lit,
    to_timestamp,
    year,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_PATH = PROJECT_ROOT / "output" / "spark_lake"

BRONZE_MASTER_PATH = str(BASE_PATH / "bronze" / "raw_qttg_bhxh")
BRONZE_DETAIL_PATH = str(BASE_PATH / "bronze" / "raw_qttg_bhxh_detail")

SILVER_MASTER_PATH = str(BASE_PATH / "silver" / "qttg_bhxh")
SILVER_DETAIL_PATH = str(BASE_PATH / "silver" / "qttg_bhxh_detail")
QUARANTINE_DETAIL_PATH = str(BASE_PATH / "quarantine" / "qttg_bhxh_detail")

VALIDATION_COLUMNS = [
    "_valid_TU_THANG",
    "_valid_DEN_THANG",
    "_valid_period_range",
    "_valid_MUC_LUONG",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform QTTG Bronze into Silver")
    parser.add_argument("--input-dir")
    parser.add_argument("--output-dir")
    args = parser.parse_args()
    if not args.input_dir:
        args.input_dir = str(BASE_PATH / "bronze")
    if not args.output_dir:
        args.output_dir = str(BASE_PATH / "silver")
    return args


def deduplicate_latest(df):
    df = df.withColumn("_created_at_order", to_timestamp(col("CREATED_AT")))
    window_spec = (
        Window.partitionBy("SO_SO_BHXH")
        .orderBy(
            col("_created_at_order").desc_nulls_last(),
            col("ID").desc_nulls_last(),
        )
    )

    return (
        df.withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num", "_created_at_order")
    )


def clean_period_columns(df):
    for column_name in ("TU_THANG", "DEN_THANG"):
        value = regexp_replace(
            trim(col(column_name).cast("string")),
            r"\.0$",
            "",
        )

        df = df.withColumn(column_name, value)

        valid_period = (
            col(column_name).rlike(r"^\d{6}$")
            & (substring(col(column_name), 1, 4).cast("int") >= 1960)
            & (
                substring(col(column_name), 1, 4).cast("int")
                <= year(current_date())
            )
            & substring(col(column_name), 5, 2)
            .cast("int")
            .between(1, 12)
        )

        df = df.withColumn(
            f"_valid_{column_name}",
            valid_period,
        )

    df = df.withColumn(
        "_valid_period_range",
        col("TU_THANG") <= col("DEN_THANG"),
    )

    return df


def clean_salary(df):
    salary_text = regexp_replace(
        trim(col("MUC_LUONG").cast("string")),
        ",",
        "",
    )

    valid_salary = (
        col("MUC_LUONG").isNull()
        | salary_text.rlike(r"^\d+(\.\d+)?$")
    )

    return (
        df.withColumn("_valid_MUC_LUONG", valid_salary)
        .withColumn("MUC_LUONG", salary_text.cast("decimal(18,2)"))
    )


def split_valid_invalid(df):
    valid_condition = col(VALIDATION_COLUMNS[0])

    for validation_column in VALIDATION_COLUMNS[1:]:
        valid_condition = valid_condition & col(validation_column)

    valid_condition = coalesce(valid_condition, lit(False))

    return (
        df.filter(valid_condition).drop(*VALIDATION_COLUMNS),
        df.filter(~valid_condition),
    )


def main():
    args = parse_args()
    bronze_path = args.input_dir.rstrip("/")
    silver_path = args.output_dir.rstrip("/")
    quarantine_path = str(Path(silver_path).parent / "quarantine" / "qttg_bhxh_detail")
    spark = (
        SparkSession.builder
        .appName("Silver QTTG")
        .getOrCreate()
    )

    try:
        master_df = spark.read.parquet(f"{bronze_path}/raw_qttg_bhxh")
        detail_df = spark.read.parquet(f"{bronze_path}/raw_qttg_bhxh_detail")

        latest_master_df = deduplicate_latest(master_df)

        # Chuẩn hóa trước để mọi record lỗi vẫn có cùng schema khi quarantine.
        detail_df = clean_period_columns(detail_df)
        detail_df = clean_salary(detail_df)

        latest_master_ref = latest_master_df.select(
            col("ID").alias("_master_id"),
            col("NLD_ID").alias("_master_nld_id"),
        ).distinct()

        detail_with_master = detail_df.join(
            latest_master_ref,
            detail_df["MASTER_ID"] == col("_master_id"),
            "left",
        )

        # Detail thuộc master cũ/orphan bị loại khỏi Silver theo yêu cầu.
        # Chỉ kiểm tra NLD_ID với các detail thuộc master mới nhất.
        current_master_detail = detail_with_master.filter(
            col("_master_id").isNotNull()
        ).cache()

        orphan_detail_df = (
            detail_with_master.filter(col("_master_id").isNull())
            .drop("_master_id", "_master_nld_id")
            .withColumn("_quarantine_reason", lit("MASTER_NOT_CURRENT"))
        )

        valid_link = col("NLD_ID").eqNullSafe(col("_master_nld_id"))

        invalid_link_df = (
            current_master_detail.filter(~valid_link)
            .drop("_master_id", "_master_nld_id")
            .withColumn("_quarantine_reason", lit("NLD_ID_MISMATCH"))
        )

        linked_detail_df = current_master_detail.filter(valid_link).drop(
            "_master_id", "_master_nld_id"
        ).cache()

        valid_detail_df, invalid_value_df = split_valid_invalid(
            linked_detail_df
        )

        invalid_value_df = invalid_value_df.withColumn(
            "_quarantine_reason", lit("INVALID_VALUE")
        )

        invalid_detail_df = orphan_detail_df.unionByName(
            invalid_link_df,
            allowMissingColumns=True,
        ).unionByName(
            invalid_value_df,
            allowMissingColumns=True,
        )

        latest_master_df.write.mode("overwrite").parquet(
            f"{silver_path}/qttg_bhxh"
        )

        valid_detail_df.write.mode("overwrite").parquet(
            f"{silver_path}/qttg_bhxh_detail"
        )

        invalid_detail_df.write.mode("overwrite").parquet(
            quarantine_path
        )

        linked_detail_df.unpersist()
        current_master_detail.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()