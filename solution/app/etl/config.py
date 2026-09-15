"""Configuration container for ETL app settings.

Dùng để chuẩn bị các biến môi trường, đường dẫn đầu vào/đầu ra,
config cho Spark, batch size và các tùy chọn scale theo từng layer.
"""

from dataclasses import dataclass


@dataclass
class ETLConfig:
    input_dir: str = ""
    output_dir: str = ""
    app_name: str = "qttg-etl"
    expected_master_rows: int = 142_857
    expected_detail_rows: int = 1_000_000
