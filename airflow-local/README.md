# Airflow local cho QTTG

## Chạy

DAG nằm tại `airflow-local/dags/dag_qttg_bronze_silver_gold.py` và tạo dependency:

```text
bronze_qttg -> silver_qttg -> gold_qttg -> validate_qttg
```

Chạy Compose ngay tại thư mục này; cấu hình Airflow và Spark đã có sẵn trong
`airflow-local/docker-compose.yml`. Khi chạy full pipeline, bổ sung Spark
Master/Worker theo file này, bảo đảm scheduler có Docker socket và Spark cùng
mount raw data, lake output và app directory. Project dùng `apache/spark:3.5.3`
với container name `tst-spark-master` và `tst-spark-worker`.

Mở `http://localhost:8082`, đăng nhập `admin/admin`, bật DAG
`qttg_bronze_silver_gold`, chọn **Trigger DAG**, rồi xem Graph/Grid và log từng
task.

Có thể kiểm tra DAG bằng lệnh trong scheduler:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags trigger qttg_bronze_silver_gold
```

Từ thư mục project, dùng thêm cờ file Compose:

```powershell
docker compose -f .\airflow-local\docker-compose.yml up -d
docker exec tst-spark-master /opt/spark/bin/spark-submit --version
```

Tên Spark container và các đường dẫn có thể đổi bằng các biến `QTTG_*` được mô
tả trong [báo cáo tuần](../docs/results/airflow-qttg-weekly.md).

## Kết quả hiện có

Báo cáo tuần ghi lại count đã xác minh: Bronze `142,857` master và `1,000,000`
detail; Silver `21,838` master và `169,658` detail; Gold `629` tháng; Validate
đã chạy trên Parquet hiện có với `ERROR_ROWS=0`.

Airflow UI đã được khởi động tại `http://localhost:8082` và DAG đã được
trigger. Spark Master/Worker cũng đã chạy và Airflow gọi được Spark. Lần test
Bronze dừng tại `PATH_NOT_FOUND` vì `requirements/data` chưa có hai raw CSV.
Sau khi đặt `RAW_QTTG_BHXH.csv` và `RAW_QTTG_BHXH_DETAIL.csv` vào thư mục đó,
chạy lại task Bronze rồi trigger full DAG. Log thành công cần có các marker
`BRONZE_OUTPUT`, `LAYER=GOLD STATUS=SUCCESS` và
`LAYER=VALIDATE STATUS=SUCCESS ... ERROR_ROWS=0`.

Validate độc lập qua Spark đã đạt:

```text
MASTER_ROWS=21838 DETAIL_ROWS=169658 REPORT_ROWS=629 ERROR_ROWS=0
```
