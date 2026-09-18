# Tổng hợp kết quả tuần: Airflow + QTTG

## Phạm vi đã hoàn thành

- Có job xử lý dữ liệu QTTG với khoảng `1,000,000` dòng detail.
- Có flow điều phối `Bronze -> Silver -> Gold -> Validate` trong DAG
  `qttg_bronze_silver_gold`.
- DAG dùng `BashOperator` gọi `spark-submit` trong Spark Master qua Docker,
  không chạy ETL trực tiếp trong Airflow.
- Có output Bronze, Silver, quarantine và Gold dạng Parquet.
- Có báo cáo Gold theo tháng và kiểm tra không trùng tháng.
- Có job validate kiểm tra orphan detail, master trùng người, khoảng tháng sai
  và tháng Gold trùng.

## DAG và dependency

File DAG: `airflow-local/dags/dag_qttg_bronze_silver_gold.py`

```text
bronze_qttg -> silver_qttg -> gold_qttg -> validate_qttg
```

Các tham số kết nối có thể cấu hình bằng biến môi trường:

- `QTTG_SPARK_CONTAINER`, mặc định `tst-spark-master`;
- `QTTG_SPARK_MASTER`, mặc định `spark://spark-master:7077`;
- `QTTG_SPARK_APP_DIR`, mặc định `/opt/spark/apps/qttg`;
- `QTTG_SPARK_RAW_DIR`, mặc định `file:///opt/spark/data/raw_qttg_1m`;
- `QTTG_SPARK_LAKE_DIR`, mặc định `file:///opt/spark/data/lake`.

Container Spark cần được mount để app directory chứa đủ bốn file job và cùng
nhìn thấy raw/lake volume theo hướng dẫn tại
`requirements/HUONG_DAN_AIRFLOW_SPARK_LOCAL.md`.

## Kết quả đã xác minh

| Layer | Job | Trạng thái | Số liệu | Output |
|---|---|---|---:|---|
| Bronze | `bronze_qttg.py` | OK | master `142,857`; detail `1,000,000` | `solution/output/spark_lake/bronze/raw_qttg_bhxh*` |
| Silver | `silver_qttg.py` | OK | master mới nhất `21,838`; detail hợp lệ `169,658` | `solution/output/spark_lake/silver/qttg_bhxh*` |
| Quarantine | `silver_qttg.py` | OK | detail loại `830,342` | `solution/output/spark_lake/quarantine/qttg_bhxh_detail` |
| Gold | `gold_qttg.py` | OK | báo cáo tháng `629`; dim tháng `629` | `solution/output/spark_lake/gold/bao_cao_bhxh_thang` và `dim_thang` |
| Validate | `validate_qttg.py` | Đã thêm | yêu cầu `ERROR_ROWS=0` | Không tạo dataset mới |

Bronze đã kiểm tra `INVALID_DETAIL=0`, `INVALID_MONTH=0`. Silver không còn
trùng `SO_SO_BHXH`; quarantine chủ yếu là detail thuộc master không phải phiên
bản hiện hành.

## Chạy Airflow

Khởi động các service theo hướng dẫn trong
`requirements/HUONG_DAN_AIRFLOW_SPARK_LOCAL.md`, mở `http://localhost:8082`,
đăng nhập `admin/admin`, bật DAG `qttg_bronze_silver_gold` và chọn **Trigger
DAG**. Theo dõi Graph/Grid để xác nhận bốn task chuyển xanh; log cần có các
marker `BRONZE_OUTPUT`, `LAYER=GOLD STATUS=SUCCESS` và
`LAYER=VALIDATE STATUS=SUCCESS ... ERROR_ROWS=0`.

Đã bổ sung `airflow-local/docker-compose.yml`, khởi động thành công Postgres,
Airflow webserver, scheduler, Spark Master và Spark Worker. DAG được import
không lỗi, unpause và trigger được trên Airflow. Airflow đã gọi được
`tst-spark-master`, Spark kết nối Worker và nhận executor thành công.

Lần chạy Bronze hiện dừng với `PATH_NOT_FOUND` vì
`requirements/data/RAW_QTTG_BHXH.csv` và
`requirements/data/RAW_QTTG_BHXH_DETAIL.csv` chưa có trên máy. Validate độc
lập qua Spark trên Silver/Gold hiện có đã đạt `MASTER_ROWS=21838`,
`DETAIL_ROWS=169658`, `REPORT_ROWS=629`, `ERROR_ROWS=0`.

## Vướng mắc còn lại

- Raw CSV không nằm trong workspace hiện tại (`requirements/data` đang trống),
  nên không thể chạy lại Bronze 1 triệu dòng từ workspace này.
- Silver/Gold hiện đang nằm ở `solution/apps/qttg`, trong khi mẫu Docker mount
  của tài liệu dùng chung `/opt/spark/apps/qttg`; cần mount/copy đủ bốn job vào
  cùng app directory khi dựng Compose.
- Chưa có ảnh chụp Airflow Graph/Grid hoặc log UI trong repo; cần bổ sung sau
  lần chạy Docker thực tế.
