# Hướng dẫn dùng Airflow cho bài tập Spark QTTG trên Windows

## 1. Phạm vi

Tài liệu này chỉ bổ sung Airflow vào bài tập Spark xử lý hai file:

- `RAW_QTTG_BHXH.csv`;
- `RAW_QTTG_BHXH_DETAIL.csv`.

Môi trường thực hành:

- Windows và Docker Desktop;
- chạy hoàn toàn trên máy cá nhân;
- một container Spark Master và một container Spark Worker;
- Airflow, Spark và PostgreSQL là các container khác nhau nhưng cùng Docker network;
- truy cập giao diện từ Windows bằng `localhost`;
- không dùng SSH, Podman, máy chủ hoặc hệ điều hành riêng.

Toàn bộ câu lệnh người học trực tiếp nhập đều là PowerShell. Các lệnh như `apt-get` trong Dockerfile hoặc `set -e` trong DAG chạy tự động bên trong image/container, không yêu cầu mở terminal của hệ điều hành khác.

Airflow chỉ điều phối thứ tự chạy:

```text
Bronze ingest CSV
        ↓
Silver chuẩn hóa và chọn phiên bản mới nhất
        ↓
Gold tạo báo cáo tháng
        ↓
Validate kết quả
```

## 2. Kiến trúc local

```text
Windows + Docker Desktop
│
├── airflow-webserver     http://localhost:8082
├── airflow-scheduler
├── postgres              metadata của Airflow
├── spark-master          http://localhost:8080
└── spark-worker          http://localhost:8081
```

Các container ở chung network `spark-net`.

Trong DAG, Airflow chạy lệnh theo chuỗi sau:

```text
BashOperator
    → Docker socket của Docker Desktop
    → docker exec <spark-master-container>
    → spark-submit
```

Không có bước SSH. Airflow cũng không chạy trực tiếp logic ETL; Airflow chỉ gọi các file PySpark và theo dõi exit code.

> `localhost` chỉ dùng trên trình duyệt hoặc PowerShell của Windows. Bên trong container, các service gọi nhau bằng tên service như `spark-master:7077`, không gọi `localhost:7077`.

## 3. Cấu trúc thư mục đề xuất

Tại thư mục project `Datahub`:

```text
Datahub/
├── airflow-local/
│   ├── Dockerfile
│   ├── dags/
│   │   └── dag_qttg_bronze_silver_gold.py
│   └── logs/
├── output/
│   ├── raw_qttg_1m/
│   │   ├── RAW_QTTG_BHXH.csv
│   │   └── RAW_QTTG_BHXH_DETAIL.csv
│   └── spark_lake/
│       ├── bronze/
│       ├── silver/
│       └── gold/
└── spark/
    └── apps/
        └── qttg/
            ├── bronze_qttg.py
            ├── silver_qttg.py
            ├── gold_qttg.py
            └── validate_qttg.py
```

Các tên file PySpark trên là cấu trúc cần thực hiện trong bài tập, không phải file đã có sẵn.

## 4. Chuẩn bị Docker Compose

### 4.1. Volume của Spark

Trong cấu hình dùng chung của `spark-master` và `spark-worker`, mount cùng dữ liệu và source code:

```yaml
volumes:
  - ./spark/apps/qttg:/opt/spark/apps/qttg:ro
  - ./output/raw_qttg_1m:/opt/spark/data/raw_qttg_1m:ro
  - ./output/spark_lake:/opt/spark/data/lake
```

Vì master và worker đều nằm trên cùng Docker Desktop, cả hai container có thể dùng cùng bind mount từ Windows.

Đường dẫn trong code PySpark:

```text
file:///opt/spark/data/raw_qttg_1m/RAW_QTTG_BHXH.csv
file:///opt/spark/data/raw_qttg_1m/RAW_QTTG_BHXH_DETAIL.csv
file:///opt/spark/data/lake/bronze
file:///opt/spark/data/lake/silver
file:///opt/spark/data/lake/gold
```

### 4.2. Dockerfile cho Airflow

Tạo `airflow-local/Dockerfile`:

```dockerfile
FROM apache/airflow:2.8.1-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
```

Airflow chỉ cần Docker CLI để gọi container Spark. Không cần cài Java, PySpark hoặc SSH client trong Airflow.

### 4.3. Thêm Airflow vào Compose local

Đoạn sau được thêm vào cùng file Compose đang chạy Spark. Nếu Spark Compose đã khai báo `spark-net`, dùng lại network đó.

```yaml
x-airflow-common: &airflow-common
  build:
    context: ./airflow-local
  image: qttg-airflow:2.8.1
  user: "${AIRFLOW_UID:-50000}:0"
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "true"
    AIRFLOW__WEBSERVER__SECRET_KEY: local-qTTg-demo-only
  volumes:
    - ./airflow-local/dags:/opt/airflow/dags
    - ./airflow-local/logs:/opt/airflow/logs
    - /var/run/docker.sock:/var/run/docker.sock
  networks:
    - spark-net
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - airflow-postgres:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 5s
      timeout: 5s
      retries: 10
    networks:
      - spark-net

  airflow-init:
    <<: *airflow-common
    command:
      - bash
      - -c
      - |
        airflow db migrate &&
        (airflow users create \
          --username admin \
          --password admin \
          --firstname Local \
          --lastname Admin \
          --role Admin \
          --email admin@example.local || echo "User admin da ton tai")

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8082:8080"
    healthcheck:
      test: ["CMD-SHELL", "curl --fail http://localhost:8080/health"]
      interval: 10s
      timeout: 5s
      retries: 10

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

volumes:
  airflow-postgres:
```

Các service Spark hiện có cũng phải khai báo:

```yaml
networks:
  - spark-net
```

Nếu file Compose đã có mục `services:`, `volumes:` hoặc `networks:`, chỉ gộp nội dung vào mục hiện có, không khai báo lặp lại cùng một khóa YAML.

Việc mount `/var/run/docker.sock` cho phép scheduler điều khiển Docker Desktop. Đây là cách đơn giản cho máy học tập; không dùng cách này cho Airflow production nhiều người sử dụng.

## 5. Khởi động trên PowerShell

Chạy tại thư mục có `docker-compose.yml`:

```powershell
New-Item -ItemType Directory -Force .\airflow-local\dags
New-Item -ItemType Directory -Force .\airflow-local\logs
New-Item -ItemType Directory -Force .\output\spark_lake

$env:AIRFLOW_UID = "50000"

docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up airflow-init
docker compose up -d postgres spark-master spark-worker airflow-webserver airflow-scheduler
docker compose ps
```

Các giao diện local:

| Thành phần | URL |
|---|---|
| Airflow | `http://localhost:8082` |
| Spark Master | `http://localhost:8080` |
| Spark Worker | `http://localhost:8081` |

Tài khoản Airflow dùng cho bài tập:

```text
username: admin
password: admin
```

## 6. Xác định tên container Spark Master

PowerShell:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Ví dụ container Spark đã tạo ở bài thực hành trước có tên:

```text
tst-spark-master
```

Tên trong DAG phải đúng với cột `NAMES` vừa kiểm tra. Nếu Compose dùng:

```yaml
container_name: qttg-spark-master
```

thì DAG phải dùng `qttg-spark-master`.

Kiểm tra scheduler có gọi được Docker Desktop:

```powershell
docker compose exec airflow-scheduler docker ps
```

Kiểm tra scheduler gọi được `spark-submit` trong Spark Master:

```powershell
docker compose exec airflow-scheduler `
  docker exec tst-spark-master `
  /opt/spark/bin/spark-submit --version
```

Thay `tst-spark-master` nếu container thực tế có tên khác.

## 7. Trách nhiệm của từng Spark job

### Bronze

- Đọc nguyên trạng hai CSV.
- Áp schema rõ ràng.
- Ghi ra `file:///opt/spark/data/lake/bronze`.
- Kiểm tra đúng `142857` dòng master và `1000000` dòng detail.
- Không chọn phiên bản mới nhất tại layer này.

### Silver

- Đọc Bronze.
- Chuẩn hóa kiểu tháng, số tiền và giá trị null.
- Với mỗi `SO_SO_BHXH`, chọn master mới nhất theo `CREATED_AT DESC, ID DESC`.
- Chỉ lấy detail có `MASTER_ID` thuộc các master vừa chọn.
- Ghi đè toàn bộ `file:///opt/spark/data/lake/silver`.
- Không dùng soft delete trong bài tập này.

### Gold

- Đọc Silver.
- Bung các khoảng `TU_THANG`–`DEN_THANG` thành tháng báo cáo.
- Tạo báo cáo một dòng cho mỗi tháng.
- Tính số người tham gia, số đơn vị, tổng quỹ lương, lương bình quân và số người lương bằng 0.
- Ghi đè toàn bộ `file:///opt/spark/data/lake/gold`.

### Validate

- Kiểm tra detail không mồ côi.
- Kiểm tra mỗi người chỉ còn một master tại Silver.
- Kiểm tra `TU_THANG <= DEN_THANG`.
- Kiểm tra Gold không trùng tháng.
- Khi có lỗi phải raise exception để task Airflow chuyển sang trạng thái failed.

## 8. DAG Airflow cho bài tập

Tạo `airflow-local/dags/dag_qttg_bronze_silver_gold.py`:

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# Lấy đúng tên từ lệnh: docker ps --format "table {{.Names}}"
SPARK_CONTAINER = "tst-spark-master"
SPARK_MASTER = "spark://spark-master:7077"
APP_DIR = "/opt/spark/apps/qttg"


def spark_submit(app_name: str, app_args: str = "") -> str:
    return f"""
set -e
docker exec {SPARK_CONTAINER} \
  /opt/spark/bin/spark-submit \
  --master {SPARK_MASTER} \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 2g \
  --executor-cores 2 \
  {APP_DIR}/{app_name} {app_args}
""".strip()


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="qttg_bronze_silver_gold",
    description="Bài tập Spark QTTG chạy local bằng Docker Desktop",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spark", "qttg", "local"],
) as dag:
    bronze = BashOperator(
        task_id="bronze_ingest_csv",
        bash_command=spark_submit(
            "bronze_qttg.py",
            "--input-dir file:///opt/spark/data/raw_qttg_1m "
            "--output-dir file:///opt/spark/data/lake/bronze",
        ),
        execution_timeout=timedelta(hours=1),
    )

    silver = BashOperator(
        task_id="silver_latest_person",
        bash_command=spark_submit(
            "silver_qttg.py",
            "--input-dir file:///opt/spark/data/lake/bronze "
            "--output-dir file:///opt/spark/data/lake/silver",
        ),
        execution_timeout=timedelta(hours=1),
    )

    gold = BashOperator(
        task_id="gold_monthly_report",
        bash_command=spark_submit(
            "gold_qttg.py",
            "--input-dir file:///opt/spark/data/lake/silver "
            "--output-dir file:///opt/spark/data/lake/gold",
        ),
        execution_timeout=timedelta(hours=1),
    )

    validate = BashOperator(
        task_id="validate_results",
        bash_command=spark_submit(
            "validate_qttg.py",
            "--lake-dir file:///opt/spark/data/lake",
        ),
        execution_timeout=timedelta(minutes=30),
    )

    bronze >> silver >> gold >> validate
```

Nếu chưa có `validate_qttg.py`, tạm bỏ task `validate` và dùng:

```python
bronze >> silver >> gold
```

## 9. Chạy DAG

Airflow tự quét thư mục DAG. Kiểm tra lỗi import:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Trigger bằng UI:

1. Mở `http://localhost:8082`.
2. Đăng nhập `admin/admin`.
3. Tìm DAG `qttg_bronze_silver_gold`.
4. Bật DAG nếu đang paused.
5. Chọn **Trigger DAG**.
6. Mở Grid hoặc Graph để theo dõi bốn task.
7. Chọn từng task → **Log** để xem log Spark.

Trigger bằng PowerShell:

```powershell
docker compose exec airflow-scheduler `
  airflow dags trigger qttg_bronze_silver_gold
```

Xem danh sách DAG run:

```powershell
docker compose exec airflow-scheduler `
  airflow dags list-runs -d qttg_bronze_silver_gold
```

## 10. Yêu cầu log để làm báo cáo

Mỗi Spark job nên in kết quả ở cuối log:

```text
LAYER=BRONZE STATUS=SUCCESS MASTER_ROWS=142857 DETAIL_ROWS=1000000
LAYER=SILVER STATUS=SUCCESS PERSON_ROWS=<actual> DETAIL_ROWS=<actual>
LAYER=GOLD STATUS=SUCCESS REPORT_ROWS=<actual>
LAYER=VALIDATE STATUS=SUCCESS ERROR_ROWS=0
```

Gold job nên in thêm một phần dữ liệu mẫu:

```python
gold_df.orderBy("THANG_ID").show(20, truncate=False)
```

Ảnh chụp cần đưa vào README báo cáo:

1. `docker compose ps` cho thấy các container healthy/running.
2. Spark Master UI tại `http://localhost:8080` có worker.
3. Airflow Graph/Grid có toàn bộ task màu xanh.
4. Log Bronze có số dòng master và detail.
5. Log Silver có số người và số detail sau chuẩn hóa.
6. Log Gold có bảng báo cáo tháng mẫu.
7. Log Validate có `ERROR_ROWS=0`.

## 11. Retry và chạy lại

Trong bài tập này, mỗi layer được chạy tách biệt và ghi theo chế độ overwrite:

- retry Bronze chỉ xây lại Bronze;
- retry Silver đọc lại Bronze và xây lại Silver;
- retry Gold đọc lại Silver và xây lại Gold;
- không dùng soft delete;
- `max_active_runs=1` tránh hai DAG run cùng ghi một thư mục.

Để an toàn, Spark job nên tính kết quả xong rồi mới ghi. Không xóa thư mục layer ngay từ đầu vì nếu job lỗi giữa chừng sẽ làm mất kết quả lần chạy trước.

## 12. Lỗi thường gặp trên Windows Docker Desktop

### Airflow không gọi được Docker

Log có `docker: command not found`:

```powershell
docker compose build --no-cache airflow-webserver airflow-scheduler
docker compose up -d airflow-webserver airflow-scheduler
```

Log có `permission denied /var/run/docker.sock`:

- kiểm tra volume `/var/run/docker.sock:/var/run/docker.sock`;
- kiểm tra bằng `docker compose exec airflow-scheduler docker ps`;
- chỉ trong máy lab, có thể tạm đặt `user: "0:0"` cho scheduler rồi khởi động lại.

### Không tìm thấy Spark container

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Sửa `SPARK_CONTAINER` trong DAG theo đúng tên thực tế.

### Airflow UI không mở được

```powershell
docker compose ps
docker compose logs --tail 200 airflow-webserver
docker compose logs --tail 200 airflow-scheduler
```

Airflow dùng `localhost:8082` vì `8080` và `8081` đã dành cho Spark.

### Spark báo không tìm thấy CSV hoặc application

```powershell
docker exec tst-spark-master `
  ls -la /opt/spark/data/raw_qttg_1m

docker exec tst-spark-master `
  ls -la /opt/spark/apps/qttg
```

Kiểm tra lại bind mount tương đối với thư mục chứa Compose.

### Worker không đọc được file

Kiểm tra cả worker:

```powershell
docker exec tst-spark-worker `
  ls -la /opt/spark/data/raw_qttg_1m
```

Volume dữ liệu phải được khai báo trong phần cấu hình dùng chung của cả master và worker.

### Kết nối sai `localhost:7077`

Trong container không dùng `spark://localhost:7077`. Dùng:

```text
spark://spark-master:7077
```

Từ PowerShell, `localhost:7077` chỉ dùng để kiểm tra port publish của Docker Desktop.

### Task xanh nhưng dữ liệu sai

Spark script đang thoát với exit code `0` dù validation không đạt. Khi số dòng hoặc khóa sai, script phải raise exception để Airflow đánh dấu task failed.

## 13. Dừng môi trường

Dừng container nhưng giữ metadata Airflow và dữ liệu layer:

```powershell
docker compose down
```

Không thêm `-v` nếu muốn giữ lịch sử DAG run trong PostgreSQL volume.

Chỉ xóa toàn bộ volume khi thực sự muốn làm lại môi trường từ đầu:

```powershell
docker compose down -v
```

Lệnh này xóa metadata Airflow trong Docker volume nhưng không xóa hai CSV bind-mounted trên Windows.

## 14. Checklist

- [ ] Docker Desktop đang chạy và hỗ trợ các image Spark/Airflow của bài tập.
- [ ] Spark Master mở được tại `http://localhost:8080`.
- [ ] Spark Worker mở được tại `http://localhost:8081`.
- [ ] Airflow mở được tại `http://localhost:8082`.
- [ ] `docker compose exec airflow-scheduler docker ps` chạy thành công.
- [ ] DAG dùng đúng tên Spark Master container.
- [ ] Master và worker đều thấy hai CSV và các file PySpark.
- [ ] Bronze trả `142857` master và `1000000` detail.
- [ ] Silver chỉ giữ phiên bản mới nhất và không làm mất lịch sử tích lũy.
- [ ] Gold tạo báo cáo tháng.
- [ ] Validate trả `ERROR_ROWS=0`.
- [ ] Đã chụp ảnh log từng layer cho README.
