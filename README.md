# ETL Learn

> Dự án học ETL/ELT theo lộ trình Bronze → Silver → Gold, sử dụng PySpark và Airflow, với dữ liệu QTTG BHXH.

## 1. Mục tiêu dự án

- Học và vận dụng quy trình ETL/ELT trong môi trường thực tế
- Tách rõ từng layer: Bronze, Silver, Gold
- Theo dõi tiến độ theo từng buổi/học phần
- Chuẩn bị nền tảng để mở rộng thành pipeline lớn hơn, có Airflow DAG, unit test, app orchestration

## 2. Kiến trúc tổng quan

```text
Raw source data
    ↓
Bronze layer
    ↓
Silver layer
    ↓
Gold layer
    ↓
Dashboard / reporting / analytics
```

## 3. Cấu trúc thư mục

```text
etl_learn/
├── README.md
├── .gitignore
├── docs/
│   ├── README.md
│   ├── roadmap.md
│   ├── bai-01/README.md
│   ├── bai-02/README.md
│   ├── bai-03/README.md
│   ├── bai-04/README.md
│   └── logs/
│       ├── README.md
│       ├── bai-01-template.md
│       ├── bai-02-template.md
│       └── ...
├── requirements/
│   ├── data/
│   ├── DDL_RAW_QTTG_BHXH.sql
│   ├── DDL_RAW_QTTG_BHXH_DETAIL.sql
│   ├── HUONG_DAN_AIRFLOW_SPARK_LOCAL.md
│   └── PHAN_TICH_NGHIEP_VU_QTTG_BHXH.md
├── solution/
│   ├── spark/apps/qttg/bronze_qttg.py
│   ├── apps/qttg/
│   │   ├── silver_qttg.py
│   │   └── gold_qttg.py
│   ├── app/etl/
│   │   ├── config.py
│   │   ├── pipeline.py
│   │   └── main.py
│   ├── config/
│   ├── tests/
│   └── output/
│       └── spark_lake/
├── scripts/
├── notebooks/
├── .env.example
└── .vscode/
```

## 4. Lộ trình bài học

| Bài | Nội dung | README bài | Nhật ký |
|---|---|---|---|
| 01 | Bronze Ingest | [README](docs/bai-01/README.md) | [Log](docs/logs/bai-01-template.md) |
| 02 | Silver và Data Quality | [README](docs/bai-02/README.md) | [Log](docs/logs/bai-02-template.md) |
| 03 | Gold và Aggregation | [README](docs/bai-03/README.md) | [Log](docs/logs/bai-03-template.md) |
| 04 | ETL/ELT App và scale-up | [README](docs/bai-04/README.md) | [Log](docs/logs/bai-04-template.md) |

## 5. Cách chạy

```powershell
python .\solution\spark\apps\qttg\bronze_qttg.py
```

Hoặc nếu dùng file app ETL tổng quát:

```powershell
python .\solution\app\etl\main.py
```

## 6. Lưu ý về tiến độ

- Mỗi bài học nên có một file log riêng để cập nhật mục tiêu, công việc đã làm, kết quả, vấn đề và việc tiếp theo.
- Dữ liệu raw lớn và Parquet output đầy đủ không nên được commit lên GitHub.
- Repo có commit bộ output mẫu nhỏ trong `solution/output/evidence/` để minh chứng kết quả Bronze, Silver và quarantine.
- Repo dùng để theo dõi code, cấu trúc, tiến độ học tập và kết quả đại diện; không phải để lưu trữ toàn bộ dữ liệu kích thước lớn.

## 7. Nguồn dữ liệu

- Dữ liệu QTTG BHXH theo yêu cầu nghiệp vụ
- DDL, tài liệu và hướng dẫn thiết kế nằm trong folder requirements
- Output Bronze được sinh ra trong folder solution/output
- [Minh chứng output Bronze/Silver trên GitHub](docs/results/qttg-output-evidence.md)

## 8. Cách cập nhật bài mới

Mỗi bài đi theo một cặp tài liệu:

- `docs/bai-xx/README.md`: mục tiêu, phạm vi, kiến thức và tiêu chí hoàn thành của bài.
- `docs/logs/bai-xx-*.md`: nhật ký thực tế, lệnh đã chạy, lỗi gặp phải và kết quả.

Khi bắt đầu bài mới, tạo README bài trước, sau đó cập nhật nhật ký trong suốt quá trình học.
