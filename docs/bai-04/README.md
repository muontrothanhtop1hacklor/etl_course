# Bài 04 - ETL/ELT App và Scale-up

## Mục tiêu
Tổ chức pipeline thành app có thể mở rộng, chạy từng stage và xử lý dữ liệu lớn hơn 1 triệu bản ghi.

## Phạm vi
- Tách config khỏi code
- Xây dựng abstraction cho pipeline
- Chuẩn bị orchestration bằng Airflow
- Tìm hiểu partitioning, memory và shuffle của Spark
- Thêm logging, test và monitoring

## Code liên quan
- [ETL config](../../solution/app/etl/config.py)
- [Pipeline base](../../solution/app/etl/pipeline.py)
- [ETL entry point](../../solution/app/etl/main.py)
- [Local config](../../solution/config/environments/local.yaml)

## Nhật ký
- [Log bài 04](../logs/bai-04-template.md)

## Tiêu chí hoàn thành
Có cấu trúc app rõ ràng, có thể bổ sung DAG Airflow và mở rộng pipeline cho dữ liệu lớn.
