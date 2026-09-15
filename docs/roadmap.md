# Roadmap dự án ETL QTTG

## Giai đoạn 1: Bronze

- Đọc dữ liệu CSV gốc
- Validate schema và mối quan hệ master-detail
- Ghi dữ liệu Bronze vào Parquet
- Thêm metadata: source_file, load_time, layer

## Giai đoạn 2: Silver

- Chọn bản ghi mới nhất của từng người
- Chuẩn hóa kiểu dữ liệu
- Xử lý null, duplicate, invalid rows
- Tạo các view chiến lược dùng cho báo cáo

## Giai đoạn 3: Gold

- Tạo các mart / aggregated tables
- Tính toán KPI theo tháng, năm, đơn vị
- Dùng cho dashboard và báo cáo

## Giai đoạn 4: App ETL/ELT quy mô lớn

- Hỗ trợ xử lý trên 1 triệu bản ghi trở lên
- Tối ưu Spark tuning, partitioning, memory, shuffle
- Tách thành pipeline riêng cho từng layer
- Có thể chạy theo Airflow / cron / orchestrator

## Kiến trúc dự kiến

```text
raw csv -> bronze -> silver -> gold -> dashboard / BI
``` 
