# Bài 04 - ETL/ELT App và Pipeline Scale-up

## 1. Mục tiêu

- Xây dựng app ETL/ELT có cấu trúc rõ ràng hơn
- Tái sử dụng code cho pipeline Bronze → Silver → Gold
- Chuẩn bị cho dữ liệu lớn hơn 1 triệu bản ghi

## 2. Bối cảnh

- Dự án đã học qua Bronze và có định hướng mở rộng
- Cần có architecture đủ sạch để tiếp tục lên production-like pipeline

## 3. Công việc sẽ làm

- Tạo app config
- Tạo base pipeline / orchestration class
- Định nghĩa các stage
- Mở rộng cho xử lý dữ liệu lớn với Spark tuning

## 4. Kết quả mong đợi

- Một app ETL có cấu trúc,
- Dễ thêm job mới,
- Dễ chạy từng stage riêng lẻ

## 5. Công nghệ cần nghiên cứu

- Airflow
- Spark tuning
- Data partitioning
- Logging và monitoring

## 6. Ghi chú

- Khi dữ liệu lớn, cần tối ưu memory, shuffle và disk I/O
- Dữ liệu raw nên tách riêng, không commit lên GitHub nếu quá lớn

## 7. Kế hoạch tiếp theo

- Tích hợp DAG Airflow local
- Viết tests cho validation layer
- Cải thiện pipeline logging và status tracking
