# Bài 01 - Bronze Ingest

## Mục tiêu
Đọc dữ liệu QTTG BHXH từ CSV, kiểm tra dữ liệu đầu vào và ghi dữ liệu raw vào tầng Bronze bằng PySpark.

## Phạm vi
- Đọc CSV master/detail
- Kiểm tra cột bắt buộc và số lượng dòng
- Kiểm tra quan hệ giữa bảng cha và bảng con
- Kiểm tra logic tháng
- Ghi Parquet kèm metadata Bronze

## Code liên quan
- [bronze_qttg.py](../../solution/spark/apps/qttg/bronze_qttg.py)
- [DDL và yêu cầu nghiệp vụ](../../requirements/)

## Nhật ký
- [Log bài 01](../logs/bai-01-template.md)

## Tiêu chí hoàn thành
Script chạy thành công với dữ liệu local và tạo được output Bronze hợp lệ.
