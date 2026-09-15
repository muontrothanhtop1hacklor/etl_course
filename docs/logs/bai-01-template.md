# Bài 01 - Bronze Ingest

## 1. Mục tiêu

- Hiểu về dữ liệu gốc QTTG BHXH
- Đọc file CSV raw vào Spark
- Thiết lập tầng Bronze đầu tiên
- Validate dữ liệu cơ bản trước khi lưu

## 2. Bối cảnh nghiệp vụ

- Dữ liệu có thể gồm nhiều file master/detail
- Cần kiểm tra schema thật sự của từng bảng
- Một số cột bắt buộc phải có đầy đủ dữ liệu
- Cần xác định mối quan hệ giữa bảng cha và bảng con

## 3. Công việc đã làm

- Kiểm tra cấu trúc file CSV
- Xác định cột bắt buộc
- Viết logic validate dữ liệu
- Tạo output Bronze bằng Parquet

## 4. Kết quả

- Script chạy thành công
- Dữ liệu được lưu vào output Bronze
- Có metadata: source_file, layer, load_time

## 5. Vấn đề gặp phải

- Hiểu sai semantics của `parents[3]`
- Cần phân biệt path gốc với path file dữ liệu
- Cần hiểu rõ không chỉ đọc file mà còn validate business rules

## 6. Học được

- `Path.parents[n]` phụ thuộc vào vị trí file code
- Bronze là tầng lưu dữ liệu thô, chưa làm sạch sâu
- Validation phải căn cứ vào business rule chứ không chỉ schema máy

## 7. Bước tiếp theo

- Tạo tầng Silver cho việc chuẩn hóa và loại dữ liệu không hợp lệ
- Chuẩn bị phân lớp rõ ràng hơn theo từng buổi học
- Tạo docs từng bài tập trung vào từng issue kỹ thuật
