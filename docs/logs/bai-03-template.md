# Bài 03 - Gold / Aggregation Layer

## 1. Mục tiêu

- Tạo các bảng tổng hợp / mart từ Silver
- Tính KPI theo thời gian và đơn vị
- Chuẩn bị dữ liệu cho dashboard hoặc báo cáo

## 2. Bối cảnh

- Silver là dữ liệu đã được làm sạch
- Gold cần tập trung vào báo cáo, phân tích và mô hình hóa KPI

## 3. Công việc sẽ làm

- Tạo aggregated tables
- Tính số liệu theo tháng, năm, phân loại
- Đưa dữ liệu vào định dạng dễ phân tích

## 4. Kết quả mong đợi

- Có bảng tổng hợp có thể dùng cho dashboard
- Dễ mở rộng thêm metric mới

## 5. Chú ý kỹ thuật

- Tránh làm quá nhiều business logic trong Bronze
- Gold nên tập trung vào aggregations và reporting

## 6. Kế hoạch tiếp theo

- Tạo app ETL/ELT để chạy pipeline có cấu trúc hơn
- Tìm hiểu Airflow và scheduler cho pipeline lớn
