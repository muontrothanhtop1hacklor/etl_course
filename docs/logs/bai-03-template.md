# Bài 03 - Gold / Aggregation Layer

Ngày thực hiện: 2026-09-17

Trạng thái: Hoàn thành

## 1. Mục tiêu

- Tạo các bảng tổng hợp / mart từ Silver
- Tính KPI theo thời gian và đơn vị
- Chuẩn bị dữ liệu cho dashboard hoặc báo cáo

## 2. Bối cảnh

- Silver là dữ liệu đã được làm sạch
- Gold cần tập trung vào báo cáo, phân tích và mô hình hóa KPI

## 3. Công việc đã làm

- Viết job [gold_qttg.py](../../solution/apps/qttg/gold_qttg.py).
- Đọc dữ liệu Silver master và detail.
- Tạo `dim_thang` bằng cách bung khoảng tháng nhỏ nhất đến lớn nhất trong
	`TU_THANG` - `DEN_THANG`.
- Join tháng báo cáo với các interval Silver.
- Tính `SO_NGUOI_THAM_GIA`, `SO_DON_VI`, `TONG_QUY_LUONG`,
	`LUONG_BINH_QUAN` và `SO_NGUOI_LUONG_0`.
- Ghi Parquet vào `solution/output/spark_lake/gold`.

## 4. Kết quả thực tế

- Output dimension: `gold/dim_thang`.
- Output báo cáo: `gold/bao_cao_bhxh_thang`.
- Số dòng báo cáo: `629`.
- Số tháng trong dimension: `629`.
- Báo cáo không có `THANG_ID` trùng.
- Spark chạy thành công với dữ liệu Silver hiện tại.

Log xác nhận:

```text
LAYER=GOLD STATUS=SUCCESS REPORT_ROWS=629 DIM_MONTH_ROWS=629
```

Một số dòng mẫu sau khi chạy:

| THANG_ID | SO_NGUOI_THAM_GIA | SO_DON_VI | TONG_QUY_LUONG | LUONG_BINH_QUAN | SO_NGUOI_LUONG_0 |
|---|---:|---:|---:|---:|---:|
| 197402 | 40 | 40 | 165500000.00 | 5338709.677419 | 9 |
| 197403 | 84 | 84 | 318650000.00 | 5223770.491803 | 23 |
| 197404 | 123 | 121 | 503750000.00 | 5857558.139535 | 37 |

## 5. Chú ý kỹ thuật

- Silver chịu trách nhiệm làm sạch và loại dữ liệu lỗi; Gold chỉ đọc dữ liệu
	sạch để tổng hợp.
- Join interval dùng điều kiện `TU_THANG <= THANG_ID <= DEN_THANG`.
- `LUONG_BINH_QUAN` chỉ lấy các mức `MUC_LUONG > 0`.
- Job dùng `overwrite` để rebuild báo cáo khi chạy lại.

## 6. Kiểm tra đã thực hiện

```powershell
python -m py_compile solution/apps/qttg/gold_qttg.py
python solution/apps/qttg/gold_qttg.py
```

Cả hai lệnh đều hoàn thành thành công.

## 7. Kế hoạch tiếp theo

- Tạo app ETL/ELT để chạy pipeline có cấu trúc hơn
- Tìm hiểu Airflow và scheduler cho pipeline lớn
