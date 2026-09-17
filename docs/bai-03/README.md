# Bài 03 - Gold và Aggregation

## Mục tiêu
Tạo báo cáo Gold theo từng tháng từ dữ liệu Silver đã làm sạch, phục vụ KPI,
dashboard và phân tích nghiệp vụ BHXH.

## Phạm vi
- Đọc Silver master và detail bằng PySpark
- Tạo danh sách tháng báo cáo từ khoảng `TU_THANG` - `DEN_THANG`
- Join tháng với interval tham gia tương ứng
- Tính các KPI theo tháng:
	- `SO_NGUOI_THAM_GIA`
	- `SO_DON_VI`
	- `TONG_QUY_LUONG`
	- `LUONG_BINH_QUAN` (chỉ tính các mức lương dương)
	- `SO_NGUOI_LUONG_0`
- Ghi kết quả dạng Parquet để có thể đọc lại bằng Spark hoặc công cụ BI

## Code liên quan
- [gold_qttg.py](../../solution/apps/qttg/gold_qttg.py)
- [Bài 02 - Silver](../bai-02/README.md)

## Output

- `solution/output/spark_lake/gold/dim_thang`
- `solution/output/spark_lake/gold/bao_cao_bhxh_thang`

Mỗi dòng trong `bao_cao_bhxh_thang` tương ứng với một tháng `THANG_ID` dạng
`YYYYMM`. Job chạy theo kiểu rebuild và overwrite output từ Silver.

## Cách chạy

Từ thư mục gốc repository:

```powershell
python solution/apps/qttg/gold_qttg.py
```

Job kiểm tra interval không hợp lệ và không cho phép báo cáo có tháng trùng.
Sau khi chạy thành công, log có dạng:

```text
LAYER=GOLD STATUS=SUCCESS REPORT_ROWS=629 DIM_MONTH_ROWS=629
```

Kết quả kiểm tra ngày 2026-09-17: tạo thành công 629 dòng báo cáo tháng và
629 dòng danh sách tháng từ dữ liệu Silver hiện có.

## Nhật ký
- [Log bài 03](../logs/bai-03-template.md)

## Tiêu chí hoàn thành
- Có một dòng Gold cho mỗi tháng báo cáo.
- Có đủ 5 KPI nghiệp vụ yêu cầu.
- Có kiểm tra dữ liệu đầu vào và kiểm tra không trùng tháng.
- Chạy thành công trên dữ liệu Silver hiện tại.
