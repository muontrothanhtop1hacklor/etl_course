# Bài 02 - Silver / Data Quality

## 1. Mục tiêu
- Chọn dữ liệu master mới nhất cho từng `SO_SO_BHXH`
- Chỉ giữ detail thuộc master mới nhất
- Chuẩn hóa tháng/lương và tách dữ liệu lỗi sang quarantine
- Tạo output Silver dùng cho Gold

## 2. Bối cảnh
- Bronze lưu dữ liệu thô, chưa làm sạch business logic
- Silver là nơi phân loại, loại trùng và đảm bảo dữ liệu đúng với chuẩn nghiệp vụ
- Mỗi người lao động chỉ nên có 1 dòng master mới nhất trong Silver

## 3. Công việc đã làm
- Đọc `raw_qttg_bhxh` và `raw_qttg_bhxh_detail` từ Bronze
- `deduplicate_latest()` với:
  - `PARTITION BY SO_SO_BHXH`
  - `ORDER BY CREATED_AT DESC, ID DESC`
  - `ROW_NUMBER() = 1`
- Join detail với master mới nhất bằng `MASTER_ID`
- Tách detail của master cũ/orphan sang quarantine
- Chuẩn hóa `TU_THANG` và `DEN_THANG`
- Kiểm tra:
  - format `YYYYMM`
  - năn trong giới hạn
  - tháng hợp lệ `01..12`
  - `TU_THANG <= DEN_THANG`
  - `MUC_LUONG` số hợp lệ
- Tách dữ liệu không hợp lệ sang `output/spark_lake/quarantine/qttg_bhxh_detail`
- Ghi Silver output:
  - `output/spark_lake/silver/qttg_bhxh`
  - `output/spark_lake/silver/qttg_bhxh_detail`

## 4. Kết quả thực tế
- Master Silver không có duplicate theo `SO_SO_BHXH`
- Detail Silver join đúng với master
- Quarantine có các record lỗi theo `MASTER_NOT_CURRENT`, `NLD_ID_MISMATCH`, `INVALID_VALUE`
- `NULL` validation được coi là `False`, không bị mất ngầm

## 5. Vấn đề cần lưu ý
- Dữ liệu lỗi không được mất mà phải tách rõ để audit
- Quy tắc `NULL = invalid` cần được xử lý rõ ràng
- `CREATED_AT` nên được cast sang timestamp để sắp xếp đúng thời gian
- Cần cache các dataframe dùng nhiều lần để giảm thời gian tính toán

## 6. Ghi chú
- Đây là bước quan trọng chuẩn bị cho Gold
- Silver nên là layer “đã sạch”, còn Gold chỉ làm aggregation và KPI
- Parquet đầy đủ không nên commit lên GitHub vì là dữ liệu sinh ra runtime; bộ sample output nhỏ được lưu tại [evidence](../../solution/output/evidence/) để minh chứng kết quả.

## 7. Kế hoạch tiếp theo
- Viết Gold layer cho KPI và báo cáo
- Tăng độ rõ ràng cho schema Silver
- Thêm kiểm tra phần business rule quan trọng hơn nếu dataset lớn hơn
