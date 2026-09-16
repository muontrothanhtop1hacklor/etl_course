# Bài 02 - Silver / Data Quality

## Mục tiêu
Xây dựng layer Silver từ Bronze cho QTTG BHXH, đảm bảo:
- mỗi `SO_SO_BHXH` còn đúng 1 master mới nhất
- detail chỉ thuộc master mới nhất
- dữ liệu tháng và lương được kiểm tra, chuẩn hóa và tách lỗi
- output Silver có thể dùng cho Gold mà không cần xử lý lại dữ liệu lỗi

## Phạm vi
- Đọc dữ liệu Bronze: master và detail
- Chọn bản ghi master mới nhất theo:
  - `PARTITION BY SO_SO_BHXH`
  - `ORDER BY CREATED_AT DESC, ID DESC`
  - `ROW_NUMBER() = 1`
- Chỉ giữ detail có `MASTER_ID` thuộc master mới nhất
- Chuẩn hóa `TU_THANG`, `DEN_THANG`
- Kiểm tra:
  - định dạng `YYYYMM`
  - năm trong khoảng `1960` đến hiện tại
  - tháng trong `1..12`
  - `TU_THANG <= DEN_THANG`
  - `MUC_LUONG` có thể cast sang số
- Tách dữ liệu lỗi sang quarantine
- Ghi output Silver:
  - `output/spark_lake/silver/qttg_bhxh`
  - `output/spark_lake/silver/qttg_bhxh_detail`

## Code liên quan
- [silver_qttg.py](../../solution/apps/qttg/silver_qttg.py)
- [Bài 01 - Bronze](../bai-01/README.md)
- [Log bài 02](../logs/bai-02-template.md)

## Quy tắc quality
- `SO_SO_BHXH` không được duplicate trong master Silver
- detail Silver phải join đúng master Silver
- record có `MASTER_ID` cũ hoặc `NLD_ID` sai phải được tách sang quarantine
- record có tháng/lương sai phải được tách sang quarantine
- `NULL` validation phải coi là không hợp lệ, không được mất ngầm

## Tiêu chí hoàn thành
- Silver Master duy nhất cho mỗi người
- Detail Silver join đúng master
- Quarantine chứa data lỗi để dễ audit và debug
- Gold có thể đọc dữ liệu sạch mà không phải xử lý bẩn từ Bronze
