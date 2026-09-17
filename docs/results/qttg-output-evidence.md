# QTTG Bronze/Silver output evidence

Kết quả chạy local bằng PySpark trên bộ dữ liệu QTTG BHXH. Các file CSV mẫu
được commit để có thể kiểm tra trực tiếp trên GitHub; bộ Parquet đầy đủ vẫn
được giữ ở local và không commit vì kích thước lớn.

## Row count

| Layer | Dataset | Rows | Output local |
|---|---|---:|---|
| Bronze | master | 142,857 | `solution/output/spark_lake/bronze/raw_qttg_bhxh` |
| Bronze | detail | 1,000,000 | `solution/output/spark_lake/bronze/raw_qttg_bhxh_detail` |
| Silver | master latest | 21,838 | `solution/output/spark_lake/silver/qttg_bhxh` |
| Silver | detail valid | 169,658 | `solution/output/spark_lake/silver/qttg_bhxh_detail` |
| Quarantine | detail rejected | 830,342 | `solution/output/spark_lake/quarantine/qttg_bhxh_detail` |

## Quality checks

- Silver master không có duplicate theo `SO_SO_BHXH`.
- Silver detail chỉ giữ detail thuộc master mới nhất.
- Quarantine hiện có `830,342` dòng với lý do `MASTER_NOT_CURRENT`.
- Bronze validation: `MASTER_ROWS=142857`, `DETAIL_ROWS=1000000`,
  `INVALID_DETAIL=0`, `INVALID_MONTH=0`.
- Silver output được ghi nhãn `layer=SILVER`; quarantine được ghi nhãn
  `layer=QUARANTINE`.

## Samples

- [Bronze master](../../solution/output/evidence/bronze_master_sample.csv)
- [Bronze detail](../../solution/output/evidence/bronze_detail_sample.csv)
- [Silver master](../../solution/output/evidence/silver_master_sample.csv)
- [Silver detail](../../solution/output/evidence/silver_detail_sample.csv)
- [Quarantine detail](../../solution/output/evidence/quarantine_detail_sample.csv)