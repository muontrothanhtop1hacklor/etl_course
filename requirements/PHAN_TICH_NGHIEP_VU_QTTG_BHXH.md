# Phân tích nghiệp vụ QTTG BHXH và bài thực hành Bronze → Silver → Gold (ETL)

> Phần thực hành với Airflow trên Windows Docker Desktop: [HUONG_DAN_AIRFLOW_SPARK_LOCAL.md](HUONG_DAN_AIRFLOW_SPARK_LOCAL.md)

## 1. Phạm vi

Tài liệu chỉ xét hai bảng:

- `RAW_QTTG_BHXH`;
- `RAW_QTTG_BHXH_DETAIL`.

Không xét hồ sơ hưởng, quy trình thu thập dữ liệu hoặc các bảng khác.

Bộ dữ liệu thực hành:

- 142.857 bản ghi người lao động theo phiên bản trong bảng cha;
- 1.000.000 dòng detail;

---

## 2. Grain của hai bảng

### 2.1. Bảng cha `RAW_QTTG_BHXH`

**Mỗi dòng của `RAW_QTTG_BHXH` là dữ liệu của đúng một người lao động tại một `CREATED_AT`.**

Người lao động được xác định bằng `SO_SO_BHXH`; `NLD_ID` là khóa kỹ thuật liên kết người.

Một người có thể xuất hiện ở nhiều dòng nếu có nhiều `CREATED_AT` khác nhau.

```text
Người A (`SO_SO_BHXH=7000000001`)
    ├── ID 101, CREATED_AT lần 1
    ├── ID 205, CREATED_AT lần 2
    └── ID 390, CREATED_AT lần 3
```

Do đó:

- mỗi dòng `RAW_QTTG_BHXH` chỉ chứa dữ liệu của một người;
- một người có thể có một hoặc nhiều dòng theo thời gian;
- sang Silver chỉ giữ dòng có `CREATED_AT` mới nhất của mỗi người.

> Cách nhớ: **một dòng là một người tại một thời điểm; cùng người có thể có nhiều dòng theo `CREATED_AT`**.

### 2.2. Bảng detail `RAW_QTTG_BHXH_DETAIL`

**Mỗi dòng detail là một giai đoạn tham gia BHXH của người trong bản ghi cha.**

Giai đoạn thường bao gồm:

- khoảng tháng `TU_THANG`–`DEN_THANG`;
- đơn vị tham gia;
- chức danh/nơi làm việc;
- mức lương đóng;
- các hệ số, phụ cấp hoặc cờ nghiệp vụ nếu có.

`MASTER_ID` của detail trỏ tới `RAW_QTTG_BHXH.ID`. `NLD_ID` của detail phải bằng `NLD_ID` của dòng cha.

```text
Bản ghi cha của người A
    ├── Detail 1: 201801–202012 tại đơn vị X
    ├── Detail 2: 202101–202312 tại đơn vị Y
    └── Detail 3: 202401–202606 tại đơn vị Z
```

Nếu cùng người có nhiều bản ghi ở Bronze thì các giai đoạn detail có thể lặp lại giữa các lần `CREATED_AT`. Vì vậy không được lấy toàn bộ detail Bronze để báo cáo; Silver phải chọn dòng người lao động mới nhất trước.

### 2.3. Quan hệ

```text
RAW_QTTG_BHXH (1 dòng của 1 người tại 1 CREATED_AT)
        │
        └── RAW_QTTG_BHXH_DETAIL (0..N giai đoạn của dòng cha đó)
```

Một dòng bảng cha có thể không có detail. Người lao động vẫn có thể có số liệu tổng hợp ở bảng cha dù danh sách chi tiết trống.

---

## 3. Ý nghĩa các nhóm cột

### 3.1. Bảng cha `RAW_QTTG_BHXH`

| Nhóm | Cột tiêu biểu | Ý nghĩa |
|---|---|---|
| Khóa | `ID` | Khóa của dòng người lao động tại một `CREATED_AT` |
| Người lao động | `NLD_ID`, `SO_SO_BHXH` | Xác định người của dòng dữ liệu |
| Phạm vi tháng | `THANG_BD`, `THANG_KT` | Khoảng tháng được lưu ở bảng cha; có thể null |
| Tổng BHXH | `TT_TG_BHXH`, `DT_TG_BHXH`, `NAM_TG_BHXH`, `THANG_TG_BHXH` | Tổng thời gian tham gia theo dữ liệu nguồn |
| BHXH bắt buộc | `NAM_TG_BHXH_BB`, `THANG_TG_BHXH_BB` | Tổng thời gian bắt buộc |
| BHTN/BHYT | các cột `*_BHTN`, `*_BHYT` | Số liệu tổng hợp liên quan |
| Nợ đóng | `NAM_NO_*`, `THANG_NO_*`, `DD_TY_LE_NO_*` | Thời gian/tỷ lệ nợ nếu có |
| Trạng thái | `IS_ERRORS`, `NGHI_VIEC`, `IS_CONTINUE`, `TRUY_DONG` | Cờ trạng thái |
| Audit Bronze | `RAW_RESPONSE`, `CREATED_AT` | Payload và thời điểm phiên bản dữ liệu được tạo |

### 3.2. Detail

| Nhóm | Cột tiêu biểu | Ý nghĩa |
|---|---|---|
| Khóa | `ID`, `MASTER_ID`, `NLD_ID` | Khóa detail, ID dòng cha và người lao động |
| Thời gian | `TU_THANG`, `DEN_THANG`, `DOT_PHAT_SINH` | Khoảng tham gia |
| Đơn vị | `MA_DON_VI`, `TEN_DON_VI` | Đơn vị đóng BHXH |
| Công việc | `CHUC_DANH_CV`, `NOI_LAM_VIEC`, `NOI_DUNG` | Công việc trong giai đoạn |
| Lương | `MUC_LUONG`, các cột `MUC_LUONG_*`, `LUONG_CHINH` | Mức/cấu phần lương đóng |
| Hệ số/phụ cấp | `HS_LUONG`, `PC_*`, `HS_*` | Hệ số và phụ cấp nếu có |
| Tỷ lệ đóng | `TYLE_*`, `TY_LE_DONG` | Tỷ lệ đóng theo loại |
| Giá trị trước | các cột `*_PRE` | Giá trị trước thay đổi nếu nguồn có cung cấp |
| Audit Bronze | `CREATED_AT` | Thời điểm detail được tạo 

---

# THỰC HÀNH BA LAYER TÁCH BIỆT

## Layer 1 — Bronze: ingest CSV

### Mục tiêu

- Nạp CSV nguyên trạng.
- Giữ được mọi bản ghi của người lao động theo `CREATED_AT`.
- Chưa chọn dữ liệu mới nhất và chưa tổng hợp báo cáo.

### Trình tự
1. Nạp bảng cha trước.
2. Nạp detail sau.
3. Kiểm tra số dòng và quan hệ.

### Kiểm tra

```sql
SELECT COUNT(*) FROM RAW_QTTG_BHXH;          -- 142857
SELECT COUNT(*) FROM RAW_QTTG_BHXH_DETAIL;   -- 1000000

SELECT COUNT(*) AS INVALID_DETAIL
FROM RAW_QTTG_BHXH_DETAIL d
LEFT JOIN RAW_QTTG_BHXH m ON m.ID = d.MASTER_ID
WHERE m.ID IS NULL OR m.NLD_ID <> d.NLD_ID;

SELECT COUNT(*) AS INVALID_MONTH
FROM RAW_QTTG_BHXH_DETAIL
WHERE NOT REGEXP_LIKE(TU_THANG, '^[0-9]{6}$')
   OR NOT REGEXP_LIKE(DEN_THANG, '^[0-9]{6}$')
   OR SUBSTR(TU_THANG, 5, 2) NOT BETWEEN '01' AND '12'
   OR SUBSTR(DEN_THANG, 5, 2) NOT BETWEEN '01' AND '12'
   OR TU_THANG > DEN_THANG;
```

Kết quả lỗi mong đợi là 0.

---

## Layer 2 — Silver: chuẩn hóa và lấy bản ghi mới nhất của mỗi người

### Mục tiêu

- Mỗi người chỉ còn một dòng mới nhất theo `CREATED_AT`.
- Detail chỉ lấy từ dòng mới nhất đó.
- Làm sạch kiểu tháng, số tiền và khóa liên kết.
- Mỗi lần chạy sẽ soft-delete dữ liệu cũ.

### Cách chạy tách biệt

```text
1. Đọc toàn bộ master Bronze.
2. Chọn bản ghi mới nhất của mỗi người.
3. Lấy detail thuộc các master ID vừa chọn.
4. Chạy validation trên kết quả mới.
```
Silver cần thêm cột `IS_DELETED` hoặc `DELETED_AT`

### Chọn bản ghi mới nhất của mỗi người

```sql
WITH LATEST_PERSON_RECORD AS (
    SELECT m.*,
           ROW_NUMBER() OVER (
               PARTITION BY m.SO_SO_BHXH
               ORDER BY m.CREATED_AT DESC, m.ID DESC
           ) AS RN
    FROM BRONZE.RAW_QTTG_BHXH m
)
SELECT *
FROM LATEST_PERSON_RECORD
WHERE RN = 1;
```

### Chọn detail Silver

```sql
WITH LATEST_PERSON_RECORD_ID AS (
    SELECT ID
    FROM (
        SELECT m.ID,
               ROW_NUMBER() OVER (
                   PARTITION BY m.SO_SO_BHXH
                   ORDER BY m.CREATED_AT DESC, m.ID DESC
               ) AS RN
        FROM BRONZE.RAW_QTTG_BHXH m
    )
    WHERE RN = 1
)
SELECT d.*
FROM BRONZE.RAW_QTTG_BHXH_DETAIL d
JOIN LATEST_PERSON_RECORD_ID m ON m.ID = d.MASTER_ID;
```

### Kiểm tra Silver

```sql
-- Mỗi người đúng một dòng Silver
SELECT SO_SO_BHXH, COUNT(*)
FROM SILVER.QTTG_BHXH
GROUP BY SO_SO_BHXH
HAVING COUNT(*) <> 1;

-- Detail phải có dòng cha và đúng người
SELECT COUNT(*) AS INVALID_DETAIL
FROM SILVER.QTTG_BHXH_DETAIL d
LEFT JOIN SILVER.QTTG_BHXH m ON m.ID = d.MASTER_ID
WHERE m.ID IS NULL OR m.NLD_ID <> d.NLD_ID;
```

---

## Layer 3 — Gold: báo cáo tham gia BHXH theo tháng

Tạo **Báo cáo tổng hợp tham gia BHXH theo tháng**.

Gold chỉ cần trả lời:

1. Trong tháng có bao nhiêu người đang tham gia?
2. Có bao nhiêu đơn vị?
3. Tổng quỹ lương đóng là bao nhiêu?
4. Mức lương đóng bình quân là bao nhiêu?
5. Có bao nhiêu người có mức lương bằng 0?

### Grain Gold

Một dòng Gold là **một tháng báo cáo**.

| Cột | Ý nghĩa |
|---|---|
| `THANG` | Tháng `YYYYMM` |
| `SO_NGUOI_THAM_GIA` | Số người có interval bao phủ tháng |
| `SO_DON_VI` | Số đơn vị có người tham gia |
| `TONG_QUY_LUONG` | Tổng `MUC_LUONG` trong tháng |
| `LUONG_BINH_QUAN` | Bình quân các mức lương dương |
| `SO_NGUOI_LUONG_0` | Số người có `MUC_LUONG=0` |

### Cách tạo

Tạo bảng `DIM_THANG` nhỏ, sau đó join tháng với interval Silver:

```sql
CREATE TABLE GOLD.BAO_CAO_BHXH_THANG AS
SELECT
    t.THANG_ID,
    COUNT(DISTINCT m.SO_SO_BHXH) AS SO_NGUOI_THAM_GIA,
    COUNT(DISTINCT d.MA_DON_VI) AS SO_DON_VI,
    SUM(d.MUC_LUONG) AS TONG_QUY_LUONG,
    AVG(CASE WHEN d.MUC_LUONG > 0 THEN d.MUC_LUONG END) AS LUONG_BINH_QUAN,
    COUNT(DISTINCT CASE WHEN d.MUC_LUONG = 0 THEN m.SO_SO_BHXH END)
        AS SO_NGUOI_LUONG_0
FROM GOLD.DIM_THANG t
JOIN SILVER.QTTG_BHXH_DETAIL d
  ON t.THANG_ID BETWEEN d.TU_THANG AND d.DEN_THANG
JOIN SILVER.QTTG_BHXH m
  ON m.ID = d.MASTER_ID
GROUP BY t.THANG_ID;
```

### Kết quả báo cáo mẫu

| THANG_ID | SO_NGUOI_THAM_GIA | SO_DON_VI | TONG_QUY_LUONG | LUONG_BINH_QUAN | SO_NGUOI_LUONG_0 |
|---|---:|---:|---:|---:|---:|
| 202601 | ... | ... | ... | ... | ... |
| 202602 | ... | ... | ... | ... | ... |
| 202603 | ... | ... | ... | ... | ... |

Gold chạy theo kiểu rebuild/overwrite từ Silver.

---

## 5. Báo cáo kết quả

- Đẩy toàn bộ source code đã thực hiện lên git, mở public
- Readme ghi rõ kết quả, thời gian chạy của từng layer,có thể screenshot kết quả chạy được trên terminal
