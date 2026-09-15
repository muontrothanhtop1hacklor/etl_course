# Bài 02 - Silver / Data Quality

## 1. Mục tiêu

- Chuẩn hóa dữ liệu từ Bronze
- Tách dữ liệu hợp lệ / không hợp lệ
- Làm sạch null, duplicate, format không đồng nhất

## 2. Bối cảnh

- Bronze lưu dữ liệu thô theo từng file nguồn
- Silver là nơi bắt đầu làm sạch và chuẩn hóa
- Dữ liệu cần được sắp xếp theo logic business hơn là chỉ lưu raw

## 3. Công việc sẽ làm

- Đọc dữ liệu từ Bronze
- Loại bỏ dữ liệu trùng lặp
- Xử lý null / invalid / month mismatch
- Chuyển dữ liệu sang định dạng chuẩn cho tầng tiếp theo

## 4. Kết quả mong đợi

- Dữ liệu Silver sạch hơn
- Ít bản ghi lỗi hơn
- Có thể dùng cho phân tích / báo cáo

## 5. Vấn đề cần lưu ý

- Không mất dữ liệu gốc khi chuyển từ Bronze sang Silver
- Cần định nghĩa rõ “hợp lệ” trước khi lọc
- Có thể tách thành 2 tập: valid_data và invalid_data

## 6. Ghi chú

- Bài này nên được triển khai sau khi Bronze đã ổn
- Cần có cấu trúc rõ ràng để dễ mở rộng theo thời gian

## 7. Kế hoạch cho buổi sau

- Viết Gold với dữ liệu đã đủ sạch để làm KPI / báo cáo
- Tối ưu pipeline cho dữ liệu lớn hơn 1 triệu bản ghi
