# Nhật ký lượt chạy toàn luồng 10 giờ (14/11/2026)

- **Mốc giờ 0:** 2026-11-13 18:53 UTC = 2026-11-14 01:53 giờ VN (xong bước 1.6).
- Chuẩn bị: pause DAG, `migrate` (2 bảng cache discount đã có), build image (scikit-learn 1.7.2 có trong image), bật crawl/speed/orchestration/serve/ops, `audit-export` (10846 lượt), unpause.

## Sự cố
(chưa có)

### Giờ 0 (01:54 VN): `theo-doi.sh` lỗi SQL ở mục ml
- Hiện tượng: `aggregate functions are not allowed in GROUP BY`.
- Nguyên nhân gốc: `GROUP BY 1` trỏ tới biểu thức `credibility_status || '=' || count(*)` có chứa hàm gộp.
- Sửa: `GROUP BY credibility_status`. Kiểm chứng ở lượt theo dõi kế tiếp.
- Ghi chú: cảnh báo `newest promoted window 152.9 h old` là dự kiến (run `mp-20261113T1800Z` đã QUALITY_PASSED, đang tới promote).

### Giờ 0 (01:57 VN): `danh_gia_ml.py snapshot` chết khi in tiếng Việt
- Hiện tượng: `UnicodeEncodeError: 'charmap' codec` khi stdout bị pipe (Python Windows dùng cp1252). Chỉ xảy ra khi snapshot đã tồn tại và nhánh "đã có snapshot" in ra.
- Sửa: `export PYTHONIOENCODING=utf-8` trong `theo-doi.sh`. Không đổi ngưỡng hay logic đo.
