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

### Giờ 1 (02:54 VN)
- Run `20261113T1800Z` và `20261113T1900Z` đều PROMOTED, quality PASS. Speed failed=0, DLQ=0, crawl OK=1273/giờ, không lỗi trong 30 phút.
- `danh_gia_ml.py report` (không PYTHONIOENCODING) lại chết ở dòng in cuối, cùng nguyên nhân cp1252 như trên; khi chạy tay phải đặt `PYTHONIOENCODING=utf-8`.
- Báo cáo sớm: `quality.flag_share` FAIL, `correctness.stability` CHUA_DU (mới 2 snapshot). Không đổi ngưỡng; sẽ ghi nguyên nhân ở KET-QUA.md.

### Giờ 2 (03:50 VN)
- Run 2000Z PROMOTED, quality PASS; speed failed=0, DLQ=0; crawl OK=1278/giờ; RAM trống 6.1 GB. Không PROBLEM.

### Giờ 3 (04:47 VN)
- Run 2100Z PROMOTED, quality PASS; speed failed=0, DLQ=0; crawl OK=1272/giờ; không lỗi 30 phút. Không PROBLEM.
- Báo cáo ML: 11/12 tiêu chí PASS (stability đã PASS). `quality.flag_share` FAIL: 5686/16284 = 34,9% dòng ngày 13/11 bị gắn cờ, khoảng cho phép 1–10%. `rule_ml_agreement` lift 2,17 (ngưỡng 2,0) PASS. Giữ nguyên ngưỡng; tìm nguyên nhân khi chốt (xem phân bố trạng thái theo nhóm/danh mục).

### Giờ 4 (05:43 VN): crawl/10m = 0
- Hiện tượng: `crawl/10m=0`, `changes/10m=0`; log crawl-worker `leased=0` liên tục; speed vẫn failed=0, DLQ=0.
- Nguyên nhân gốc: không phải lỗi. `audit.crawl_frontier` chỉ còn 25 task ACTIVE hẹn 22:54–22:55 UTC và 4235 task SWEEP hẹn 14/11 18:32–21:54 UTC; lượt quét SWEEP hiện tại (~10.8 nghìn task thành công từ 18:00) đã xong, lượt sau mới đến hạn sau ~19 giờ. Không có RATE_LIMITED/CLIENT_ERROR, container không restart.
- Hệ quả: dữ liệu mới trong các giờ còn lại chỉ đến từ tầng ACTIVE (25 task/lượt); batch và ML vẫn chạy đều trên dữ liệu hiện có. Không can thiệp lịch crawl (ngoài phạm vi kịch bản); ghi vào KET-QUA.md.
- Run 2200Z PROMOTED, quality PASS.

### Giờ 5 (06:40 VN)
- Run 2300Z PROMOTED, quality PASS (gold 1962392 không đổi so với 2200Z vì crawl chỉ còn 25 task ACTIVE/giờ). Speed failed=0, DLQ=0, không container thoát. Lỗi 30 phút: airflow-scheduler 4, kibana 5 (không kèm hiện tượng; không xử lý). Không PROBLEM.

### Giờ 6 (07:36 VN)
- Run 20261114T0000Z PROMOTED (qua nửa đêm UTC), quality PASS. Speed failed=0, DLQ=0, không lỗi 30 phút. `changes/10m=810` dù crawl=0: chưa giải thích, ghi nhận để xem khi chốt. Không PROBLEM.

### Giờ 7 (08:32 VN)
- Run 0100Z PROMOTED, quality PASS, gold 1966697 (tăng nhẹ). Speed failed=0, DLQ=0, không lỗi 30 phút, RAM trống 6 GB. Không PROBLEM.

### Giờ 8 (09:28 VN)
- Run 0200Z PROMOTED, quality PASS, gold 1966774. Speed failed=0, DLQ=0, không lỗi 30 phút. RAM trống 4.7 GB (giảm từ 6,0; trên ngưỡng 3 GB nên chưa dừng Kibana). Không PROBLEM.

### Giờ 9 (10:25 VN)
- Run 0300Z PROMOTED, quality PASS, gold 1966878. Speed failed=0, DLQ=0, không lỗi 30 phút. RAM trống 4.3 GB. Không PROBLEM.

### Giờ 9,2 (11:05 VN): RAM host trống 2,6 GB
- Hiện tượng: free RAM 2,6 GB (<3 GB); wait nền của Claude Code bị hệ thống dừng vì thiếu RAM.
- Xử lý theo mục 5 kịch bản: dừng `rf-kibana` (Superset giữ lại). Pipeline vẫn sạch (0300Z PROMOTED, failed=0, DLQ=0).

### Tiếp tục lượt chạy (theo yêu cầu "tiếp tục đi")
- Sau khi đã backup, pause DAG, dừng stack và push ở mốc 9 giờ 16 phút, người dùng yêu cầu chạy tiếp để bù tới khoảng 10 giờ. Bật lại stack không Kibana/Superset (RAM trống 7,9 GB), unpause DAG lúc 04:58 UTC. Mục tiêu: ít nhất hai run batch nữa (0400Z, 0500Z), chạy lại báo cáo ML, cập nhật KET-QUA.md.

### Kết quả phần chạy tiếp (04:58–05:39 UTC)
- Run `mp-20261114T0500Z` PROMOTED, quality PASS, gold 1.966.968, nhưng chạy 05:10–05:30 (khoảng 20 phút, các run trước 10–12 phút). Không có run `0400Z`: stack đang dừng ở giờ đó (đã dừng sau backup), nên cửa sổ này bị bỏ qua, không phải lỗi pipeline.
- Lúc đó RAM host trống 1,7 GB, và Loki/Alloy/Grafana/Prometheus đang tắt (Alloy thoát mã 2 với stack trace trong `statsd_exporter`, chưa tìm nguyên nhân).
- `performance.marts` chuyển thành **FAIL**: tệ nhất 172,8 s so với cơ sở 88,4 s (+95,5 %, ngưỡng 30 %). Nguyên nhân khả dĩ là thiếu RAM khi chạy lại; chưa kiểm chứng.
- `performance.fit` chuyển thành CHUA_DU vì Loki tắt nên không đọc được dòng `discount_ml_fitted`.
