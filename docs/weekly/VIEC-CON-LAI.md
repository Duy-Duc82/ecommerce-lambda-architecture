# Báo cáo tuần: việc còn lại (cần chạy trên PC)

Ghi ngày 25/11/2026. Báo cáo: `bao-cao-ky-thuat.tex`; biên dịch `xelatex -output-directory=build bao-cao-ky-thuat.tex` (2 lần).

## Cần bật stack

1. **Ảnh Superset và Kibana.** Chụp dashboard lịch sử giá (Superset) và thay đổi giá (Kibana), lưu vào `figures/`
   (ví dụ `out-1-superset.png`, `out-2-kibana.png`), thêm `\figslot{...}` vào mục 9 hoặc một mục "Kết quả" riêng.
   Xong thì bỏ dòng "Phục vụ: chưa kiểm tra giao diện Superset/Kibana" trong bảng Hạn chế (mục 10).
2. **Số dòng quarantine theo lý do.** Đọc `_runs/run_id=<run>/silver.json` của một run (khoá `quarantine_by_reason`,
   `silver_rows`, `duplicates_before_dedup`), thêm một bảng nhỏ vào mục 5.1 sau danh sách các bước làm sạch.
3. **Một ví dụ cụ thể.** Một sản phẩm bị gắn cờ giá bất thường hoặc khuyến mãi SUSPECT, kèm lịch sử giá theo ngày
   (lấy từ `docs/evidence/fullrun-20261114/nhan-tay-lich-su-gia.csv` hoặc mart `offer_price_history_daily`).
   Nên vẽ thành biểu đồ đường giá, đặt ở mục 5.2/5.3.

## Không cần stack

4. ~~Hình 1 còn chữ phụ~~: đã xong 25/11. Quy ước: trong hình chỉ để tên thành phần và luồng, thông số để trong chữ.
   Vẽ lại: Graphviz `dot` trong PATH, `pip install diagrams==0.24.4`, chạy `python fig-0-cong-nghe.py` trong `figures/src`
   rồi chuyển `fig-0a-pipeline.pdf`, `fig-0b-giam-sat.pdf` lên `figures/`.
5. Khoảng trắng cuối trang 6 và 11: nếu muốn bỏ thì thu nhỏ Hình 3 (luồng crawl) và Hình 6 (speed).
