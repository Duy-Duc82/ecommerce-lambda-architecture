# Hình cho báo cáo LaTeX

Mỗi sơ đồ/biểu đồ trong `docs/report` là một ô trống chờ hình. Vẽ xong, lưu file vào thư mục này
**đúng tên** dưới đây, đuôi `.pdf` (vector, nên dùng), `.png` (≥ 2000 px ngang) hoặc `.jpg`.
Lần biên dịch sau ô trống tự được thay bằng hình. Spec (bảng node, cạnh, màu, caption) nằm trong `docs/system/`.

| Tên file | Spec | Nội dung | Chương |
|---|---|---|---|
| `logo-hust` | — | logo trường cho trang bìa | bìa |
| `fig-1-1-toan-canh-lambda` | Hình 1.1, `01-tong-quan.md` §1.2 | toàn cảnh Lambda | 3 |
| `fig-1-2-tang-module` | Hình 1.2, `01-tong-quan.md` §1.4 | tầng module | 3 |
| `fig-3-1-vong-doi-task` | Hình 3.1, `03-xu-ly-trong-tang.md` §3.1 | state diagram task frontier | 4 |
| `fig-4-3-sequence-thu-thap` | Hình 4.3, `04-pipeline.md` §4.2 | sequence một lượt thu thập | 4 |
| `fig-2-3-star-schema` | Hình 2.3, `02-luong-du-lieu.md` §H7 | ER star schema (có thể dựa `docs/model/marketplace_star_schema.png`) | 5 |
| `fig-3-3-scd2-listing` | Hình 3.3, `03-xu-ly-trong-tang.md` §3.6 | SCD2 `dim_product_listing` | 5 |
| `fig-2-1-luong-du-lieu` | Hình 2.1, `02-luong-du-lieu.md` §2.1 | luồng dữ liệu H1–H9 | 6 |
| `fig-2-2-stage-silver` | Hình 2.2, `02-luong-du-lieu.md` §H6 | stage silver | 6 |
| `fig-2-4-quality-publish-promote` | Hình 2.4, `02-luong-du-lieu.md` §H9 | quality, publish, promote | 6 |
| `fig-3-2-speed-queries` | Hình 3.2, `03-xu-ly-trong-tang.md` §3.4 | hai query speed | 6 |
| `fig-4-4-speed-lakehouse` | Hình 4.4, `04-pipeline.md` §4.5 | speed lakehouse | 6 |
| `fig-3-4-cay-quyet-dinh-anomaly` | Hình 3.4, `03-xu-ly-trong-tang.md` §3.7 | cây quyết định giá bất thường | 6 |
| `fig-4-1-ban-do-pipeline` | Hình 4.1, `04-pipeline.md` §4.1 | bản đồ pipeline | 7 |
| `fig-4-2-gantt-mot-gio` | Hình 4.2, `04-pipeline.md` §4.1 | Gantt một giờ vận hành | 7 |
| `fig-4-5-dag-marketplace-batch` | Hình 4.5, `04-pipeline.md` §4.6 | DAG batch | 7 |
| `fig-4-6-state-machine-run` | Hình 4.6, `04-pipeline.md` §4.6 | state machine run | 7 |
| `fig-3-5-quyet-dinh-quality` | Hình 3.5, `03-xu-ly-trong-tang.md` §3.8 | quyết định cổng chất lượng | 8 |
| `fig-2-5-truy-nguoc-lineage` | Hình 2.5, `02-luong-du-lieu.md` §2.7 | truy ngược lineage | 8 |
| `fig-9-1-dashboard-superset` | — | ảnh chụp Superset | 9 |
| `fig-9-2-dashboard-kibana` | — | ảnh chụp Kibana | 9 |
| `fig-1-3-trien-khai-local` | Hình 1.3, `01-tong-quan.md` §1.6 | triển khai local | 10 |
| `fig-1-4-trien-khai-hybrid` | Hình 1.4, `01-tong-quan.md` §1.6 | triển khai hybrid | 10 |
| `fig-11-1-giam-sat` | — (`docs/OBSERVABILITY.md`) | kiến trúc giám sát | 11 |
| `fig-4-7-dag-bao-tri` | Hình 4.7, `04-pipeline.md` §4.10 | DAG bảo trì, backup | 11 |
| `fig-12-1-gold-rows-theo-run` | — (`docs/evidence/fullrun-20261114/`) | biểu đồ dòng Gold theo run | 12 |
| `fig-12-2-thoi-gian-stage` | — (`timings/<stage>.json`) | biểu đồ thời gian stage | 12 |
| `fig-12-3-do-tre-speed` | — (`audit.marketplace_speed_batch`) | biểu đồ độ trễ speed | 12 |
