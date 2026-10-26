# Mô hình dữ liệu marketplace: file để vẽ sơ đồ

Star schema của Phase 10, đặc tả ở `docs/DATA_MODEL.md` mục 4 (duyệt 24/10, sửa theo D0 ngày 26/10 cho hai nguồn Tiki, Lazada và bảng rộng của Silver): 7 dimension, 1 closure table, 6 fact, tổng cộng 14 bảng và 40 quan hệ khoá ngoại. Star chỉ nằm trong lake dạng Delta; các kiểu PostgreSQL trong file chỉ để vẽ.

> `.mmd`, `.dbml`, `.puml`, `.sql` theo thiết kế D0 (`star-schema.sql` khớp registry `domain/datasets.py`, test `tests/unit/domain/test_marketplace_datasets.py` kiểm). Riêng `marketplace_star_schema.png` còn là bản 12 bảng cũ, render lại khi Docker chạy.

| File | Công cụ | Cách mở | Đã kiểm tra |
|---|---|---|---|
| `marketplace_star_schema.png` | (ảnh) | Mở trực tiếp | Render từ file `.puml` bằng `plantuml/plantuml:1.2024.7` |
| `star-schema.puml` | PlantUML | https://www.plantuml.com/plantuml, hoặc extension "PlantUML" trong VS Code (Alt+D) | Render thành công |
| `star-schema.dbml` | dbdiagram.io | Dán vào https://dbdiagram.io/d | Parse bằng `@dbml/core` 3.14.1: 14 bảng, 40 ref (26/10) |
| `star-schema.sql` | DBeaver, pgAdmin, DataGrip | Chạy trên một database trống, rồi DBeaver → schema `dw` → View Diagram, hoặc pgAdmin → ERD Tool | Bản 12 bảng đã chạy trên Postgres 18.3; bản 14 bảng chỉ mới kiểm bằng test registry |
| `star-schema.mmd` | Mermaid | Dán vào https://mermaid.live, hoặc xem bằng Markdown preview của VS Code/GitHub | Parse bằng `mermaid` 11.4.1 (26/10); ERD trong `DATA_MODEL.md` chép từ file này |
| `current-marts.mmd` | Mermaid | như trên | Mô hình **trước Phase 10** (10 mart đọc thẳng Silver); parse bằng `mermaid` 11.4.1 |

Muốn render lại ảnh PNG sau khi sửa file `.puml`:

```sh
docker run --rm -v "$PWD":/data plantuml/plantuml:1.2024.7 -tpng /data/star-schema.puml
```
