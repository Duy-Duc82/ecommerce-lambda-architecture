# Mô hình dữ liệu marketplace: file để vẽ sơ đồ

Star schema của Phase 10, đặc tả ở `docs/DATA_MODEL.md` mục 4 (đã duyệt 24/10): 6 dimension, 1 closure table, 5 fact, tổng cộng 12 bảng và 31 quan hệ khoá ngoại. Code dựng các bảng này làm từ P1 trở đi.

| File | Công cụ | Cách mở | Đã kiểm tra |
|---|---|---|---|
| `marketplace_star_schema.png` | (ảnh) | Mở trực tiếp | Render từ file `.puml` bằng `plantuml/plantuml:1.2024.7` |
| `star-schema.puml` | PlantUML | https://www.plantuml.com/plantuml, hoặc extension "PlantUML" trong VS Code (Alt+D) | Render thành công |
| `star-schema.dbml` | dbdiagram.io | Dán vào https://dbdiagram.io/d | Parse bằng `@dbml/core` 3.14.1: 12 bảng, 31 ref |
| `star-schema.sql` | DBeaver, pgAdmin, DataGrip | Chạy trên một database trống, rồi DBeaver → schema `dw` → View Diagram, hoặc pgAdmin → ERD Tool | Chạy trên Postgres 18.3: 12 bảng, 31 FK |
| `star-schema.mmd` | Mermaid | Dán vào https://mermaid.live, hoặc xem bằng Markdown preview của VS Code/GitHub | Parse bằng `mermaid` 11.4.1; ERD trong `DATA_MODEL.md` chép từ file này |
| `current-marts.mmd` | Mermaid | như trên | Mô hình **trước Phase 10** (10 mart đọc thẳng Silver); parse bằng `mermaid` 11.4.1 |

Muốn render lại ảnh PNG sau khi sửa file `.puml`:

```sh
docker run --rm -v "$PWD":/data plantuml/plantuml:1.2024.7 -tpng /data/star-schema.puml
```
