# Báo cáo refactor kiến trúc (`refactor/architecture`)

Plan: `docs/PLAN_REFACTOR_ARCHITECTURE.md`. Worktree `C:\code\data-refactor`, branch `refactor/architecture`, tạo từ `phase-10-dimensional-model`.

Mỗi phase có một mục: việc đã làm, số test, các quyết định nhỏ tự chốt. Bảng nghiệm thu AC1–AC14 ở cuối file.

---

## R0. Worktree, branch, mốc test

- `git -C C:\code\data worktree add C:\code\data-refactor -b refactor/architecture phase-10-dimensional-model`.
- **Lệch so với plan:** HEAD của `phase-10-dimensional-model` lúc tạo branch là `f991af5`, không phải `53a94bf` như ghi trong plan mục 0. Ba commit thêm vào sau `53a94bf`: `b39deac` (DATA_MODEL star schema), `67bf754` (registry Gold dataset `config/marketplace_datasets.py`), `f991af5` (các dimension của star). Refactor làm trên `f991af5`, nên các module này cũng được chuyển chỗ như phần còn lại.
- Commit đầu: chép `PLAN_REFACTOR_ARCHITECTURE.md` và `PLAN_PHASE10_REVISED.md` từ `C:\code\data-phase10\docs\` (không đụng các file chưa commit ở worktree đó).
- **Mốc test** (chạy ở worktree mới, `C:\code\data\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider -rs`):

  | | Giá trị |
  |---|---|
  | passed | **1244** |
  | skipped | 0 |
  | deselected | 11 (11 drill, marker `drill`) |
  | thời gian | 952 s |

  Số passed cao hơn mốc 1179 ghi trong plan vì branch đã có thêm test của registry dataset và dimension (hai commit nói trên). Mốc dùng cho AC4 là **1244**.
- **Trạng thái Docker trước khi làm** (để so ở AC11/AC14): `docs/evidence/refactor-20261024/baseline/`
  - `ps_before_short.txt`: 49 container, tất cả `exited`;
  - `volumes_before.txt`: 76 volume;
  - `images_before.txt`: ID của 3 tag cũ
    - `ecommerce/marketplace-python:1` = `sha256:23e5681c…`
    - `ecommerce/spark-marketplace:4.0.1` = `sha256:b81bc0fc…`
    - `ecommerce/spark-warehouse:3.5.1` = `sha256:dadc7cda…`
