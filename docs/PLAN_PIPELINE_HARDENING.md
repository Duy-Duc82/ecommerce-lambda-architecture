# Căn chỉnh pipeline từ đầu đến cuối (góc nhìn DE)

Ngày 2026-10-27. Trạng thái: **đã duyệt ngày 2026-10-27** (mục 6). Nhánh `phase-10/star-on-refactor`.

Đi cùng `docs/PLAN_PHASE10_ON_REFACTOR.md`: D4 (mart dựng từ fact) làm xong trước; D5–D7 được xếp lại vào các gói H bên dưới vì chúng chạm cùng chỗ.

## 1. Vì sao cần

Một lượt rà soát toàn mã (27/10, chỉ đọc) tìm ra bốn lỗi làm pipeline sai khi chạy lại hoặc chạy bù, và bảy lỗ hổng thiết kế. Bốn lỗi P0 đã được kiểm lại trực tiếp trên mã.

| | Lỗi | Bằng chứng | Hậu quả |
|---|---|---|---|
| P0-1 | Chạy lại một run cũ (backfill, `--force`) xoá phần mới của star | `stages/dimensions.py`, `stages/facts.py` cắt Silver ở `as_of`; `lake.merge_statement` có `WHEN NOT MATCHED BY SOURCE THEN DELETE` | mọi dòng star sau `as_of` cũ bị xoá, run kế tiếp phải dựng lại |
| P0-2 | Chạy run cũ cũng làm ngắn Silver | `stages/silver.py:81` lọc `fetched_at <= as_of`; `*_listing_wide` ghi đè cả bảng (`partition_by=()`); `page_context`, `quarantine` chỉ partition theo `source_code` | Silver dùng chung mất dữ liệu, không chỉ khi có `--silver-since` |
| P0-3 | Runtime mặc định Parquet | `settings`: `DATA_LAKE_TABLE_FORMAT` mặc định `parquet`, compose không đặt | star không có MERGE, không version, ghi không nguyên tử; Delta MERGE trên MinIO chưa từng chạy thử |
| P0-4 | Star được ghi trước cổng chất lượng | `dimensions`, `facts` MERGE vào `dw/` trước `quality`; cổng chỉ xét Silver và mart | run hỏng chất lượng đã đổi star và không có gì đánh dấu |

P1 (thiết kế): không có gì tăng dần (Bronze đọc toàn bộ với `recursiveFileLookup` nên không cắt partition; Silver đọc lại ở 4 stage; crawl audit quét JDBC toàn bảng); run không tái lập được (manifest v1 không ghi version `dw`, `marts` đọc bản mới nhất); không có luật chất lượng cho star (khoá ngoại, tỉ lệ khoá `-1`, SCD2 chồng khoảng); payload drift chỉ được báo, không chặn; DAG không có độ trễ, timeout, SLA; `quality` bị retry 3 lần dù lỗi tất định; không có thiết kế backfill.

## 2. Nguyên tắc

1. **Bronze là nguồn sự thật, chỉ ghi thêm.** Mọi tầng sau dựng lại được từ Bronze.
2. **Silver không bao giờ bị cắt theo `as_of`.** Một ngày `observed_date` của Silver luôn là toàn bộ Bronze của ngày đó. `as_of` chỉ áp lúc đọc.
3. **Mỗi run sở hữu một cửa sổ ngày** (các `observed_date` có Bronze mới từ watermark trước tới `as_of`, cộng số ngày trễ cho phép) và chỉ ghi lại các partition trong cửa sổ đó. Không lệnh nào xoá dữ liệu ngoài cửa sổ.
4. **Tăng dần phải bằng dựng lại toàn bộ.** Chế độ `--rebuild` là chuẩn so sánh, có test parity.
5. **Không gì được thấy trước khi qua cổng chất lượng.** Run hỏng thì star quay về version đã được chứng nhận.
6. **Run tái lập được:** manifest ghi version Delta của mọi bảng nó đọc và ghi.

## 3. Gói công việc

| | Việc | Kiểm |
|---|---|---|
| H0 | Xong D4 (mart từ fact, test parity 10 mart); commit | suite Spark xanh |
| H1 | **Lake đúng.** Delta là mặc định ở compose và env (Parquet chỉ còn cho test bộ nhớ); `dw` không phải Delta thì dừng ngay. Silver: mọi bảng partition theo `observed_date` (và `source_code`), ghi bằng `replaceWhere` cho các ngày của cửa sổ, đọc Bronze của đúng các ngày đó, không lọc `as_of`. Star: bỏ `NOT MATCHED BY SOURCE DELETE` ngoài `--rebuild`; fact thay theo `date_key` của cửa sổ; dim upsert theo khoá chạm tới. Smoke Delta MERGE trên MinIO trong Docker | test: chạy run cũ sau run mới không làm mất dòng nào; chạy lại hai lần cho cùng bảng |
| H2 | **Tăng dần.** Watermark theo `ingested_at` của Bronze lưu trong lake; Bronze đọc theo đường dẫn partition; SCD2 `dim_product_listing` tính lại chỉ cho các offer chạm tới, đọc lịch sử của chúng từ `fact_offer_observation`; đo dữ liệu và thời gian mỗi stage vào summary | test parity tăng dần = `--rebuild` trên fixture nhiều ngày; thời gian run không tăng theo tổng Silver (đo ở H7) |
| H3 | **Cổng chất lượng và tái lập.** Luật bắt buộc mới: khoá fact có trong dim (anti-join), một version hiện hành và không chồng khoảng cho SCD2, không trùng khoá surrogate; luật khuyến nghị: tỉ lệ khoá `-1` theo dim, tỉ lệ quarantine, tỉ lệ null theo trường khai báo, trường khai báo biến mất (drift). Manifest v2 ghi version `dw`; `marts`, `quality` đọc `VERSION AS OF`; run hỏng chất lượng thì `RESTORE` các bảng `dw` về version của manifest hiện hành | test: run hỏng không để lại dấu trên star; đọc lại run cũ cho cùng mart |
| H4 | **Điều phối.** DAG: độ trễ trước khi chạy (crawl trong cửa sổ kịp ghi xong), `execution_timeout`, cảnh báo quá hạn, `retries=0` cho `quality`, `--resume` chỉ khi là lần retry; DAG `marketplace_backfill` riêng chạy theo khoảng ngày (an toàn nhờ H1). Cân nhắc gộp các stage Spark vào một container để không khởi động JVM 7 lần mỗi giờ | `tests/airflow`; chạy trên `mp-refactor` |
| H5 | **Audit trong lake** (D7 cũ): run và kết quả quality, crawl attempt/run ghi ra lake dạng Delta; `read_crawl_audit` đọc lake theo cửa sổ thay cho quét JDBC. `crawl_frontier` ở lại Postgres | mart `crawl_reliability_daily`, `source_coverage_daily` khớp bản đọc JDBC |
| H6 | **Mô hình và dọn dẹp.** Đưa `first_seen_at`, `last_seen_at`, `observed_listing_count` ra khỏi `dim_seller`, `dim_brand` (để dim đổi chậm thật); kiểm trùng slug danh mục Lazada khác cha; ghi vào summary khi `merge_dw` thay schema; sửa docs lệch mã (ADR-0005, `DATA_MODEL.md` mục tăng dần, `ARCHITECTURE.md`, docstring `scheduler.py`) | suite |
| H7 | **E2E và bằng chứng.** Chạy `mp-refactor` vài giờ với Tiki và Lazada, rồi chạy bù một ngày cũ; ghi số dòng, thời gian từng stage, version Delta, kết quả quality vào `docs/evidence/hardening-<ngày>/` | báo cáo evidence |

Mỗi gói: test viết cùng mã, chạy toàn bộ suite, `ruff`, `lint-imports`; commit đứng tên người dùng.

## 4. Rủi ro

| | Rủi ro | Xử lý |
|---|---|---|
| R1 | Delta trên MinIO (S3A) không cho ghi nguyên tử nhiều writer | chỉ một writer (`max_active_runs=1` + lock); smoke ở H1 trước khi phụ thuộc |
| R2 | SCD2 tăng dần khác dựng lại toàn bộ khi có dữ liệu đến muộn | test parity với sighting đến trễ; `listing_key` hiện là hash(`offer_id`, `valid_from`) nên một sighting sớm hơn đến muộn đổi khoá: tính lại toàn bộ version của offer đó và thay fact của nó |
| R3 | Đổi partition Silver phá dữ liệu đã có | Silver dựng lại được từ Bronze: một lần `--rebuild` sau khi đổi |
| R4 | Khối lượng lớn, dễ trễ hạn 24/11 | thứ tự H0→H1→H3→H2→H4→H5→H6→H7; H1 và H3 là phần đúng đắn, bắt buộc; H2 trở đi có thể rút gọn |

## 5. Cần người dùng quyết

1. **Duyệt kế hoạch và thứ tự** ở mục 3 và 4 (R4).
2. **Đơn vị cửa sổ:** ngày `observed_date`, với số ngày trễ cho phép mặc định là 1 (đề xuất), hay theo giờ.
3. **Khi run hỏng chất lượng:** `RESTORE` star về version đã chứng nhận (đề xuất), hay giữ star và chỉ đánh dấu run.
4. **Bật Docker** để chạy smoke Delta trên MinIO (H1) và E2E (H7).

## 6. Quyết định của người dùng (2026-10-27)

1. Duyệt kế hoạch và thứ tự H0→H1→H3→H2→H4→H5→H6→H7.
2. Cửa sổ theo ngày `observed_date`, trễ cho phép 1 ngày.
3. Run hỏng chất lượng: `RESTORE` các bảng `dw` về version của manifest hiện hành.
4. Docker: được bật khi tới H1 (smoke) và H7 (E2E), tắt sau khi xong.

## 7. Tiến độ (cập nhật 2026-10-27 23:30)

| Gói | Trạng thái | Commit |
|---|---|---|
| H0 | xong: mart dựng từ fact, parity 10 mart; sửa lỗi hết heap của suite test (lineage, status store) | `d150731`, `12bef19` |
| H1 | xong ở mức code và test Spark; **chưa** smoke Delta trên MinIO | `a27a0f3` |
| H3 | xong: 4 luật star, manifest v2 `dw_versions`, `VERSION AS OF`, `RESTORE` khi trượt cổng, inferred member cho `dim_category` | `12bef19` |
| H4 | xong ở mức unit; **chưa** chạy `tests/airflow` trong image Airflow | `c8c4e6e` |
| docs | ADR-0007, `DATA_MODEL.md` §4.10, ADR-0005, `ARCHITECTURE.md` | `03fd4d7` |
| H2, H5, H6 (phần còn lại), H7 | chưa làm | |

**Việc tiếp theo:**
1. Chạy toàn bộ suite tại `03fd4d7` trên worktree `C:\code\data-refactor-verify` (`PYTHONPATH=<worktree>/src`, khoảng 1,5–2 giờ, không chạy Spark nào khác cùng lúc). Lượt chạy tối 27/10 bị dừng vì tắt máy.
   - Lỗi `Net.bind0` hoặc "Python worker failed to connect back" là do cạn cổng tạm trên Windows; chạy lại riêng các test đó.
2. Bật Docker và chạy E2E trên `mp-refactor` với Bronze có sẵn của ngày 25/10:
   - `batch run --all --rebuild` (lake cũ là Parquet);
   - một run theo cửa sổ;
   - một run có `as_of` cũ hơn;
   - `tests/airflow` trong image `ecommerce/airflow:3.3.2-r1`.
   Ghi kết quả vào `docs/evidence/hardening-<ngày>/`.
3. Sang H2 (tăng dần), rồi H5, H6, H7.

