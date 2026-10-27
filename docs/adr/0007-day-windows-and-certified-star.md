# ADR-0007: Run sở hữu một cửa sổ ngày; star chỉ được giữ khi đã qua cổng chất lượng

Trạng thái: chấp nhận (2026-10-27). Kế hoạch: `docs/PLAN_PIPELINE_HARDENING.md` (H1, H3, H4).

## Bối cảnh

Một lượt rà soát ngày 27/10 tìm ra bốn lỗi làm pipeline sai khi chạy lại hoặc chạy bù:

1. Stage `dimensions` và `facts` cắt Silver tại `as_of` của run, còn `MERGE` vào `dw/` thì xoá mọi dòng mà nguồn không có (`WHEN NOT MATCHED BY SOURCE THEN DELETE`). Hệ quả: một run có `as_of` cũ xoá hết phần star mà các run mới hơn đã thêm.
2. Stage `silver` lọc `fetched_at <= as_of` rồi ghi đè ngày của `as_of`. Bảng `*_listing_wide` thì bị ghi đè cả bảng; `page_context` và `quarantine` chỉ partition theo `source_code`. Hệ quả: một run cũ làm Silver ngắn đi.
3. Runtime mặc định là Parquet, nên star không có `MERGE`, không có version và không ghi nguyên tử.
4. Star được ghi trước cổng chất lượng, và cổng không kiểm star. Một run hỏng vẫn để lại star đã đổi.

## Quyết định

**Cửa sổ ngày (H1).**
- Mỗi run sở hữu một tập ngày `observed_date`: ngày của `as_of` cùng `late_days` ngày trước đó (mặc định 1), hoặc mọi ngày từ `--silver-since`, hoặc mọi ngày Bronze có (`--rebuild`).
- Stage `silver` đọc đúng các partition Bronze `observed_date=<ngày>` của cửa sổ, lấy **toàn bộ** record của các ngày đó, rồi thay đúng các partition ngày đó của mọi bảng Silver. Mọi bảng Silver partition theo `source_code` và `observed_date`.
- `as_of` không cắt Silver lúc ghi. Nó chỉ cắt lúc một stage sau đọc Silver hoặc star (mart, quality).
- Star là toàn bộ những gì Silver biết. `dimensions` và `facts` đọc cả Silver, nên `MERGE` có xoá là đúng: dòng bị xoá là dòng mà dữ liệu đầy đủ không còn sinh ra.
- Delta là định dạng mặc định. Star từ chối Parquet.

**Star đã chứng nhận (H3).**
- Cổng chất lượng judge star tại đúng version mà run vừa dựng, theo bốn luật:
  - `star_foreign_keys_resolve`: 44 tham chiếu, gồm 40 khoá ngoại cộng 4 tham chiếu `offer_key` tới khoá bền của SCD2;
  - `star_surrogate_keys_unique`;
  - `star_listing_versions_consistent`;
  - `star_unknown_member_rate` (luật khuyến nghị).
- Version luật nâng lên `quality-rules.v3`.
- Manifest v2 ghi `dw_versions`. Pointer `current` vì thế cũng là "star cuối cùng đã được chứng nhận".
- `marts` và `quality` đọc star bằng `VERSION AS OF` theo các version run đã dựng (`facts.built_star_versions`).
- Run trượt cổng thì `RESTORE` mọi bảng `dw/` về version mà manifest hiện hành ghi. Run scratch thì để nguyên star của nó.

**Inferred member.** Một danh mục đã được yêu cầu (target của một sighting hay của một lượt crawl, kể cả lượt lỗi) mà chưa path nào nêu tên vẫn có dòng trong `dim_category`, với `level` và `parent_key` là NULL. Nhờ vậy khoá ngoại của `fact_crawl_attempt` và `page_category_key` luôn trỏ được. Luật khoá ngoại mới đã phát hiện ra lỗ hổng này trên chính fixture.

**Điều phối (H4).**
- Cửa sổ chờ `MARKETPLACE_BATCH_SETTLE_MINUTES` (mặc định 10 phút) rồi mới chạy.
- Mỗi stage có `execution_timeout` 45 phút, mỗi run có `dagrun_timeout` 3 giờ.
- `quality` không retry.
- `--resume` chỉ được thêm khi đó là lần retry.
- DAG `marketplace_reprocess` (chạy tay, `since` hoặc `rebuild`) là một run thường, mới hơn, có cửa sổ dài hơn. Vì vậy không có "backfill" nào ghi đè dữ liệu mới.

## Hệ quả

- Lake đã ghi bằng Parquet trước đây phải dựng lại một lần bằng `batch run --rebuild` (hoặc DAG `marketplace_reprocess` với `rebuild=true`).
- Bronze đến muộn hơn `late_days` không tự vào Silver; phải chạy `marketplace_reprocess` với `since`.
- Star vẫn được dựng lại toàn bộ mỗi run. Giới hạn đó là việc của H2 (tăng dần): khi đó phần ghi theo cửa sổ dùng lại chính quy tắc ở đây.
- Test:
  - `tests/spark/test_silver_stage.py`: một run cũ sau một run mới không làm đổi bảng Silver nào; rebuild đọc mọi ngày.
  - `tests/spark/test_batch_stages.py`: star giữ nguyên sau một run cũ.
  - `tests/spark/test_star_quality.py`: từng luật; run trượt cổng đưa star về bản đã chứng nhận; inferred member.
  - `tests/unit/orchestration/test_tasks.py`.
