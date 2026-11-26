# Nhật ký lỗi: chạy toàn luồng 07/11/2026

Stack `mp-refactor` (container `rf-*`), code mount từ worktree `C:\code\data-refactor` (nhánh `chore/repo-cleanup`).
Bật lại lúc 11:09 (giờ VN) để crawl dữ liệu hôm nay, dự kiến chạy đến khoảng 15:00.
Giờ trong file là giờ Việt Nam (UTC+7). Run id của batch theo giờ UTC (`mp-20261107T0400Z` = 11:00 giờ VN).

## Tổng kết lúc 15:00

Từ 11:09 đến 15:00 (khoảng 3 tiếng 50 phút):

- **Crawl Tiki:** 4.281 trang danh mục, 0 lỗi, 112.544 bản ghi sản phẩm. Không gặp `RATE_LIMITED` hay `BLOCKED`.
- **Speed:** 2.227 micro-batch, 142.067 dòng vào, 2 dòng INVALID (lỗi 5). 72.751 sản phẩm được cập nhật trong Elasticsearch, phát hiện 16.923 thay đổi.
- **Batch:** run 12:00, 13:00 và 14:00 đều PROMOTED, quality PASS. Run 15:00 đang chạy lúc chốt.
- **Lỗi:** 6 mục. Lỗi 1–3 và 5 đã xử lý và có kiểm chứng. Lỗi 4 và luật anomaly coverage là cảnh báo do dữ liệu đứt quãng, không phải lỗi code. Lỗi 6 là giới hạn RAM của máy.

| # | Giờ | Lỗi | Trạng thái |
|---|---|---|---|
| 1 | 11:10 | Batch không chạy vì cả 4 DAG đang pause | Đã xử lý |
| 2 | 11:22 | Run batch kích thủ công lỗi `'data_interval_end' is undefined` | Đã xử lý (run hỏng đã chuyển sang failed) |
| 3 | 11:22 | Run `mp-20261107T0400Z` QUALITY_FAILED, không promote | Đã sửa (chép bù audit sang lake), run 12:00 PASS |
| 4 | 12:21 | Advisory `counter_invalid_transition_rate` FAIL (51–57%) | Không phải lỗi code: đứt quãng 7 ngày, `GAP_TOO_LONG` |
| 5 | 12:42 | Speed crash liên tục vì `discount_amount` âm, Kibana đứng 45 phút | Đã sửa code (normalizer và speed), không mất dữ liệu |
| 6 | 12:30 | Giám sát nền của Claude bị dừng vì máy thiếu RAM | Chuyển sang kiểm tra theo lượt |

---

## 1. Các DAG Airflow đang pause

**Hiện tượng.** Đã bật scheduler nhưng 10 phút không có run batch nào. Bảng `dag` có `is_paused = true` cho cả 4 DAG.

**Nguyên nhân.** Lúc kết thúc lượt chạy 31/10 (17:24 UTC), các DAG được pause có chủ ý bằng `airflow dags pause` để dừng stack. Bảng `log` của Airflow ghi lại 4 lệnh `cli_dag_pause` lúc 17:24–17:34 UTC ngày 31/10. Khi bật lại stack, không có bước nào bỏ pause.

**Cách sửa.** `airflow dags unpause marketplace_batch` và `source_discovery`. `marketplace_maintenance` vẫn để pause vì nó prune dữ liệu Bronze cũ. `marketplace_reprocess` chỉ chạy khi gọi tay.

**Kiểm chứng.** Run `scheduled__2026-11-07T04:00` bắt đầu lúc 11:11.

**Bài học.** Script bật lại stack (`xem-lai-bi.sh` hoặc runbook) nên có bước unpause khi muốn chạy luồng, và bước pause khi chỉ muốn xem BI.

## 2. Run kích thủ công lỗi `data_interval_end`

**Hiện tượng.** `airflow dags trigger marketplace_batch` tạo run `manual__2026-11-07T04:11:25`. Task `silver` lỗi ngay ở bước render template:
`UndefinedError: 'data_interval_end' is undefined`, rồi chuyển sang `up_for_retry`.

**Nguyên nhân.** `src/ecomdata/orchestration/tasks.py:114-115` dựng `as_of` và `run_id` từ `{{ data_interval_end }}`. Ở Airflow 3, run kích thủ công mà không truyền logical date thì không có data interval (`logical_date = null`), nên biến này không tồn tại. Run theo lịch thì có, nên luồng tự động không bị ảnh hưởng.

**Ảnh hưởng.** Do `max_active_runs=1`, run hỏng này (đang retry) sẽ chặn run 12:00.

**Cách sửa.** Chuyển run sang `failed` qua REST API (`PATCH /api/v2/dags/marketplace_batch/dagRuns/...`), có ghi chú lý do. Muốn kích thủ công thì phải truyền mốc thời gian:
`airflow dags trigger marketplace_batch --logical-date 2026-11-07T05:00:00+00:00`.

**Việc nên làm sau.** Thêm fallback trong template, ví dụ `(data_interval_end or logical_date or ts)`, hoặc ghi rõ cách kích thủ công trong runbook.

## 3. Run `mp-20261107T0400Z` QUALITY_FAILED

**Hiện tượng.** Run theo lịch lúc 11:11: silver → dimensions → facts → marts đều xong, nhưng `quality` FAIL nên `publish` và `promote` bị bỏ qua. Superset vẫn giữ bản Gold ngày 31/10, đúng thiết kế. Bảng `audit.marketplace_quality_result` cho thấy:

| Luật | Mức | Kết quả | Chi tiết |
|---|---|---|---|
| `silver_parse_attempt_reconciliation` | MANDATORY | SKIPPED | `crawl attempt audit is empty` |
| `crawl_reliability_evidence_present` | ADVISORY | FAIL | `crawl_reliability_daily` rỗng dù Silver có 152.342 dòng |
| `price_anomaly_evaluation_coverage` | ADVISORY | FAIL | 0 offer-ngày đủ lịch sử baseline |

Theo `quality.py:619`, luật bắt buộc bị SKIPPED thì tính là FAIL, để không có Gold nào lên BI khi chưa được kiểm.

**Nguyên nhân gốc.**
1. Airflow có `AIRFLOW_SOURCE_MOUNT=C:\code\data-refactor`, nên task batch chạy code của worktree chứ không chạy code đóng trong image ngày 31/10.
2. Worktree đã có commit `c33c5a5` (02/11, "H5 crawl audit in the lake"). Commit này đổi mặc định `MARKETPLACE_CRAWL_AUDIT_SOURCE` từ Postgres sang `lake` (`settings/__init__.py:376`). Batch đọc audit crawl ở `s3a://ecommerce-bronze/marketplace/crawl_audit/`.
3. Crawler chỉ ghi audit vào lake từ khi chạy code mới (hôm nay). Các lượt crawl 25/10 và 31/10 chỉ có trong `audit.crawl_request_attempt` của Postgres, nên trên lake chỉ có thư mục `observed_date=2026-11-07`.
4. `as_of` của run là 11:00 (04:00 UTC). Crawler bật lúc 11:09, nên mọi lượt crawl hôm nay đều sau `as_of` và bị `crawl_audit_as_of` lọc bỏ. Kết quả: audit rỗng, luật bắt buộc SKIPPED, run FAIL.

Luật advisory `crawl_reliability_evidence_present` FAIL cùng nguyên nhân.

Luật `price_anomaly_evaluation_coverage` FAIL vì lý do khác, và hôm nay không thể PASS. Cơ chế phát hiện giá bất thường (`processing/batch/anomaly.py`) so giá của từng offer với lịch sử giá của chính nó. Nó cần ít nhất `MARKETPLACE_ANOMALY_MIN_SAMPLES = 7` ngày có giá trong cửa sổ `MARKETPLACE_ANOMALY_WINDOW_DAYS = 14` ngày. Hiện lake chỉ có 3 ngày crawl (25/10, 31/10, 07/11), nên mọi offer đều `INSUFFICIENT_HISTORY` và coverage bằng 0. Đây là luật cảnh báo, không chặn publish. Muốn PASS phải crawl đủ 7 ngày trong 14 ngày. Hạ ngưỡng xuống chỉ để luật PASS là làm đẹp số liệu, nên không làm.

**Cách sửa.** Chép bù audit cũ từ Postgres sang lake bằng lệnh dự án đã có cho đúng tình huống này (chạy lại nhiều lần không đổi kết quả):

```
docker exec rf-crawl-worker python -m ecomdata.ops audit-export
{"attempts": 7361, "event": "crawl_audit_exported", "runs": 7361}
```

Sau đó lake có `observed_date=2026-10-25`, `2026-10-31`, `2026-11-07`.

**Kiểm chứng.** Chờ run `mp-20261107T0500Z` (12:00): luật reconciliation phải PASS, `crawl_reliability_daily` khác rỗng, run phải PROMOTED. Kết quả ghi ở dưới.

**Bài học.** Khi chuyển nguồn audit sang lake, bước `ops audit-export` phải nằm trong runbook nâng cấp stack, hoặc nằm trong DAG maintenance.

---

## 4. Luật cảnh báo `counter_invalid_transition_rate` FAIL ở run 12:00

**Hiện tượng.** Run `mp-20261107T0500Z` PROMOTED, quality PASS, nhưng luật advisory này FAIL: 85.232 trên 166.681 bước chuyển (51,1%) bị đánh dấu không hợp lệ. Ngưỡng của luật là quá bán (`_ADVISORY_MAJORITY`). Run 31/10 không FAIL luật này.

**Điều tra.** Tách theo lý do trong `cache.marketplace_counter_delta_daily` (`invalid_reasons_json`), chỉ tính counter có giá trị:

| Ngày | Lý do | Số bước không hợp lệ |
|---|---|---|
| 31/10 | `GAP_TOO_LONG` (kể cả kèm lý do khác) | 50.513 |
| 07/11 | `GAP_TOO_LONG` (kể cả kèm lý do khác) | 34.626 |
| cả 3 ngày | `COUNTER_DECREASED` (chỉ lý do này) | 26 |
| cả 3 ngày | `MISSING_VALUE` | 73 |

Như vậy hơn 99% là `GAP_TOO_LONG`.

**Nguyên nhân gốc.** `marts.py` coi hai lần quan sát liên tiếp của cùng một counter (sold_count, review_count) là không hợp lệ nếu chúng cách nhau quá `MARKETPLACE_COUNTER_MAX_GAP_SECONDS = 86400` (24 giờ). Stack đã tắt từ 25/10 đến 31/10 và từ 31/10 đến 07/11. Vì vậy lần quan sát đầu tiên hôm nay của mỗi sản phẩm được nối với lần quan sát ngày 31/10, cách nhau khoảng 7 ngày. Sau khoảng trống thứ hai, các bước chuyển kiểu này vượt quá nửa tổng số, nên luật FAIL. Ngày 31/10 mới có một khoảng trống nên chưa vượt.

**Kết luận.** Code không có lỗi. Đây là cơ chế bảo vệ đúng: chênh lệch "đã bán" sau 7 ngày không thể quy thành tốc độ bán theo giờ. Luật advisory làm đúng việc của nó là báo rằng dữ liệu bị đứt quãng. Không hạ ngưỡng và không nới `MAX_GAP`, vì làm vậy sẽ sinh tốc độ bán sai.

**Cách xử lý để có kết quả đúng.** Crawl liên tục.

*Đính chính (13:23).* Lúc 12:20 tôi ghi rằng "trong buổi chạy hôm nay tỉ lệ sẽ giảm dần". Dự đoán đó sai. Run 13:00 cho tỉ lệ **tăng** lên 56,7% (119.308 / 210.236). Lý do: tier SWEEP quét mỗi danh mục một lần mỗi ngày, nên phần lớn sản phẩm hôm nay chỉ được thấy một lần. Bước chuyển duy nhất của chúng là từ lần quan sát ngày 31/10, tức `GAP_TOO_LONG`. Mỗi giờ SWEEP lại quét thêm danh mục mới, nên số bước `GAP_TOO_LONG` tăng nhanh hơn số bước hợp lệ (bước hợp lệ chỉ đến từ tier ACTIVE quét lại mỗi giờ). Số của ngày 07/11 trong cache: 68.647 `GAP_TOO_LONG` so với 14.562 bước hợp lệ. Tỉ lệ sẽ còn tăng trong hôm nay, và chỉ giảm khi lượt SWEEP thứ hai (ngày mai) quét lại các danh mục trong vòng 24 giờ. Kết luận "không phải lỗi code" vẫn giữ nguyên.

**Ghi chú phụ.** Có 9 bước `NON_POSITIVE_INTERVAL` hôm nay: hai lần quan sát cùng một sản phẩm có cùng thời điểm, có thể do sản phẩm nằm ở hai danh mục được quét cùng lúc. Số này nhỏ, sẽ xem thêm sau buổi báo cáo.

## 5. Lớp speed crash liên tục, Kibana đứng từ 11:59 đến 12:44

**Hiện tượng.** Lúc 12:42, `rf-speed` vừa mới khởi động lại ("Up 12 seconds"). Thay đổi giá cuối cùng trong Elasticsearch có `detected_at` 11:59. Log lặp lại:
`WireContractError: event contract validation failed: discount_amount must be non-negative and finite`,
xảy ra trong `stream.py` `_process_group` → `marketplace_observation_from_wire`, rồi `[STREAM_FAILED] ... FOREACH_BATCH_USER_FUNCTION_ERROR`. Container tự khởi động lại, đọc lại đúng micro-batch đó và chết tiếp.

**Nguyên nhân gốc (hai tầng).**
1. *Normalizer để lọt dữ liệu sai hợp đồng.* `processing/normalize/common.py` đọc `discount_amount` và `discount_percent` bằng `money()`, tức chỉ ép kiểu số, không kiểm tra dấu. Tiki trả `discount` âm khi giá bán cao hơn giá niêm yết. Trong khi đó hợp đồng domain (`MarketplaceObservation.__post_init__`) chỉ cho phép `discount_amount >= 0` và `0 <= discount_percent <= 100`. Event sai được đẩy vào topic `marketplace.observations.v1`. Các trường tùy chọn khác như `list_price` và các counter đã có xử lý đúng: giá trị không hợp lệ thành null (`F.when(list_price > 0, ...)`, `whole()`). Riêng discount bị sót.
2. *Speed coi lỗi một dòng là lỗi cả micro-batch.* Bộ lọc cột `decode_observation_stream` chỉ kiểm tra envelope, lineage và `current_price >= 0`. Phần kiểm tra hợp đồng còn lại nằm trong `_process_group` và ném exception ra ngoài. Offset Kafka không được commit, nên query khởi động lại sẽ đọc lại đúng dòng đó mãi: một event độc hại làm chết cả lớp speed.

Chỉ có 2 event như vậy trong khoảng 100 nghìn sản phẩm. Hôm 31/10 chưa gặp trường hợp này.

**Cách sửa.**
- `processing/speed/stream.py`: `_process_group` bắt `WireContractError`/`ValueError` cho từng dòng và trả `SpeedOutput` loại `INVALID` (`error_type=OBSERVATION_CONTRACT`, kèm thông điệp lỗi và offset nguồn). Đây là cùng đường `INVALID` mà bộ lọc cột đang dùng, và được đếm vào `invalid_rows` của `audit.marketplace_speed_batch`. Các dòng hợp lệ cùng nhóm vẫn được xử lý.
- `processing/normalize/common.py`: thêm `_bounded()`. Discount âm thành null, phần trăm ngoài 0..100 thành null. Sửa này dùng chung cho speed normalizer và batch Silver, nên Silver cũng không còn chứa discount âm.
- Test: `test_a_row_that_breaks_the_wire_contract_is_reported_invalid_and_does_not_stop_the_group` (thuần Python, đã chạy, PASS cùng 3 test cũ của `_process_group`) và `test_a_discount_outside_the_domain_contract_is_null_not_a_contract_breach` (cần Spark, chưa chạy trên máy vì RAM đang thấp, để CI chạy).

**Kiểm chứng.**
- Container mount code từ worktree, nên lần tự khởi động lại lúc 12:44 đã nạp code mới. Micro-batch 7167 (12:44:38): 120 dòng vào, **2 INVALID**, 118 xử lý. Sau đó mọi micro-batch đều SUCCEEDED, không còn `WireContractError`.
- Không mất dữ liệu: Elasticsearch có thay đổi trong từng khung 15 phút của khoảng bị đứng (12:00: 1.121, 12:15: 1.505, 12:30: 1.382), vì Kafka giữ phần tồn và speed xử lý bù khi chạy lại.
- Silver có ghi đè lại các ngày trong cửa sổ late_days với normalizer mới không: xem run 13:00 bên dưới.

**Việc nên làm sau.** Commit hai thay đổi trên một nhánh riêng và để CI chạy test Spark. Nên ghi các dòng INVALID của speed vào topic `marketplace.observations.v1.dlq` (đã khai báo trong `domain/topics.py` nhưng chưa dùng), để xem lại được nội dung event chứ không chỉ có số lượng.

## 6. Giám sát nền bị dừng do máy thiếu RAM (12:30)

Claude Code dừng hai tác vụ giám sát nền (vòng cập nhật `hom-nay.html` và vòng chờ run 13:00), vì máy chỉ còn khoảng 3 GB RAM trống trên 31 GB. Docker chiếm phần lớn, riêng `rf-speed` khoảng 1,7 GB. Pipeline vẫn chạy, chỉ phần giám sát của Claude bị dừng. Từ đây kiểm tra theo từng lượt, không chạy vòng nền dài.

---

## Kết quả kiểm chứng

| Run (giờ VN) | Trạng thái | Quality | Ghi chú |
|---|---|---|---|
| `mp-20261107T0400Z` (11:00) | QUALITY_FAILED | FAIL | Lỗi 3, trước khi chép bù audit. Run 12:00 đã thay thế. |
| `mp-20261107T0500Z` (12:00) | **PROMOTED** | **PASS** | Lỗi 3 đã hết: `silver_parse_attempt_reconciliation` PASS. Còn 2 luật advisory FAIL: `counter_invalid_transition_rate` (lỗi 4) và `price_anomaly_evaluation_coverage` (thiếu lịch sử, xem lỗi 3). Superset đã có dữ liệu hôm nay. |
| `mp-20261107T0600Z` (13:00) | **PROMOTED** | **PASS** | Run đầu tiên chạy normalizer đã sửa (lỗi 5), không có luật bắt buộc nào FAIL. Advisory: `counter_invalid_transition_rate` 56,7% (xem đính chính ở lỗi 4), `price_anomaly_evaluation_coverage` 0. Run kết thúc lúc 13:23, sensor `window_settled` chờ khoảng 15 phút cho cửa sổ crawl ổn định. |
| `mp-20261107T0700Z` (14:00) | **PROMOTED** | **PASS** | 12 phút. Advisory giống run trước: counter 56,8%, anomaly coverage 0. Speed không khởi động lại lần nào kể từ 12:46. |

## 7. Bảng "Top giảm giá" trên trang hom-nay.html hiển thị sai (phát hiện sau 15:00)

- **Triệu chứng:** bảng top 10 có mức giảm 40–81%, trông bất thường.
- **Nguyên nhân 1 (lỗi script của trang):** script chỉ lấy 500 bản ghi đầu tiên (`size: 500`) trong 10.671 lượt đổi giá hôm nay, nên top 10 chỉ được tính trên một mẫu nhỏ. Mức giảm lớn nhất thật là −91%, không phải −81%.
- **Sửa:** `tao-trang-hom-nay.py` chuyển sang scroll để duyệt toàn bộ. Sau khi chạy lại, top 10 là −91% … −68%.
- **Nguyên nhân 2 (dữ liệu thật, không phải lỗi pipeline):** các mức giảm lớn có thật trên Tiki. Đối chiếu lịch sử giá theo ngày (`cache.marketplace_offer_price_history_daily`) thấy 3 kiểu:

| Kiểu | Ví dụ (25/10 → 31/10 → 07/11) | Giải thích |
|---|---|---|
| A. Giá "nhảy" lên bằng giá niêm yết rồi giảm lại | Bút chì Deli: 30.000 → **90.000** → 30.000 (niêm yết 90.000); tã SunMate: 89.000 → **225.000** → 73.000; bút sáp Deli: 17.000 → **69.000** → 17.000 | Ngày 31/10 khuyến mãi tạm tắt nên giá bằng giá niêm yết. Hôm nay khuyến mãi bật lại. "Giảm 67%" chỉ là quay về giá thường. Giá niêm yết bị đẩy cao, có dấu hiệu "giảm giá ảo". |
| B. Giá niêm yết ảo, hôm nay mới bật sale | Kính cường lực: 99.999 → 19.000 (niêm yết 99.999); ốp lưng: 186.998 → 36.000; cáp Remax: 329.998 → 97.000 | Người bán đặt giá niêm yết rất cao (99.999, 186.998…) và trước đó bán đúng giá đó, hôm nay bật giảm giá. |
| C. Đổi biến thể hoặc đổi người bán | Truyện tranh: 116.800 → 22.000, niêm yết **170.000 → 26.000**; trà Thái Nguyên: 155.000 → 31.150, niêm yết **155.000 → 35.000**; serum: 1.841.400 → 157.680 | Giá niêm yết cũng đổi theo, nên khả năng cao trang sản phẩm đã đổi biến thể mặc định (khác dung tích hoặc số lượng) hay đổi người bán, chứ không phải sản phẩm đó giảm giá. |

- **Yếu tố chung:** hai lần crawl liền nhau của cùng sản phẩm cách nhau **7 ngày** (31/10 → 07/11), nên mức đổi giá gộp cả một tuần, không phải biến động trong ngày.
- **Tỉ trọng:** hôm nay có 5.661 lượt giảm giá. 3.428 lượt giảm dưới 10%, 1.480 lượt giảm 10–30%, 588 lượt giảm 30–50%, và chỉ **165 lượt (2,9%) giảm từ 50% trở lên**. Top 10 là phần đuôi cực đoan.
- **Hướng xử lý tiếp:** kiểu A và B chính là đầu vào cho bài toán ML phát hiện giảm giá ảo trong kế hoạch. Kiểu C cần thêm định danh biến thể (SKU con) vào offer_id, hoặc gắn cờ khi giá niêm yết đổi cùng lúc với giá bán.
