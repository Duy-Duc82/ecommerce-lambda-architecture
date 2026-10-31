# Báo cáo hằng ngày — thu thập `mp-live`

Nhánh này chỉ chứa báo cáo tự động, sinh bởi Claude Code chạy headless từ
Windows Task Scheduler trên máy thu thập. Nhánh không chung lịch sử với code.

- `reports/YYYY-MM-DD.md`: báo cáo theo ngày (giờ máy, UTC+7).
- `automation/`: script và prompt mà các tác vụ đã lên lịch dùng.

Lịch:

| Tác vụ | Giờ | Việc |
|---|---|---|
| `mp-bench-check` | 05:43 ngày 2026-10-05, chạy một lần | Đánh giá benchmark `bench all --repeat 3` |
| `mp-daily-report` | 07:47 hằng ngày, tới 2026-11-04 | Batch hôm nay, validate, evaluate, báo cáo |
| `mp-keep-awake` | Khi đăng nhập | Giữ máy không ngủ tới 2026-11-04 12:00 |
| `mp-shutdown` | 08:50 ngày 2026-10-05, chạy một lần | Chờ báo cáo sáng xong (tối đa tới 11:00), dừng stack, thoát Docker, tắt máy |

## Báo cáo (mới nhất trước)

- [2026-10-31 09:35](reports/2026-10-31.md) — CẦN XEM: mp-live vẫn tắt (0/10); Docker tắt, máy mới bật 09:26; không batch; Docker (mp-refactor) chạy đêm qua tới 01:18 rồi lỗi engine WSL; ổ C còn 221.3 GiB (−2.9)
- [2026-10-30 23:12](reports/2026-10-30.md) — CẦN XEM: mp-live vẫn tắt (0/10); Docker tắt, máy mới bật 23:05; không batch; ổ C còn 224.2 GiB (+5.9)
- [2026-10-29 17:49](reports/2026-10-29.md) — CẦN XEM: mp-live vẫn tắt (0/10); Docker tắt, máy mới bật 17:42; không batch; ổ C còn 218.3 GiB (−4.0)
- [2026-10-28 18:22](reports/2026-10-28.md) — CẦN XEM: mp-live vẫn tắt (0/10); Docker tắt, máy mới bật 18:13; không batch; ổ C còn 222.3 GiB (−4.2)
- [2026-10-27 19:00](reports/2026-10-27.md) — CẦN XEM: mp-live vẫn tắt (0/10); Docker tắt, máy mới bật 18:44; không batch; ổ C còn 226.5 GiB (+1.2)
- [2026-10-26 19:40](reports/2026-10-26.md) — CẦN XEM: mp-live vẫn tắt (0/10); Docker tắt, máy mới bật 19:34; không batch; ổ C còn 225.3 GiB (−9.5)
- [2026-10-25 10:38](reports/2026-10-25.md) — CẦN XEM: mp-live vẫn tắt (0/10); Docker tắt lúc kiểm (dừng 00:44 10-25), stack thử mp-tikihtml đã dừng; ổ C giảm 22.8 GiB
- [2026-10-24 07:47](reports/2026-10-24.md) — CẦN XEM: mp-live vẫn tắt (0/10); Docker bật, đang chạy stack thử mp-tikihtml (crawl trang danh mục, batch mỗi giờ, SUCCEEDED)
- [2026-10-23 22:20](reports/2026-10-23.md) — CẦN XEM: Docker tắt lúc kiểm; tối 10-22 có mở Docker 20:55-00:43, 5 container chạy ~2h10m (không rõ project); không batch
- [2026-10-22 17:07](reports/2026-10-22.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), gần 16 ngày không thu thập; máy bật 16:58, lỡ giờ batch
- [2026-10-21 09:32](reports/2026-10-21.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), gần 15 ngày không thu thập; máy bật 09:23, lỡ giờ batch

- [2026-10-20 18:39](reports/2026-10-20.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), gần 14 ngày không thu thập; job 10-19 lỗi Claude CLI, không ra báo cáo
- [2026-10-18 14:18](reports/2026-10-18.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), gần 12 ngày không thu thập; job 10-17 bị cắt vì máy tắt, không ra báo cáo
- [2026-10-16 18:00](reports/2026-10-16.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), gần 10 ngày không thu thập
- [2026-10-15 17:58](reports/2026-10-15.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), gần 9 ngày không thu thập; job 10-14 không ra báo cáo
- [2026-10-13 18:12](reports/2026-10-13.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), mp-live không chạy, gần 7 ngày không thu thập
- [2026-10-12 21:08](reports/2026-10-12.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), mp-live không chạy; báo cáo 10-11 trước đó chưa được push
- [2026-10-11 14:42](reports/2026-10-11.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), mp-live không chạy; mp-keep-awake dừng (0xC000013A)
- [2026-10-10 18:00](reports/2026-10-10.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), mp-live không chạy, không có batch
- [2026-10-09 18:53](reports/2026-10-09.md) — CẦN XEM: Docker tắt từ 10-06, mp-live không chạy; job 10-06..10-08 không ra báo cáo

- [2026-10-05 07:47](reports/2026-10-05.md) — LỖI: scheduler bỏ qua batch mp-20261005T0000Z, WP0 chưa nghiệm thu; crawl vẫn 215/216 OK
- [2026-10-05 05:43](reports/2026-10-05.md) — CẦN XEM: benchmark xong 36/36, ổn định; trang 1882 p3 rơi khỏi lịch crawl mp-live
