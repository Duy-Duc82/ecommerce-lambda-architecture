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

- [2026-10-10 18:00](reports/2026-10-10.md) — CẦN XEM: Docker vẫn tắt (từ 10-06), mp-live không chạy, không có batch
- [2026-10-09 18:53](reports/2026-10-09.md) — CẦN XEM: Docker tắt từ 10-06, mp-live không chạy; job 10-06..10-08 không ra báo cáo

- [2026-10-05 07:47](reports/2026-10-05.md) — LỖI: scheduler bỏ qua batch mp-20261005T0000Z, WP0 chưa nghiệm thu; crawl vẫn 215/216 OK
- [2026-10-05 05:43](reports/2026-10-05.md) — CẦN XEM: benchmark xong 36/36, ổn định; trang 1882 p3 rơi khỏi lịch crawl mp-live
