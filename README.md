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

_(chưa có)_
