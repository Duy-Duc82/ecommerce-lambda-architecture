# Kịch bản demo (Phase 9 plan §11, P2-06)

Demo chạy **offline**: crawler đọc stub phục vụ fixture Tiki đã đóng băng,
không gọi Tiki và không cần internet. Demo chạy trong project riêng `mp-demo`,
cạnh stack thu thập thật `mp-live`, và không bao giờ chạm vào stack đó.

Ngân sách thời gian: **12 phút** cho bước 1–7, chưa tính phần khởi động.

## Chuẩn bị (trước giờ trình bày)

```powershell
# Kiểm RAM trước: demo đầy đủ cần thêm khoảng 7–8 GB bên cạnh mp-live.
docker stats --no-stream

# Chạy thử toàn bộ, không dừng giữa các bước, và giữ lại một bản backup.
.\scripts\mp.ps1 -EnvFile env/demo.env demo --auto --snapshot
```

Khởi động (smoke trong `mp-demo`) mất khoảng 5–8 phút. Hãy chạy trước khi
vào phòng, rồi trình bày từ bước 1 bằng:

```powershell
.\scripts\mp.ps1 -EnvFile env/demo.env demo
```

Nếu máy không đủ RAM cho Kibana và Superset, thêm `--without-serve`. Nếu
không muốn chạy hai bước sự cố, thêm `--skip-faults`.

## Step 1 — Pipeline đang chạy, hoàn toàn offline (1 phút)

**Màn hình:** `validate` pass toàn bộ, kèm con trỏ phiên bản đang phục vụ.

**Nói:** "Một lệnh dựng toàn bộ kiến trúc Lambda: crawler, Kafka, Silver,
speed layer, batch và cache. Nguồn dữ liệu ở đây là stub phục vụ một phản hồi
Tiki thật đã đóng băng, nên demo không phụ thuộc mạng. Dữ liệu thật được thu
riêng ở stack `mp-live` từ 2026-10-04."

## Step 2 — Raw-first: mọi observation đều truy về được byte gốc (2 phút)

**Màn hình:** `raw_uri` của một lượt crawl, object đó trong Bronze, và
observation Silver mang `raw_sha256` trỏ về nó.

**Nói:** "Crawler lưu phản hồi thô vào Bronze trước khi parse. Mỗi
observation trong Silver mang URI và checksum của chính những byte đó. Khi
parser có lỗi hoặc nguồn đổi schema, có thể parse lại từ Bronze mà không phải
crawl lại."

## Step 3 — Realtime: biến động giá tới Elasticsearch và Kibana (2 phút)

**Màn hình:** số change event theo loại. Mở Kibana, dashboard *Marketplace
realtime changes*.

**Nói:** "Stub giảm giá 5 % ở lần phục vụ thứ hai, giảm 40 % ở lần thứ ba,
rồi trả về giá gốc. Speed layer phát `PRICE_CHANGED` và `LARGE_PRICE_DROP`
trong khoảng một trigger. Mỗi event có `_id` tất định, nên phát lại không tạo
bản trùng."

## Step 4 — Batch truth: publish có quality gate và có phiên bản (2 phút)

**Màn hình:** run đang phục vụ, con trỏ khớp với cache, các rule chất lượng
PASS. Mở Superset, biểu đồ lịch sử giá.

**Nói:** "Batch đọc Silver tại một mốc `as_of` cố định, dựng Gold, chạy quality
gate, rồi mới publish cache và dịch con trỏ. Con trỏ chỉ dịch sau khi cache
publish thành công."

## Step 5 — Sự cố: MinIO sập dưới Silver sink (drill D3) (2 phút)

**Màn hình:** các bước inject, observe, recover và verify của D3.

**Nói:** "Tắt MinIO. Silver sink ngừng commit offset, lag tăng, và **không**
record hợp lệ nào bị đẩy sang DLQ. Bật MinIO lên, lag về 0, Silver đủ đúng số
observation đã được xác nhận. Không sửa dữ liệu bằng tay."

## Step 6 — Quality gate: run hỏng không bao giờ tới cache (drill D8) (2 phút)

**Màn hình:** batch `QUALITY_FAILED`, cache và con trỏ giữ nguyên. Gỡ lỗi
xong thì run tiếp `SUCCEEDED`.

**Nói:** "Gài một dòng audit mà Silver không khớp. Batch vẫn ghi Gold và kết
quả kiểm tra, nhưng không publish. Người dùng tiếp tục thấy phiên bản tốt
cuối cùng."

## Step 7 — Đánh giá (1 phút)

**Màn hình:** các file benchmark và báo cáo đánh giá mới nhất.

**Nói:** "Mọi con số benchmark đều gắn nhãn `replayed_fixture`. Đó là fixture
phát lại, không phải observation thật. Độ tin cậy crawl, freshness và tăng
trưởng lưu trữ được đo trên dữ liệu thật của `mp-live`, và báo cáo ghi rõ đã
đủ ngưỡng 30 ngày hay chưa."

## Phương án dự phòng (plan §11.3)

1. **Backup demo**: `--snapshot` lưu id vào `data/ops/demo-backup/latest.json`.
   Để dựng lại trạng thái đó mà không cần crawl:

   ```powershell
   .\scripts\mp.ps1 -EnvFile env/demo.env down
   .\scripts\mp.ps1 -EnvFile env/demo.env restore -BackupId <id> -Project mp-demo-replay
   ```

2. **Video quay sẵn**: quay màn hình theo đúng bảy bước trên. Video để ngoài
   git, còn đường dẫn được ghi vào `INDEX.md` của evidence bundle.

## Sau buổi demo

```powershell
docker compose -f docker-compose.yml -p mp-demo --profile "*" down --volumes
```
