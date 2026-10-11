# Raw crawl data, 2026-10-11 (18:04–20:25 ICT)

Collected by the scripts in `../scripts/` for the data-source survey (`../bao-cao-nguon-du-lieu.html`).
Plain HTTP or headless Chromium, robots.txt respected, no APIs. Personal fields from Chợ Tốt
(phone, account name, full name, coordinates) were dropped at parse time.

This data is kept on a separate branch for the source decision. The sites' terms restrict
redistribution, so do not merge it into a public main branch.

| File | Produced by | Content |
|---|---|---|
| `rounds.jsonl` | `crawl_round.py` | Group A, 10 rounds: Tiki ×2, Chợ Tốt, TGDD, ĐMX, Phong Vũ, FPT, Nguyễn Kim, Websosanh, Bách Hóa Xanh. One line per (round, source): `ts, site, url, status, n, secs, items[{id, name, price, extra}]` |
| `rounds2.jsonl` | `crawl_round2.py` | Group B, 10 rounds: meta, MediaMart, Pico, Kingfoodmart, Hoàng Hà, Bonbanh, plus a Tiki product-page canary |
| `rounds3.jsonl` | `lazada_round.py` | Lazada (Playwright), 3 rounds |
| `snapshots.jsonl` | `snapshot_adapter.py` | All of the above mapped to `PRICE_SNAPSHOT` after validation |
| `tiki_demand_A.json`, `tiki_demand_B.json` | `tiki_demand.py` | 1,193 Tiki products in 34 categories at 19:44 and 20:15 |
| `chotot_life_A.json`, `chotot_life_B.json` | `chotot_life.py` | 300 Chợ Tốt listings (3 iPhone models) at 19:55 and 20:21 |
| `wss_spread_rows.json` | `wss_spread.py` | 100 random Websosanh compare pages with lowPrice, highPrice, offerCount |
| `loop.log`, `loop2.log`, `lazada.log` | shell loops | Per-round status, item count and seconds per page |

Timestamps in the JSONL files are UTC. ICT = UTC+7.
