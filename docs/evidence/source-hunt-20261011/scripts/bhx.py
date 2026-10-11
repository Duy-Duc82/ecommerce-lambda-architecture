"""Render a Bach Hoa Xanh category headless; count prices in rendered text and list XHR hosts."""
import re, sys, time
from playwright.sync_api import sync_playwright

url = sys.argv[1] if len(sys.argv) > 1 else "https://www.bachhoaxanh.com/sua-tuoi"
with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_page(locale="vi-VN")
    xhr = []
    pg.on("response", lambda r: xhr.append((r.status, r.url[:120])) if r.request.resource_type in ("xhr", "fetch") else None)
    t0 = time.time()
    pg.goto(url, wait_until="domcontentloaded", timeout=60000)
    pg.wait_for_timeout(8000)
    t = pg.inner_text("body")
    prices = re.findall(r"\d{1,3}(?:[.,]\d{3})+\s*(?:₫|đ)", t)
    print("title:", pg.title(), "| secs:", round(time.time() - t0, 1), "| prices:", len(prices), prices[:8])
    i = t.find(prices[0]) if prices else 0
    print(t[max(0, i - 300):i + 600])
    for s, u in xhr[:25]:
        print(s, u)
    b.close()
