import re, sys, time
from playwright.sync_api import sync_playwright
targets = [
  ("shopee_search", "https://shopee.vn/search?keyword=iphone%2015"),
  ("shopee_cat", "https://shopee.vn/%C4%90i%E1%BB%87n-Tho%E1%BA%A1i-Ph%E1%BB%A5-Ki%E1%BB%87n-cat.11036030"),
  ("tiki_cat", "https://tiki.vn/dien-thoai-may-tinh-bang/c1789"),
]
with sync_playwright() as p:
    for headless in (True, False):
        b = p.chromium.launch(headless=headless)

        for name, url in targets:
            if name.startswith("tiki") and not headless: continue
            api = []; pg = b.new_page(locale="vi-VN")
            pg.on("response", lambda r: api.append((r.status, r.url[:90])) if "/api/" in r.url else None)
            try:
                pg.goto(url, timeout=45000, wait_until="domcontentloaded")
                pg.wait_for_timeout(8000)
            except Exception as e:
                print(name, headless, "ERR", e); continue
            txt = pg.inner_text("body")
            prices = re.findall(r"(?:₫\s?[\d.]{4,}|[\d.]{4,}\s?₫)", txt)
            print(f"\n== {name} headless={headless}\nfinal_url={pg.url}\ntitle={pg.title()}\nprices_found={len(prices)} sample={prices[:5]}")
            print("text_head:", re.sub(r"\s+"," ",txt)[:300])
            print("api_calls:", api[:6])
            pg.screenshot(path=f"{name}_{'hl' if headless else 'gui'}.png")
            time.sleep(3)
        b.close()
