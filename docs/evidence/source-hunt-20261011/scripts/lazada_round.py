"""One Lazada round (Playwright headless): category page 1, same JSONL format as crawl_round.py."""
import re, json, time, sys
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

OUT = sys.argv[1] if len(sys.argv) > 1 else "rounds3.jsonl"
URL = "https://www.lazada.vn/dien-thoai-di-dong/"
JS = r"""els => els.map(e => {
  const a = e.querySelector('a[title]') || e.querySelector('a');
  const t = e.innerText;
  const p = t.match(/[\d.]{4,}\s?₫/);
  const sold = t.match(/([\d.,]+[kK]?)\s*(Đã bán|đã bán)|(Đã bán|đã bán)\s*([\d.,]+[kK]?)/);
  return {id: e.getAttribute('data-item-id'), title: a ? (a.title || a.innerText).slice(0, 80) : null,
          price: p ? p[0] : null, sold: sold ? sold[0] : null};
})"""
ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
rec = dict(ts=ts, site="lazada", url=URL)
t0 = time.time()
try:
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        pg = b.new_context(locale="vi-VN", viewport={"width": 1366, "height": 900}).new_page()
        pg.goto(URL, timeout=45000, wait_until="domcontentloaded")
        pg.wait_for_timeout(7000)
        cards = pg.eval_on_selector_all('[data-qa-locator="product-item"]', JS)
        blocked = bool(re.search(r"punish|x5sec|slide to verify", pg.content(), re.I))
        b.close()
    items = [dict(id=c["id"], name=c["title"], price=int(re.sub(r"\D", "", c["price"])), extra=dict(sold=c["sold"]))
             for c in cards if c["id"] and c["price"]]
    rec.update(status=200, n=len(items), blocked=blocked, items=items)
except Exception as e:
    rec.update(status="ERR", n=0, err=str(e)[:150], items=[])
rec["secs"] = round(time.time() - t0, 2)
open(OUT, "a", encoding="utf-8").write(json.dumps(rec, ensure_ascii=False) + "\n")
print("lazada", rec["status"], "n=", rec["n"], rec["secs"], "s", "blocked" if rec.get("blocked") else "", rec.get("err", ""))
print(json.dumps(rec["items"][:2], ensure_ascii=False))
