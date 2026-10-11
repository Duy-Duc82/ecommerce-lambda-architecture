"""Re-fetch 4 of the slow-test URLs that had no JSON-LD price; classify real page vs challenge."""
import re, random, time
from fetch import get

_, x = get("https://tiki.vn/clover/sitemap_products_233.xml")
locs = re.findall(r"<loc>\s*(.*?)\s*</loc>", x)
random.seed(7)
sample = random.sample(locs, 20)
for i in (0, 2, 3, 4):
    time.sleep(15)
    st, h = get(sample[i])
    nd = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', h, re.S)
    t = nd.group(1) if nd else ""
    inv = re.search(r'"inventory_status":"(\w+)"', t)
    pr = re.search(r'"price":(\d+)', t)
    print(i, st, len(h) // 1024, "KB", "next_data" if nd else "NO next_data", "inventory:", inv.group(1) if inv else None, "price:", pr.group(1) if pr else None)
