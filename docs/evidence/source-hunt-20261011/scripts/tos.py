"""Grep ToS / operating-rules pages for clauses about automated collection."""
import re, time, html
from fetch import get

PAGES = ["https://hotro.tiki.vn/knowledge-base/post/850-dieu-khoan-su-dung",
         "https://tiki.vn/quy-che-hoat-dong-sgdtmdt",
         "https://www.thegioididong.com/thoa-thuan-su-dung-trang-mxh",
         "https://trogiup.chotot.com/nguoi-ban/quy-dinh-chung/"]
KW = r"(tự động|robot|\bbot\b|crawl|thu thập dữ liệu|khai thác dữ liệu|sao chép|spider|scrap|công cụ|phần mềm)"
for u in PAGES:
    try:
        st, h = get(u)
    except Exception as e:
        print(u, "ERR", str(e)[:60]); continue
    t = html.unescape(re.sub(r"<[^>]+>", " ", re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", h, flags=re.S)))
    t = re.sub(r"\s+", " ", t)
    hits = [m.start() for m in re.finditer(KW, t, re.I)]
    print("==", u, st, "len", len(t), "hits", len(hits))
    shown = -1000
    for p in hits:
        if p - shown > 300:
            print("   ...", t[max(0, p - 200):p + 250])
            shown = p
    time.sleep(3)
