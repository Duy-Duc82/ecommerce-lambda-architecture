import re, json
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    b=p.chromium.launch(headless=True); pg=b.new_page(locale="vi-VN",viewport={"width":1366,"height":900})
    sets={}
    for page in (1,2,3):
        pg.goto(f"https://tiki.vn/dien-thoai-may-tinh-bang/c1789?page={page}",wait_until="domcontentloaded",timeout=45000); pg.wait_for_timeout(8000)
        for _ in range(4): pg.mouse.wheel(0,2500); pg.wait_for_timeout(1200)
        ids=set(re.findall(r"-p(\d+)\.html",pg.content())); sets[page]=ids
        print("render page",page,"product ids",len(ids),"overlap with p1",len(ids & sets[1]))
    # click "Xem thêm" on landing
    pg.goto("https://tiki.vn/dien-thoai-may-tinh-bang/c1789",wait_until="domcontentloaded"); pg.wait_for_timeout(6000)
    n0=len(set(re.findall(r"-p(\d+)\.html",pg.content())))
    for i in range(5):
        pg.mouse.wheel(0,6000); pg.wait_for_timeout(1500)
        btn=pg.get_by_text("Xem thêm",exact=False)
        try:
            btn.last.click(timeout=3000); pg.wait_for_timeout(2500)
        except Exception: pass
    n1=len(set(re.findall(r"-p(\d+)\.html",pg.content())))
    print("infinite/xem-them: before",n0,"after 5 rounds",n1)
    b.close()
