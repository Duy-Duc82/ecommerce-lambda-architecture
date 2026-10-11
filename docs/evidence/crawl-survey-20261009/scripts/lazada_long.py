import re, json, time
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    b=p.chromium.launch(headless=True); pg=b.new_page(locale="vi-VN",viewport={"width":1366,"height":900})
    ids=set(); t0=time.time(); blocked_at=None
    for page in range(6,26):
        pg.goto(f"https://www.lazada.vn/dien-thoai-di-dong/?page={page}",wait_until="domcontentloaded",timeout=45000); pg.wait_for_timeout(5000)
        c=pg.eval_on_selector_all('[data-qa-locator="product-item"]',"els=>els.map(e=>e.getAttribute('data-item-id'))")
        html=pg.content()
        if not c:
            blocked_at=page; pg.screenshot(path=f"raw/lazada_long_block_p{page}.png")
            print("page",page,"NO CARDS; captcha?",bool(re.search(r"punish|x5sec|slide|captcha|nc_1",html,re.I)),pg.url[:100]); break
        ids.update(c); print("page",page,"cards",len(c),"cum unique",len(ids),flush=True); time.sleep(3)
    print(json.dumps({"pages_ok":(blocked_at or 26)-6,"unique":len(ids),"blocked_at":blocked_at,"minutes":round((time.time()-t0)/60,1)}))
    b.close()
