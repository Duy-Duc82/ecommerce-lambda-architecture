import re, json, time, sys
from playwright.sync_api import sync_playwright
BASE="https://www.lazada.vn/dien-thoai-di-dong/"
log=[]
def cards(pg):
    return pg.eval_on_selector_all('[data-qa-locator="product-item"]', """els=>els.map(e=>{
      const a=e.querySelector('a[title]')||e.querySelector('a');
      const t=e.innerText; const m=t.match(/[\d.]{4,}\s?₫/);
      return {id:e.getAttribute('data-item-id'), sku:e.getAttribute('data-sku-simple'), title:a?(a.title||a.innerText).slice(0,60):null,
              href:a?a.href.split('?')[0]:null, price:m?m[0]:null}})""")
with sync_playwright() as p:
    b=p.chromium.launch(headless=True); ctx=b.new_context(locale="vi-VN",viewport={"width":1366,"height":900}); pg=ctx.new_page()
    allids=set()
    for page in range(1,6):
        url=BASE+("" if page==1 else f"?page={page}")
        t=time.time(); pg.goto(url,timeout=45000,wait_until="domcontentloaded"); pg.wait_for_timeout(7000)
        html=pg.content(); c=cards(pg)
        blocked=bool(re.search(r"punish|x5sec|captcha|slide to verify|nc_1_n1z",html,re.I)) and len(c)==0
        pd=re.search(r"window\.pageData\s*=\s*(\{.*?\})\s*;?\s*</script>",html,re.S)
        pdinfo=None
        if pd:
            try:
                d=json.loads(pd.group(1)); items=d.get("mods",{}).get("listItems",[])
                pdinfo={"listItems":len(items),"keys":sorted(items[0].keys())[:40] if items else [],"total":d.get("mainInfo",{}).get("totalResults")}
                if page==1 and items: json.dump(items[:3],open("lazada_items_sample.json","w",encoding="utf-8"),ensure_ascii=False,indent=1)
            except Exception as e: pdinfo={"err":str(e)[:80]}
        new=[x for x in c if x["id"] and x["id"] not in allids]; allids.update(x["id"] for x in c if x["id"])
        r={"page":page,"secs":round(time.time()-t,1),"final":pg.url[:90],"cards":len(c),"new_ids":len(new),"with_price":sum(1 for x in c if x["price"]),"blocked":blocked,"pageData":pdinfo,"sample":c[:2]}
        print(json.dumps(r,ensure_ascii=False)); sys.stdout.flush(); log.append(r)
        if blocked: pg.screenshot(path=f"raw/lazada_block_p{page}.png"); break
        time.sleep(5)
    # product detail page
    if allids:
        href=next(x["href"] for x in c if x["href"]) if c else None
        if href:
            pg.goto(href,timeout=45000,wait_until="domcontentloaded"); pg.wait_for_timeout(8000)
            html=pg.content(); txt=pg.inner_text("body")
            ld=re.findall(r'<script[^>]*ld\+json[^>]*>(.*?)</script>',html,re.S)
            r={"detail":href,"final":pg.url[:100],"prices":re.findall(r"[\d.]{4,}\s?₫",txt)[:3],"ld_json_types":[re.findall(r'"@type"\s*:\s*"(\w+)"',x)[:3] for x in ld],
               "blocked":bool(re.search(r"punish|x5sec|slide to verify",html,re.I)),"has_seller":"Đánh giá" in txt or "Nhà bán" in txt}
            print(json.dumps(r,ensure_ascii=False)); log.append(r); pg.screenshot(path="raw/lazada_detail.png")
    b.close()
json.dump(log,open("lazada.json","w",encoding="utf-8"),ensure_ascii=False,indent=1)
