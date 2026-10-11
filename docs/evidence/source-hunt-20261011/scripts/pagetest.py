"""Does page 2 of each listing return a different id set? Tries several URL patterns."""
import json, time, sys
from fetch import get
import crawl_round as cr

TESTS = [
    ("tiki", "https://tiki.vn/dien-thoai-may-tinh-bang/c1789", ["?page=2", "?page=3"], cr.p_tiki),
    ("chotot", "https://www.chotot.com/mua-ban-dien-thoai", ["?page=2", "?page=3"], cr.p_chotot),
    ("phongvu", "https://phongvu.vn/c/laptop", ["?page=2", "?page=3"], cr.p_phongvu),
    ("fptshop", "https://fptshop.com.vn/dien-thoai", ["?page=2", "?trang=2"], cr.p_fpt),
    ("nguyenkim", "https://www.nguyenkim.com/dien-thoai-di-dong/", ["page-2/", "?page=2"], cr.p_ld_itemlist),
    ("dmx", "https://www.dienmayxanh.com/tivi", ["?pi=2", "?page=2"], cr.p_dmx),
    ("tgdd", "https://www.thegioididong.com/dtdd", ["?pi=2", "?page=2"], cr.p_ld_itemlist),
]
res = {}
for site, base, sufs, parser in TESTS:
    if len(sys.argv) > 1 and site not in sys.argv[1:]:
        continue
    st, h = get(base)
    ids1 = {str(i["id"]) for i in parser(h)}
    r = {"p1": len(ids1)}
    for s in sufs:
        time.sleep(6)
        try:
            st2, h2 = get(base + s)
            ids2 = {str(i["id"]) for i in parser(h2)}
            r[s] = dict(status=st2, n=len(ids2), new_vs_p1=len(ids2 - ids1))
        except Exception as e:
            r[s] = str(e)[:80]
    res[site] = r
    print(site, json.dumps(r), flush=True)
    time.sleep(6)
json.dump(res, open("pagetest.json", "w"), indent=1)
