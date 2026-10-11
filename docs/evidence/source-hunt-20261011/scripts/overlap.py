"""Cross-site overlap of phone models in round-1 listings via a normalized model key."""
import re, json, unicodedata
from collections import defaultdict

PHONE_SITES = {"tiki", "chotot", "tgdd", "fptshop", "nguyenkim", "websosanh"}
STOP = r"\b(dien thoai|điện thoại|hang chinh hang|chinh hang|5g|4g|lte|vn/a|vna|chính hãng|hàng|new|99%|cu|likenew)\b"


def strip_acc(s):
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn").replace("đ", "d").replace("Đ", "D")


def key(name):
    s = strip_acc(name.lower())
    s = re.sub(r"\(.*?\)|\[.*?\]", " ", s)
    s = re.sub(STOP, " ", s)
    mem = re.findall(r"(\d+)\s*(gb|tb)", s)
    storage = max((int(a) * (1024 if b == "tb" else 1) for a, b in mem), default=None)
    m = re.search(r"(iphone\s*\d+\s*(?:pro max|pro|plus|air|e)?|galaxy\s*[a-z]\d+\s*(?:ultra|fe|\+|plus)?|"
                  r"(?:redmi|poco|xiaomi)\s*(?:note\s*)?\d+\s*(?:pro\+|pro|ultra|s|c)?|reno\s*\d+\s*(?:pro|f)?|"
                  r"oppo\s*a\d+|vivo\s*[a-z]\d+|realme\s*(?:c)?\d+|tecno\s*\w+\s*\d+|nothing phone\s*\(?\d)", s)
    if not m:
        return None
    model = re.sub(r"\s+", " ", m.group(1)).strip().replace("samsung ", "")
    return f"{model}|{storage}" if storage else model


if __name__ == "__main__":
    latest = {}
    for line in open("rounds.jsonl", encoding="utf-8"):
        r = json.loads(line)
        if r["site"] in PHONE_SITES and r["n"] and ("c1789" in r["url"] or r["site"] != "tiki"):
            latest.setdefault(r["site"], r)
    models = defaultdict(dict)
    for site, r in latest.items():
        for it in r["items"]:
            k = key(it.get("name") or "")
            if k:
                models[k].setdefault(site, it["price"])
    multi = {k: v for k, v in models.items() if len(v) >= 2}
    print("sites:", {s: len(r["items"]) for s, r in latest.items()})
    print("keyed models:", len(models), "| on >=2 sites:", len(multi), "| on >=3:", sum(len(v) >= 3 for v in multi.values()))
    for k, v in sorted(multi.items(), key=lambda kv: -len(kv[1]))[:20]:
        print(f"  {k:32}", v)
    json.dump({k: v for k, v in multi.items()}, open("overlap.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
