import json, urllib.robotparser, os
from urllib.parse import urlparse
d=json.load(open("probe.json",encoding="utf-8"))
for r in d:
    f=f"raw/{r['site']}_robots.txt"
    if not os.path.exists(f): print(r['site'],"no robots"); continue
    rp=urllib.robotparser.RobotFileParser(); rp.parse(open(f,encoding="utf-8").read().splitlines())
    p=urlparse(r['url'])
    print(f"{r['site']:14} *:{rp.can_fetch('*',r['url'])}  Googlebot:{rp.can_fetch('Googlebot',r['url'])} crawl-delay:{rp.crawl_delay('*')}")
