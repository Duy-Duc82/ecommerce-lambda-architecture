import sys,urllib.request,gzip,ssl
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
def get(u):
    r=urllib.request.Request(u,headers={"User-Agent":UA,"Accept-Language":"vi-VN","Accept-Encoding":"gzip"})
    with urllib.request.urlopen(r,timeout=30,context=ssl.create_default_context()) as x:
        b=x.read()
        if x.headers.get("Content-Encoding")=="gzip": b=gzip.decompress(b)
        return x.status,b.decode("utf-8","replace")
if __name__=="__main__":
    s,h=get(sys.argv[1]); open(sys.argv[2],"w",encoding="utf-8").write(h); print(s,len(h))
