import re, json

PUSH = re.compile(r'self\.__next_f\.push\(\[1,"((?:[^"\\]|\\.)*)"\]\)')


def rsc(h):
    return "".join(json.loads('"' + m + '"') for m in PUSH.findall(h))
