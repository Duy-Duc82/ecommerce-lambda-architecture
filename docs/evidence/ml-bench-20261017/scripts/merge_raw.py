"""Concatenate several raw collection directories into one, file by file (ids and timestamps do not collide).

usage: python merge_raw.py <out_dir> <raw_dir> [<raw_dir> ...]
"""
import shutil, sys
from pathlib import Path

out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)
for name in ("chotot_models.jsonl", "chotot_stream.jsonl", "chotot_track.jsonl", "dmx.jsonl", "tiki.jsonl", "pairs.jsonl"):
    with open(out / name, "wb") as f:
        for d in sys.argv[2:]:
            p = Path(d) / name
            if p.exists():
                f.write(p.read_bytes())
for d in sys.argv[2:]:
    if (Path(d) / "entity_sitemaps.json").exists():
        shutil.copy(Path(d) / "entity_sitemaps.json", out / "entity_sitemaps.json")
print({p.name: sum(1 for _ in open(p, encoding="utf-8")) for p in out.glob("*.jsonl")})
