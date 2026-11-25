"""Vẽ chart-1 (crawl theo giờ) và chart-2 (thời gian stage batch) cho báo cáo tuần.

Dữ liệu: lượt chạy 13-14/11/2026, stack mp-refactor.
  crawl.csv  : giờ VN, số lượt tải thành công, số lượt lỗi, số bản ghi parse được (audit.crawl_request_attempt)
  stages.csv : run_id, task_id, số giây, trạng thái (bảng task_instance của Airflow, DAG marketplace_batch)
  vi-du-gia.csv : ngày, giá bán, giá niêm yết, % giảm hiển thị, giá thấp nhất trước đó, trạng thái, điểm luật, hạng ML
                  (cache.marketplace_discount_credibility_daily, offer nồi áp suất Elmich PCE-1805)
Chạy: python charts.py <thư mục csv> <thư mục figures>
"""
import csv, sys
from collections import OrderedDict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

src, out = sys.argv[1], sys.argv[2]
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.grid": True, "grid.color": "#e5e7eb",
                     "grid.linewidth": 0.6, "axes.axisbelow": True, "axes.edgecolor": "#9ca3af"})
BLUE, ORANGE = "#1f4e79", "#e07b00"

# chart-1
rows = list(csv.reader(open(f"{src}/crawl.csv", encoding="utf-8")))
hours = [r[0][11:16] for r in rows]
ok = [int(r[1]) for r in rows]
rec = [int(r[3]) for r in rows]
fig, ax = plt.subplots(figsize=(7.2, 2.8))
bars = ax.bar(hours, ok, color=BLUE, width=0.6, label="Trang tải thành công / giờ")
ax.bar_label(bars, fontsize=7.5, padding=2)
ax.set_ylabel("trang / giờ")
ax.set_xlabel("giờ Việt Nam, 14/11/2026")
ax2 = ax.twinx()
ax2.plot(hours, [r / 60 for r in rec], color=ORANGE, marker="o", ms=3.5, lw=1.6, label="Bản ghi / phút (trục phải)")
ax2.set_ylabel("bản ghi / phút")
ax2.spines["right"].set_visible(True); ax2.grid(False)
ax2.set_ylim(0, max(rec) / 60 * 1.2); ax.set_ylim(0, max(ok) * 1.2)
h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
ax.legend(h1 + h2, l1 + l2, loc="upper right", frameon=False, fontsize=8)
fig.tight_layout(); fig.savefig(f"{out}/chart-1-crawl-theo-gio.pdf"); plt.close(fig)

# chart-2
STAGES = ["silver", "dimensions", "facts", "marts", "quality", "publish", "promote"]
COLORS = ["#90caf9", "#1f4e79", "#2e7d32", "#81c784", "#e07b00", "#8e24aa", "#9e9e9e"]
runs = OrderedDict()
for run_id, task, sec, state in csv.reader(open(f"{src}/stages.csv", encoding="utf-8")):
    if task in STAGES and state == "success":
        runs.setdefault(run_id, {})[task] = int(sec)
runs = OrderedDict((k, v) for k, v in runs.items() if all(s in v for s in STAGES))
labels = [k.split("__")[1][5:16].replace("T", " ") for k in runs]  # "11-13 18:00" (UTC)
fig, ax = plt.subplots(figsize=(7.2, 3.0))
bottom = [0] * len(runs)
for s, c in zip(STAGES, COLORS):
    vals = [runs[k][s] / 60 for k in runs]
    ax.bar(labels, vals, bottom=bottom, color=c, width=0.65, label=s)
    bottom = [b + v for b, v in zip(bottom, vals)]
for i, b in enumerate(bottom):
    ax.text(i, b + 0.2, f"{b:.1f}", ha="center", fontsize=7.5)
ax.set_ylabel("phút")
ax.set_xlabel("run batch (giờ as_of, UTC)")
ax.tick_params(axis="x", labelrotation=30, labelsize=7.5)
ax.set_ylim(0, max(bottom) * 1.15)
ax.legend(ncol=7, loc="upper center", bbox_to_anchor=(0.5, 1.13), frameon=False, fontsize=7.5,
          handlelength=1.2, columnspacing=0.9)
fig.tight_layout(); fig.savefig(f"{out}/chart-2-thoi-gian-stage.pdf"); plt.close(fig)
print("runs:", len(runs))

# chart-3: một khuyến mãi bị gắn SUSPECT
rows = list(csv.reader(open(f"{src}/vi-du-gia.csv", encoding="utf-8")))
days = [r[0][8:10] + "/" + r[0][5:7] for r in rows]
price = [int(r[1]) / 1000 for r in rows]
listp = [int(r[2]) / 1000 for r in rows]
fig, ax = plt.subplots(figsize=(7.2, 2.9))
ax.plot(days, listp, color="#9ca3af", ls="--", lw=1.4, marker="s", ms=3.5, label="Giá niêm yết (giá gạch)")
ax.plot(days, price, color=BLUE, lw=1.8, marker="o", ms=4, label="Giá bán")
ax.axhline(1008, color=ORANGE, lw=1, ls=":", label="Giá thấp nhất trước 13/11 (1.008 nghìn)")
for d, p, r in zip(days, price, rows):
    ax.annotate(f"{p:,.0f}".replace(",", ".") + f"\n-{r[3].replace('.', ',')}% ·{r[6]} điểm", (d, p), textcoords="offset points",
                xytext=(0, -26), ha="center", fontsize=7.5)
    ax.annotate(r[5], (d, listp[0]), textcoords="offset points", xytext=(0, 7), ha="center", fontsize=7,
                color="#b91c1c" if r[5] == "SUSPECT" else "#6b7280", fontweight="bold" if r[5] == "SUSPECT" else "normal")
ax.set_ylim(0, 2400)
ax.set_ylabel("nghìn đồng")
ax.set_xlabel("ngày crawl (2026)")
ax.legend(loc="lower center", ncol=3, frameon=False, fontsize=7.5)
fig.tight_layout(); fig.savefig(f"{out}/chart-3-vi-du-khuyen-mai.pdf"); plt.close(fig)
