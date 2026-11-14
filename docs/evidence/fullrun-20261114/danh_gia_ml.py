"""Đánh giá phần ML (ADR-0009) trên stack mp-refactor đang chạy, theo CHIEN-LUOC-DANH-GIA-ML.md.

Mỗi tiêu chí in ra số đo, ngưỡng và PASS/FAIL/CHUA_DU (chưa đủ dữ liệu để kết luận).
Ngưỡng nằm ở hằng số THRESHOLDS bên dưới, đặt trước khi chạy; không sửa chúng sau
khi đã thấy số.

Chạy trên host, từ gốc repo, bằng Python của .venv (cần psycopg2, minio, requests):

    python docs/evidence/fullrun-20261114/danh_gia_ml.py snapshot        # mỗi giờ: chụp verdict + đo
    python docs/evidence/fullrun-20261114/danh_gia_ml.py report          # in và ghi bao-cao-ml.json
    python docs/evidence/fullrun-20261114/danh_gia_ml.py export-labels   # cuối lượt: xuất 3 danh sách để gán nhãn tay
    python docs/evidence/fullrun-20261114/danh_gia_ml.py score-labels    # sau khi gán nhãn: precision@30

Kết nối lấy từ env/base.env + env/ports.refactor.env (mặc định của compose nếu thiếu).
Đầu ra nằm trong thư mục này: snapshots/, bao-cao-ml.json, nhan-tay.csv.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
SNAPSHOTS = HERE / "snapshots"
REPORT = HERE / "bao-cao-ml.json"
LABELS = HERE / "nhan-tay.csv"

# Đặt trước khi chạy (CHIEN-LUOC-DANH-GIA-ML.md). Không chỉnh sau khi thấy kết quả.
THRESHOLDS = {
    "stability_min": 0.99,  # tỉ lệ (offer, ngày) giữ verdict giữa hai snapshot liên tiếp
    "marts_growth_max": 0.30,  # stage marts chậm thêm tối đa 30 % so với mốc
    "marts_baseline_seconds": None,  # mốc: lấy từ run đầu tiên của lượt nếu không có mốc cũ
    "fit_seconds_max": 60.0,
    "flag_share_min": 0.01,  # (QUESTIONABLE + SUSPECT) / offer-ngày hiển thị giảm giá
    "flag_share_max": 0.10,
    "agreement_lift_min": 2.0,  # luật ≥ 50 nằm trong top 5 % ML nhiều gấp ít nhất 2 lần ngẫu nhiên
    "top_k": 30,
}
HISTORY_REASONS = ("LIST_PRICE_RAISED", "CLAIM_EXCEEDS_HISTORY", "NOT_BELOW_REFERENCE")
LABEL_VALUES = {"A", "B", "C", "N"}


def env() -> dict[str, str]:
    values: dict[str, str] = {}
    for name in ("env/base.env", "env/ports.refactor.env", "env/refactor.env"):
        path = ROOT / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.split(" #")[0].strip().strip('"').strip("'")
    values.update({k: v for k, v in os.environ.items() if k.startswith(("POSTGRES_", "MINIO_", "LOKI_"))})
    return values


def connect(cfg):
    import psycopg2

    return psycopg2.connect(
        host="127.0.0.1",
        port=int(cfg.get("POSTGRES_HOST_PORT", cfg.get("POSTGRES_PORT", "5436"))),
        user=cfg.get("POSTGRES_USER", "admin"),
        password=cfg.get("POSTGRES_PASSWORD", "admin123"),
        dbname=cfg.get("POSTGRES_DB", "data_warehouse"),
    )


def rows(conn, sql, params=()):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        names = [c.name for c in cur.description]
        return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def one(conn, sql, params=()):
    result = rows(conn, sql, params)
    return result[0] if result else {}


def verdict(ok: bool | None) -> str:
    return "CHUA_DU" if ok is None else ("PASS" if ok else "FAIL")


# ---------------------------------------------------------------- snapshot


def snapshot(conn) -> Path | None:
    """Chụp verdict của ngày mới nhất trong cache, đặt tên theo run đang phục vụ."""
    if not one(conn, "SELECT to_regclass('cache.marketplace_discount_credibility_daily') AS t")["t"]:
        print("chưa có bảng cache.marketplace_discount_credibility_daily: chạy mp.ps1 migrate")
        return None
    version = one(conn, "SELECT run_id, published_at FROM audit.marketplace_cache_version")
    if not version:
        print("chưa có cache nào được publish")
        return None
    SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    path = SNAPSHOTS / f"{version['run_id']}.csv"
    if path.exists():
        print(f"đã có snapshot của {version['run_id']}")
        return path
    data = rows(
        conn,
        """SELECT marketplace, offer_id, observed_date::text AS observed_date, currency, credibility_status, rule_score,
                  ml_rank_percent
           FROM cache.marketplace_discount_credibility_daily
           WHERE observed_date = (SELECT max(observed_date) FROM cache.marketplace_discount_credibility_daily)""",
    )
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["marketplace", "offer_id", "observed_date", "currency", "credibility_status", "rule_score",
                        "ml_rank_percent"],
        )
        writer.writeheader()
        writer.writerows(data)
    print(f"snapshot {version['run_id']}: {len(data)} dòng")
    return path


def stability() -> dict:
    """Giữa hai snapshot liên tiếp cùng ngày: bao nhiêu (offer, ngày) giữ nguyên verdict.

    Chỉ so các dòng có ở cả hai: một offer mới được crawl trong giờ đó không phải là bất ổn.
    """
    files = sorted(SNAPSHOTS.glob("*.csv")) if SNAPSHOTS.exists() else []
    pairs = []
    for older, newer in zip(files, files[1:], strict=False):
        load = lambda p: {  # noqa: E731
            (r["marketplace"], r["offer_id"], r["observed_date"], r["currency"]): r["credibility_status"]
            for r in csv.DictReader(p.open(encoding="utf-8"))
        }
        a, b = load(older), load(newer)
        common = set(a) & set(b)
        if common:
            same = sum(a[k] == b[k] for k in common)
            pairs.append({"from": older.stem, "to": newer.stem, "common": len(common), "same": same,
                          "share": same / len(common)})
    worst = min((p["share"] for p in pairs), default=None)
    return {
        "pairs": pairs,
        "worst_share": worst,
        "threshold": THRESHOLDS["stability_min"],
        "verdict": verdict(None if worst is None else worst >= THRESHOLDS["stability_min"]),
    }


# ---------------------------------------------------------------- 1. correctness


def correctness(conn) -> dict:
    out: dict = {}
    runs = rows(
        conn,
        """SELECT run_id, status, started_at, completed_at FROM audit.marketplace_batch_run
           WHERE run_id LIKE 'mp-2026%%' AND started_at >= now() - interval '14 hours' ORDER BY started_at""",
    )
    finished = [r for r in runs if r["status"] not in ("RUNNING", "CREATED")]
    promoted = [r for r in finished if r["status"] in ("PROMOTED", "SUCCEEDED")]
    # Run đầu tiên được phép trượt (chép audit cũ sang lake, ADR-0008); từ run thứ hai phải PROMOTED.
    out["batch_runs"] = {
        "runs": [{k: str(v) for k, v in r.items()} for r in runs],
        "finished": len(finished),
        "promoted": len(promoted),
        "verdict": verdict(None if len(finished) < 2 else all(r in promoted for r in finished[1:])),
    }
    coverage = rows(
        conn,
        """SELECT run_id, status, observed_value FROM audit.marketplace_quality_result
           WHERE check_name = 'discount_credibility_evaluation_coverage' ORDER BY checked_at DESC LIMIT 5""",
    )
    latest = coverage[0] if coverage else None
    out["coverage_rule"] = {
        "latest": latest,
        "verdict": verdict(None if latest is None else latest["status"] == "PASS" and (latest["observed_value"] or 0) > 0),
    }
    counts = one(
        conn,
        """SELECT (SELECT count(*) FROM cache.marketplace_discount_credibility_daily) AS credibility,
                  (SELECT count(*) FROM cache.marketplace_offer_price_history_daily) AS price_history""",
    )
    out["grain"] = {**counts, "verdict": verdict(counts["credibility"] == counts["price_history"] and counts["credibility"] > 0)}
    bad = one(
        conn,
        """SELECT count(*) FILTER (WHERE rule_score NOT BETWEEN 0 AND 100) AS score_out_of_range,
                  count(*) FILTER (WHERE left(credibility_reasons_json, 1) <> '[') AS reasons_not_array,
                  count(*) FILTER (WHERE ml_method = 'ISOLATION_FOREST' AND ml_rank_percent IS NULL) AS ranked_without_rank,
                  count(*) FILTER (WHERE ml_method = 'ISOLATION_FOREST' AND NOT (displayed_discount_percent > 0))
                      AS ranked_without_reduction
           FROM cache.marketplace_discount_credibility_daily""",
    )
    out["row_invariants"] = {**bad, "verdict": verdict(sum(bad.values()) == 0)}
    reasons = rows(
        conn,
        """SELECT observed_date::text AS observed_date, reason, count(*) AS n
           FROM cache.marketplace_discount_credibility_daily,
                LATERAL jsonb_array_elements_text(credibility_reasons_json::jsonb) AS reason
           GROUP BY 1, 2 ORDER BY 1, 2""",
    )
    today = max((r["observed_date"] for r in reasons), default=None)
    history_fired = sum(r["n"] for r in reasons if r["observed_date"] == today and r["reason"] in HISTORY_REASONS)
    out["history_rules"] = {
        "by_day": reasons,
        "latest_day": today,
        "history_reasons_latest_day": history_fired,
        "verdict": verdict(None if today is None else history_fired > 0),
    }
    rec = rows(
        conn,
        """WITH m AS (SELECT marketplace, observed_date, currency,
                             count(*) FILTER (WHERE credibility_status IN ('QUESTIONABLE','SUSPECT')) AS flagged
                      FROM cache.marketplace_discount_credibility_daily GROUP BY 1, 2, 3),
                d AS (SELECT marketplace, observed_date, currency,
                             sum(questionable_offer_count + suspect_offer_count) AS flagged
                      FROM cache.marketplace_discount_daily GROUP BY 1, 2, 3)
           SELECT m.marketplace, m.observed_date::text AS observed_date, m.flagged AS offer_mart, d.flagged AS category_mart
           FROM m FULL JOIN d USING (marketplace, observed_date, currency)
           WHERE m.flagged IS DISTINCT FROM d.flagged""",
    )
    out["category_reconciles"] = {"mismatches": rec, "verdict": verdict(not rec)}
    out["stability"] = stability()
    return out


# ---------------------------------------------------------------- 2. performance


def stage_seconds(cfg, run_ids) -> dict[str, dict[str, float]]:
    from minio import Minio

    client = Minio(
        f"127.0.0.1:{cfg.get('MINIO_API_HOST_PORT', '9030')}",
        access_key=cfg.get("MINIO_ROOT_USER", "minioadmin"),
        secret_key=cfg.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False,
    )
    bucket = cfg.get("MINIO_BUCKET_SILVER", "ecommerce-silver")
    out: dict[str, dict[str, float]] = {}
    for run_id in run_ids:
        for stage in ("silver", "dimensions", "facts", "marts", "quality", "publish", "promote"):
            try:
                body = client.get_object(bucket, f"marketplace/_runs/run_id={run_id}/timings/{stage}.json").read()
            except Exception:  # noqa: BLE001 - a stage that did not run has no timing
                continue
            seconds = json.loads(body).get("seconds")
            if seconds is not None:
                out.setdefault(run_id, {})[stage] = float(seconds)
    return out


def fit_lines(cfg) -> list[dict]:
    """Các dòng discount_ml_fitted trong Loki, 14 giờ gần nhất."""
    import requests

    url = f"http://127.0.0.1:{cfg.get('LOKI_HOST_PORT', '3104')}/loki/api/v1/query_range"
    end = int(datetime.now(UTC).timestamp() * 1e9)
    params = {"query": '{service=~".+"} |= "discount_ml_fitted"', "start": end - int(14 * 3600 * 1e9), "end": end,
              "limit": 500}
    try:
        data = requests.get(url, params=params, timeout=30).json()
    except Exception as exc:  # noqa: BLE001
        return [{"error": f"Loki: {exc}"}]
    found = []
    for stream in data.get("data", {}).get("result", []):
        for _, line in stream.get("values", []):
            start = line.find("{")
            try:
                found.append(json.loads(line[start:]))
            except ValueError:
                continue
    return found


def performance(conn, cfg) -> dict:
    runs = rows(
        conn,
        """SELECT run_id FROM audit.marketplace_batch_run
           WHERE run_id LIKE 'mp-2026%%' AND started_at >= now() - interval '14 hours' ORDER BY started_at""",
    )
    timings = stage_seconds(cfg, [r["run_id"] for r in runs])
    marts = [t["marts"] for t in timings.values() if "marts" in t]
    baseline = THRESHOLDS["marts_baseline_seconds"] or (marts[0] if marts else None)
    worst = max(marts, default=None)
    growth = None if baseline is None or worst is None else (worst - baseline) / baseline
    fits = fit_lines(cfg)
    fit_seconds = [f["fit_seconds"] for f in fits if "fit_seconds" in f]
    ranked = one(
        conn,
        """SELECT count(*) FILTER (WHERE ml_method = 'ISOLATION_FOREST') AS ranked,
                  count(*) FILTER (WHERE displayed_discount_percent > 0) AS showing_reduction
           FROM cache.marketplace_discount_credibility_daily""",
    )
    return {
        "stage_seconds": timings,
        "marts": {
            "baseline_seconds": baseline,
            "worst_seconds": worst,
            "growth": growth,
            "threshold": THRESHOLDS["marts_growth_max"],
            # Mốc lấy từ chính lượt này thì chỉ đo được độ ổn định, ghi rõ trong báo cáo.
            "baseline_from_this_run": THRESHOLDS["marts_baseline_seconds"] is None,
            "verdict": verdict(None if growth is None or len(marts) < 2 else growth <= THRESHOLDS["marts_growth_max"]),
        },
        "fit": {
            "lines": fits[-5:],
            "count": len(fit_seconds),
            "max_seconds": max(fit_seconds, default=None),
            "threshold": THRESHOLDS["fit_seconds_max"],
            "verdict": verdict(None if not fit_seconds else max(fit_seconds) <= THRESHOLDS["fit_seconds_max"]),
        },
        "ranked_rows": {**ranked, "verdict": verdict(ranked["showing_reduction"] > 0 and ranked["ranked"] == ranked["showing_reduction"])},
    }


# ---------------------------------------------------------------- 3. quality (without labels)


def quality(conn) -> dict:
    latest = one(conn, "SELECT max(observed_date) AS d FROM cache.marketplace_discount_credibility_daily")["d"]
    share = one(
        conn,
        """SELECT count(*) FILTER (WHERE displayed_discount_percent > 0) AS showing,
                  count(*) FILTER (WHERE displayed_discount_percent > 0 AND credibility_status IN ('QUESTIONABLE','SUSPECT'))
                      AS flagged
           FROM cache.marketplace_discount_credibility_daily WHERE observed_date = %s""",
        (latest,),
    )
    flag_share = share["flagged"] / share["showing"] if share.get("showing") else None
    agree = one(
        conn,
        """SELECT count(*) FILTER (WHERE rule_score >= 50) AS strong,
                  count(*) FILTER (WHERE rule_score >= 50 AND ml_rank_percent >= 95) AS strong_in_top5
           FROM cache.marketplace_discount_credibility_daily
           WHERE observed_date = %s AND ml_method = 'ISOLATION_FOREST'""",
        (latest,),
    )
    lift = (agree["strong_in_top5"] / agree["strong"]) / 0.05 if agree.get("strong") else None
    return {
        "latest_day": str(latest),
        "flag_share": {
            **share,
            "share": flag_share,
            "range": [THRESHOLDS["flag_share_min"], THRESHOLDS["flag_share_max"]],
            "verdict": verdict(
                None if flag_share is None else THRESHOLDS["flag_share_min"] <= flag_share <= THRESHOLDS["flag_share_max"]
            ),
        },
        "rule_ml_agreement": {
            **agree,
            "lift_over_random": lift,
            "threshold": THRESHOLDS["agreement_lift_min"],
            # Ít hơn 10 dòng điểm luật ≥ 50 thì chưa kết luận được.
            "verdict": verdict(None if not agree.get("strong") or agree["strong"] < 10 else lift >= THRESHOLDS["agreement_lift_min"]),
        },
    }


# ---------------------------------------------------------------- labels


def export_labels(conn) -> None:
    """Ba danh sách cùng ngày: top luật, top ML (luật không gắn cờ), baseline anomaly LOW.

    Chỉ xuất và để dành: việc gán nhãn và cách gán sẽ được bàn lại sau (KICH-BAN-CHAY.md
    mục 6). Vì vậy ngoài danh sách còn xuất toàn bộ lịch sử giá theo ngày của các offer
    ứng viên (nhan-tay-lich-su-gia.csv), để gán nhãn được cả khi stack đã tắt hoặc volume
    đã đổi. Cột label và note để trống; cột list cho biết dòng thuộc danh sách nào.
    """
    k = THRESHOLDS["top_k"]
    latest = one(conn, "SELECT max(observed_date) AS d FROM cache.marketplace_discount_credibility_daily")["d"]
    common = """c.marketplace, c.offer_id, c.observed_date::text AS observed_date, o.product_title, o.source_url,
                c.evaluated_price, c.list_price, round(c.displayed_discount_percent::numeric, 1) AS displayed_pct,
                c.reference_min_price, c.reference_sample_size,
                round(c.effective_discount_percent::numeric, 1) AS effective_pct, c.credibility_status,
                c.credibility_reasons_json, c.rule_score, round(c.ml_rank_percent::numeric, 2) AS ml_rank"""
    base = """FROM cache.marketplace_discount_credibility_daily c
               LEFT JOIN cache.marketplace_offer_current o USING (offer_id)
               WHERE c.observed_date = %s"""
    lists = {
        "rules": rows(conn, f"SELECT 'rules' AS list, {common} {base} AND c.rule_score > 0 "
                            f"ORDER BY c.rule_score DESC, c.ml_rank_percent DESC NULLS LAST LIMIT {k}", (latest,)),
        "ml": rows(conn, f"SELECT 'ml' AS list, {common} {base} AND c.ml_method = 'ISOLATION_FOREST' "
                         f"AND c.credibility_status IN ('CREDIBLE','INSUFFICIENT_HISTORY') "
                         f"ORDER BY c.ml_rank_percent DESC LIMIT {k}", (latest,)),
        "anomaly_baseline": rows(
            conn,
            f"""SELECT 'anomaly_baseline' AS list, {common}
                FROM cache.marketplace_price_anomaly_daily a
                JOIN cache.marketplace_discount_credibility_daily c USING (marketplace, offer_id, observed_date, currency)
                LEFT JOIN cache.marketplace_offer_current o USING (offer_id)
                WHERE a.observed_date = %s AND a.anomaly_status = 'ANOMALOUS_LOW'
                ORDER BY a.robust_score ASC LIMIT {k}""",
            (latest,),
        ),
    }
    merged = sorted((r for rs in lists.values() for r in rs), key=lambda r: (r["offer_id"], r["list"]))
    if not merged:
        print("không có dòng nào để gán nhãn")
        return
    fields = ["label", "note", *merged[0].keys()]
    with LABELS.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for r in merged:
            writer.writerow({"label": "", "note": "", **r})
    print(f"{LABELS}: " + ", ".join(f"{name}={len(rs)}" for name, rs in lists.items()))

    offers = sorted({r["offer_id"] for r in merged})
    history = rows(
        conn,
        """SELECT h.marketplace, h.offer_id, h.observed_date::text AS observed_date, h.currency, h.first_price,
                  h.last_price, h.min_price, h.max_price, h.first_list_price, h.last_list_price,
                  h.observation_count, h.distinct_price_count, h.last_availability
           FROM cache.marketplace_offer_price_history_daily h
           WHERE h.offer_id = ANY(%s) ORDER BY h.offer_id, h.observed_date""",
        (offers,),
    )
    path = HERE / "nhan-tay-lich-su-gia.csv"
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(history[0].keys()) if history else ["offer_id"])
        writer.writeheader()
        writer.writerows(history)
    print(f"{path}: {len(history)} dòng lịch sử giá của {len(offers)} offer")


def score_labels() -> dict:
    if not LABELS.exists():
        sys.exit(f"chưa có {LABELS}; chạy export-labels trước")
    data = list(csv.DictReader(LABELS.open(encoding="utf-8-sig")))
    blank = [r for r in data if r["label"].strip().upper() not in LABEL_VALUES]
    result: dict = {"unlabelled": len(blank), "lists": {}}
    for name in sorted({r["list"] for r in data}):
        part = [r for r in data if r["list"] == name and r["label"].strip().upper() in LABEL_VALUES]
        counts = {v: sum(r["label"].strip().upper() == v for r in part) for v in sorted(LABEL_VALUES)}
        result["lists"][name] = {
            "labelled": len(part),
            "counts": counts,
            # A và B là giảm giá ảo theo định nghĩa; C là đổi biến thể, không tính là đúng.
            "precision": (counts["A"] + counts["B"]) / len(part) if part else None,
        }
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result


# ---------------------------------------------------------------- main


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["snapshot", "report", "export-labels", "score-labels"])
    args = parser.parse_args()
    if args.command == "score-labels":
        report = json.loads(REPORT.read_text(encoding="utf-8")) if REPORT.exists() else {}
        report["labels"] = score_labels()
        REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
        return
    cfg = env()
    with connect(cfg) as conn:
        if args.command == "snapshot":
            snapshot(conn)
            return
        if args.command == "export-labels":
            export_labels(conn)
            return
        report = {
            "generated_at": datetime.now(UTC).isoformat(),
            "thresholds": THRESHOLDS,
            "correctness": correctness(conn),
            "performance": performance(conn, cfg),
            "quality": quality(conn),
        }
    REPORT.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    for section in ("correctness", "performance", "quality"):
        for name, item in report[section].items():
            if isinstance(item, dict) and "verdict" in item:
                print(f"{item['verdict']:8} {section}.{name}")
    print(f"chi tiết: {REPORT}")


if __name__ == "__main__":
    main()
