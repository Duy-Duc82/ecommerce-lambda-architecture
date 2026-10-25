"""Batch cost at scale: fit/score time of the anomaly methods at 10k, 100k and 1M listings.

Rows are resampled from the real Cho Tot snapshot (prices jittered by +-3% so groups are not copies).
Rules in pandas and in PySpark (local[4], percentile_approx for median and MAD, the same shape as the
batch layer's ROLLING_MAD rule); IsolationForest, ECOD, LOF in scikit-learn / PyOD on one machine.
A method is skipped above the size where its previous run took > 300 s.

usage: python scale_bench.py <raw_dir> <results_dir>
"""
import json, sys, time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from chotot_anomaly_bench import FEATS, add_ratio, assign_keys, frame, group_stats, load  # noqa: E402


def pandas_rule(df):
    lp = np.log(df["price"])
    g = lp.groupby(df["key"])
    med = g.transform("median")
    mad = (lp - med).abs().groupby(df["key"]).transform("median")
    return ((lp - med).abs() / (1.4826 * mad.replace(0, np.nan))) > 3.5


def spark_rule(spark, df):
    from pyspark.sql import functions as F
    s = spark.createDataFrame(df[["key", "price"]]).withColumn("lp", F.log("price")).cache()
    s.count()
    t0 = time.perf_counter()
    med = s.groupBy("key").agg(F.percentile_approx("lp", 0.5).alias("med"))
    j = s.join(med, "key").withColumn("dev", F.abs(F.col("lp") - F.col("med")))
    mad = j.groupBy("key").agg(F.percentile_approx("dev", 0.5).alias("mad"))
    n = j.join(mad, "key").where(F.col("dev") / (1.4826 * F.col("mad")) > 3.5).count()
    secs = time.perf_counter() - t0
    s.unpersist()
    return secs, n


def main():
    raw, res = Path(sys.argv[1]), Path(sys.argv[2])
    rows, _ = load(raw)
    base = frame(rows)
    base = base[base["price"] > 1000].reset_index(drop=True)
    base = assign_keys(base, base)
    base = base[base["key"] != "none"].reset_index(drop=True)
    base = add_ratio(base, group_stats(base))
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[4]").appName("scale_bench").config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "8").config("spark.driver.memory", "4g").getOrCreate())
    from pyod.models.ecod import ECOD
    from sklearn.ensemble import IsolationForest
    from sklearn.neighbors import LocalOutlierFactor
    methods = {
        "rule_mad_pandas": lambda d, X: pandas_rule(d),
        "rule_mad_pyspark_local4": None,
        "isolation_forest": lambda d, X: IsolationForest(n_estimators=200, random_state=0, n_jobs=-1).fit(X).score_samples(X),
        "ecod": lambda d, X: ECOD().fit(X).decision_scores_,
        "lof_k20": lambda d, X: LocalOutlierFactor(n_neighbors=20, n_jobs=-1).fit_predict(X),
    }
    out, slow = dict(source_rows=len(base), runs={}), set()
    rng = np.random.default_rng(0)
    for n in (10_000, 100_000, 1_000_000):
        d = base.sample(n, replace=True, random_state=1).reset_index(drop=True)
        d["price"] = d["price"] * rng.uniform(0.97, 1.03, n)
        X = d[FEATS].values.astype(float) + rng.normal(0, 1e-3, (n, len(FEATS)))
        for name, fn in methods.items():
            if name in slow:
                out["runs"].setdefault(name, {})[str(n)] = "skipped (previous size > 300 s)"
                continue
            if name == "rule_mad_pyspark_local4":
                secs, _ = spark_rule(spark, d)
            else:
                t0 = time.perf_counter()
                fn(d, X)
                secs = time.perf_counter() - t0
            out["runs"].setdefault(name, {})[str(n)] = round(secs, 3)
            print(n, name, round(secs, 3), flush=True)
            if secs > 300:
                slow.add(name)
    spark.stop()
    json.dump(out, open(res / "scale_bench.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
