"""Mandatory data-quality gate over Silver, crawl audit and Gold.

Phase 7 plan section 8. Every rule in ``config.quality_rules`` produces exactly
one result on every run — a check that cannot run reports SKIPPED with a reason
rather than disappearing, and :func:`decide` treats a skipped mandatory check as
a failure. An unreachable audit database must never read as a green gate.

Nothing here opens a database, touches object storage, starts a Spark session
or reads a clock. ``checked_at`` is the run's ``as_of``, so rerunning the same
context reproduces the same results byte for byte, which is what makes the
replay checks in the plan's section 13 meaningful.

Spark actions, when every check passes: one aggregate over the observations
covering the seven row-level Silver checks plus the totals, one probe and one
count for the audit reconciliation, and one count or aggregate each for the
listing key, ``offer_current``, the two Gold reconciliations, freshness and the
four advisory checks — thirteen in all. A failing check adds one bounded
``limit().collect()`` to name its first offending keys.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from config.quality_rules import (
    ADVISORY,
    MANDATORY,
    QUALITY_RULES,
    mandatory_rule_names,
    quality_rule,
)
from config.settings import MARKETPLACE_QUALITY_SAMPLE_LIMIT

if TYPE_CHECKING:
    from batch_layer.marketplace_warehouse import MarketplaceBatchContext

PASS = "PASS"
FAIL = "FAIL"
SKIPPED = "SKIPPED"

# An advisory rate above this is worth a reviewer's attention. It never blocks
# publication, so it is a reporting threshold, not a gate.
_ADVISORY_MAJORITY = 0.5

_SHA256_HEX = r"^[0-9a-f]{64}$"
_ISO_CURRENCY = r"^[A-Z]{3}$"


@dataclass(frozen=True)
class QualityResult:
    run_id: str
    check_name: str
    severity: str
    dataset_name: str
    status: str
    observed_value: float | None
    expectation: str
    rule_version: str
    failure_sample_json: str | None
    checked_at: datetime


@dataclass(frozen=True)
class QualityDecision:
    run_id: str
    passed: bool
    rule_version: str
    evaluated_at: datetime
    mandatory_total: int
    mandatory_failures: int
    advisory_failures: int
    skipped: int


class QualityGateFailure(RuntimeError):
    """Raised when publication is refused. Carries the evidence with it."""

    def __init__(self, decision: QualityDecision, results: Sequence[QualityResult]):
        self.decision = decision
        self.results = tuple(results)
        self.failing = tuple(
            result.check_name
            for result in self.results
            if result.severity == MANDATORY and result.status != PASS
        )
        super().__init__(
            f"run {decision.run_id} failed {decision.mandatory_failures} mandatory "
            f"quality check(s): {', '.join(self.failing) or 'none reported'}"
        )


def _canonical(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _result(
    check_name: str,
    context: "MarketplaceBatchContext",
    *,
    status: str,
    observed_value: float | None,
    keys: Sequence[str] = (),
    reason: str | None = None,
) -> QualityResult:
    rule = quality_rule(check_name)
    sample = None
    if reason is not None:
        sample = _canonical({"reason": reason})
    elif keys:
        sample = _canonical({"keys": sorted(keys)[:MARKETPLACE_QUALITY_SAMPLE_LIMIT]})
    return QualityResult(
        run_id=context.run_id,
        check_name=check_name,
        severity=rule.severity,
        dataset_name=rule.dataset_name,
        status=status,
        observed_value=None if observed_value is None else float(observed_value),
        expectation=rule.expectation,
        rule_version=context.quality_rule_version,
        failure_sample_json=sample,
        checked_at=context.as_of,
    )


def _violations(predicate: Column) -> Column:
    return F.coalesce(F.sum(F.when(predicate, 1).otherwise(0)), F.lit(0))


def _sample_keys(frame: DataFrame, key: Column) -> tuple[str, ...]:
    """Identifiers only, bounded and sorted.

    A failure sample points a human at the first offending rows. It must never
    carry a raw body, a product title, a URL query or a credential, so callers
    pass an explicit identifier expression rather than a whole row.
    """
    rows = frame.select(key.alias("_key")).limit(MARKETPLACE_QUALITY_SAMPLE_LIMIT).collect()
    return tuple(str(row["_key"]) for row in rows if row["_key"] is not None)


def _counted(
    check_name: str,
    context: "MarketplaceBatchContext",
    *,
    violations: int,
    frame: DataFrame | None = None,
    key: Column | None = None,
) -> QualityResult:
    if violations == 0:
        return _result(check_name, context, status=PASS, observed_value=0.0)
    keys = _sample_keys(frame, key) if frame is not None and key is not None else ()
    return _result(check_name, context, status=FAIL, observed_value=float(violations), keys=keys)


def _silver_row_checks(observations: DataFrame, context: "MarketplaceBatchContext") -> dict[str, Column]:
    """The seven row-level Silver predicates, each True when the row offends."""
    future_limit = context.as_of + timedelta(seconds=context.future_tolerance_seconds)
    blank = lambda name: F.col(name).isNull() | (F.trim(F.col(name)) == "")  # noqa: E731
    return {
        "raw_artifact_checksum_and_uri_present":
            blank("raw_uri") | F.col("raw_sha256").isNull() | ~F.col("raw_sha256").rlike(_SHA256_HEX),
        "silver_observation_raw_lineage_complete":
            blank("crawl_run_id") | blank("adapter_version") | F.col("fetched_at").isNull(),
        "offer_key_and_price_complete":
            F.col("offer_id").isNull() | F.col("marketplace").isNull()
            | F.col("observed_at").isNull() | F.col("current_price").isNull(),
        "price_non_negative":
            (F.col("current_price") < F.lit(0))
            | (F.col("list_price").isNotNull() & (F.col("list_price") < F.lit(0))),
        # The boundary is inclusive: a row exactly at as_of plus the tolerance
        # is still acceptable clock skew.
        "observed_at_within_future_tolerance":
            F.col("observed_at") > F.lit(future_limit).cast("timestamp"),
        "currency_valid":
            F.col("currency").isNull() | ~F.col("currency").rlike(_ISO_CURRENCY)
            | ~F.col("currency").isin(list(context.allowed_currencies)),
    }


def _evaluate_silver(observations: DataFrame, context: "MarketplaceBatchContext") -> tuple[list[QualityResult], int]:
    predicates = _silver_row_checks(observations, context)
    aggregates = [F.count(F.lit(1)).alias("_total"), F.countDistinct("observation_id").alias("_distinct")]
    aggregates += [_violations(predicate).alias(name) for name, predicate in predicates.items()]
    totals = observations.agg(*aggregates).collect()[0]

    results = [
        _counted(name, context, violations=int(totals[name]), frame=observations.filter(predicate), key=F.col("observation_id"))
        for name, predicate in predicates.items()
    ]

    duplicated = int(totals["_total"]) - int(totals["_distinct"])
    duplicates = observations.groupBy("observation_id").count().filter(F.col("count") > 1)
    results.append(
        _counted("observation_id_unique", context, violations=duplicated, frame=duplicates, key=F.col("observation_id"))
    )

    listing = (
        observations.groupBy("marketplace_id", "platform_listing_id")
        .agg(F.countDistinct("offer_id").alias("_offers"))
        .filter(F.col("_offers") > 1)
    )
    results.append(
        _counted(
            "offer_listing_key_unique", context,
            violations=listing.count(),
            frame=listing,
            key=F.concat_ws("/", F.col("marketplace_id"), F.col("platform_listing_id")),
        )
    )
    return results, int(totals["_total"])


def _evaluate_audit_reconciliation(
    observations: DataFrame, attempts: DataFrame, context: "MarketplaceBatchContext"
) -> QualityResult:
    name = "silver_parse_attempt_reconciliation"
    missing = {"crawl_run_id", "parsed_count"} - set(attempts.columns)
    if missing:
        return _result(
            name, context, status=SKIPPED, observed_value=None,
            reason=f"crawl attempt audit lacks {','.join(sorted(missing))}",
        )
    if attempts.limit(1).count() == 0:
        # Not a pass. Without audit evidence the reconciliation has nothing to
        # compare against, and a mandatory skip fails the gate in decide().
        return _result(name, context, status=SKIPPED, observed_value=None, reason="crawl attempt audit is empty")

    silver_runs = observations.groupBy("crawl_run_id").agg(F.count(F.lit(1)).alias("_silver_rows"))
    audit_runs = attempts.groupBy("crawl_run_id").agg(F.sum("parsed_count").alias("_parsed"))
    # Left join: only crawl runs present in this input are reconciled, so a run
    # whose observations fall outside the read window is not a discrepancy. A
    # run present in Silver with no audit row is one, because those rows have
    # no recorded provenance.
    mismatched = (
        silver_runs.join(audit_runs, "crawl_run_id", "left")
        .filter(F.coalesce(F.col("_parsed"), F.lit(-1)) != F.col("_silver_rows"))
    )
    return _counted(name, context, violations=mismatched.count(), frame=mismatched, key=F.col("crawl_run_id"))


def _evaluate_gold(
    observations: DataFrame,
    marts: Mapping[str, DataFrame],
    context: "MarketplaceBatchContext",
    silver_rows: int,
) -> list[QualityResult]:
    results: list[QualityResult] = []

    duplicated_offers = marts["offer_current"].groupBy("offer_id").count().filter(F.col("count") > 1)
    results.append(
        _counted(
            "offer_current_single_row_per_offer", context,
            violations=duplicated_offers.count(), frame=duplicated_offers, key=F.col("offer_id"),
        )
    )

    price_daily = marts["offer_price_history_daily"]
    counted = price_daily.agg(F.coalesce(F.sum("observation_count"), F.lit(0)).alias("_rows")).collect()[0]["_rows"]
    difference = abs(int(counted) - silver_rows)
    results.append(
        _result(
            "gold_daily_row_count_reconciles", context,
            status=PASS if difference == 0 else FAIL, observed_value=float(difference),
            keys=() if difference == 0 else (f"gold={int(counted)}", f"silver={silver_rows}"),
        )
    )

    # Recomputed from Silver on purpose. Comparing the mart against itself would
    # pass no matter what the aggregation did.
    recomputed = (
        observations.withColumn("observed_date", F.to_date("observed_at"))
        .groupBy("marketplace", "offer_id", "observed_date", "currency")
        .agg(F.min("current_price").alias("_min"), F.max("current_price").alias("_max"))
    )
    keys = ["marketplace", "offer_id", "observed_date", "currency"]
    joined = price_daily.select(*keys, "min_price", "max_price").join(recomputed, keys, "full_outer")
    divergent = joined.filter(
        ~F.col("min_price").cast("decimal(38,6)").eqNullSafe(F.col("_min").cast("decimal(38,6)"))
        | ~F.col("max_price").cast("decimal(38,6)").eqNullSafe(F.col("_max").cast("decimal(38,6)"))
    )
    results.append(
        _counted(
            "gold_daily_price_aggregates_reconcile", context,
            violations=divergent.count(), frame=divergent,
            key=F.concat_ws("/", F.col("offer_id"), F.col("observed_date")),
        )
    )

    freshness = marts["offer_freshness"]
    stale_rule = (
        ~F.col("as_of").cast("timestamp").eqNullSafe(F.lit(context.as_of).cast("timestamp"))
        | ~F.col("freshness_rule_version").eqNullSafe(F.lit(context.freshness_rule_version))
        | F.col("freshness_status").isNull()
    )
    mislabelled = freshness.filter(stale_rule)
    results.append(
        _counted(
            "freshness_rule_version_and_as_of_applied", context,
            violations=mislabelled.count(), frame=mislabelled, key=F.col("offer_id"),
        )
    )
    return results


def _evaluate_advisory(
    marts: Mapping[str, DataFrame], context: "MarketplaceBatchContext", silver_rows: int
) -> list[QualityResult]:
    results: list[QualityResult] = []

    # Only counters the source actually published can have a good or bad
    # transition. A counter that was never present has a null last_value and
    # every "invalid" reason on it is MISSING_VALUE, so including those rows
    # would report a permanent hundred-percent invalid rate for any marketplace
    # that simply does not expose that counter.
    counters = marts["counter_delta_daily"].filter(F.col("last_value").isNotNull()).agg(
        F.coalesce(F.sum("invalid_transition_count"), F.lit(0)).alias("_invalid"),
        F.coalesce(F.sum("valid_transition_count"), F.lit(0)).alias("_valid"),
    ).collect()[0]
    transitions = int(counters["_invalid"]) + int(counters["_valid"])
    invalid_rate = (int(counters["_invalid"]) / transitions) if transitions else 0.0
    results.append(
        _result(
            "counter_invalid_transition_rate", context,
            status=PASS if invalid_rate <= _ADVISORY_MAJORITY else FAIL,
            observed_value=invalid_rate,
            keys=() if invalid_rate <= _ADVISORY_MAJORITY else (f"invalid={int(counters['_invalid'])}", f"transitions={transitions}"),
        )
    )

    anomaly = marts["price_anomaly_daily"].agg(
        F.count(F.lit(1)).alias("_rows"),
        _violations(~F.col("anomaly_status").startswith("INSUFFICIENT")).alias("_evaluated"),
    ).collect()[0]
    rows = int(anomaly["_rows"])
    coverage = (int(anomaly["_evaluated"]) / rows) if rows else 0.0
    # Zero coverage over a non-empty mart means the window or the minimum
    # sample size does not fit this data at all, which is worth saying out loud
    # even though it blocks nothing.
    results.append(
        _result(
            "price_anomaly_evaluation_coverage", context,
            status=FAIL if rows and coverage == 0.0 else PASS,
            observed_value=coverage,
            keys=() if not (rows and coverage == 0.0) else (f"offer_days={rows}",),
        )
    )

    coverage_mart = marts["source_coverage_daily"]
    missing_rate = coverage_mart.filter((F.col("eligible_offer_count") > 0) & F.col("coverage_rate").isNull())
    results.append(
        _counted(
            "source_coverage_rate_denominator_present", context,
            violations=missing_rate.count(), frame=missing_rate,
            key=F.concat_ws("/", F.col("marketplace"), F.col("observed_date")),
        )
    )

    reliability_rows = marts["crawl_reliability_daily"].count()
    absent = silver_rows > 0 and reliability_rows == 0
    results.append(
        _result(
            "crawl_reliability_evidence_present", context,
            status=FAIL if absent else PASS, observed_value=float(reliability_rows),
            keys=() if not absent else (f"silver_rows={silver_rows}",),
        )
    )
    return results


def evaluate_quality_gates(
    observations: DataFrame,
    marts: Mapping[str, DataFrame],
    attempts: DataFrame,
    runs: DataFrame,
    context: "MarketplaceBatchContext",
) -> tuple[QualityResult, ...]:
    """Run every registered rule and return exactly one result for each.

    ``runs`` is accepted for symmetry with the mart builders and the plan's
    signature; the reconciliation reads its evidence from ``attempts``, which
    is where parsed counts live.
    """
    required = {rule.dataset_name for rule in QUALITY_RULES} - {"silver", "audit"}
    absent = required - set(marts)
    if absent:
        raise ValueError(f"quality evaluation needs these marts: {sorted(absent)}")

    results, silver_rows = _evaluate_silver(observations, context)
    results.append(_evaluate_audit_reconciliation(observations, attempts, context))
    results.extend(_evaluate_gold(observations, marts, context, silver_rows))
    results.extend(_evaluate_advisory(marts, context, silver_rows))

    produced = {result.check_name for result in results}
    expected = {rule.check_name for rule in QUALITY_RULES}
    if produced != expected:
        raise ValueError(f"quality evaluation produced the wrong check set: {sorted(produced ^ expected)}")
    order = {rule.check_name: index for index, rule in enumerate(QUALITY_RULES)}
    return tuple(sorted(results, key=lambda result: order[result.check_name]))


def decide(results: Sequence[QualityResult], context: "MarketplaceBatchContext") -> QualityDecision:
    """Turn results into the one decision that gates the manifest and the cache.

    A mandatory check that is missing or SKIPPED counts as a failure. Treating
    either as a pass would let an unavailable dependency publish a run that was
    never actually checked.
    """
    by_name = {result.check_name: result for result in results}
    mandatory = mandatory_rule_names()
    failures = 0
    for name in mandatory:
        result = by_name.get(name)
        if result is None or result.status != PASS:
            failures += 1
    advisory_failures = sum(
        1 for result in results if result.severity == ADVISORY and result.status == FAIL
    )
    return QualityDecision(
        run_id=context.run_id,
        passed=failures == 0,
        rule_version=context.quality_rule_version,
        evaluated_at=context.as_of,
        mandatory_total=len(mandatory),
        mandatory_failures=failures,
        advisory_failures=advisory_failures,
        skipped=sum(1 for result in results if result.status == SKIPPED),
    )
