"""Benchmark summaries and the report — Phase 9 plan section 8 (tests 13-14)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from ops import bench

T0 = datetime(2026, 10, 4, 14, tzinfo=timezone.utc)


def _attempt(second, *, status="SUCCEEDED", latency=100, parsed=40, raw=1000, duration=1):
    return {"status": status, "started_at": T0 + timedelta(seconds=second),
            "completed_at": T0 + timedelta(seconds=second + duration), "latency_ms": latency,
            "parsed_count": parsed, "raw_bytes": raw}


def test_percentile_is_nearest_rank_and_ignores_missing_values():
    assert bench.percentile([5, 1, 4, 2, 3], 0.5) == 3
    assert bench.percentile([5, 1, 4, 2, 3, None], 0.95) == 5
    assert bench.percentile([], 0.5) is None


def test_spread_reports_median_min_max_over_runs():
    assert bench.spread([3, None, 1, 2]) == {"median": 2, "min": 1, "max": 3, "runs": 3}
    assert bench.spread([None]) == {"median": None, "min": None, "max": None, "runs": 0}


def test_runs_are_summarised_metric_by_metric_numbers_only():
    runs = [{"metrics": {"rps": 10.0, "status": "SUCCEEDED"}}, {"metrics": {"rps": 30.0, "status": "SUCCEEDED"}},
            {"metrics": {"rps": 20.0, "status": "QUALITY_FAILED"}}]

    assert bench.summarise_runs(runs) == {"rps": {"median": 20.0, "min": 10.0, "max": 30.0, "runs": 3}}


def test_crawl_metrics_measure_from_first_start_to_last_completion():
    attempts = [_attempt(0, latency=100), _attempt(4, latency=300), _attempt(9, latency=200, status="FAILED", parsed=0)]

    m = bench.crawl_metrics(attempts)

    assert m["seconds"] == 10 and m["requests"] == 3 and m["completed"] == 2
    assert m["requests_per_second"] == pytest.approx(0.3)
    assert m["parsed_per_second"] == pytest.approx(8.0)
    assert (m["latency_p50_ms"], m["latency_p95_ms"]) == (200, 300)


def test_a_crawl_run_without_attempts_fails_rather_than_reporting_zero():
    with pytest.raises(bench.BenchFailed):
        bench.crawl_metrics([])


def test_silver_latency_states_its_one_second_resolution():
    pairs = [(T0 + timedelta(seconds=s), T0) for s in (1, 2, 3, 10)]

    m = bench.silver_latency_metrics(pairs)

    assert (m["silver_latency_p50_ms"], m["silver_latency_p95_ms"], m["silver_latency_max_ms"]) == (2000, 10000, 10000)
    assert m["silver_latency_resolution_ms"] == 1000 and m["silver_latency_samples"] == 4


def test_speed_metrics_count_busy_time_and_do_not_average_percentiles():
    batches = [
        {"input_rows": 100, "started_at": T0, "completed_at": T0 + timedelta(seconds=2),
         "latency_p50_ms": 1000, "latency_p95_ms": 3000, "latency_max_ms": 3500},
        {"input_rows": 300, "started_at": T0 + timedelta(seconds=30), "completed_at": T0 + timedelta(seconds=40),
         "latency_p50_ms": 5000, "latency_p95_ms": 9000, "latency_max_ms": 9500},
        {"input_rows": 0, "started_at": T0 + timedelta(seconds=60), "completed_at": T0 + timedelta(seconds=61),
         "latency_p50_ms": None, "latency_p95_ms": None, "latency_max_ms": None},
    ]
    progress = [{"num_input_rows": 100, "trigger_execution_ms": 2000}, {"num_input_rows": 300, "trigger_execution_ms": 10000},
                {"num_input_rows": 0, "trigger_execution_ms": 50}]

    m = bench.speed_metrics(batches, progress, sent=400, send_seconds=20, drain_seconds=12.5)

    assert m["input_rows"] == 400 and m["batches_with_rows"] == 2
    assert m["processed_rows_per_second"] == pytest.approx(10.0)     # 400 rows over 40 busy seconds
    assert m["offered_rate"] == 20
    assert (m["batch_duration_p50_ms"], m["batch_duration_p95_ms"]) == (2000, 10000)
    assert m["latency_batch_p50_median_ms"] == 3000
    assert (m["latency_batch_p95_max_ms"], m["latency_max_ms"]) == (9000, 9500)
    assert m["kept_up"] is True


def test_a_speed_step_that_never_drained_did_not_keep_up():
    m = bench.speed_metrics([], [], sent=10, send_seconds=1, drain_seconds=None)

    assert m["kept_up"] is False and m["processed_rows_per_second"] is None


def test_docker_stats_lines_are_parsed_in_mebibytes():
    line = json.dumps({"Name": "bench-speed", "CPUPerc": "143.25%", "MemUsage": "1.5GiB / 15.6GiB"})

    assert bench.parse_stats_line(line) == {"name": "bench-speed", "cpu_pct": 143.25, "mem_mib": 1536.0}
    assert bench.parse_stats_line("not json") is None


def test_resources_are_summarised_per_container():
    samples = [{"name": "bench-kafka", "cpu_pct": 10.0, "mem_mib": 500.0},
               {"name": "bench-kafka", "cpu_pct": 30.0, "mem_mib": 700.0}]

    assert bench.resource_summary(samples) == {
        "bench-kafka": {"cpu_mean_pct": 20.0, "cpu_max_pct": 30.0, "mem_max_mib": 700.0, "samples": 2}}


def _result(scenario="ingest", dataset="replayed_fixture", variant="n=10000"):
    return {"scenario": scenario, "dataset": dataset, "variant": variant,
            "summary": {"silver_records_per_second": {"median": 812.5, "min": 790.0, "max": 840.1, "runs": 3}}}


def test_the_report_carries_the_dataset_label_on_every_row():
    text = bench.render_report([_result(), _result(variant="n=50000")])

    rows = [line for line in text.splitlines() if line.startswith("| replayed")]
    assert len(rows) == 2
    assert all(row.startswith("| replayed_fixture |") for row in rows)
    assert "| silver_records_per_second | 812.5 | 790 | 840.1 | 3 |" in text


def test_the_report_refuses_two_dataset_labels_in_one_table():
    with pytest.raises(bench.BenchFailed, match="several dataset labels"):
        bench.render_report([_result(), _result(dataset="live")])


@pytest.mark.parametrize("project,prefix", [("mp-live", "bench-"), ("mp-bench", ""), ("", "")])
def test_a_benchmark_refuses_anything_but_an_isolated_project(monkeypatch, project, prefix):
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", project)
    monkeypatch.setenv("MP_CONTAINER_PREFIX", prefix)

    with pytest.raises(bench.BenchFailed):
        bench.refuse_live()


def test_a_benchmark_runs_in_the_bench_project(monkeypatch):
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "mp-bench")
    monkeypatch.setenv("MP_CONTAINER_PREFIX", "bench-")

    bench.refuse_live()


def test_mp_refuses_a_benchmark_on_the_live_stack():
    from pathlib import Path

    script = (Path(__file__).resolve().parents[1] / "scripts" / "mp.ps1").read_text(encoding="utf-8")
    assert 'Assert-NotLive "bench"' in script


def test_a_partial_page_counts_as_crawled():
    """Found on the bench stack, 2026-10-04: every stub page carries the
    fixture's deliberately invalid row, so every attempt ends PARTIAL. A wait
    for SUCCEEDED attempts never ended, and would have failed `bench all`."""
    attempts = [_attempt(0, status="PARTIAL"), _attempt(2, status="PARTIAL"), _attempt(4, status="FAILED", parsed=0)]

    m = bench.crawl_metrics(attempts)

    assert (m["completed"], m["partial"], m["failed"]) == (2, 2, 1)
    assert "PARTIAL" in bench.CRAWLED_SQL and "SUCCEEDED" in bench.CRAWLED_SQL


# -- speed-cost and speed-soak ----------------------------------------------

def _cost_batch(rows, duration, *, add=None, stages=(10, None, 5, 20, 3)):
    clients, collect, kafka, es, redis = stages
    add = duration - 100 if add is None else add
    return {"records": rows, "trigger_execution_ms": duration, "add_batch_ms": add, "query_planning_ms": 30,
            "latest_offset_ms": 10, "get_batch_ms": 1, "wal_commit_ms": 20, "commit_offsets_ms": 15,
            "state_rows_total": 1000, "stage_clients_ms": clients,
            "stage_collect_ms": add - 60 if collect is None else collect,
            "stage_kafka_ms": kafka, "stage_es_ms": es, "stage_redis_ms": redis}


def test_a_line_is_fitted_exactly_through_points_on_it():
    fit = bench.fit_line([(0, 1000), (100, 1200), (1000, 3000)])

    assert fit["fixed"] == pytest.approx(1000) and fit["per_unit"] == pytest.approx(2)
    assert fit["r2"] == pytest.approx(1) and fit["points"] == 3


def test_a_line_needs_two_distinct_x_values():
    assert bench.fit_line([(10, 5), (10, 7)])["per_unit"] is None


def test_a_cost_step_reports_duration_and_where_it_went():
    step = bench.cost_step_metrics([_cost_batch(100, 1200), _cost_batch(100, 1400), _cost_batch(0, 50)], size=100)

    assert step["batches"] == 2                       # the empty batch is not a cost of rows
    assert (step["duration_p50_ms"], step["duration_max_ms"]) == (1200, 1400)
    assert step["rows_per_second"] == pytest.approx(200 / 2.6)
    assert step["overhead_p50_ms"] == 100             # trigger minus addBatch
    assert step["collect_ms_p50"] == 1040
    assert step["audit_other_ms_p50"] == 1100 - (10 + 1040 + 5 + 20 + 3)


def test_a_cost_step_without_rows_fails_rather_than_reporting_zero():
    with pytest.raises(bench.BenchFailed):
        bench.cost_step_metrics([_cost_batch(0, 50)], size=10)


def test_capacity_at_a_trigger_solves_fixed_plus_rows_within_it():
    assert bench.capacity_at_trigger(1000, 2, 30) == pytest.approx(14500 / 30)
    assert bench.capacity_at_trigger(5000, 2, 2) == 0
    assert bench.capacity_at_trigger(None, 2, 30) is None


def test_cost_metrics_fit_every_batch_and_flatten_each_step():
    batches = [_cost_batch(k, 1000 + 2 * k) for k in (1, 10, 100, 1000) for _ in range(2)]
    steps = [bench.cost_step_metrics([b for b in batches if b["records"] == k], size=k) for k in (1, 10, 100, 1000)]

    m = bench.cost_metrics(steps, batches)

    assert m["fixed_cost_ms"] == pytest.approx(1000) and m["per_row_cost_ms"] == pytest.approx(2)
    assert m["capacity_at_trigger_30s_rows_per_second"] == pytest.approx(14500 / 30)
    assert m["saturated_rows_per_second"] == pytest.approx(1000 / 3)
    assert m["k1000_duration_p50_ms"] == 3000
    assert all(isinstance(v, (int, float)) for k, v in m.items() if k.startswith("k") and v is not None)


@pytest.mark.parametrize("text,seconds", [("30 seconds", 30), ("0 seconds", 0), ("500 milliseconds", 0.5),
                                          ("1 minute", 60), ("2 second", 2)])
def test_a_trigger_is_read_in_seconds(text, seconds):
    assert bench.trigger_seconds(text) == seconds


def test_an_unknown_trigger_unit_is_refused():
    with pytest.raises(ValueError):
        bench.trigger_seconds("3 hours")


def _soak(durations_by_window, mem_by_window, *, drain=20.0, failed=0, window=300):
    progress, batches, resources = [], [], []
    for w, durations in enumerate(durations_by_window):
        for i, d in enumerate(durations):
            at = T0 + timedelta(seconds=w * window + 30 * i)
            progress.append({"progress_at": at, "num_input_rows": 100, "trigger_execution_ms": d})
            batches.append({"input_rows": 100, "completed_at": at + timedelta(milliseconds=d),
                            "latency_p50_ms": d + 15000, "latency_p95_ms": d + 28000})
    for w, mem in enumerate(mem_by_window):
        resources.append({"name": "bench-speed", "mem_mib": mem, "at": T0 + timedelta(seconds=w * window + 1)})
    return bench.soak_metrics(batches, progress, resources, start=T0, window_seconds=window, trigger_seconds=30,
                              sent=1000, send_seconds=10, drain_seconds=drain, failed_batches=failed,
                              speed_container="bench-speed")


def test_a_flat_soak_is_stable():
    m = _soak([[4000, 4200]] * 4, [1400, 1420, 1410, 1300])

    assert m["stable"] is True and len(m["windows"]) == 4
    assert m["duration_p50_drift"] == pytest.approx(0) and m["speed_mem_drift"] == pytest.approx(1410 / 1400 - 1)
    assert m["batches_over_trigger"] == 0


def test_a_soak_whose_batches_slow_down_is_not_stable():
    m = _soak([[4000], [6000], [9000], [9000]], [1400] * 4)

    assert m["stable"] is False and m["duration_p50_drift"] == pytest.approx(1.25)


def test_a_soak_that_did_not_drain_or_failed_a_batch_is_not_stable():
    assert _soak([[4000]] * 3, [1400] * 3, drain=None)["stable"] is False
    assert _soak([[4000]] * 3, [1400] * 3, failed=1)["stable"] is False


def test_a_soak_counts_batches_longer_than_its_trigger():
    assert _soak([[31000, 4000]] * 3, [1400] * 3)["batches_over_trigger"] == 3


def test_the_new_scenarios_stay_out_of_all():
    assert "speed-cost" not in bench.SCENARIOS and "speed-soak" not in bench.SCENARIOS
    assert {"speed-cost", "speed-soak"} <= set(bench.RUNNERS)
