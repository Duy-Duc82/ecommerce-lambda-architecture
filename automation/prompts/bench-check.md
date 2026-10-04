TASK: check the detached benchmark `bench all --repeat 3` (started 2026-10-04 14:48 UTC).

The log data/ops/bench-all.log is UTF-16 (read with `iconv -f UTF-16 -t UTF-8`). Results are in data/ops/bench/*.json.

1. If the log contains `exit=`:
   - Run `.\scripts\mp.ps1 -EnvFile env/bench.env bench report`.
   - Evaluate: stability across the 3 repeats (spread), failures, anything odd. Batch on replayed data is always QUALITY_FAILED by design (fake crawl runs are not in the audit); its timings are still valid, say so.
   - Every result has live_stack_running_containers = 10: measured while mp-live was running. State this.
   - Make sure the mp-bench stack is down (`docker ps`); if bench-* containers remain and no ops.bench process runs, bring it down with the commands in PROGRESS §26.6 (set $env:MP_CONTAINER_PREFIX="bench-" first). Never touch mp-live.
   - Add a new PROGRESS.md section (Vietnamese, house style) with the key numbers and conditions, commit locally on phase-9-evaluation-plan ("docs: record the full benchmark run").
   - If exit is non-zero or there is bench_failed: investigate the cause, do NOT rerun everything, record the finding.
2. If not finished: record progress (which scenario/variant/repeat), estimate the finish time, and say the 07:47 daily job will pick it up.
3. Write and push the report as described above.
