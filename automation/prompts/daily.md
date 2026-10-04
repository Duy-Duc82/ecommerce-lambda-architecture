TASK: the daily check of the live collection.

Every day:
1. `.\scripts\mp.ps1 -EnvFile env/live.env status`. Today's batch is `mp-<YYYYMMDD UTC>T0000Z`. If it is not terminal yet, wait (max 60 min). Then `.\scripts\mp.ps1 -EnvFile env/live.env validate`.
2. If the batch is QUALITY_FAILED or FAILED: read audit.marketplace_quality_result (and the batch audit row) and explain which rule failed and why. Do not fix anything.
3. Run `evaluate reliability`, `evaluate freshness`, `evaluate storage` with env/live.env. Report the key numbers and the day-over-day change versus the previous report in C:\code\data-reports\reports (if any).
4. Check container health (all 10 mp-live services up/healthy), disk free on C:, and Docker VM memory if cheap to get. Report crawl attempts in the last 24h by status.
5. If the benchmark (data/ops/bench-all.log, UTF-16) finished but PROGRESS.md has no section recording it yet, do the benchmark recording described in the bench-check procedure: bench report, new PROGRESS section, local docs commit.

Only on 2026-10-05 (WP0 acceptance, PROGRESS §26.6 step 1-2):
- Accept or reject WP0: batch mp-20261005T0000Z SUCCEEDED, pointer set, validate passes.
- If the benchmark is recorded, run `.\scripts\mp.ps1 -EnvFile env/live.env evidence` and explain every item still MISSING (should only be things not yet due).
- Add a PROGRESS.md section with the results, update the "resume ở đây" section, update the memory file phase-8-wp7-queued.md, commit locally on phase-9-evaluation-plan (docs only).

On 2026-11-03 or later (30 days of collection reached):
- Run all evaluate commands and `evidence`, compare with the first days, and put a "30-day summary" section at the top of the report. Add the same to PROGRESS.md and commit locally. Say clearly that the user should now do the integration run and the feature-freeze tag.

Then write and push the report as described above.
