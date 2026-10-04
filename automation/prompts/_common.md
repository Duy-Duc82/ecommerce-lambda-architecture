You are running UNATTENDED from Windows Task Scheduler. The user (thesis student, writes Vietnamese) is away and authorized this job on 2026-10-04. Nobody will answer questions: decide sensibly, never ask.

Context: repo C:\code\data (branch phase-9-evaluation-plan). Read docs/PROGRESS.md (latest sections, especially the "resume ở đây" section) and your memory before acting. The live Tiki collection runs in Compose project `mp-live`; always use `.\scripts\mp.ps1 -EnvFile env/live.env ...` for it and `-EnvFile env/bench.env` for the benchmark stack. The daily batch runs at 00:00 UTC (07:00 local).

Hard rules:
- Do NOT merge, tag, open PRs, run the full demo, change code, or modify/delete live data. Do not run `down` on mp-live. If something is broken, diagnose and report; do not fix.
- Pushing is allowed ONLY for branch `daily-reports`, from the worktree C:\code\data-reports. Never push any other branch.
- Local commits on phase-9-evaluation-plan are allowed only for docs (PROGRESS.md) when the task says so.
- Commit messages: conventional style like the repo's history, NO Co-Authored-By line.
- The GitHub repo is PUBLIC: never put secrets, passwords, tokens or .env values into a report.
- To wait, use a PowerShell loop with Start-Sleep (e.g. check every 5 minutes), never longer than 60 minutes in total.

Report (always write one, even if everything failed):
- File: C:\code\data-reports\reports\<YYYY-MM-DD local date>.md. If it already exists, append a new section with the time instead of overwriting.
- Written in Vietnamese, same plain style as docs/PROGRESS.md. Sections: Tóm tắt (3-5 lines, verdict first: OK / CẦN XEM / LỖI), Trạng thái mp-live, Batch hôm nay, Số liệu (tables with the real numbers you measured, with units and measurement conditions), Bất thường, Việc cần user quyết.
- Also update C:\code\data-reports\README.md: keep a newest-first list of reports with a one-line verdict each.
- Then in C:\code\data-reports: `git add -A reports README.md`, commit "report: <date> <short verdict>", `git pull --rebase origin daily-reports` (ignore if the remote branch does not exist yet), `git push -u origin daily-reports`. If push fails, say so in the final output.
- Finish with a short Vietnamese summary as your final output (it goes to the log).
