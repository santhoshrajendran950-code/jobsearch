# jobsearch

Daily NYC data-science job digest for well-funded startups. Pulls public Ashby /
Greenhouse / Lever job-board JSON APIs for the companies in `companies.yaml`,
filters for NYC in-person/hybrid DS / ML roles, scores fit against `profile.md`
with `claude -p`, renders an HTML digest and emails it via the Gmail MCP tool.

- `jobsearch.py --backfill` — every open matching role (one-time)
- `jobsearch.py --daily` — roles first published in the last 24h; sends nothing if none
- `seen_jobs.json` — roles already emailed (key: company|title|url), updated only after a successful send
- `run_daily.sh` + `com.santhosh.jobsearch.plist` — launchd job, 08:00 America/New_York
- `runs/`, `digests/` — per-run candidates, scored JSON, subject line and rendered HTML

Setup: `python3 -m venv .venv && .venv/bin/pip install requests pyyaml pypdf`.
