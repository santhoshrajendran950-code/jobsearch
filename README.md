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
- **`docs/new-postings.md`** — one dated section per morning with the newly published DS/ML roles (newest first)
- **`docs/top-100.md`** — the 100 best-fit DS/ML roles that are still live on the boards, rebuilt every morning
- **`docs/ai-fde-new-postings.md`** / **`docs/ai-fde-top-100.md`** — the same two files for the second track

## Two tracks

Every title is routed to one track by regex (`track_of()`), scored with that track's rubric in `profile.md`,
and written to that track's docs. The daily email carries both, in separate sections.

- `ds` — Data Scientist / ML Engineer / Applied Scientist / decision, marketing, growth science.
- `ai` — Forward Deployed Engineer, Deployment Strategist, AI Engineer, Applied AI, AI Solutions Engineer.
  Pure SWE titles (full-stack, backend, platform, infra, MTS), research, sales engineering and
  domain-expert FDE roles (banker, investor, lawyer) are filtered out; the scorer excludes what slips through.

`--track ds|ai` restricts a run to one track (used for the one-off AI backfill on 2026-10-16).

## Marking roles

Edit `docs/top-100.md` on github.com and type into the **Status** column (`applied`, `interested`,
`skip`, anything you like), then commit. The morning run does `git pull` first, stores each mark on
the job in `seen_jobs.json`, and regenerates the table with your marks kept. A mark starting with
`skip`, `pass`, `reject`, `no` or `closed` moves the row to the collapsed Skipped list at the bottom;
change the cell there to bring it back. Rows disappear on their own once the posting is taken down.

Each morning run ends with `git commit` + `git push` of everything that changed (docs, state, run
outputs, newly discovered companies). If you edit on GitHub while the run is in progress the push
fails and is retried by the next day's pull/rebase.

Setup: `python3 -m venv .venv && .venv/bin/pip install requests pyyaml pypdf`.
