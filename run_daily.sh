#!/bin/bash
# Daily NYC DS job digest. Called by launchd (com.santhosh.jobsearch) at 08:00 America/New_York.
#   1. discover: a few web searches for NYC startups not yet in companies.yaml (claude -p + WebSearch)
#   2. fetch/filter: Ashby/Greenhouse/Lever JSON APIs, roles first published in the last 24h, not in seen_jobs.json
#   3. score: claude -p rates each role against profile.md
#   4. send: claude -p + Gmail MCP emails digests/YYYY-MM-DD.html; nothing is sent when there are no new matches
#   5. docs: rewrites docs/new-postings.md + docs/top-100.md (reads your Status marks back first), then git commit + push
# Needs: git on PATH, ~/.ssh/config host alias github-personal (passphrase-less key), claude CLI logged in.
set -u
cd /Users/santhosh/jobsearch
export PATH="/Users/santhosh/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"
export HOME=/Users/santhosh
LOG=log/launchd.log
echo "=== $(date '+%Y-%m-%d %H:%M:%S') daily run start ===" >> "$LOG"
.venv/bin/python jobsearch.py --daily >> "$LOG" 2>&1
rc=$?
echo "=== $(date '+%Y-%m-%d %H:%M:%S') daily run end rc=$rc ===" >> "$LOG"
exit $rc
