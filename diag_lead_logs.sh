#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# diag_lead_logs.sh  —  extract every log line touching a specific lead
#
# Usage:
#   bash diag_lead_logs.sh 33762109
#   bash diag_lead_logs.sh 33762109 /custom/path/to/logs
#
# Output sections (in chronological order per file):
#   1. leads.log   — status transitions, order# changes, tab writes
#   2. webhooks.log — incoming webhook events for this lead
#   3. amo_api.log  — every AMO API call that references this lead
#   4. app.log     — anything else (errors, warnings) for this lead
# ─────────────────────────────────────────────────────────────────────────────

LEAD_ID="${1:?Usage: bash diag_lead_logs.sh <lead_id>}"
LOG_DIR="${2:-logs}"

# Rotate-safe: include .log and .log.1 .. .log.10 backups, sorted oldest-first
gather() {
    local base="$LOG_DIR/$1"
    # collect all rotated backups (highest number = oldest) + current file
    local files=()
    for n in 10 9 8 7 6 5 4 3 2 1; do
        [[ -f "${base}.${n}" ]] && files+=("${base}.${n}")
    done
    [[ -f "${base}" ]] && files+=("${base}")
    if [[ ${#files[@]} -eq 0 ]]; then
        echo "(file not found: ${base})"
        return
    fi
    grep -h --color=never "$LEAD_ID" "${files[@]}" 2>/dev/null
}

hr() { printf '\n%s\n' "══════════════════════════════════════════════════════"; }

hr; echo "  LEAD: $LEAD_ID"
hr

echo ""
echo "▶ leads.log  (status & order# tracking)"
echo "──────────────────────────────────────────"
gather "leads.log" | sort

echo ""
echo "▶ webhooks.log  (incoming AMO webhook events)"
echo "──────────────────────────────────────────"
gather "webhooks.log" | sort

echo ""
echo "▶ amo_api.log  (outgoing API calls)"
echo "──────────────────────────────────────────"
gather "amo_api.log" | sort

echo ""
echo "▶ app.log  (errors / warnings / everything else)"
echo "──────────────────────────────────────────"
gather "app.log" | sort

hr
echo "  Done. $(date '+%Y-%m-%d %H:%M:%S')"
hr
