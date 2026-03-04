"""
kpi_store.py — Persistent SQLite store for staff KPI events.

Business logic recorded here
─────────────────────────────
• When a lead enters КОНСУЛЬТАЦИЯ with Код сотрудника → record a **consul** event
  for that staff on today's date (idempotent per lead – first entry wins).

• When that lead later moves to ЗАКАЗ → record a **zakas** event for the SAME
  staff on the date the ЗАКАЗ transition happened (may differ from consul date).
  Enforces a 5-day ДУМКА recovery window: if the last ДУМКА was more than
  DUMKA_RECOVERY_DAYS ago the order is NOT credited.

• When that lead moves to ДУМКА → record a **dumka** event on that date.

• A lead may recover: ДУМКА → (re-contact within 5 days) → ЗАКАЗ.
  The ЗАКАЗ is credited on the recovery date, NOT the original consul date.

Tables
──────
lead_consul_log
    One row per lead.  Records who first earned the "Лид" for this lead so
    that subsequent ЗАКАЗ / ДУМКА events can be attributed correctly.

kpi_events
    One row per KPI moment (consul / zakas / dumka).
    Multiple zakas per lead are allowed (recovered from ДУМКА several times).

backfill_log
    Tracks completed backfill runs to prevent re-running on every restart.
"""

from __future__ import annotations

import sqlite3
import threading
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_DUMKA_RECOVERY_DAYS = 5  # leads in ДУМКА can be recovered within this window


class KPIStore:
    """Thread-safe SQLite-backed KPI event store."""

    def __init__(
        self,
        db_path: str | Path = "./data/kpi_events.db",
        tz_offset: float = 5.0,
        dumka_recovery_days: int = _DEFAULT_DUMKA_RECOVERY_DAYS,
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.tz_offset = tz_offset
        self.dumka_recovery_days = dumka_recovery_days
        self._lock = threading.Lock()
        self._init_db()

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.executescript("""
                    CREATE TABLE IF NOT EXISTS lead_consul_log (
                        lead_id       TEXT PRIMARY KEY,
                        staff_code    TEXT NOT NULL,
                        consul_date   TEXT NOT NULL,
                        pipeline_name TEXT NOT NULL DEFAULT '',
                        budget        REAL NOT NULL DEFAULT 0
                    );

                    CREATE TABLE IF NOT EXISTS kpi_events (
                        id            INTEGER PRIMARY KEY AUTOINCREMENT,
                        lead_id       TEXT NOT NULL,
                        staff_code    TEXT NOT NULL,
                        event_type    TEXT NOT NULL,
                        event_date    TEXT NOT NULL,
                        pipeline_name TEXT NOT NULL DEFAULT '',
                        budget        REAL NOT NULL DEFAULT 0,
                        created_at    TEXT NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_kpi_date_type
                        ON kpi_events (event_date, event_type);
                    CREATE INDEX IF NOT EXISTS idx_kpi_lead
                        ON kpi_events (lead_id, event_type);
                    CREATE INDEX IF NOT EXISTS idx_consul_staff
                        ON lead_consul_log (staff_code);

                    CREATE TABLE IF NOT EXISTS backfill_log (
                        id         INTEGER PRIMARY KEY AUTOINCREMENT,
                        date_from  TEXT NOT NULL,
                        date_to    TEXT NOT NULL,
                        completed_at TEXT NOT NULL
                    );
                """)

    def _tz(self) -> timezone:
        return timezone(timedelta(hours=self.tz_offset))

    def _now_str(self) -> str:
        return datetime.now(self._tz()).isoformat(timespec="seconds")

    def _today_str(self) -> str:
        return datetime.now(self._tz()).strftime("%Y-%m-%d")

    def _ts_to_date(self, ts: int) -> str:
        """Convert Unix timestamp to YYYY-MM-DD in configured timezone."""
        return datetime.fromtimestamp(ts, self._tz()).strftime("%Y-%m-%d")

    # ─────────────────────────────────────────────────────────────────────────
    # Write: consul
    # ─────────────────────────────────────────────────────────────────────────

    def record_consul(
        self,
        lead_id: str,
        staff_code: str,
        event_date: str | None = None,
        pipeline_name: str = "",
        budget: float = 0.0,
    ) -> bool:
        """Record a КОНСУЛЬТАЦИЯ event.

        Idempotent: if lead_id already exists in lead_consul_log the call is a
        no-op (returns False).  Returns True when a new record is inserted.
        """
        if not lead_id or not staff_code:
            return False
        event_date = event_date or self._today_str()
        now = self._now_str()
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO lead_consul_log "
                    "(lead_id, staff_code, consul_date, pipeline_name, budget) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (str(lead_id), staff_code, event_date, pipeline_name, budget),
                )
                if cur.rowcount == 0:
                    return False  # already recorded
                conn.execute(
                    "INSERT INTO kpi_events "
                    "(lead_id, staff_code, event_type, event_date, pipeline_name, budget, created_at) "
                    "VALUES (?, ?, 'consul', ?, ?, ?, ?)",
                    (str(lead_id), staff_code, event_date, pipeline_name, budget, now),
                )
                return True

    # ─────────────────────────────────────────────────────────────────────────
    # Write: zakas
    # ─────────────────────────────────────────────────────────────────────────

    def record_zakas(
        self,
        lead_id: str,
        event_date: str | None = None,
        budget: float = 0.0,
        pipeline_name: str = "",
    ) -> bool:
        """Record a ЗАКАЗ event.

        Deduplication rules
        ───────────────────
        1. No prior zakas → allow (first sale).
        2. Prior zakas exists, NO ДУМКА after it → block.  The lead is simply
           moving through downstream stages (В процессе → У курера → Успешно);
           this is NOT a new sale.
        3. Prior zakas exists AND a ДУМКА occurred after it → ДУМКА recovery
           case.  Allow only if the gap between that ДУМКА and event_date is
           within ``dumka_recovery_days``.
        """
        if not lead_id:
            return False
        event_date = event_date or self._today_str()
        with self._lock:
            with self._connect() as conn:
                consul_row = conn.execute(
                    "SELECT staff_code, pipeline_name FROM lead_consul_log WHERE lead_id = ?",
                    (str(lead_id),),
                ).fetchone()
                if not consul_row:
                    return False

                staff_code   = consul_row["staff_code"]
                pipeline_use = pipeline_name or consul_row["pipeline_name"]

                # Check whether this lead already has a zakas event
                last_zakas = conn.execute(
                    "SELECT event_date FROM kpi_events "
                    "WHERE lead_id = ? AND event_type = 'zakas' "
                    "ORDER BY event_date DESC LIMIT 1",
                    (str(lead_id),),
                ).fetchone()

                if last_zakas:
                    # Rule 2 / 3: look for a ДУМКА that occurred AFTER the last zakas
                    dumka_after = conn.execute(
                        "SELECT event_date FROM kpi_events "
                        "WHERE lead_id = ? AND event_type = 'dumka' "
                        "AND event_date >= ? "
                        "ORDER BY event_date DESC LIMIT 1",
                        (str(lead_id), last_zakas["event_date"]),
                    ).fetchone()
                    if not dumka_after:
                        # No ДУМКА since the last zakas → downstream stage hop, not a new sale
                        return False
                    # ДУМКА recovery path — enforce window
                    dumka_date = datetime.strptime(dumka_after["event_date"], "%Y-%m-%d").date()
                    order_date = datetime.strptime(event_date, "%Y-%m-%d").date()
                    days_since = (order_date - dumka_date).days
                    if days_since > self.dumka_recovery_days:
                        return False  # beyond recovery window

                now = self._now_str()
                conn.execute(
                    "INSERT INTO kpi_events "
                    "(lead_id, staff_code, event_type, event_date, pipeline_name, budget, created_at) "
                    "VALUES (?, ?, 'zakas', ?, ?, ?, ?)",
                    (str(lead_id), staff_code, event_date, pipeline_use, budget, now),
                )
                return True

    # ─────────────────────────────────────────────────────────────────────────
    # Write: dumka
    # ─────────────────────────────────────────────────────────────────────────

    def record_dumka(
        self,
        lead_id: str,
        event_date: str | None = None,
        pipeline_name: str = "",
    ) -> bool:
        """Record a ДУМКА event.

        Only records if we have a consul entry for this lead (so we know who to
        attribute the ДУМКА to).  Duplicate ДУМКА on the same lead+date is
        silently ignored.
        """
        if not lead_id:
            return False
        event_date = event_date or self._today_str()
        with self._lock:
            with self._connect() as conn:
                consul_row = conn.execute(
                    "SELECT staff_code, pipeline_name FROM lead_consul_log WHERE lead_id = ?",
                    (str(lead_id),),
                ).fetchone()
                if not consul_row:
                    return False

                staff_code   = consul_row["staff_code"]
                pipeline_use = pipeline_name or consul_row["pipeline_name"]

                # Deduplicate: same lead+date
                existing = conn.execute(
                    "SELECT id FROM kpi_events "
                    "WHERE lead_id = ? AND event_type = 'dumka' AND event_date = ?",
                    (str(lead_id), event_date),
                ).fetchone()
                if existing:
                    return False

                now = self._now_str()
                conn.execute(
                    "INSERT INTO kpi_events "
                    "(lead_id, staff_code, event_type, event_date, pipeline_name, budget, created_at) "
                    "VALUES (?, ?, 'dumka', ?, ?, 0, ?)",
                    (str(lead_id), staff_code, event_date, pipeline_use, now),
                )
                return True

    # ─────────────────────────────────────────────────────────────────────────
    # Read helpers
    # ─────────────────────────────────────────────────────────────────────────

    def has_consul(self, lead_id: str) -> bool:
        """Return True if a consul record exists for this lead."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM lead_consul_log WHERE lead_id = ?", (str(lead_id),)
            ).fetchone()
        return row is not None

    def get_lead_consul(self, lead_id: str) -> Optional[Dict[str, Any]]:
        """Return the consul log row for a lead, or None."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM lead_consul_log WHERE lead_id = ?", (str(lead_id),)
            ).fetchone()
        return dict(row) if row else None

    # ─────────────────────────────────────────────────────────────────────────
    # Aggregated stats (used by dashboard)
    # ─────────────────────────────────────────────────────────────────────────

    def get_staff_stats(self, date_from: str, date_to: str) -> List[Dict[str, Any]]:
        """Per-staff KPI aggregation for the given date range.

        Each event is counted on the date it *happened*, which means:
          • consul counts on the date the lead hit КОНСУЛЬТАЦИЯ
          • zakas  counts on the date the lead hit ЗАКАЗ (may be a later date)
          • dumka  counts on the date the lead hit ДУМКА

        Returns list of dicts: {staff_code, consul, zakas, dumka, summa}
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    staff_code,
                    SUM(CASE WHEN event_type = 'consul' THEN 1      ELSE 0   END) AS consul,
                    SUM(CASE WHEN event_type = 'zakas'  THEN 1      ELSE 0   END) AS zakas,
                    SUM(CASE WHEN event_type = 'dumka'  THEN 1      ELSE 0   END) AS dumka,
                    SUM(CASE WHEN event_type = 'zakas'  THEN budget ELSE 0.0 END) AS summa
                FROM kpi_events
                WHERE event_date BETWEEN ? AND ?
                GROUP BY staff_code
                """,
                (date_from, date_to),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_monthly_stats(self, month: str) -> List[Dict[str, Any]]:
        """Per-staff KPI for an entire month.  month = 'YYYY-MM'."""
        date_from = f"{month}-01"
        y, m = int(month[:4]), int(month[5:7])
        if m == 12:
            last_day = date(y + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = date(y, m + 1, 1) - timedelta(days=1)
        return self.get_staff_stats(date_from, last_day.strftime("%Y-%m-%d"))

    def get_daily_breakdown(self, date_from: str, date_to: str) -> List[Dict[str, Any]]:
        """Per-staff per-day KPI breakdown (for monthly report with daily detail)."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    event_date,
                    staff_code,
                    SUM(CASE WHEN event_type = 'consul' THEN 1      ELSE 0   END) AS consul,
                    SUM(CASE WHEN event_type = 'zakas'  THEN 1      ELSE 0   END) AS zakas,
                    SUM(CASE WHEN event_type = 'dumka'  THEN 1      ELSE 0   END) AS dumka,
                    SUM(CASE WHEN event_type = 'zakas'  THEN budget ELSE 0.0 END) AS summa
                FROM kpi_events
                WHERE event_date BETWEEN ? AND ?
                GROUP BY event_date, staff_code
                ORDER BY event_date, staff_code
                """,
                (date_from, date_to),
            ).fetchall()
        return [dict(r) for r in rows]

    # ─────────────────────────────────────────────────────────────────────────
    # Backfill tracking
    # ─────────────────────────────────────────────────────────────────────────

    def is_backfill_done(self, date_from: str) -> bool:
        """Return True if a backfill starting from date_from was already completed."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM backfill_log WHERE date_from <= ? LIMIT 1",
                (date_from,),
            ).fetchone()
        return row is not None

    def mark_backfill_done(self, date_from: str, date_to: str) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    "INSERT INTO backfill_log (date_from, date_to, completed_at) VALUES (?, ?, ?)",
                    (date_from, date_to, self._now_str()),
                )

    def clear_all_data(self) -> None:
        """Delete ALL KPI events, consul log, and backfill records.

        Call this before a full re-backfill to start from a clean state.
        Table schemas are preserved; only row data is wiped.
        """
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM kpi_events")
                conn.execute("DELETE FROM lead_consul_log")
                conn.execute("DELETE FROM backfill_log")
        print("[KPI] clear_all_data: all tables wiped")

    # ─────────────────────────────────────────────────────────────────────────
    # Back-fill from AMO Events API
    # ─────────────────────────────────────────────────────────────────────────

    def backfill_from_amo(
        self,
        amo,
        date_from: str,
        date_to: str,
        consul_status_ids: set,
        zakas_status_ids: set,
        dumka_status_ids: set,
        sotuv_pipeline_ids: set,
    ) -> Dict[str, int]:
        """Replay AMO events and populate the KPI store.

        Steps
        ─────
        1. Fetch all lead_status_changed events in [date_from, date_to] via
           the AMO Events API.
        2. Classify each event as consul / zakas / dumka based on the new
           status ID.
        3. Batch-fetch the leads to get Код сотрудника.
        4. Record events in chronological order so consul is inserted before
           zakas/dumka for the same lead.

        Returns a summary: {consul, zakas, dumka, skipped, errors}
        """
        import time as _time

        def _ts_to_date(ts: int) -> str:
            return datetime.fromtimestamp(ts, self._tz()).strftime("%Y-%m-%d")

        try:
            ts_from = int(datetime.strptime(date_from, "%Y-%m-%d").timestamp())
            ts_to   = int(datetime.strptime(date_to,   "%Y-%m-%d").timestamp()) + 86399
        except ValueError as exc:
            return {"error": str(exc)}

        print(f"[KPI-BACKFILL] Fetching AMO events {date_from} → {date_to} …")

        # ── Step 1: collect all status-change events ──────────────────────────
        all_events: List[Dict] = []
        page = 1
        while True:
            endpoint = (
                f"/api/v4/events?filter[type][]=lead_status_changed"
                f"&filter[created_at][from]={ts_from}"
                f"&filter[created_at][to]={ts_to}"
                f"&limit=250&page={page}"
            )
            try:
                data = amo.get(endpoint)
            except RuntimeError as exc:
                if "204" in str(exc) or "No Content" in str(exc):
                    break
                print(f"[KPI-BACKFILL] Events API error: {exc}")
                break
            batch = (data.get("_embedded") or {}).get("events") or []
            if not batch:
                break
            all_events.extend(batch)
            if not (data.get("_links") or {}).get("next"):
                break
            page += 1
            _time.sleep(0.1)

        print(f"[KPI-BACKFILL] {len(all_events)} total events — classifying …")

        # ── Step 2: classify events per lead ──────────────────────────────────
        # lead_id → [(event_type, event_date_str, event_ts)]
        lead_events: Dict[int, list] = defaultdict(list)
        for ev in all_events:
            lead_id = int(ev.get("entity_id", 0) or 0)
            if not lead_id:
                continue
            after   = (ev.get("value_after") or [{}])[0]
            new_sid = int((after.get("lead_status") or {}).get("id", 0) or 0)
            ev_ts   = int(ev.get("created_at", 0) or 0)
            ev_date = _ts_to_date(ev_ts)

            if new_sid in consul_status_ids:
                lead_events[lead_id].append(("consul", ev_date, ev_ts))
            elif new_sid in zakas_status_ids:
                lead_events[lead_id].append(("zakas", ev_date, ev_ts))
            elif new_sid in dumka_status_ids:
                lead_events[lead_id].append(("dumka", ev_date, ev_ts))

        unique_lead_ids = list(lead_events.keys())
        print(f"[KPI-BACKFILL] {len(unique_lead_ids)} unique leads with KPI events.")

        if not unique_lead_ids:
            return {"consul": 0, "zakas": 0, "dumka": 0, "skipped": 0, "errors": 0}

        # ── Step 3: batch-fetch leads (Код сотрудника) ────────────────────────
        lead_details: Dict[int, Dict] = {}
        BATCH = 50
        for i in range(0, len(unique_lead_ids), BATCH):
            batch_ids = unique_lead_ids[i : i + BATCH]
            ids_param = "&".join(f"filter[id][]={lid}" for lid in batch_ids)
            try:
                data = amo.get(f"/api/v4/leads?{ids_param}&limit={BATCH}")
                for lead in (data.get("_embedded") or {}).get("leads") or []:
                    lead_details[int(lead["id"])] = lead
            except Exception as exc:
                print(f"[KPI-BACKFILL] Batch lead fetch error: {exc}")
            _time.sleep(0.12)

        # ── Step 4: record events ──────────────────────────────────────────────
        counts: Dict[str, int] = {
            "consul": 0, "zakas": 0, "dumka": 0, "skipped": 0, "errors": 0
        }

        for lead_id, events in lead_events.items():
            lead = lead_details.get(lead_id)
            if not lead:
                counts["skipped"] += 1
                continue

            # Pipeline filter
            pipeline_id = int(lead.get("pipeline_id", 0) or 0)
            if sotuv_pipeline_ids and pipeline_id not in sotuv_pipeline_ids:
                counts["skipped"] += 1
                continue

            # Extract Код сотрудника
            staff_code = ""
            for cf in (lead.get("custom_fields_values") or []):
                fname = " ".join((cf.get("field_name") or "").split())
                if fname == "Код сотрудника":
                    vals = cf.get("values") or []
                    if vals:
                        staff_code = str(vals[0].get("value", "")).strip()
                    break
            if not staff_code:
                counts["skipped"] += 1
                continue
            try:
                staff_code = str(int(staff_code))
            except ValueError:
                counts["skipped"] += 1
                continue

            budget        = float(lead.get("price", 0) or 0)
            pipeline_name = ""

            # Sort chronologically so consul is recorded before zakas/dumka
            events.sort(key=lambda x: x[2])

            for event_type, ev_date, _ in events:
                try:
                    if event_type == "consul":
                        ok = self.record_consul(str(lead_id), staff_code, ev_date, pipeline_name, budget)
                        if ok:
                            counts["consul"] += 1

                    elif event_type == "zakas":
                        # If the consul event is outside the backfill window, synthesise one
                        # so the zakas can be attributed to the correct staff member.
                        if not self.has_consul(str(lead_id)):
                            self.record_consul(str(lead_id), staff_code, ev_date, pipeline_name, budget)
                        ok = self.record_zakas(str(lead_id), ev_date, budget, pipeline_name)
                        if ok:
                            counts["zakas"] += 1

                    elif event_type == "dumka":
                        if not self.has_consul(str(lead_id)):
                            self.record_consul(str(lead_id), staff_code, ev_date, pipeline_name, budget)
                        ok = self.record_dumka(str(lead_id), ev_date, pipeline_name)
                        if ok:
                            counts["dumka"] += 1

                except Exception as exc:
                    print(f"[KPI-BACKFILL] Error on lead {lead_id} / {event_type}: {exc}")
                    counts["errors"] += 1

        print(f"[KPI-BACKFILL] Complete: {counts}")
        return counts
