"""
dashboard_router.py — Staff KPI Dashboard

Mounted into the main FastAPI app in sync_service.py via:
    app.include_router(create_dashboard_router(service))

Routes:
    GET  /dashboard              → Interactive HTML dashboard page
    GET  /api/dashboard/stats    → JSON KPI data (consumed by the page via fetch())
"""

from __future__ import annotations

import io
import json
import os
import secrets
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, Dict, List

from fastapi import APIRouter, Form, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse

# ── Staff sheet cache (avoid re-fetching on every dashboard refresh) ─────────
_staff_cache: Dict = {"data": None, "ts": 0.0}
_STAFF_CACHE_TTL = 300  # seconds

# ── Stats result cache: keyed by (date_from, date_to) ─────────────────────
# Avoids hammering AMO on every auto-refresh or repeated date query.
# TTL: 60 s when today is in the range (data is live), 300 s for past-only ranges.
_stats_cache: Dict = {}  # (date_from, date_to) → {"ts": float, "data": dict}
_leads_cache: Dict = {}  # (date_from, date_to) → {"ts": float, "leads": list}

# ── Session store ─────────────────────────────────────────────────
# token → {"username": str, "created_at": float}
_sessions: Dict[str, Dict] = {}
_SESSION_TTL = int(os.getenv("DASHBOARD_SESSION_TTL", str(8 * 3600)))  # default 8 h


def _load_admins() -> List[Dict[str, str]]:
    """Load admin credentials from env.

    Priority:
    1. DASHBOARD_ADMINS_JSON  = '[{"username":"alice","password":"s3cr3t"}, ...]'
    2. DASHBOARD_ADMIN_USERNAME + DASHBOARD_ADMIN_PASSWORD  (single admin fallback)
    """
    raw = os.getenv("DASHBOARD_ADMINS_JSON", "").strip()
    if raw:
        try:
            admins = json.loads(raw)
            if isinstance(admins, list) and admins:
                return admins
        except Exception:
            pass
    u = os.getenv("DASHBOARD_ADMIN_USERNAME", "admin").strip()
    p = os.getenv("DASHBOARD_ADMIN_PASSWORD", "").strip()
    if p:
        return [{"username": u, "password": p}]
    return []  # no credentials configured — login will always fail


def _check_session(token: str) -> str | None:
    """Return username if token is valid and not expired, else None."""
    if not token:
        return None
    entry = _sessions.get(token)
    if not entry:
        return None
    if time.time() - entry["created_at"] > _SESSION_TTL:
        _sessions.pop(token, None)
        return None
    return entry["username"]

# ── Status display names that count as a confirmed order for KPI ─────────────
# Includes every stage at or beyond "Заказ" — the order has been placed.
ZAKAS_DISPLAY_NAMES: set[str] = {"Заказ", "В процессе", "У курера", "Успешно"}

# Statuses that count as rejection
OTKAZ_DISPLAY_NAMES: set[str] = {"Отказ", "Закрыто и не реализовано"}

# Statuses that count as consideration (lead is thinking/hesitating)
DUMKA_DISPLAY_NAMES: set[str] = {"Раздумье"}


def _norm(name: str) -> str:
    """Lowercase + collapse whitespace for fuzzy name matching."""
    return " ".join(name.lower().split())


# ─────────────────────────────────────────────────────────────────────────────
# Factory – call once from sync_service.py, passing the live SyncService.
# ─────────────────────────────────────────────────────────────────────────────

def create_dashboard_router(service) -> APIRouter:
    router = APIRouter()

    # ── Auth helpers ──────────────────────────────────────────────────────────
    def _user(request: Request) -> str | None:
        return _check_session(request.cookies.get("dash_token", ""))

    def _login_response(error: str = "") -> HTMLResponse:
        err_block = (
            f'<div class="err" style="margin-bottom:16px">{error}</div>'
            if error else ""
        )
        return HTMLResponse(_LOGIN_HTML.replace("{error_block}", err_block))

    # ── Login / Logout ────────────────────────────────────────────────────────
    @router.get("/login", response_class=HTMLResponse, tags=["auth"])
    def login_page(request: Request):
        if _user(request):
            return RedirectResponse("/dashboard", status_code=302)
        return _login_response()

    @router.post("/login", tags=["auth"])
    def login_submit(
        request: Request,
        username: str = Form(...),
        password: str = Form(...),
    ):
        admins = _load_admins()
        if not admins:
            return _login_response("Нет настроенных аккаунтов. Установите DASHBOARD_ADMIN_PASSWORD в .env")
        match = any(
            a.get("username", "").strip() == username.strip()
            and a.get("password", "") == password
            for a in admins
        )
        if not match:
            return _login_response("Неверный логин или пароль.")
        token = secrets.token_hex(32)
        _sessions[token] = {"username": username.strip(), "created_at": time.time()}
        resp = RedirectResponse("/dashboard", status_code=302)
        resp.set_cookie(
            "dash_token", token,
            max_age=_SESSION_TTL, httponly=True, samesite="lax",
        )
        return resp

    @router.get("/logout", tags=["auth"])
    def logout(request: Request):
        token = request.cookies.get("dash_token", "")
        _sessions.pop(token, None)
        resp = RedirectResponse("/login", status_code=302)
        resp.delete_cookie("dash_token")
        return resp

    # ── HTML page ─────────────────────────────────────────────────────────────
    @router.get("/dashboard", response_class=HTMLResponse, tags=["dashboard"])
    def dashboard_page(request: Request) -> HTMLResponse:
        if not _user(request):
            return RedirectResponse("/login", status_code=302)
        return HTMLResponse(_DASHBOARD_HTML)

    # ── JSON stats API ────────────────────────────────────────────────────────
    @router.get("/api/dashboard/stats", tags=["dashboard"])
    def dashboard_stats(
        date_from: str = Query(default="", description="YYYY-MM-DD, defaults to today"),
        date_to:   str = Query(default="", description="YYYY-MM-DD, defaults to today"),
        group:     str = Query(default="", description="Group filter: A / B / C / D"),
        force:     int = Query(default=0,  description="Set to 1 to bypass cache"),
    ) -> Dict[str, Any]:
        import traceback
        _empty = {"groups": {}, "date_from": date_from, "date_to": date_to,
                  "total_consul": 0, "total_zakas": 0, "total_otkaz": 0, "total_dumka": 0,
                  "total_summa": 0, "avg_conversion": 0.0}
        try:
            today = date.today().strftime("%Y-%m-%d")
            if not date_from:
                date_from = today
            if not date_to:
                date_to = today

            # ── Serve from cache when available ─────────────────────────────────
            cache_key = (date_from, date_to)
            has_today = (date_from <= today <= date_to)
            cache_ttl = 60 if has_today else 300
            cached    = _stats_cache.get(cache_key)
            if cached and not force and (time.monotonic() - cached["ts"]) < cache_ttl:
                return cached["data"]

            # ── 1. Load Staff sheet: code → {code, group, full_name} ──────────
            # Cached for _STAFF_CACHE_TTL seconds to avoid hitting Google Sheets API
            # on every dashboard refresh.
            staff_by_code: Dict[str, Dict] = {}
            now_ts = time.monotonic()
            if _staff_cache["data"] is not None and (now_ts - _staff_cache["ts"]) < _STAFF_CACHE_TTL:
                staff_by_code = _staff_cache["data"]
            else:
                try:
                    ws = service.sheet._get_or_create_sheet("Staff")
                    rows = ws.get_all_values()
                    for row in rows[1:]:
                        if len(row) < 3:
                            continue
                        code      = str(row[1]).strip()  # Код сотрудника
                        full_name = str(row[2]).strip()  # Сотрудник
                        dept      = str(row[3]).strip() if len(row) >= 4 else ""
                        if not full_name or not code:
                            continue
                        info = {"code": code, "group": dept, "full_name": full_name}
                        staff_by_code[code] = info
                        try:
                            staff_by_code[str(int(code))] = info
                        except ValueError:
                            pass
                    _staff_cache["data"] = staff_by_code
                    _staff_cache["ts"]   = now_ts
                except Exception as exc:
                    print(f"[DASHBOARD] Could not load Staff sheet: {exc}")
                    if _staff_cache["data"] is not None:
                        staff_by_code = _staff_cache["data"]  # use stale data on error

            # ── 1b. Build sotuv (sales) pipeline ID set for filtering ─────────
            pipeline_keyword = getattr(service.cfg, "PIPELINE_KEYWORD", "sotuv").lower()
            sotuv_pipeline_ids: set[int] = set()
            if pipeline_keyword:
                for pid, pname in service.pipeline_id_to_name.items():
                    if pipeline_keyword in pname.lower():
                        sotuv_pipeline_ids.add(pid)
            # If keyword matches nothing, fall back to all pipelines
            if not sotuv_pipeline_ids:
                sotuv_pipeline_ids = set(service.pipeline_id_to_name.keys())

            # ── 1c. Build ORDER_SIDS: status IDs = order stages in sotuv pipelines
            order_status_ids: set[int] = {
                sid
                for pid in sotuv_pipeline_ids
                for dname, sid in (service.pipeline_status_display_to_id or {}).get(pid, {}).items()
                if dname in ZAKAS_DISPLAY_NAMES
            }

            # ── 2. Fetch leads + events in PARALLEL to minimise wall-clock time ──
            try:
                ts_from = int(datetime.strptime(date_from, "%Y-%m-%d").timestamp())
                ts_to   = int(datetime.strptime(date_to,   "%Y-%m-%d").timestamp()) + 86399
            except ValueError:
                ts_from = ts_to = 0

            leads: List[Dict] = []
            raw_event_ids: set[int] = set()  # from events API (before created-filter)
            events_loaded = False

            def _fetch_leads():
                return service.amo.fetch_leads_by_date_range(date_from, date_to)

            def _fetch_events():
                if not (ts_from and order_status_ids):
                    return set()
                # Fetch without created_lead_ids; we'll intersect after leads return.
                return service.amo.fetch_order_event_lead_ids(
                    ts_from=ts_from,
                    ts_to=ts_to,
                    order_status_ids=order_status_ids,
                    created_lead_ids=None,
                )

            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_leads  = pool.submit(_fetch_leads)
                fut_events = pool.submit(_fetch_events)
                try:
                    leads = fut_leads.result()
                except Exception as exc:
                    print(f"[DASHBOARD] Leads fetch failed: {exc}")
                    leads = []
                try:
                    raw_event_ids = fut_events.result()
                    events_loaded = True
                except Exception as exc:
                    print(f"[DASHBOARD] Events fetch failed, falling back to status check: {exc}")

            # Cache raw leads for the export endpoint (same TTL as stats)
            _leads_cache[cache_key] = {"ts": time.monotonic(), "leads": leads}

            # Intersect events with leads created in the same window (sotuv only)
            created_lead_ids: set[int] = {
                int(lead["id"])
                for lead in leads
                if int(lead.get("pipeline_id", 0) or 0) in sotuv_pipeline_ids
            }
            ordered_today_ids: set[int] = raw_event_ids & created_lead_ids if events_loaded else set()

            # ── 3. Aggregate per-staff AND per-Приемщик ───────────────────────
            user_stats: Dict[str, Dict] = {}
            priemshchik_stats: Dict[str, Dict] = {}
            skipped_unknown = 0  # leads whose code isn't in the Staff sheet

            for lead in leads:
                # ── pipeline filter: only sotuv leads ─────────────────────────
                lead_pipeline_id = int(lead.get("pipeline_id", 0) or 0)
                if sotuv_pipeline_ids and lead_pipeline_id not in sotuv_pipeline_ids:
                    continue

                status_id      = int(lead.get("status_id", 0) or 0)
                status_display = service.status_id_to_display_name.get(status_id, "")
                budget         = float(lead.get("price", 0) or 0)
                lead_id        = int(lead.get("id", 0) or 0)
                # zakas = lead entered order stage on the same date it was created.
                # Uses Events API for accuracy; falls back to current status if events unavailable.
                if events_loaded:
                    is_zakas = lead_id in ordered_today_ids
                else:
                    is_zakas = status_display in ZAKAS_DISPLAY_NAMES
                is_otkaz       = status_display in OTKAZ_DISPLAY_NAMES
                is_dumka       = status_display in DUMKA_DISPLAY_NAMES
                cf_values      = lead.get("custom_fields_values") or []

                # ── Extract "Группа" custom field (fallback group source) ─────
                lead_group_override = ""
                for cf in cf_values:
                    fname = " ".join((cf.get("field_name") or "").split())
                    if fname == "Группа":
                        vals = cf.get("values") or []
                        if vals:
                            lead_group_override = str(vals[0].get("value", "")).strip()
                        break

                # ── 3a. Приемщик aggregation (runs for ALL leads) ─────────────
                for cf in cf_values:
                    fname = " ".join((cf.get("field_name") or "").split())
                    if fname in ("Приемщик", "Приёмщик"):
                        vals = cf.get("values") or []
                        if vals:
                            p_name = str(vals[0].get("value", "")).strip()
                            if p_name:
                                if p_name not in priemshchik_stats:
                                    priemshchik_stats[p_name] = {
                                        "name":   p_name,
                                        "consul": 0,
                                        "zakas":  0,
                                        "otkaz":  0,
                                        "dumka":  0,
                                        "summa":  0.0,
                                    }
                                priemshchik_stats[p_name]["consul"] += 1
                                if is_zakas:
                                    priemshchik_stats[p_name]["zakas"] += 1
                                    priemshchik_stats[p_name]["summa"] += budget
                                if is_otkaz:
                                    priemshchik_stats[p_name]["otkaz"] += 1
                                if is_dumka:
                                    priemshchik_stats[p_name]["dumka"] += 1
                        break

                # ── 3b. Staff aggregation (only leads with valid Код сотрудника) ─
                raw_code = ""
                for cf in cf_values:
                    fname = " ".join((cf.get("field_name") or "").split())
                    if fname == "Код сотрудника":
                        vals = cf.get("values") or []
                        if vals:
                            raw_code = str(vals[0].get("value", "")).strip()
                        break

                if not raw_code:
                    continue

                # Skip non-numeric codes (garbage / test data)
                try:
                    norm_code = str(int(raw_code))
                except ValueError:
                    skipped_unknown += 1
                    continue

                # Skip codes not in the Staff sheet
                staff_info = staff_by_code.get(norm_code) or staff_by_code.get(raw_code)
                if not staff_info:
                    skipped_unknown += 1
                    continue

                # Use dept from Staff sheet; fall back to lead's "Группа" custom field
                dept         = staff_info["group"] or lead_group_override
                display_name = staff_info["full_name"]

                if norm_code not in user_stats:
                    user_stats[norm_code] = {
                        "code":   norm_code,
                        "name":   display_name,
                        "group":  dept,
                        "consul": 0,
                        "zakas":  0,
                        "otkaz":  0,
                        "dumka":  0,
                        "summa":  0.0,
                    }
                # If same staff, update group if blank (later Группа field may fill it)
                elif not user_stats[norm_code]["group"] and dept:
                    user_stats[norm_code]["group"] = dept

                user_stats[norm_code]["consul"] += 1
                if is_zakas:
                    user_stats[norm_code]["zakas"] += 1
                    user_stats[norm_code]["summa"] += budget
                if is_otkaz:
                    user_stats[norm_code]["otkaz"] += 1
                if is_dumka:
                    user_stats[norm_code]["dumka"] += 1

            # ── 4. Build staff rows with conversion ────────────────────────────
            rows_out: List[Dict] = []
            for st in user_stats.values():
                consul = st["consul"]
                zakas  = st["zakas"]
                otkaz  = st["otkaz"]
                dumka  = st["dumka"]
                conv   = round(zakas / consul * 100, 1) if consul else 0.0
                rows_out.append({
                    "code":       st["code"],
                    "name":       st["name"],
                    "group":      st["group"],
                    "summa":      int(st["summa"]),
                    "zakas":      zakas,
                    "otkaz":      otkaz,
                    "dumka":      dumka,
                    "consul":     consul,
                    "conversion": conv,
                })

            # ── 5. Group filter ───────────────────────────────────────────────
            if group:
                rows_out = [r for r in rows_out if r["group"].upper() == group.upper()]

            # ── 6. Sort ───────────────────────────────────────────────────────
            rows_out.sort(key=lambda x: (-x["summa"], x["name"]))

            # ── 7. Group by Отдел, add row numbers ────────────────────────────
            groups: Dict[str, List] = {}
            for r in rows_out:
                g = r["group"] or "—"
                groups.setdefault(g, []).append(r)
            for g_rows in groups.values():
                for i, row in enumerate(g_rows, 1):
                    row["num"] = i

            # ── 8. Totals ─────────────────────────────────────────────────────
            all_consul = sum(r["consul"] for r in rows_out)
            all_zakas  = sum(r["zakas"]  for r in rows_out)
            all_otkaz  = sum(r["otkaz"]  for r in rows_out)
            all_dumka  = sum(r["dumka"]  for r in rows_out)
            all_summa  = sum(r["summa"]  for r in rows_out)
            avg_conv   = round(all_zakas / all_consul * 100, 1) if all_consul else 0.0

            # ── 9. Build Приемщик rows ────────────────────────────────────────
            priemshchik_rows: List[Dict] = []
            for st in priemshchik_stats.values():
                consul = st["consul"]
                zakas  = st["zakas"]
                otkaz  = st["otkaz"]
                dumka  = st["dumka"]
                conv   = round(zakas / consul * 100, 1) if consul else 0.0
                priemshchik_rows.append({
                    "name":       st["name"],
                    "summa":      int(st["summa"]),
                    "zakas":      zakas,
                    "otkaz":      otkaz,
                    "dumka":      dumka,
                    "consul":     consul,
                    "conversion": conv,
                })
            priemshchik_rows.sort(key=lambda x: (-x["summa"], x["name"]))
            for i, r in enumerate(priemshchik_rows, 1):
                r["num"] = i

            result = {
                "date_from":       date_from,
                "date_to":         date_to,
                "total_consul":    all_consul,
                "total_zakas":     all_zakas,
                "total_otkaz":     all_otkaz,
                "total_dumka":     all_dumka,
                "total_summa":     all_summa,
                "avg_conversion":  avg_conv,
                "skipped_unknown": skipped_unknown,
                "groups":          groups,
                "priemshchik":     priemshchik_rows,
            }
            _stats_cache[cache_key] = {"ts": time.monotonic(), "data": result}
            return result

        except Exception as exc:
            print(f"[DASHBOARD] Error in stats endpoint: {traceback.format_exc()}")
            return {**_empty, "error": str(exc), "date_from": date_from, "date_to": date_to}

    # ── XLSX Export endpoint ──────────────────────────────────────────────────
    @router.get("/api/dashboard/export", tags=["dashboard"])
    def dashboard_export(
        request:  Request,
        date_from: str = Query(default=""),
        date_to:   str = Query(default=""),
        group:     str = Query(default=""),
    ):
        import traceback
        if not _user(request):
            from fastapi import HTTPException
            raise HTTPException(status_code=401, detail="Unauthorized")
        try:
            from openpyxl import Workbook
            from openpyxl.styles import (Alignment, Border, Font, PatternFill,
                                          Side)
            from openpyxl.utils import get_column_letter

            today = date.today().strftime("%Y-%m-%d")
            if not date_from: date_from = today
            if not date_to:   date_to   = today

            # ── Re-use cached stats (triggers a fetch if needed) ─────────────
            stats = dashboard_stats(request=request, date_from=date_from, date_to=date_to, group=group, force=0)

            # ── Re-use cached raw leads for the detail sheet ──────────────
            cache_key = (date_from, date_to)
            leads_entry = _leads_cache.get(cache_key)
            raw_leads: List[Dict] = leads_entry["leads"] if leads_entry else []

            # ── Style helpers ──────────────────────────────────────────────
            HDR_FILL  = PatternFill("solid", fgColor="1E3A5F")
            HDR_FONT  = Font(bold=True, color="FFFFFF", size=10)
            SUMM_FILL = PatternFill("solid", fgColor="0D1829")
            SUMM_FONT = Font(bold=True, color="93C5FD", size=10)
            GRP_FILL  = PatternFill("solid", fgColor="163352")
            GRP_FONT  = Font(bold=True, color="BAE6FD", size=10)
            THIN = Side(style="thin", color="334155")
            BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
            CENTER = Alignment(horizontal="center", vertical="center")
            LEFT   = Alignment(horizontal="left",   vertical="center")

            def hdr(ws, row, cols):
                """Write a styled header row."""
                for c, val in enumerate(cols, 1):
                    cell = ws.cell(row=row, column=c, value=val)
                    cell.fill   = HDR_FILL
                    cell.font   = HDR_FONT
                    cell.border = BORDER
                    cell.alignment = CENTER

            def autofit(ws):
                """Set column widths based on max content length."""
                for col in ws.columns:
                    max_len = max((len(str(c.value or "")) for c in col), default=8)
                    ws.column_dimensions[get_column_letter(col[0].column)].width = min(max_len + 4, 40)

            wb = Workbook()
            wb.remove(wb.active)  # remove default sheet

            # ═════════════════════════════════════════════════════════════════
            # Sheet 1: Сводка (Summary)
            # ═════════════════════════════════════════════════════════════════
            ws1 = wb.create_sheet("Сводка")
            ws1.sheet_view.showGridLines = False
            summary_rows = [
                ("Период",          f"{date_from}  –  {date_to}"),
                ("Группа фильтр",  group or "Все"),
                ("Консультации",     stats["total_consul"]),
                ("Заказы",           stats["total_zakas"]),
                ("Отказы",           stats["total_otkaz"]),
                ("Раздумья",        stats["total_dumka"]),
                ("Сумма заказов",  stats["total_summa"]),
                ("Конверсия",       f"{stats['avg_conversion']}%"),
            ]
            for r, (label, val) in enumerate(summary_rows, 1):
                lc = ws1.cell(row=r, column=1, value=label)
                lc.font = HDR_FONT; lc.fill = HDR_FILL; lc.border = BORDER; lc.alignment = LEFT
                vc = ws1.cell(row=r, column=2, value=val)
                vc.font = Font(bold=True, color="E2E8F0", size=11)
                vc.fill = PatternFill("solid", fgColor="151F32")
                vc.border = BORDER; vc.alignment = LEFT
            ws1.column_dimensions["A"].width = 22
            ws1.column_dimensions["B"].width = 24

            # ═════════════════════════════════════════════════════════════════
            # Sheet 2: Сотрудники (Staff KPI by group)
            # ═════════════════════════════════════════════════════════════════
            ws2 = wb.create_sheet("Сотрудники")
            ws2.sheet_view.showGridLines = False
            STAFF_COLS = ["#", "Отдел", "Код", "Сотрудник",
                          "Сумма", "Заказы", "Раздумья", "Отказы",
                          "Консультации", "Конверсия %"]
            row_num = 1
            hdr(ws2, row_num, STAFF_COLS)
            row_num += 1
            for g_name, g_rows in stats.get("groups", {}).items():
                # Group header row
                gc = ws2.cell(row=row_num, column=1, value=f"Отдел {g_name}")
                gc.fill = GRP_FILL; gc.font = GRP_FONT; gc.alignment = LEFT; gc.border = BORDER
                ws2.merge_cells(start_row=row_num, start_column=1,
                                end_row=row_num,   end_column=len(STAFF_COLS))
                for c2 in range(2, len(STAFF_COLS) + 1):
                    ws2.cell(row=row_num, column=c2).fill = GRP_FILL
                    ws2.cell(row=row_num, column=c2).border = BORDER
                row_num += 1
                for r in g_rows:
                    vals = [r.get("num",""), g_name, r["code"], r["name"],
                            r["summa"], r["zakas"], r["dumka"], r["otkaz"],
                            r["consul"], r["conversion"]]
                    for c2, v in enumerate(vals, 1):
                        cell = ws2.cell(row=row_num, column=c2, value=v)
                        cell.border = BORDER
                        cell.alignment = CENTER if c2 != 4 else LEFT
                        if c2 == 5:  # summa
                            cell.number_format = "#,##0"
                        if c2 == 10:  # conversion
                            cell.font = Font(bold=True, color=(
                                "4ADE80" if (v or 0) >= 50 else
                                "FACC15" if (v or 0) >= 25 else "F87171"))
                    row_num += 1
            autofit(ws2)

            # ═════════════════════════════════════════════════════════════════
            # Sheet 3: Приемщики
            # ═════════════════════════════════════════════════════════════════
            ws3 = wb.create_sheet("Приемщики")
            ws3.sheet_view.showGridLines = False
            PR_COLS = ["#", "Приемщик",
                       "Сумма", "Заказы", "Раздумья",
                       "Отказы", "Консультации", "Конверсия %"]
            hdr(ws3, 1, PR_COLS)
            for ri, r in enumerate(stats.get("priemshchik", []), 2):
                vals = [r.get("num",ri-1), r["name"],
                        r["summa"], r["zakas"], r["dumka"],
                        r["otkaz"], r["consul"], r["conversion"]]
                for c2, v in enumerate(vals, 1):
                    cell = ws3.cell(row=ri, column=c2, value=v)
                    cell.border = BORDER
                    cell.alignment = CENTER if c2 != 2 else LEFT
                    if c2 == 3:
                        cell.number_format = "#,##0"
                    if c2 == 8:
                        cell.font = Font(bold=True, color=(
                            "4ADE80" if (v or 0) >= 50 else
                            "FACC15" if (v or 0) >= 25 else "F87171"))
            autofit(ws3)

            # ═════════════════════════════════════════════════════════════════
            # Sheet 4: Лиды (детально по каждому лиду)
            # ═════════════════════════════════════════════════════════════════
            ws4 = wb.create_sheet("Лиды")
            ws4.sheet_view.showGridLines = False
            LEAD_COLS = ["ИД", "Дата создания", "Воронка", "Статус",
                         "Код сотрудника", "Сотрудник", "Отдел",
                         "Бюджет", "Заказ", "Отказ", "Раздумье"]
            hdr(ws4, 1, LEAD_COLS)

            # Build staff lookup from cache if available
            staff_lkp: Dict[str, Dict] = _staff_cache.get("data") or {}

            sotuv_ids = set()
            if hasattr(service, "pipeline_id_to_name"):
                kw = getattr(service.cfg, "PIPELINE_KEYWORD", "sotuv").lower()
                for pid2, pname2 in service.pipeline_id_to_name.items():
                    if kw in pname2.lower():
                        sotuv_ids.add(pid2)

            ri = 2
            for lead in raw_leads:
                lpid = int(lead.get("pipeline_id", 0) or 0)
                if sotuv_ids and lpid not in sotuv_ids:
                    continue
                lid      = lead.get("id", "")
                created  = lead.get("created_at", 0)
                created_str = datetime.fromtimestamp(created).strftime("%d.%m.%Y %H:%M") if created else ""
                pipeline_name = service.pipeline_id_to_name.get(lpid, str(lpid))
                sid      = int(lead.get("status_id", 0) or 0)
                status   = service.status_id_to_display_name.get(sid, "")
                budget   = float(lead.get("price", 0) or 0)
                cf_values = lead.get("custom_fields_values") or []

                staff_code = ""
                priemshchik_val = ""
                for cf in cf_values:
                    fname = " ".join((cf.get("field_name") or "").split())
                    vals  = cf.get("values") or []
                    if fname == "Код сотрудника" and vals:
                        staff_code = str(vals[0].get("value", "")).strip()
                    elif fname in ("Приемщик", "Приёмщик") and vals:
                        priemshchik_val = str(vals[0].get("value", "")).strip()

                try:
                    norm_code = str(int(staff_code)) if staff_code else ""
                except ValueError:
                    norm_code = ""
                staff_info = staff_lkp.get(norm_code) or staff_lkp.get(staff_code) or {}
                staff_name  = staff_info.get("full_name", "")
                staff_group = staff_info.get("group", "")

                is_z = status in ZAKAS_DISPLAY_NAMES
                is_o = status in OTKAZ_DISPLAY_NAMES
                is_d = status in DUMKA_DISPLAY_NAMES

                row_vals = [lid, created_str, pipeline_name, status,
                            staff_code, staff_name, staff_group,
                            budget, "✔" if is_z else "",
                            "✔" if is_o else "", "✔" if is_d else ""]
                for c2, v in enumerate(row_vals, 1):
                    cell = ws4.cell(row=ri, column=c2, value=v)
                    cell.border = BORDER
                    cell.alignment = CENTER if c2 not in (3, 4, 6) else LEFT
                    if c2 == 8:
                        cell.number_format = "#,##0"
                ri += 1
            autofit(ws4)

            # ── Serialize to bytes ───────────────────────────────────────────────
            buf = io.BytesIO()
            wb.save(buf)
            buf.seek(0)

            fname_label = f"{date_from}_{date_to}" + (f"_Группа{group}" if group else "")
            filename = f"KPI_{fname_label}.xlsx"
            return StreamingResponse(
                buf,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f'attachment; filename="{filename}"'},
            )
        except Exception as exc:
            print(f"[DASHBOARD] Export error: {traceback.format_exc()}")
            return {"error": str(exc)}

    return router


# ─────────────────────────────────────────────────────────────────────────────
# Login page
# ─────────────────────────────────────────────────────────────────────────────

_LOGIN_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Вход — KPI Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    body { background:#0b1120; font-family:'Inter',system-ui,sans-serif; }
    input { background:#ffffff !important; border:1px solid #2d3f5a; color:#0f172a !important;
            -webkit-text-fill-color:#0f172a !important;
            color-scheme: light !important;
            border-radius:8px; padding:10px 14px; width:100%; font-size:14px;
            outline:none; box-sizing:border-box; }
    input::placeholder { color:#94a3b8 !important; opacity:1; }
    input:focus, input:active, input:hover {
      background:#ffffff !important;
      color:#0f172a !important;
      -webkit-text-fill-color:#0f172a !important;
      color-scheme: light !important;
      border-color:#3b82f6 !important; }
    input:-webkit-autofill,
    input:-webkit-autofill:focus,
    input:-webkit-autofill:hover,
    input:-webkit-autofill:active {
      -webkit-box-shadow: 0 0 0 1000px #ffffff inset !important;
      -webkit-text-fill-color: #0f172a !important;
      color-scheme: light !important; }
    .btn-login { background:#2563eb; color:#fff; border-radius:8px; padding:11px;
                 width:100%; font-size:14px; font-weight:600; cursor:pointer;
                 border:none; transition:background .15s; }
    .btn-login:hover { background:#1d4ed8; }
    .err { background:#7f1d1d40; color:#fca5a5; border:1px solid #7f1d1d70;
           border-radius:8px; padding:10px 14px; font-size:13px; }
  </style>
</head>
<body class="min-h-screen flex items-center justify-center p-4">
  <div style="width:100%;max-width:380px">
    <div class="flex items-center gap-3 justify-center mb-8">
      <div style="width:40px;height:40px;background:#2563eb22;border:1px solid #2563eb44;
                  border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:20px">📊</div>
      <div>
        <div style="color:#f1f5f9;font-weight:700;font-size:18px">Staff KPI Dashboard</div>
        <div style="color:#475569;font-size:12px">amoCRM — реальное время</div>
      </div>
    </div>
    <div style="background:#151f32;border:1px solid #1e2d45;border-radius:14px;padding:28px 32px">
      <h2 style="color:#e2e8f0;font-weight:600;margin-bottom:22px;font-size:15px">Вход в систему</h2>
      {error_block}
      <form method="post" action="/login">
        <div style="margin-bottom:14px">
          <label style="color:#64748b;font-size:11px;text-transform:uppercase;
                        letter-spacing:.06em;display:block;margin-bottom:6px">Логин</label>
          <input type="text" name="username" autocomplete="username"
                 style="background:#ffffff !important;color:#0f172a !important;color-scheme:light"
                 placeholder="Введите логин" required />
        </div>
        <div style="margin-bottom:22px">
          <label style="color:#64748b;font-size:11px;text-transform:uppercase;
                        letter-spacing:.06em;display:block;margin-bottom:6px">Пароль</label>
          <input type="password" name="password" autocomplete="current-password"
                 style="background:#ffffff !important;color:#0f172a !important;color-scheme:light"
                 placeholder="Введите пароль" required />
        </div>
        <button type="submit" class="btn-login">Войти</button>
      </form>
    </div>
    <p style="color:#334155;font-size:11px;text-align:center;margin-top:16px">
      amoCRM → Google Sheets &nbsp;·&nbsp; KPI Dashboard
    </p>
  </div>
  <script>
    // Force visible text on all inputs — overrides any browser/autofill override
    function fixInputs() {
      document.querySelectorAll('input').forEach(function(el) {
        el.style.setProperty('color', '#0f172a', 'important');
        el.style.setProperty('background-color', '#ffffff', 'important');
        el.style.setProperty('opacity', '1', 'important');
      });
    }
    fixInputs();
    // Re-apply after autofill kicks in (Chrome fires it ~100ms after load)
    setTimeout(fixInputs, 200);
    setTimeout(fixInputs, 600);
    document.querySelectorAll('input').forEach(function(el) {
      el.addEventListener('animationstart', fixInputs);
      el.addEventListener('change', fixInputs);
    });
  </script>
</body>
</html>"""

# ─────────────────────────────────────────────────────────────────────────────
# Self-contained HTML dashboard page
# ─────────────────────────────────────────────────────────────────────────────

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>KPI Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    body { background:#0b1120; color:#e2e8f0; font-family:'Inter',system-ui,sans-serif; }

    /* ── Cards ── */
    .card  { background:#151f32; border:1px solid #1e2d45; border-radius:14px; }
    .glass { background:rgba(21,31,50,.85); backdrop-filter:blur(12px); border:1px solid rgba(255,255,255,.07); border-radius:14px; }

    /* ── Group header accent colours ── */
    .ghdr-a    { background:linear-gradient(90deg,#1d4ed820,#0b1120 80%); border-left:3px solid #3b82f6; }
    .ghdr-b    { background:linear-gradient(90deg,#15803d20,#0b1120 80%); border-left:3px solid #22c55e; }
    .ghdr-c    { background:linear-gradient(90deg,#9d174d20,#0b1120 80%); border-left:3px solid #ec4899; }
    .ghdr-d    { background:linear-gradient(90deg,#92400e20,#0b1120 80%); border-left:3px solid #f59e0b; }
    .ghdr-baza { background:linear-gradient(90deg,#5b21b620,#0b1120 80%); border-left:3px solid #a78bfa; }
    .ghdr-def  { background:linear-gradient(90deg,#33415520,#0b1120 80%); border-left:3px solid #64748b; }

    /* ── Table ── */
    .tbl { border-collapse:collapse; width:100%; }
    .tbl th { background:#0d1829; color:#64748b; font-size:10px; text-transform:uppercase;
              letter-spacing:.06em; padding:9px 10px; white-space:nowrap; cursor:pointer;
              user-select:none; position:sticky; top:0; z-index:1; }
    .tbl th:hover { color:#94a3b8; }
    .tbl th .sort-icon { opacity:.3; margin-left:2px; font-size:9px; }
    .tbl th.sorted-asc  .sort-icon::after { content:'▲'; opacity:1; }
    .tbl th.sorted-desc .sort-icon::after { content:'▼'; opacity:1; }
    .tbl th.sorted-asc  .sort-icon, .tbl th.sorted-desc .sort-icon { opacity:1; color:#3b82f6; }
    .tbl td { padding:7px 10px; font-size:12px; border-bottom:1px solid rgba(30,45,69,.7); }
    .tbl tr:last-child td { border-bottom:none; }
    .tbl tbody tr:hover td { background:rgba(59,130,246,.06); }

    /* ── Conversion bar ── */
    .conv-bar-wrap { display:flex; align-items:center; gap:5px; justify-content:flex-end; }
    .conv-bar-bg   { width:40px; height:4px; background:#1e2d45; border-radius:2px; flex-shrink:0; }
    .conv-bar-fill { height:100%; border-radius:2px; transition:width .4s ease; }

    /* ── Conversion text colours ── */
    .conv-high { color:#4ade80; font-weight:700; }
    .conv-mid  { color:#facc15; font-weight:700; }
    .conv-low  { color:#f87171; font-weight:700; }

    /* ── Badges ── */
    .badge { display:inline-block; padding:2px 9px; border-radius:999px; font-size:11px; font-weight:700; }
    .badge-a    { background:#1d4ed830; color:#93c5fd; border:1px solid #1d4ed870; }
    .badge-b    { background:#15803d30; color:#86efac; border:1px solid #15803d70; }
    .badge-c    { background:#9d174d30; color:#f9a8d4; border:1px solid #9d174d70; }
    .badge-d    { background:#92400e30; color:#fcd34d; border:1px solid #92400e70; }
    .badge-baza { background:#5b21b630; color:#c4b5fd; border:1px solid #5b21b670; }
    .badge-def  { background:#33415530; color:#94a3b8; border:1px solid #33415570; }

    /* ── Buttons ── */
    .btn { padding:6px 14px; border-radius:8px; font-size:13px; font-weight:600; cursor:pointer; transition:all .15s; border:1px solid transparent; }
    .btn-primary { background:#2563eb; color:#fff; border-color:#2563eb; }
    .btn-primary:hover { background:#1d4ed8; }
    .btn-outline { background:transparent; border:1px solid #1e2d45; color:#64748b; }
    .btn-outline:hover { border-color:#3b82f6; color:#e2e8f0; }
    .btn-preset { padding:5px 11px; border-radius:6px; font-size:12px; font-weight:500; cursor:pointer;
                  background:transparent; border:1px solid #1e2d45; color:#64748b; transition:all .15s; }
    .btn-preset:hover { border-color:#3b82f6; color:#cbd5e1; }
    .btn-preset.active { background:#1e3a5f; border-color:#2563eb; color:#93c5fd; }
    .btn-active      { background:#2563eb !important; color:#fff !important; border-color:#2563eb !important; }
    .btn-active-a    { background:#1d4ed8 !important; color:#bfdbfe !important; border-color:#1d4ed8 !important; }
    .btn-active-b    { background:#15803d !important; color:#bbf7d0 !important; border-color:#15803d !important; }
    .btn-active-c    { background:#9d174d !important; color:#fce7f3 !important; border-color:#9d174d !important; }
    .btn-active-d    { background:#92400e !important; color:#fde68a !important; border-color:#92400e !important; }
    .btn-active-baza { background:#5b21b6 !important; color:#ede9fe !important; border-color:#5b21b6 !important; }
    .btn-active-pr   { background:#0e7490 !important; color:#a5f3fc !important; border-color:#0e7490 !important; }

    /* ── Summary cards ── */
    .scard { border-radius:14px; padding:18px 20px; display:flex; align-items:center; gap:14px; }
    .scard-icon { width:42px; height:42px; border-radius:10px; display:flex; align-items:center; justify-content:center; font-size:19px; flex-shrink:0; }
    .scard-val  { font-size:24px; font-weight:700; line-height:1.1; font-variant-numeric:tabular-nums; }
    .scard-lbl  { font-size:11px; color:#64748b; margin-top:2px; }

    /* ── Skeleton ── */
    .skeleton { background:linear-gradient(90deg,#151f32 25%,#1e2d45 50%,#151f32 75%);
                background-size:200% 100%; animation:shimmer 1.4s infinite; border-radius:8px; }
    @keyframes shimmer { 0%{background-position:200% 0} 100%{background-position:-200% 0} }

    /* ── Spinner ── */
    .spinner { border:3px solid #1e2d45; border-top-color:#3b82f6; border-radius:50%;
               width:24px; height:24px; animation:spin .7s linear infinite; }
    @keyframes spin { to { transform:rotate(360deg); } }

    /* ── Rank medals ── */
    .rank-1 { color:#facc15; font-size:14px; }
    .rank-2 { color:#94a3b8; font-size:14px; }
    .rank-3 { color:#c2722a; font-size:14px; }

    /* ── Inputs ── */
    input[type="date"], input[type="text"] {
      background:#151f32; border:1px solid #1e2d45; color:#e2e8f0;
      border-radius:8px; padding:7px 10px; font-size:13px;
    }
    input[type="date"]:focus, input[type="text"]:focus { outline:none; border-color:#3b82f6; }

    /* ── Grid layout ── */
    #groups-container { display:grid; grid-template-columns:repeat(3,1fr); gap:14px; align-items:start; }
    #groups-container.single-group    { grid-template-columns:1fr; }
    #groups-container.single-group .tbl-scroll { max-height:680px; }
    #groups-container.two-groups      { grid-template-columns:repeat(2,1fr); }
    #groups-container.three-groups    { grid-template-columns:repeat(3,1fr); }
    .group-card.hidden-group { display:none; }

    /* ── Table max-height scroll ── */
    .tbl-scroll { max-height:480px; overflow-y:auto; }
    ::-webkit-scrollbar { width:4px; height:4px; }
    ::-webkit-scrollbar-track { background:#0b1120; }
    ::-webkit-scrollbar-thumb { background:#1e2d45; border-radius:2px; }
    ::-webkit-scrollbar-thumb:hover { background:#334155; }

    /* ── Tooltip ── */
    [data-tip] { position:relative; }
    [data-tip]:hover::after {
      content:attr(data-tip); position:absolute; bottom:calc(100% + 4px); left:50%; transform:translateX(-50%);
      background:#1e2d45; color:#e2e8f0; font-size:11px; padding:4px 8px; border-radius:6px;
      white-space:nowrap; z-index:99; pointer-events:none;
    }
  </style>
</head>
<body class="min-h-screen p-4 md:p-6">

  <!-- ── Header ── -->
  <div class="flex items-start justify-between mb-6 gap-4">
    <div>
      <div class="flex items-center gap-2.5 mb-0.5">
        <div class="w-8 h-8 rounded-lg bg-blue-600/20 flex items-center justify-center text-blue-400 text-lg">📊</div>
        <h1 class="text-xl font-bold text-white tracking-tight">Staff KPI Dashboard</h1>
      </div>
      <p class="text-slate-500 text-xs ml-10">amoCRM — реальное время</p>
    </div>
    <div class="flex items-center gap-2.5 flex-wrap justify-end">
      <div id="spinner" class="spinner hidden"></div>
      <span id="last-updated" class="text-slate-600 text-xs hidden sm:inline"></span>
      <button id="btn-refresh" class="btn btn-outline text-xs py-1.5" onclick="loadStats(true)">↻ Обновить</button>
      <button id="btn-export"  class="btn btn-outline text-xs py-1.5" style="border-color:#22c55e55;color:#86efac" onclick="exportXlsx()">↓ XLSX</button>
      <a href="/logout" class="btn btn-outline text-xs py-1.5" style="border-color:#47556944;color:#64748b;text-decoration:none">→ Выйти</a>
      <label class="flex items-center gap-1.5 text-slate-500 text-xs cursor-pointer select-none">
        <input type="checkbox" id="auto-refresh" class="accent-blue-500" onchange="toggleAutoRefresh()" />
        Авто 5 мин
      </label>
    </div>
  </div>

  <!-- ── Filters ── -->
  <div class="card p-4 mb-5 flex flex-wrap items-end gap-3">
    <div>
      <label class="block text-[10px] text-slate-500 mb-1 uppercase tracking-wide">От</label>
      <input type="date" id="f-from" style="width:140px" />
    </div>
    <div>
      <label class="block text-[10px] text-slate-500 mb-1 uppercase tracking-wide">До</label>
      <input type="date" id="f-to"   style="width:140px" />
    </div>
    <div class="flex gap-1.5 self-end pb-0.5">
      <button id="preset-today" class="btn-preset active" onclick="setPreset('today',this)">Сегодня</button>
      <button id="preset-week"  class="btn-preset"        onclick="setPreset('week', this)">Неделя</button>
      <button id="preset-month" class="btn-preset"        onclick="setPreset('month',this)">Месяц</button>
    </div>
    <button class="btn btn-primary self-end" onclick="loadStats()">Применить</button>

    <div class="w-px h-7 bg-slate-700/60 self-end hidden sm:block"></div>

    <div class="self-end">
      <label class="block text-[10px] text-slate-500 mb-1 uppercase tracking-wide">Группа</label>
      <div class="flex gap-1 flex-wrap" id="group-btns">
        <button class="btn btn-outline btn-active text-xs py-1" data-g="" onclick="setGroup(this,'')">Все</button>
        <button class="btn btn-outline text-xs py-1" data-g="A"    onclick="setGroup(this,'A')">A</button>
        <button class="btn btn-outline text-xs py-1" data-g="B"    onclick="setGroup(this,'B')">B</button>
        <button class="btn btn-outline text-xs py-1" data-g="C"    onclick="setGroup(this,'C')">C</button>
        <button class="btn btn-outline text-xs py-1" data-g="D"    onclick="setGroup(this,'D')">D</button>
        <button class="btn btn-outline text-xs py-1" data-g="Baza" onclick="setGroup(this,'Baza')">Baza</button>
        <button class="btn btn-outline text-xs py-1" data-g="__pr__" onclick="setGroup(this,'__pr__')">Приемщики</button>
      </div>
    </div>

    <div class="flex-1 min-w-[150px] self-end">
      <label class="block text-[10px] text-slate-500 mb-1 uppercase tracking-wide">Поиск сотрудника</label>
      <input type="text" id="f-staff" placeholder="Введите имя…" oninput="filterStaff()"
             style="width:100%" />
    </div>
  </div>

  <!-- ── Summary Cards ── -->
  <div id="summary-area" class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3 mb-5">
    <!-- skeleton while loading first time -->
    <div class="skeleton h-20 rounded-xl"></div>
    <div class="skeleton h-20 rounded-xl"></div>
    <div class="skeleton h-20 rounded-xl"></div>
    <div class="skeleton h-20 rounded-xl"></div>
    <div class="skeleton h-20 rounded-xl"></div>
  </div>

  <!-- ── Group Tables ── -->
  <div id="groups-container" class="mb-5">
    <!-- Skeleton grid while loading -->
    <div id="skeleton-grid" class="grid grid-cols-2 md:grid-cols-4 gap-3">
      <div class="skeleton h-64 rounded-xl"></div>
      <div class="skeleton h-64 rounded-xl"></div>
      <div class="skeleton h-64 rounded-xl"></div>
      <div class="skeleton h-64 rounded-xl"></div>
    </div>
  </div>

  <!-- ── Приемщик Section ── -->
  <div id="priemshchik-section" class="hidden mb-5">
    <div class="flex items-center gap-2 mb-3">
      <span class="text-lg">👤</span>
      <h2 class="text-white font-semibold text-base">Статистика Приемщиков</h2>
    </div>
    <div id="priemshchik-container" class="card overflow-hidden"></div>
  </div>

  <!-- ── Error ── -->
  <div id="error-banner" class="hidden mb-4 card p-4 flex items-start gap-3"
       style="border-color:#7f1d1d;background:rgba(127,29,29,.2)">
    <span class="text-red-400 text-base flex-shrink-0">⚠</span>
    <div class="flex-1 min-w-0">
      <div class="text-red-300 font-semibold text-sm">Ошибка загрузки</div>
      <div id="error-text" class="text-red-400/80 text-xs mt-0.5 break-all"></div>
    </div>
    <button onclick="clearError()" class="text-red-500 hover:text-red-300 text-base flex-shrink-0">✕</button>
  </div>

  <div id="empty-msg" class="hidden text-center text-slate-600 py-20 text-base">
    Нет данных за выбранный период.
  </div>

  <p class="text-center text-slate-700 text-xs mt-8 pb-4">
    amoCRM → Google Sheets &nbsp;·&nbsp; KPI Dashboard
  </p>

<script>
// ── State ────────────────────────────────────────────────────────────────────
let activeGroup  = '';
let autoTimer    = null;
let cachedData   = null;
let sortState    = {};   // tableId → { col, dir }

// ── Init ─────────────────────────────────────────────────────────────────────
(function init() {
  const today = new Date().toISOString().slice(0,10);
  document.getElementById('f-from').value = today;
  document.getElementById('f-to').value   = today;
  loadStats();
})();

// ── Date presets ─────────────────────────────────────────────────────────────
function setPreset(p, btn) {
  const today = new Date();
  let from = new Date(today), to = new Date(today);
  if (p === 'week') {
    const day = today.getDay() || 7;
    from.setDate(today.getDate() - day + 1);
  } else if (p === 'month') {
    from = new Date(today.getFullYear(), today.getMonth(), 1);
  }
  document.getElementById('f-from').value = from.toISOString().slice(0,10);
  document.getElementById('f-to').value   = to.toISOString().slice(0,10);
  document.querySelectorAll('.btn-preset').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  loadStats();
}

// ── Group filter ──────────────────────────────────────────────────────────────
const G_ACTIVE = {'':'btn-active',A:'btn-active-a',B:'btn-active-b',C:'btn-active-c',D:'btn-active-d',Baza:'btn-active-baza','__pr__':'btn-active-pr'};

function setGroup(btn, g) {
  activeGroup = g;
  document.querySelectorAll('#group-btns .btn').forEach(b =>
    b.classList.remove('btn-active','btn-active-a','btn-active-b','btn-active-c','btn-active-d','btn-active-baza','btn-active-pr'));
  btn.classList.add(G_ACTIVE[g] || 'btn-active');
  applyGroupVisibility();
}

function applyGroupVisibility() {
  const container = document.getElementById('groups-container');
  const cards     = container.querySelectorAll('.group-card');
  const ps        = document.getElementById('priemshchik-section');
  const isPr      = activeGroup === '__pr__';

  // Приемщики mode: hide all group cards, show only priemshchik section
  if (isPr) {
    cards.forEach(c => c.classList.add('hidden-group'));
    container.classList.remove('single-group','two-groups','three-groups');
    if (ps) ps.classList.remove('hidden');
    filterStaff();
    return;
  }

  // Normal group filter
  let visible = 0;
  cards.forEach(c => {
    const show = !activeGroup || c.dataset.group.toUpperCase() === activeGroup.toUpperCase();
    c.classList.toggle('hidden-group', !show);
    if (show) visible++;
  });
  container.classList.remove('single-group','two-groups','three-groups');
  if (visible === 1)      container.classList.add('single-group');
  else if (visible === 2) container.classList.add('two-groups');
  else if (visible === 3) container.classList.add('three-groups');
  // Приемщик table only visible in "Все" mode
  if (ps) ps.classList.toggle('hidden', !!activeGroup);
  filterStaff();
}

// ── Auto-refresh ──────────────────────────────────────────────────────────────
function toggleAutoRefresh() {
  clearInterval(autoTimer);
  if (document.getElementById('auto-refresh').checked)
    autoTimer = setInterval(loadStats, 5*60*1000);
}

// ── Load stats ───────────────────────────────────────────────────────────────
async function loadStats(force = false) {
  const from = document.getElementById('f-from').value;
  const to   = document.getElementById('f-to').value;
  document.getElementById('spinner').classList.remove('hidden');
  document.getElementById('btn-refresh').disabled = true;

  try {
    const forceParam = force ? '&force=1' : '';
    const res  = await fetch(`/api/dashboard/stats?date_from=${from}&date_to=${to}${forceParam}`);
    const ct   = res.headers.get('content-type') || '';
    if (!ct.includes('application/json')) {
      showError(`Ошибка ${res.status}: ${(await res.text()).slice(0,200)}`); return;
    }
    const data = await res.json();
    if (data.error) { showError('API: ' + data.error); return; }
    clearError();
    cachedData = data;
    renderSummary(data);
    renderGroups(data);
    renderPriemshchik(data);
    applyGroupVisibility();
    filterStaff();
    const now = new Date().toLocaleTimeString('ru-RU',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
    const lu  = document.getElementById('last-updated');
    lu.textContent = 'Обновлено ' + now;
    lu.classList.remove('hidden');
  } catch(e) {
    showError('Ошибка: ' + e);
  } finally {
    document.getElementById('spinner').classList.add('hidden');
    document.getElementById('btn-refresh').disabled = false;
  }
}

// ── Summary cards ─────────────────────────────────────────────────────────────
const SCARD_DEFS = [
  { id:'s-consul', icon:'💬', label:'Консультации', color:'#1e3a5f', iconBg:'#1d4ed820', iconColor:'#60a5fa' },
  { id:'s-zakas',  icon:'✅', label:'Заказы',       color:'#14532d', iconBg:'#15803d20', iconColor:'#4ade80' },
  { id:'s-otkaz',  icon:'✕',  label:'Отказы',       color:'#450a0a', iconBg:'#7f1d1d20', iconColor:'#f87171' },
  { id:'s-summa',  icon:'₸',  label:'Сумма заказов',color:'#422006', iconBg:'#92400e20', iconColor:'#fbbf24' },
  { id:'s-conv',   icon:'%',  label:'Конверсия',     color:'#2e1065', iconBg:'#5b21b620', iconColor:'#c4b5fd' },
];

function renderSummary(data) {
  const vals = {
    's-consul': fmtNum(data.total_consul),
    's-zakas':  fmtNum(data.total_zakas),
    's-otkaz':  fmtNum(data.total_otkaz || 0),
    's-summa':  fmtMoney(data.total_summa),
    's-conv':   data.avg_conversion + '%',
  };
  const area = document.getElementById('summary-area');
  area.innerHTML = SCARD_DEFS.map(d => `
    <div class="scard" style="background:${d.color}30;border:1px solid ${d.color}80">
      <div class="scard-icon" style="background:${d.iconBg};color:${d.iconColor}">${d.icon}</div>
      <div>
        <div class="scard-val" style="color:${d.iconColor}">${vals[d.id]}</div>
        <div class="scard-lbl">${d.label}</div>
      </div>
    </div>`).join('');
}

// ── Group tables ──────────────────────────────────────────────────────────────
const G_BADGE = { A:'badge-a', B:'badge-b', C:'badge-c', D:'badge-d', BAZA:'badge-baza' };
const G_HDR   = { A:'ghdr-a',  B:'ghdr-b',  C:'ghdr-c',  D:'ghdr-d',  BAZA:'ghdr-baza' };
const COLS    = ['num','name','summa','zakas','dumka','otkaz','consul','conversion'];
const COL_KEY = { 2:'summa', 3:'zakas', 4:'dumka', 5:'otkaz', 6:'consul', 7:'conversion' };

function renderGroups(data) {
  const container = document.getElementById('groups-container');
  const emptyMsg  = document.getElementById('empty-msg');

  // Remove any skeleton left-over
  const sk = document.getElementById('skeleton-grid');
  if (sk) sk.remove();

  container.innerHTML = '';
  document.querySelectorAll('.group-card').forEach(c => c.remove());

  const groups = data.groups || {};
  const ORDER  = ['A','B','C','D','Baza'];
  const keys   = [...ORDER.filter(k => groups[k]), ...Object.keys(groups).filter(k => !ORDER.includes(k)).sort()];

  if (!keys.length) { emptyMsg.classList.remove('hidden'); return; }
  emptyMsg.classList.add('hidden');

  for (const g of keys) {
    const rows     = groups[g];
    const gUp      = g.toUpperCase();
    const badgeCls = G_BADGE[gUp] || 'badge-def';
    const hdrCls   = G_HDR[gUp]   || 'ghdr-def';
    const tc = rows.reduce((s,r)=>s+r.consul,0);
    const tz = rows.reduce((s,r)=>s+r.zakas, 0);
    const to = rows.reduce((s,r)=>s+(r.otkaz||0),0);
    const td = rows.reduce((s,r)=>s+(r.dumka||0),0);
    const ts = rows.reduce((s,r)=>s+r.summa, 0);
    const tv = tc ? +(tz/tc*100).toFixed(1) : 0;
    const tableId = 'tbl-' + g;

    const card = document.createElement('div');
    card.className   = 'card overflow-hidden group-card';
    card.dataset.group = g;

    card.innerHTML = `
      <div class="${hdrCls} px-3 py-2.5 flex items-center justify-between">
        <span class="font-semibold text-white text-sm flex items-center gap-2">
          <span class="badge ${badgeCls}">${g}</span>
          Отдел ${g}
        </span>
        <span class="text-xs text-slate-400">${rows.length} чел.</span>
      </div>
      <div class="tbl-scroll">
        <table class="tbl" id="${tableId}">
          <thead><tr>
            <th class="text-left w-6" data-col="0"                   >#<span class="sort-icon"></span></th>
            <th class="text-left"     data-col="1"                   >Сотрудник<span class="sort-icon"></span></th>
            <th class="text-right"    data-col="2" data-key="summa"  >Сумма<span class="sort-icon"></span></th>
            <th class="text-right"    data-col="3" data-key="zakas"  >Заказ<span class="sort-icon"></span></th>
            <th class="text-right"    data-col="4" data-key="dumka"  >Думка<span class="sort-icon"></span></th>
            <th class="text-right"    data-col="5" data-key="otkaz"  >Отказ<span class="sort-icon"></span></th>
            <th class="text-right"    data-col="6" data-key="consul" >Консульт.<span class="sort-icon"></span></th>
            <th class="text-right"    data-col="7" data-key="conv"   >Конв.<span class="sort-icon"></span></th>
          </tr></thead>
          <tbody>${buildRows(rows, tableId)}</tbody>
          <tfoot>
            <tr style="background:#08101c;border-top:1px solid #1e2d45">
              <td colspan="2" class="px-2.5 py-2 text-xs text-slate-400 font-semibold">Итого</td>
              <td class="px-2.5 py-2 text-right text-xs font-bold text-yellow-400">${fmtMoney(ts)}</td>
              <td class="px-2.5 py-2 text-right text-xs font-bold text-green-400">${tz}</td>
              <td class="px-2.5 py-2 text-right text-xs font-bold text-slate-400">${td}</td>
              <td class="px-2.5 py-2 text-right text-xs font-bold text-red-400">${to}</td>
              <td class="px-2.5 py-2 text-right text-xs font-bold text-blue-400">${tc}</td>
              <td class="px-2.5 py-2 text-right text-xs font-bold ${convCls(tv)}">${convBar(tv)}</td>
            </tr>
          </tfoot>
        </table>
      </div>`;

    // bind sort
    card.querySelectorAll('th[data-col]').forEach(th => {
      th.addEventListener('click', () => sortTable(tableId, parseInt(th.dataset.col), th.dataset.key));
    });

    container.appendChild(card);
  }
}

function buildRows(rows, tableId) {
  // rows should already be sorted by summa desc from server; assign rank within that
  return rows.map((r, idx) => {
    const rank      = idx + 1;
    const rankHtml  = rank === 1 ? '<span class="rank-1">🥇</span>'
                    : rank === 2 ? '<span class="rank-2">🥈</span>'
                    : rank === 3 ? '<span class="rank-3">🥉</span>'
                    : `<span class="text-slate-600 text-[10px]">${rank}</span>`;
    return `<tr class="staff-row" data-name="${r.name.toLowerCase()}"
               data-summa="${r.summa}" data-zakas="${r.zakas}" data-dumka="${r.dumka||0}"
               data-otkaz="${r.otkaz||0}" data-consul="${r.consul}" data-conv="${r.conversion}">
      <td class="pl-2.5 pr-1 w-7">${rankHtml}</td>
      <td>
        <div class="font-medium text-slate-200 text-xs leading-tight">${r.name}</div>
        ${r.code ? `<div class="text-slate-600 text-[10px]">код ${r.code}</div>` : ''}
      </td>
      <td class="text-right font-semibold text-xs text-yellow-400">${fmtMoney(r.summa)}</td>
      <td class="text-right font-semibold text-xs text-green-400">${r.zakas}</td>
      <td class="text-right text-xs text-slate-400">${r.dumka||0}</td>
      <td class="text-right text-xs text-red-400">${r.otkaz||0}</td>
      <td class="text-right text-xs text-blue-300">${r.consul}</td>
      <td class="text-right text-xs">${convBar(r.conversion)}</td>
    </tr>`;
  }).join('');
}

// ── Sort ──────────────────────────────────────────────────────────────────────
function sortTable(tableId, colIdx, key) {
  const table  = document.getElementById(tableId);
  if (!table) return;
  const prev   = sortState[tableId] || { col:-1, dir:'desc' };
  const dir    = (prev.col === colIdx && prev.dir === 'desc') ? 'asc' : 'desc';
  sortState[tableId] = { col:colIdx, dir };

  // Update header icons
  table.querySelectorAll('th').forEach(th => {
    th.classList.remove('sorted-asc','sorted-desc');
    if (parseInt(th.dataset.col) === colIdx) th.classList.add(dir==='asc'?'sorted-asc':'sorted-desc');
  });

  const tbody = table.querySelector('tbody');
  const rows  = Array.from(tbody.querySelectorAll('tr.staff-row'));

  const dataAttr = {2:'summa',3:'zakas',4:'dumka',5:'otkaz',6:'consul',7:'conv'}[colIdx];

  rows.sort((a, b) => {
    let av, bv;
    if (colIdx === 1) {
      av = a.dataset.name; bv = b.dataset.name;
      return dir==='asc' ? av.localeCompare(bv) : bv.localeCompare(av);
    }
    if (!dataAttr) return 0;
    av = parseFloat(a.dataset[dataAttr]||0);
    bv = parseFloat(b.dataset[dataAttr]||0);
    return dir==='asc' ? av-bv : bv-av;
  });

  rows.forEach(r => tbody.appendChild(r));
}

// ── Приемщик ──────────────────────────────────────────────────────────────────
function renderPriemshchik(data) {
  const rows      = data.priemshchik || [];
  const section   = document.getElementById('priemshchik-section');
  const container = document.getElementById('priemshchik-container');
  if (!rows.length) { section.classList.add('hidden'); return; }
  if (!activeGroup || activeGroup === '__pr__') section.classList.remove('hidden');

  const tc = rows.reduce((s,r)=>s+r.consul,0);
  const tz = rows.reduce((s,r)=>s+r.zakas, 0);
  const to = rows.reduce((s,r)=>s+(r.otkaz||0),0);
  const ts = rows.reduce((s,r)=>s+r.summa, 0);
  const tv = tc ? +(tz/tc*100).toFixed(1) : 0;

  container.innerHTML = `
    <div class="ghdr-a px-3 py-2.5 flex items-center justify-between">
      <span class="font-semibold text-white text-sm">Приемщики</span>
      <span class="text-xs text-slate-400">${rows.length} чел.</span>
    </div>
    <div class="tbl-scroll">
      <table class="tbl">
        <thead><tr>
          <th class="text-left w-6">#</th>
          <th class="text-left">Приемщик</th>
          <th class="text-right">Сумма</th>
          <th class="text-right">Заказ</th>
          <th class="text-right">Отказ</th>
          <th class="text-right">Консультации</th>
          <th class="text-right">Конв.</th>
        </tr></thead>
        <tbody>${rows.map((r,i) => `
          <tr>
            <td class="pl-2.5 text-slate-600 text-[10px]">${i+1}</td>
            <td class="font-medium text-slate-200 text-xs">${r.name}</td>
            <td class="text-right font-semibold text-xs text-yellow-400">${fmtMoney(r.summa)}</td>
            <td class="text-right font-semibold text-xs text-green-400">${r.zakas}</td>
            <td class="text-right text-xs text-red-400">${r.otkaz||0}</td>
            <td class="text-right text-xs text-blue-300">${r.consul}</td>
            <td class="text-right text-xs">${convBar(r.conversion)}</td>
          </tr>`).join('')}
        </tbody>
        <tfoot>
          <tr style="background:#08101c;border-top:1px solid #1e2d45">
            <td colspan="2" class="px-2.5 py-2 text-xs text-slate-400 font-semibold">Итого</td>
            <td class="px-2.5 py-2 text-right text-xs font-bold text-yellow-400">${fmtMoney(ts)}</td>
            <td class="px-2.5 py-2 text-right text-xs font-bold text-green-400">${tz}</td>
            <td class="px-2.5 py-2 text-right text-xs font-bold text-red-400">${to}</td>
            <td class="px-2.5 py-2 text-right text-xs font-bold text-blue-400">${tc}</td>
            <td class="px-2.5 py-2 text-right text-xs font-bold ${convCls(tv)}">${convBar(tv)}</td>
          </tr>
        </tfoot>
      </table>
    </div>`;
}

// ── Staff search ──────────────────────────────────────────────────────────────
function filterStaff() {
  const q = document.getElementById('f-staff').value.toLowerCase().trim();
  document.querySelectorAll('.staff-row').forEach(tr => {
    const matchName  = !q || (tr.dataset.name||'').includes(q);
    const matchGroup = !activeGroup ||
      tr.closest('.group-card')?.dataset?.group?.toUpperCase() === activeGroup.toUpperCase();
    tr.style.display = (matchName && matchGroup) ? '' : 'none';
  });
}

// ── Export ────────────────────────────────────────────────────────────────────
function exportXlsx() {
  const from = document.getElementById('f-from').value;
  const to   = document.getElementById('f-to').value;
  const grp  = activeGroup;
  const btn  = document.getElementById('btn-export');
  btn.disabled = true; btn.textContent = '⏳ ...';
  fetch(`/api/dashboard/export?date_from=${from}&date_to=${to}&group=${grp}`)
    .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.blob(); })
    .then(blob => {
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = `KPI_${from}_${to}${grp ? '_'+grp : ''}.xlsx`;
      a.click(); URL.revokeObjectURL(a.href);
    })
    .catch(e => showError('Ошибка экспорта: ' + e.message))
    .finally(() => { btn.disabled = false; btn.textContent = '↓ XLSX'; });
}

// ── Error ─────────────────────────────────────────────────────────────────────
function showError(msg) {
  document.getElementById('error-text').textContent = msg;
  document.getElementById('error-banner').classList.remove('hidden');
}
function clearError() {
  document.getElementById('error-banner').classList.add('hidden');
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmtNum(n)   { return (n||0).toLocaleString('ru-RU'); }
function fmtMoney(n) {
  if (!n) return '0';
  if (n >= 1_000_000) return (n/1_000_000).toFixed(1).replace('.',',') + ' млн';
  return n.toLocaleString('ru-RU');
}
function convCls(v)  { return v >= 50 ? 'conv-high' : v >= 25 ? 'conv-mid' : 'conv-low'; }
function convBar(v)  {
  const pct  = Math.min(v, 100);
  const col  = v >= 50 ? '#4ade80' : v >= 25 ? '#facc15' : '#f87171';
  return `<div class="conv-bar-wrap">
    <span class="${convCls(v)}">${v}%</span>
    <div class="conv-bar-bg"><div class="conv-bar-fill" style="width:${pct}%;background:${col}"></div></div>
  </div>`;
}
</script>
</body>
</html>"""
