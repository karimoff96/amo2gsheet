"""
dashboard_router.py — Staff KPI Dashboard

Mounted into the main FastAPI app in sync_service.py via:
    app.include_router(create_dashboard_router(service))

Routes:
    GET  /dashboard              → Interactive HTML dashboard page
    GET  /api/dashboard/stats    → JSON KPI data (consumed by the page via fetch())
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse

# ── Status display names that count as "reached Заказ" for KPI ───────────────
ZAKAS_DISPLAY_NAMES: set[str] = {"Заказ"}


def _norm(name: str) -> str:
    """Lowercase + collapse whitespace for fuzzy name matching."""
    return " ".join(name.lower().split())


# ─────────────────────────────────────────────────────────────────────────────
# Factory – call once from sync_service.py, passing the live SyncService.
# ─────────────────────────────────────────────────────────────────────────────

def create_dashboard_router(service) -> APIRouter:
    router = APIRouter()

    # ── HTML page ─────────────────────────────────────────────────────────────
    @router.get("/dashboard", response_class=HTMLResponse, tags=["dashboard"])
    def dashboard_page() -> str:
        return _DASHBOARD_HTML

    # ── JSON stats API ────────────────────────────────────────────────────────
    @router.get("/api/dashboard/stats", tags=["dashboard"])
    def dashboard_stats(
        date_from: str = Query(default="", description="YYYY-MM-DD, defaults to today"),
        date_to:   str = Query(default="", description="YYYY-MM-DD, defaults to today"),
        group:     str = Query(default="", description="Group filter: A / B / C / D"),
    ) -> Dict[str, Any]:
        import traceback
        _empty = {"groups": {}, "date_from": date_from, "date_to": date_to,
                  "total_consul": 0, "total_zakas": 0, "total_summa": 0, "avg_conversion": 0.0}
        try:
            today = date.today().strftime("%Y-%m-%d")
            if not date_from:
                date_from = today
            if not date_to:
                date_to = today

            # ── 1. Load Staff sheet: code → {code, group, full_name} ──────────
            staff_by_code: Dict[str, Dict] = {}
            try:
                ws = service.sheet._get_or_create_sheet("Staff")
                rows = ws.get_all_values()
                for row in rows[1:]:
                    if len(row) < 2:
                        continue
                    code      = str(row[0]).strip()
                    full_name = str(row[1]).strip()
                    dept      = str(row[2]).strip() if len(row) >= 3 else ""
                    if not full_name or not code:
                        continue
                    info = {"code": code, "group": dept, "full_name": full_name}
                    staff_by_code[code] = info
                    try:
                        staff_by_code[str(int(code))] = info
                    except ValueError:
                        pass
            except Exception as exc:
                print(f"[DASHBOARD] Could not load Staff sheet: {exc}")

            # ── 2. Fetch leads from AMO ───────────────────────────────────────
            leads = service.amo.fetch_leads_by_date_range(date_from, date_to)

            # ── 3. Aggregate per-staff AND per-Приемщик ───────────────────────
            user_stats: Dict[str, Dict] = {}
            priemshchik_stats: Dict[str, Dict] = {}
            skipped_unknown = 0  # leads whose code isn't in the Staff sheet

            for lead in leads:
                status_id      = int(lead.get("status_id", 0) or 0)
                status_display = service.status_id_to_display_name.get(status_id, "")
                budget         = float(lead.get("price", 0) or 0)
                is_zakas       = status_display in ZAKAS_DISPLAY_NAMES
                cf_values      = lead.get("custom_fields_values") or []

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
                                        "summa":  0.0,
                                    }
                                priemshchik_stats[p_name]["consul"] += 1
                                if is_zakas:
                                    priemshchik_stats[p_name]["zakas"] += 1
                                    priemshchik_stats[p_name]["summa"] += budget
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

                dept         = staff_info["group"]
                display_name = staff_info["full_name"]

                if norm_code not in user_stats:
                    user_stats[norm_code] = {
                        "code":   norm_code,
                        "name":   display_name,
                        "group":  dept,
                        "consul": 0,
                        "zakas":  0,
                        "summa":  0.0,
                    }

                user_stats[norm_code]["consul"] += 1
                if is_zakas:
                    user_stats[norm_code]["zakas"] += 1
                    user_stats[norm_code]["summa"] += budget

            # ── 4. Build staff rows with conversion ────────────────────────────
            rows_out: List[Dict] = []
            for st in user_stats.values():
                consul = st["consul"]
                zakas  = st["zakas"]
                conv   = round(zakas / consul * 100, 1) if consul else 0.0
                rows_out.append({
                    "code":       st["code"],
                    "name":       st["name"],
                    "group":      st["group"],
                    "summa":      int(st["summa"]),
                    "zakas":      zakas,
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
            all_summa  = sum(r["summa"]  for r in rows_out)
            avg_conv   = round(all_zakas / all_consul * 100, 1) if all_consul else 0.0

            # ── 9. Build Приемщик rows ────────────────────────────────────────
            priemshchik_rows: List[Dict] = []
            for st in priemshchik_stats.values():
                consul = st["consul"]
                zakas  = st["zakas"]
                conv   = round(zakas / consul * 100, 1) if consul else 0.0
                priemshchik_rows.append({
                    "name":       st["name"],
                    "summa":      int(st["summa"]),
                    "zakas":      zakas,
                    "consul":     consul,
                    "conversion": conv,
                })
            priemshchik_rows.sort(key=lambda x: (-x["summa"], x["name"]))
            for i, r in enumerate(priemshchik_rows, 1):
                r["num"] = i

            return {
                "date_from":       date_from,
                "date_to":         date_to,
                "total_consul":    all_consul,
                "total_zakas":     all_zakas,
                "total_summa":     all_summa,
                "avg_conversion":  avg_conv,
                "skipped_unknown": skipped_unknown,
                "groups":          groups,
                "priemshchik":     priemshchik_rows,
            }

        except Exception as exc:
            print(f"[DASHBOARD] Error in stats endpoint: {traceback.format_exc()}")
            return {**_empty, "error": str(exc), "date_from": date_from, "date_to": date_to}

    return router


# ─────────────────────────────────────────────────────────────────────────────
# Self-contained HTML dashboard page
# ─────────────────────────────────────────────────────────────────────────────

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>KPI — Staff Performance Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { background: #0f172a; color: #e2e8f0; font-family: 'Inter', system-ui, sans-serif; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; }
    .group-header   { background: #1e3a5f; }
    .group-header-b { background: #1a3a2a; }
    .group-header-c { background: #3a1a2a; }
    .group-header-d { background: #3a2a0a; }
    .tbl th { background: #0f172a; color: #94a3b8; font-size: 11px; text-transform: uppercase;
              letter-spacing: 0.05em; padding: 8px 8px; white-space: nowrap; }
    .tbl td { padding: 6px 8px; font-size: 12px; border-bottom: 1px solid #1e293b; }
    .tbl tr:last-child td { border-bottom: none; }
    .tbl tr:hover td { background: #253348; }
    .conv-high { color: #4ade80; font-weight: 600; }
    .conv-mid  { color: #facc15; font-weight: 600; }
    .conv-low  { color: #f87171; font-weight: 600; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 11px; font-weight: 700; }
    .badge-a { background: #1d4ed8; color: #bfdbfe; }
    .badge-b { background: #15803d; color: #bbf7d0; }
    .badge-c { background: #9d174d; color: #fce7f3; }
    .badge-d { background: #92400e; color: #fde68a; }
    input[type="date"] {
      background: #1e293b; border: 1px solid #334155; color: #e2e8f0;
      border-radius: 6px; padding: 6px 10px; font-size: 13px;
    }
    input[type="date"]:focus { outline: none; border-color: #3b82f6; }
    .btn { padding: 7px 16px; border-radius: 7px; font-size: 13px; font-weight: 600; cursor: pointer; transition: all .15s; }
    .btn-primary { background: #2563eb; color: white; }
    .btn-primary:hover { background: #1d4ed8; }
    .btn-outline { background: transparent; border: 1px solid #334155; color: #94a3b8; }
    .btn-outline:hover { border-color: #3b82f6; color: #e2e8f0; }
    .btn-active { background: #2563eb !important; color: white !important; border-color: #2563eb !important; }
    /* group-specific active tint */
    .btn-active-a { background: #1d4ed8 !important; color: #bfdbfe !important; border-color: #1d4ed8 !important; }
    .btn-active-b { background: #15803d !important; color: #bbf7d0 !important; border-color: #15803d !important; }
    .btn-active-c { background: #9d174d !important; color: #fce7f3 !important; border-color: #9d174d !important; }
    .btn-active-d { background: #92400e !important; color: #fde68a !important; border-color: #92400e !important; }
    .spinner { border: 3px solid #1e293b; border-top-color: #3b82f6; border-radius: 50%;
               width: 26px; height: 26px; animation: spin .7s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: #0f172a; }
    ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }

    /* ── 4-column layout ── */
    #groups-container {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 12px;
      align-items: start;
    }
    /* single-group: centre one card at ~560 px */
    #groups-container.single-group {
      grid-template-columns: minmax(0, 560px);
      justify-content: center;
    }
    /* card hidden by group filter */
    .group-card.hidden-group { display: none; }
  </style>
</head>
<body class="min-h-screen p-4">

  <!-- ── Header ───────────────────────────────────────────────────────────── -->
  <div class="flex items-center justify-between mb-5">
    <div>
      <h1 class="text-2xl font-bold text-white">📊 Staff KPI Dashboard</h1>
      <p class="text-slate-400 text-sm mt-0.5">Real-time performance — amoCRM</p>
    </div>
    <div class="flex items-center gap-3">
      <div id="spinner" class="spinner hidden"></div>
      <span id="last-updated" class="text-slate-500 text-xs"></span>
      <button id="btn-refresh" class="btn btn-outline" onclick="loadStats()">↻ Обновить</button>
      <label class="flex items-center gap-2 text-slate-400 text-sm cursor-pointer">
        <input type="checkbox" id="auto-refresh" class="accent-blue-500" onchange="toggleAutoRefresh()" />
        Авто (5 мин)
      </label>
    </div>
  </div>

  <!-- ── Filters ──────────────────────────────────────────────────────────── -->
  <div class="card p-4 mb-5 flex flex-wrap items-end gap-4">
    <!-- Date range -->
    <div>
      <label class="block text-xs text-slate-400 mb-1">Дата от</label>
      <input type="date" id="f-from" />
    </div>
    <div>
      <label class="block text-xs text-slate-400 mb-1">Дата до</label>
      <input type="date" id="f-to" />
    </div>

    <!-- Date presets -->
    <div class="flex gap-2 pt-5">
      <button class="btn btn-outline text-xs" onclick="setPreset('today')">Сегодня</button>
      <button class="btn btn-outline text-xs" onclick="setPreset('week')">Неделя</button>
      <button class="btn btn-outline text-xs" onclick="setPreset('month')">Месяц</button>
    </div>

    <button class="btn btn-primary pt-5" onclick="loadStats()">Применить</button>

    <!-- Divider -->
    <div class="h-8 w-px bg-slate-600 self-end mb-1 hidden sm:block"></div>

    <!-- Group tabs -->
    <div>
      <label class="block text-xs text-slate-400 mb-1">Группа</label>
      <div class="flex gap-1.5" id="group-btns">
        <button class="btn btn-outline btn-active" data-g="" onclick="setGroup(this, '')">Все</button>
        <button class="btn btn-outline" data-g="A"  onclick="setGroup(this, 'A')">A</button>
        <button class="btn btn-outline" data-g="B"  onclick="setGroup(this, 'B')">B</button>
        <button class="btn btn-outline" data-g="C"  onclick="setGroup(this, 'C')">C</button>
        <button class="btn btn-outline" data-g="D"  onclick="setGroup(this, 'D')">D</button>
      </div>
    </div>

    <!-- Staff search -->
    <div class="flex-1 min-w-[160px]">
      <label class="block text-xs text-slate-400 mb-1">Поиск сотрудника</label>
      <input type="text" id="f-staff" placeholder="Имя…" oninput="filterStaff()"
             class="w-full bg-slate-800 border border-slate-600 text-slate-200 rounded-md
                    px-3 py-[7px] text-sm focus:outline-none focus:border-blue-500" />
    </div>
  </div>

  <!-- ── Summary Cards ────────────────────────────────────────────────────── -->
  <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-5">
    <div class="card p-4 text-center">
      <div class="text-2xl font-bold text-blue-400" id="s-consul">—</div>
      <div class="text-xs text-slate-400 mt-1">Лиды (консультация)</div>
    </div>
    <div class="card p-4 text-center">
      <div class="text-2xl font-bold text-green-400" id="s-zakas">—</div>
      <div class="text-xs text-slate-400 mt-1">Лиды (заказ)</div>
    </div>
    <div class="card p-4 text-center">
      <div class="text-2xl font-bold text-yellow-400" id="s-summa">—</div>
      <div class="text-xs text-slate-400 mt-1">Сумма (заказы)</div>
    </div>
    <div class="card p-4 text-center">
      <div class="text-2xl font-bold text-purple-400" id="s-conv">—</div>
      <div class="text-xs text-slate-400 mt-1">Конверсия (средняя)</div>
    </div>
  </div>

  <!-- ── Group Tables — always rendered; visibility toggled client-side ────── -->
  <div id="groups-container">
    <!-- filled by JS on first load -->
  </div>

  <!-- ── Приемщик Statistics ─────────────────────────────────────────────── -->
  <div id="priemshchik-section" class="hidden mb-5">
    <h2 class="text-white font-bold text-lg mb-3">👤 Статистика Приемщиков</h2>
    <div id="priemshchik-container" class="card overflow-hidden"></div>
  </div>

  <!-- ── Inline error banner (replaces alert popups) ──────────────────────── -->
  <div id="error-banner" class="hidden mb-4 card border-red-700 bg-red-950/60 p-4 flex items-start gap-3">
    <span class="text-red-400 text-lg">⚠</span>
    <div>
      <div class="text-red-300 font-semibold text-sm">Ошибка загрузки</div>
      <div id="error-text" class="text-red-400 text-xs mt-0.5 break-all"></div>
    </div>
    <button onclick="clearError()" class="ml-auto text-red-500 hover:text-red-300 text-lg leading-none">✕</button>
  </div>

  <div id="empty-msg" class="hidden text-center text-slate-500 py-16 text-lg">
    Нет данных за выбранный период.
  </div>

  <p class="text-center text-slate-600 text-xs mt-8">
    amoCRM → Google Sheets sync &nbsp;|&nbsp; Dashboard v1.0
  </p>

<script>
// ── State ────────────────────────────────────────────────────────────────────
let activeGroup = '';   // '' = all groups visible
let autoTimer   = null;

// ── Init ─────────────────────────────────────────────────────────────────────
(function init() {
  const today = new Date().toISOString().slice(0, 10);
  document.getElementById('f-from').value = today;
  document.getElementById('f-to').value   = today;
  loadStats();
})();

// ── Date presets ──────────────────────────────────────────────────────────────
function setPreset(p) {
  const today = new Date();
  let from = new Date(today), to = new Date(today);
  if (p === 'week') {
    const day = today.getDay() || 7;
    from.setDate(today.getDate() - day + 1);
  } else if (p === 'month') {
    from = new Date(today.getFullYear(), today.getMonth(), 1);
  }
  document.getElementById('f-from').value = from.toISOString().slice(0, 10);
  document.getElementById('f-to').value   = to.toISOString().slice(0, 10);
  loadStats();
}

// ── Group filter (client-side only — no extra API call) ───────────────────────
const GROUP_ACTIVE_CLS = { '': 'btn-active', A: 'btn-active-a', B: 'btn-active-b', C: 'btn-active-c', D: 'btn-active-d' };

function setGroup(btn, g) {
  activeGroup = g;

  // update button styles
  document.querySelectorAll('#group-btns .btn').forEach(b => {
    b.classList.remove('btn-active','btn-active-a','btn-active-b','btn-active-c','btn-active-d');
  });
  btn.classList.add(GROUP_ACTIVE_CLS[g] || 'btn-active');

  // show/hide cards without reloading data
  applyGroupVisibility();
}

function applyGroupVisibility() {
  const container = document.getElementById('groups-container');
  const cards     = container.querySelectorAll('.group-card');
  let visible = 0;

  cards.forEach(card => {
    const show = !activeGroup || card.dataset.group.toUpperCase() === activeGroup.toUpperCase();
    card.classList.toggle('hidden-group', !show);
    if (show) visible++;
  });

  // switch grid layout: 4-col row vs centred single card
  container.classList.toggle('single-group', visible === 1);

  // also re-run staff search so hidden cards don't interfere with "no results" logic
  filterStaff();
}

// ── Auto-refresh ──────────────────────────────────────────────────────────────
function toggleAutoRefresh() {
  clearInterval(autoTimer);
  if (document.getElementById('auto-refresh').checked)
    autoTimer = setInterval(loadStats, 5 * 60 * 1000);
}

// ── Load stats from API ───────────────────────────────────────────────────────
async function loadStats() {
  const from = document.getElementById('f-from').value;
  const to   = document.getElementById('f-to').value;

  document.getElementById('spinner').classList.remove('hidden');
  document.getElementById('btn-refresh').disabled = true;

  try {
    // Always fetch ALL groups; filtering is client-side for instant switching
    const res = await fetch(`/api/dashboard/stats?date_from=${from}&date_to=${to}`);

    let data;
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) {
      data = await res.json();
    } else {
      const text = await res.text();
      showError(`Сервер вернул ошибку ${res.status}: ${text.slice(0, 200)}`);
      return;
    }

    if (data.error) { showError('Ошибка API: ' + data.error); return; }

    clearError();
    renderSummary(data);
    renderGroups(data);
    renderPriemshchik(data);
    applyGroupVisibility();
    filterStaff();

    const now = new Date().toLocaleTimeString('ru-RU', {hour:'2-digit',minute:'2-digit',second:'2-digit'});
    document.getElementById('last-updated').textContent = 'Обновлено: ' + now;
  } catch(e) {
    showError('Ошибка загрузки: ' + e);
  } finally {
    document.getElementById('spinner').classList.add('hidden');
    document.getElementById('btn-refresh').disabled = false;
  }
}

// ── Summary cards ──────────────────────────────────────────────────────────────
function renderSummary(data) {
  document.getElementById('s-consul').textContent = fmtNum(data.total_consul);
  document.getElementById('s-zakas').textContent  = fmtNum(data.total_zakas);
  document.getElementById('s-summa').textContent  = fmtMoney(data.total_summa);
  document.getElementById('s-conv').textContent   = data.avg_conversion + '%';
}

// ── Group tables ───────────────────────────────────────────────────────────────
const GROUP_COLORS = { A:'badge-a', B:'badge-b', C:'badge-c', D:'badge-d' };
const GROUP_HDR    = { A:'group-header', B:'group-header-b', C:'group-header-c', D:'group-header-d' };

function renderGroups(data) {
  const container = document.getElementById('groups-container');
  const emptyMsg  = document.getElementById('empty-msg');
  container.innerHTML = '';

  const groups = data.groups || {};
  // Always render in fixed order A → B → C → D, then any others
  const ORDER = ['A','B','C','D'];
  const keys  = [...ORDER.filter(k => groups[k]), ...Object.keys(groups).filter(k => !ORDER.includes(k)).sort()];

  if (keys.length === 0) { emptyMsg.classList.remove('hidden'); return; }
  emptyMsg.classList.add('hidden');

  for (const g of keys) {
    const rows      = groups[g];
    const badgeCls  = GROUP_COLORS[g.toUpperCase()] || 'badge-a';
    const hdrCls    = GROUP_HDR[g.toUpperCase()]    || 'group-header';
    const tc = rows.reduce((s,r)=>s+r.consul,0);
    const tz = rows.reduce((s,r)=>s+r.zakas, 0);
    const ts = rows.reduce((s,r)=>s+r.summa, 0);
    const tv = tc ? Math.round(tz/tc*1000)/10 : 0;

    const card = document.createElement('div');
    card.className = 'card overflow-hidden group-card';
    card.dataset.group = g;

    card.innerHTML = `
      <div class="${hdrCls} px-3 py-2.5 flex items-center justify-between">
        <span class="font-bold text-white text-sm flex items-center gap-2">
          <span class="badge ${badgeCls}">${g}</span>
          Отдел ${g}
        </span>
        <span class="text-xs text-slate-300">${rows.length} чел.</span>
      </div>
      <div class="overflow-x-auto">
        <table class="tbl w-full">
          <thead><tr>
            <th class="text-left w-6">№</th>
            <th class="text-left">Сотрудник</th>
            <th class="text-right">Сумма</th>
            <th class="text-right">Заказ</th>
            <th class="text-right">Консульт.</th>
            <th class="text-right">Конв.</th>
          </tr></thead>
          <tbody>${rows.map(r => rowHtml(r)).join('')}</tbody>
          <tfoot>
            <tr class="border-t border-slate-600 bg-slate-900/40">
              <td colspan="2" class="px-2 py-2 text-xs text-slate-400 font-semibold">Итого</td>
              <td class="px-2 py-2 text-right text-xs font-bold text-yellow-300">${fmtMoney(ts)}</td>
              <td class="px-2 py-2 text-right text-xs font-bold text-green-300">${tz}</td>
              <td class="px-2 py-2 text-right text-xs font-bold text-blue-300">${tc}</td>
              <td class="px-2 py-2 text-right text-xs font-bold ${convCls(tv)}">${tv}%</td>
            </tr>
          </tfoot>
        </table>
      </div>`;
    container.appendChild(card);
  }
}

function rowHtml(r) {
  return `<tr class="staff-row" data-name="${r.name.toLowerCase()}">
    <td class="text-slate-500 text-[10px]">${r.num}</td>
    <td>
      <div class="font-medium text-white text-xs leading-tight">${r.name}</div>
      ${r.code ? `<div class="text-slate-500 text-[10px]">код: ${r.code}</div>` : ''}
    </td>
    <td class="text-right text-yellow-300 font-semibold text-xs">${fmtMoney(r.summa)}</td>
    <td class="text-right text-green-300 font-semibold text-xs">${r.zakas}</td>
    <td class="text-right text-blue-300 text-xs">${r.consul}</td>
    <td class="text-right text-xs ${convCls(r.conversion)}">${r.conversion}%</td>
  </tr>`;
}

// ── Приемщик table ──────────────────────────────────────────────────────────
function renderPriemshchik(data) {
  const rows     = data.priemshchik || [];
  const section  = document.getElementById('priemshchik-section');
  const container = document.getElementById('priemshchik-container');

  if (!rows.length) { section.classList.add('hidden'); return; }
  section.classList.remove('hidden');

  const tc = rows.reduce((s,r)=>s+r.consul, 0);
  const tz = rows.reduce((s,r)=>s+r.zakas,  0);
  const ts = rows.reduce((s,r)=>s+r.summa,  0);
  const tv = tc ? Math.round(tz/tc*1000)/10 : 0;

  container.innerHTML = `
    <div class="group-header px-3 py-2.5 flex items-center justify-between">
      <span class="font-bold text-white text-sm">Приемщики</span>
      <span class="text-xs text-slate-300">${rows.length} чел.</span>
    </div>
    <div class="overflow-x-auto">
      <table class="tbl w-full">
        <thead><tr>
          <th class="text-left w-6">№</th>
          <th class="text-left">Приемщик</th>
          <th class="text-right">Сумма</th>
          <th class="text-right">Заказ</th>
          <th class="text-right">Консультация</th>
          <th class="text-right">Конв.</th>
        </tr></thead>
        <tbody>${rows.map(r => `
          <tr>
            <td class="text-slate-500 text-[10px]">${r.num}</td>
            <td class="font-medium text-white text-xs">${r.name}</td>
            <td class="text-right text-yellow-300 font-semibold text-xs">${fmtMoney(r.summa)}</td>
            <td class="text-right text-green-300 font-semibold text-xs">${r.zakas}</td>
            <td class="text-right text-blue-300 text-xs">${r.consul}</td>
            <td class="text-right text-xs ${convCls(r.conversion)}">${r.conversion}%</td>
          </tr>`).join('')}
        </tbody>
        <tfoot>
          <tr class="border-t border-slate-600 bg-slate-900/40">
            <td colspan="2" class="px-2 py-2 text-xs text-slate-400 font-semibold">Итого</td>
            <td class="px-2 py-2 text-right text-xs font-bold text-yellow-300">${fmtMoney(ts)}</td>
            <td class="px-2 py-2 text-right text-xs font-bold text-green-300">${tz}</td>
            <td class="px-2 py-2 text-right text-xs font-bold text-blue-300">${tc}</td>
            <td class="px-2 py-2 text-right text-xs font-bold ${convCls(tv)}">${tv}%</td>
          </tr>
        </tfoot>
      </table>
    </div>`;
}

// ── Staff name search (client-side, instant) ──────────────────────────────────
function filterStaff() {
  const q = document.getElementById('f-staff').value.toLowerCase().trim();
  document.querySelectorAll('.staff-row').forEach(tr => {
    const name     = tr.dataset.name || '';
    const groupOk  = !activeGroup ||
      tr.closest('.group-card')?.dataset?.group?.toUpperCase() === activeGroup.toUpperCase();
    tr.style.display = (groupOk && (!q || name.includes(q))) ? '' : 'none';
  });
}

// ── Error banner ─────────────────────────────────────────────────────────────
function showError(msg) {
  document.getElementById('error-text').textContent = msg;
  document.getElementById('error-banner').classList.remove('hidden');
}
function clearError() {
  document.getElementById('error-banner').classList.add('hidden');
  document.getElementById('error-text').textContent = '';
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmtNum(n)   { return (n||0).toLocaleString('ru-RU'); }
function fmtMoney(n) {
  if (!n) return '0';
  if (n >= 1_000_000) return (n/1_000_000).toFixed(1).replace('.',',') + ' млн';
  return n.toLocaleString('ru-RU');
}
function convCls(v) {
  return v >= 50 ? 'conv-high' : v >= 25 ? 'conv-mid' : 'conv-low';
}
</script>
</body>
</html>"""
