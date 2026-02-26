# dashboard.py
import os
import csv
import json
import sqlite3
import re
import time
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Tuple, Set

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTPUT_DIR = "./outputs"
DOCS_DIR = "./docs"
DB_PATH_DEFAULT = "./data/moco_jobs.sqlite"

JSEARCH_SEARCH_URL = "https://jsearch.p.rapidapi.com/search"


# ----------------------------
# Basics
# ----------------------------

def utc_now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def ensure_dirs():
    os.makedirs(DOCS_DIR, exist_ok=True)


def find_latest(prefix: str, suffix: str = ".csv"):
    if not os.path.exists(OUTPUT_DIR):
        return None
    files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(prefix) and f.endswith(suffix)]
    if not files:
        return None
    files.sort()
    return files[-1]


def read_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def connect_db(db_path: str):
    if not os.path.exists(db_path):
        return None
    return sqlite3.connect(db_path)


def pill(text, cls=""):
    return f'<span class="pill {cls}">{text}</span>'


def link(title, url):
    if url and title:
        return f'<a href="{url}" target="_blank" rel="noopener">{title}</a>'
    return title or ""


def sunday_to_saturday_range(d: date) -> Tuple[date, date]:
    # Week starts Sunday, ends Saturday
    days_since_sunday = (d.weekday() + 1) % 7
    start = d - timedelta(days=days_since_sunday)
    end = start + timedelta(days=6)
    return start, end


def most_recent_completed_week(today: date) -> Tuple[date, date]:
    """
    Returns the most recent *completed* Sunday->Saturday week.
    - If today is Saturday, we use the week that ended last Saturday (7 days ago).
    - Otherwise we use the most recent Saturday before today.
    """
    # weekday: Mon=0 ... Sat=5 ... Sun=6
    days_since_saturday = (today.weekday() - 5) % 7
    if days_since_saturday == 0:
        days_since_saturday = 7
    end = today - timedelta(days=days_since_saturday)
    start = end - timedelta(days=6)
    return start, end


def last_n_completed_weeks(today: date, n: int = 3) -> List[Tuple[date, date]]:
    """
    Returns last n completed Sun->Sat week ranges, newest first.
    """
    out: List[Tuple[date, date]] = []
    start, end = most_recent_completed_week(today)
    for _ in range(n):
        out.append((start, end))
        end = start - timedelta(days=1)
        start = end - timedelta(days=6)
    return out


def month_start_end(month_yyyy_mm: str) -> Tuple[date, date]:
    start = datetime.strptime(month_yyyy_mm + "-01", "%Y-%m-%d").date()
    if start.month == 12:
        end = date(start.year + 1, 1, 1)
    else:
        end = date(start.year, start.month + 1, 1)
    return start, end


# ----------------------------
# Normalization + weekly-only exclusions
# ----------------------------

def normalize_company(name: str) -> str:
    if not name:
        return ""
    s = name.strip().lower()
    s = re.sub(r"[^\w\s&-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    suffixes = [" inc", " llc", " ltd", " co", " corporation", " corp", " company", " incorporated", " limited"]
    for suf in suffixes:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
    return s


EXCLUDE_WEEKLY_COMPANIES = {
    "uber",
    "uber technologies",
    "doordash",
    "lyft",
    "amazon",
    "walmart",
    "montgomery county public schools",
    "mcps",
    "pizza hut",
    "pizza_hut",
}

EXCLUDE_WEEKLY_NORMS = {normalize_company(x) for x in EXCLUDE_WEEKLY_COMPANIES if normalize_company(x)}


def _sql_not_in_clause(col: str, values: List[str]) -> Tuple[str, List[str]]:
    if not values:
        return "", []
    placeholders = ",".join(["?"] * len(values))
    return f" AND {col} NOT IN ({placeholders}) ", values


def html_shell(title: str, active: str, meta_line_html: str, body_html: str):
    tabs = [
        ("Daily", "index.html", "daily"),
        ("Weekly", "weekly.html", "weekly"),
        ("Technology", "technology.html", "tech"),
        ("Life Sciences", "lifesciences.html", "life"),
        ("Aero/Defense/Sat", "aerodefense.html", "aero"),
        ("Company Indicators", "indicators.html", "indicators"),
        ("Trends", "trends.html", "trends"),
        ("Search", "search.html", "search"),
    ]

    nav = '<div class="tabs">'
    for label, href, key in tabs:
        cls = "tab active" if key == active else "tab"
        nav += f'<a class="{cls}" href="{href}">{label}</a>'
    nav += "</div>"

    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px; }}
    .meta {{ color: #444; margin-bottom: 16px; line-height: 1.5; }}
    .pill {{ display:inline-block; padding:2px 10px; border:1px solid #ddd; border-radius: 999px; font-size: 12px; margin-right: 8px; }}
    .pill.ok {{ border-color: #9ad29a; background: #f2fbf2; }}
    .pill.warn {{ border-color: #f0d090; background: #fff8e6; }}
    .tabs {{ display:flex; gap:10px; flex-wrap:wrap; margin: 10px 0 18px; }}
    .tab {{ padding:8px 12px; border:1px solid #ddd; border-radius: 999px; text-decoration:none; color:#222; }}
    .tab.active {{ background:#111; color:#fff; border-color:#111; }}
    .grid {{ display:grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 12px; margin: 16px 0 22px; }}
    .card {{ border: 1px solid #eee; border-radius: 14px; padding: 14px; }}
    .card h3 {{ margin: 0 0 10px; font-size: 14px; color: #333; }}
    .big {{ font-size: 22px; font-weight: 650; }}
    .small {{ color: #666; font-size: 12px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border-bottom: 1px solid #eee; padding: 10px; text-align: left; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: white; }}
    code {{ background: #f6f6f6; padding: 2px 6px; border-radius: 6px; }}
    .badge {{ display:inline-block; padding:2px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #ddd; }}
    .badge.verified {{ border-color: #9ad29a; background: #f2fbf2; }}
    .badge.unverified {{ border-color: #ddd; background: #fafafa; }}
    .muted {{ color:#666; }}
    .hint {{ border-left: 3px solid #eee; padding-left: 10px; margin: 14px 0; }}
    input[type="text"] {{ width:100%; padding:10px; border:1px solid #ddd; border-radius:10px; }}
    .two-col {{ display:grid; grid-template-columns: 1fr 1fr; gap: 14px; }}
    @media (max-width: 900px) {{
      .grid {{ grid-template-columns: repeat(2, minmax(180px, 1fr)); }}
      .two-col {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <h1>MoCo Hiring Monitor</h1>
  {nav}

  <div class="meta">
    {meta_line_html}
  </div>

  {body_html}

  <hr style="margin: 28px 0; border: none; border-top: 1px solid #eee;" />
  <div class="small">
    Hosting: GitHub Pages serves <code>docs/</code>. Data accumulates because <code>data/moco_jobs.sqlite</code> is committed after each run.
  </div>
</body>
</html>
"""


# ----------------------------
# Coverage stats
# ----------------------------

def read_latest_daily_run_date() -> Optional[str]:
    latest_daily = find_latest("new_companies_")
    if not latest_daily:
        return None
    return latest_daily.replace("new_companies_", "").replace(".csv", "")


def read_latest_run_stats_for_date(db_path: str, run_date: str) -> Optional[Dict[str, Any]]:
    conn = connect_db(db_path)
    if not conn:
        return None
    try:
        row = conn.execute("""
            SELECT jobs_scanned_count, jobs_in_moco_count, new_jobs_count, new_companies_count, started_utc, finished_utc
            FROM runs
            WHERE run_date = ?
            ORDER BY run_id DESC
            LIMIT 1
        """, (run_date,)).fetchone()
        if not row:
            return None
        return {
            "jobs_scanned_count": int(row[0] or 0),
            "jobs_in_moco_count": int(row[1] or 0),
            "new_jobs_count": int(row[2] or 0),
            "new_companies_count": int(row[3] or 0),
            "started_utc": row[4] or "",
            "finished_utc": row[5] or ""
        }
    finally:
        conn.close()


def sum_run_stats_over_range(db_path: str, start_date: date, end_date_inclusive: date) -> Optional[Dict[str, Any]]:
    conn = connect_db(db_path)
    if not conn:
        return None
    start_s = start_date.isoformat()
    end_excl = (end_date_inclusive + timedelta(days=1)).isoformat()
    try:
        row = conn.execute("""
            SELECT
              COALESCE(SUM(jobs_scanned_count), 0),
              COALESCE(SUM(jobs_in_moco_count), 0),
              COALESCE(SUM(new_jobs_count), 0),
              COALESCE(SUM(new_companies_count), 0)
            FROM runs
            WHERE run_date >= ?
              AND run_date < ?
        """, (start_s, end_excl)).fetchone()
        if not row:
            return None
        return {
            "jobs_scanned_count": int(row[0] or 0),
            "jobs_in_moco_count": int(row[1] or 0),
            "new_jobs_count": int(row[2] or 0),
            "new_companies_count": int(row[3] or 0),
        }
    finally:
        conn.close()


def sector_week_stats(db_path: str, field_tag: str, start: date, end: date) -> Dict[str, Any]:
    conn = connect_db(db_path)
    if not conn:
        return {"jobs_scanned_count": 0, "jobs_in_moco_count": 0, "new_jobs_count": 0, "new_companies_count": 0,
                "started_utc": "", "finished_utc": ""}

    start_s = start.isoformat()
    end_excl = (end + timedelta(days=1)).isoformat()
    like_tag = f"%,{field_tag},%"

    jobs_captured = conn.execute("""
        SELECT COUNT(*)
        FROM jobs
        WHERE first_seen_run_date >= ?
          AND first_seen_run_date < ?
          AND fields LIKE ?
    """, (start_s, end_excl, like_tag)).fetchone()[0] or 0

    new_jobs = conn.execute("""
        SELECT COUNT(DISTINCT job_id)
        FROM jobs
        WHERE first_seen_run_date >= ?
          AND first_seen_run_date < ?
          AND fields LIKE ?
    """, (start_s, end_excl, like_tag)).fetchone()[0] or 0

    new_companies = conn.execute("""
        SELECT COUNT(DISTINCT c.employer_norm)
        FROM companies c
        JOIN jobs j ON j.employer_norm = c.employer_norm
        WHERE c.first_seen_run_date >= ?
          AND c.first_seen_run_date < ?
          AND j.first_seen_run_date >= ?
          AND j.first_seen_run_date < ?
          AND j.fields LIKE ?
    """, (start_s, end_excl, start_s, end_excl, like_tag)).fetchone()[0] or 0

    conn.close()

    return {
        "jobs_scanned_count": int(jobs_captured),
        "jobs_in_moco_count": int(jobs_captured),
        "new_jobs_count": int(new_jobs),
        "new_companies_count": int(new_companies),
        "started_utc": "",
        "finished_utc": "",
    }


def render_stats_grid(stats: Optional[Dict[str, Any]], extra_note: Optional[str] = None) -> str:
    if not stats:
        return """
  <div class="card">
    <h3>Coverage stats</h3>
    <div class="small">Run at least one daily scan to populate coverage stats.</div>
  </div>
"""
    note = (extra_note or stats.get("note") or "").strip()
    note_html = f'<div class="small muted" style="margin-top:8px;">{note}</div>' if note else ""
    return f"""
  <div class="grid">
    <div class="card">
      <h3>Jobs scanned</h3>
      <div class="big">{stats.get("jobs_scanned_count", 0)}</div>
      <div class="small">Tab-specific definition</div>
    </div>
    <div class="card">
      <h3>Jobs inside MoCo</h3>
      <div class="big">{stats.get("jobs_in_moco_count", 0)}</div>
      <div class="small">After MoCo filter / captured-in-DB</div>
    </div>
    <div class="card">
      <h3>New companies</h3>
      <div class="big">{stats.get("new_companies_count", 0)}</div>
      <div class="small">First time seen (tab period)</div>
    </div>
    <div class="card">
      <h3>New unique jobs</h3>
      <div class="big">{stats.get("new_jobs_count", 0)}</div>
      <div class="small">Distinct job IDs first seen (tab period)</div>
    </div>
  </div>
  {note_html}
"""


# ----------------------------
# Requests: retry + backoff (for JSearch)
# ----------------------------

def build_retry_session() -> requests.Session:
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def _is_job_in_moco_light(job: Dict[str, Any]) -> bool:
    city = str(job.get("job_city") or "").lower()
    state = str(job.get("job_state") or "").lower()
    loc = str(job.get("job_location") or "").lower()

    moco_cities = [
        "rockville", "bethesda", "silver spring", "gaithersburg", "germantown",
        "wheaton", "takoma park", "chevy chase", "potomac", "olney", "kensington"
    ]

    if state == "md" and any(c in city for c in moco_cities):
        return True
    if "montgomery county" in loc and "md" in loc:
        return True
    return False


def fetch_open_job_ids_for_company(
    company_name: str,
    rapidapi_key: str,
    rapidapi_host: str,
    *,
    session: requests.Session,
    max_pages: int = 6,
    per_call_sleep_s: float = 0.25,
    connect_timeout_s: int = 10,
    read_timeout_s: int = 90,
) -> Set[str]:
    """
    "Open now" approximation:
      - query = company_name
      - pulls multiple pages
      - filters to exact normalized employer match
      - filters to MoCo-ish location
      - returns set(job_id)
    """
    if not rapidapi_key:
        return set()

    headers = {"X-RapidAPI-Key": rapidapi_key, "X-RapidAPI-Host": rapidapi_host}
    target_norm = normalize_company(company_name)

    out: Set[str] = set()
    seen_any = False

    for page in range(1, max_pages + 1):
        time.sleep(per_call_sleep_s)

        params = {
            "query": company_name,
            "page": str(page),
            "num_pages": "1",
            "country": "us",
        }

        try:
            r = session.get(JSEARCH_SEARCH_URL, headers=headers, params=params, timeout=(connect_timeout_s, read_timeout_s))
            r.raise_for_status()
            payload = r.json()
        except Exception:
            break

        data = payload.get("data") or []
        if not isinstance(data, list) or not data:
            if seen_any:
                break
            else:
                continue

        seen_any = True

        page_added = 0
        for j in data:
            emp = str(j.get("employer_name") or "")
            if normalize_company(emp) != target_norm:
                continue
            if not _is_job_in_moco_light(j):
                continue
            jid = str(j.get("job_id") or "").strip()
            if jid:
                if jid not in out:
                    out.add(jid)
                    page_added += 1

        if page_added == 0 and page >= 2:
            break

    return out


# ----------------------------
# Persisted month-over-month metrics table
# ----------------------------

def ensure_metrics_table(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS company_still_open_monthly (
        metric_month TEXT NOT NULL,
        employer_norm TEXT NOT NULL,
        employer_name TEXT DEFAULT '',
        window_start TEXT NOT NULL,
        window_end TEXT NOT NULL,
        window_jobs INTEGER DEFAULT 0,
        open_now_jobs INTEGER DEFAULT 0,
        still_open_jobs INTEGER DEFAULT 0,
        still_open_rate REAL DEFAULT 0.0,
        computed_utc TEXT DEFAULT '',
        PRIMARY KEY (metric_month, employer_norm)
    );
    """)
    conn.commit()


def upsert_company_still_open_monthly(
    conn: sqlite3.Connection,
    metric_month: str,
    employer_norm: str,
    employer_name: str,
    window_start: date,
    window_end: date,
    window_jobs: int,
    open_now_jobs: int,
    still_open_jobs: int,
):
    rate = 0.0
    if window_jobs > 0:
        rate = still_open_jobs / float(window_jobs)

    conn.execute("""
    INSERT INTO company_still_open_monthly (
        metric_month, employer_norm, employer_name,
        window_start, window_end,
        window_jobs, open_now_jobs, still_open_jobs, still_open_rate,
        computed_utc
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(metric_month, employer_norm) DO UPDATE SET
        employer_name = excluded.employer_name,
        window_start = excluded.window_start,
        window_end = excluded.window_end,
        window_jobs = excluded.window_jobs,
        open_now_jobs = excluded.open_now_jobs,
        still_open_jobs = excluded.still_open_jobs,
        still_open_rate = excluded.still_open_rate,
        computed_utc = excluded.computed_utc
    """, (
        metric_month, employer_norm, employer_name,
        window_start.isoformat(), window_end.isoformat(),
        int(window_jobs), int(open_now_jobs), int(still_open_jobs), float(rate),
        utc_now_iso(),
    ))
    conn.commit()


def get_window_job_ids(conn: sqlite3.Connection, employer_norm: str, start_d: date, end_d_inclusive: date) -> Set[str]:
    start_s = start_d.isoformat()
    end_excl = (end_d_inclusive + timedelta(days=1)).isoformat()
    rows = conn.execute("""
        SELECT DISTINCT job_id
        FROM jobs
        WHERE employer_norm = ?
          AND first_seen_run_date >= ?
          AND first_seen_run_date < ?
          AND job_id IS NOT NULL AND TRIM(job_id) <> ''
    """, (employer_norm, start_s, end_excl)).fetchall()
    return {str(r[0]) for r in rows if r and r[0]}


# ----------------------------
# NEW: Weekly (3-week) persisted metrics table
# ----------------------------

def ensure_weekly3_table(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS company_still_open_weekly3 (
        week_end TEXT NOT NULL,
        employer_norm TEXT NOT NULL,
        employer_name TEXT DEFAULT '',
        window_start TEXT NOT NULL,
        window_end TEXT NOT NULL,
        window_jobs INTEGER DEFAULT 0,
        open_now_jobs INTEGER DEFAULT 0,
        still_open_jobs INTEGER DEFAULT 0,
        still_open_rate REAL DEFAULT 0.0,
        computed_utc TEXT DEFAULT '',
        PRIMARY KEY (week_end, employer_norm)
    );
    """)
    conn.commit()


def upsert_company_still_open_weekly3(
    conn: sqlite3.Connection,
    week_end: date,
    employer_norm: str,
    employer_name: str,
    window_start: date,
    window_end: date,
    window_jobs: int,
    open_now_jobs: int,
    still_open_jobs: int,
):
    rate = 0.0
    if window_jobs > 0:
        rate = still_open_jobs / float(window_jobs)

    conn.execute("""
    INSERT INTO company_still_open_weekly3 (
        week_end, employer_norm, employer_name,
        window_start, window_end,
        window_jobs, open_now_jobs, still_open_jobs, still_open_rate,
        computed_utc
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(week_end, employer_norm) DO UPDATE SET
        employer_name = excluded.employer_name,
        window_start = excluded.window_start,
        window_end = excluded.window_end,
        window_jobs = excluded.window_jobs,
        open_now_jobs = excluded.open_now_jobs,
        still_open_jobs = excluded.still_open_jobs,
        still_open_rate = excluded.still_open_rate,
        computed_utc = excluded.computed_utc
    """, (
        week_end.isoformat(), employer_norm, employer_name,
        window_start.isoformat(), window_end.isoformat(),
        int(window_jobs), int(open_now_jobs), int(still_open_jobs), float(rate),
        utc_now_iso(),
    ))
    conn.commit()


# ----------------------------
# Pages
# ----------------------------

def build_daily_page(db_path: str) -> str:
    now = utc_now_iso()

    latest_daily = find_latest("new_companies_")
    latest_monthly = find_latest("top_companies_")

    daily_rows = []
    run_date = None
    if latest_daily:
        run_date = latest_daily.replace("new_companies_", "").replace(".csv", "")
        daily_rows = read_csv(os.path.join(OUTPUT_DIR, latest_daily))

    monthly_rows = []
    month_label = None
    if latest_monthly:
        month_label = latest_monthly.replace("top_companies_", "").replace(".csv", "")
        monthly_rows = read_csv(os.path.join(OUTPUT_DIR, latest_monthly))

    stats = read_latest_run_stats_for_date(db_path, run_date) if run_date else None

    meta = (
        f'{pill("Last updated")} {now}<br/>'
        f'{pill("Latest daily run")} {run_date or "No runs yet"} &nbsp;'
        f'{pill("Latest monthly report")} {month_label or "None yet"}'
    )

    body = ""
    body += render_stats_grid(stats, extra_note="Daily tab uses the latest run’s API-returned counts.")

    body += """
  <h2>New companies detected (latest daily run)</h2>
  <p class="muted">Companies first seen on the latest daily run. “Verified by Places” means a Google Places match was found whose coordinates fall inside the Montgomery County boundary.</p>

  <table>
    <thead>
      <tr>
        <th>Company</th>
        <th>Verified by Places</th>
        <th>Unique jobs (today)</th>
        <th>Sample job 1</th>
        <th>Sample job 2</th>
        <th>Sample job 3</th>
        <th>Verify (MD)</th>
      </tr>
    </thead>
    <tbody>
"""
    if daily_rows:
        for r in daily_rows:
            verified = (r.get("places_verified", "") or "").strip() in ("1", "true", "True")
            addr = (r.get("places_address", "") or "").strip()
            badge = f'<span class="badge {"verified" if verified else "unverified"}" title="{addr}">' + \
                    ("Verified" if verified else "Unverified") + "</span>"

            body += "<tr>"
            body += f"<td>{r.get('company','')}</td>"
            body += f"<td>{badge}</td>"
            body += f"<td>{r.get('unique_jobs_found_today','')}</td>"
            body += f"<td>{link(r.get('sample_1_title',''), r.get('sample_1_link',''))}</td>"
            body += f"<td>{link(r.get('sample_2_title',''), r.get('sample_2_link',''))}</td>"
            body += f"<td>{link(r.get('sample_3_title',''), r.get('sample_3_link',''))}</td>"
            md_link = r.get("md_entity_search_link", "https://egov.maryland.gov/businessexpress/entitysearch")
            body += f"<td><a href='{md_link}' target='_blank' rel='noopener'>MD Business Express</a></td>"
            body += "</tr>"
    else:
        body += "<tr><td colspan='7'>No new companies found (or no daily runs yet).</td></tr>"

    body += """
    </tbody>
  </table>
"""

    body += """
  <h2>Top companies hiring (latest monthly report)</h2>
  <p class="muted">Ranked by unique job IDs captured in that month.</p>

  <table>
    <thead>
      <tr>
        <th>Rank</th>
        <th>Company</th>
        <th>Unique postings</th>
      </tr>
    </thead>
    <tbody>
"""
    if monthly_rows:
        for i, r in enumerate(monthly_rows[:30], start=1):
            body += "<tr>"
            body += f"<td>{i}</td>"
            body += f"<td>{r.get('company','')}</td>"
            body += f"<td>{r.get('unique_job_postings_found','')}</td>"
            body += "</tr>"
    else:
        body += "<tr><td colspan='3'>No monthly report found yet.</td></tr>"

    body += """
    </tbody>
  </table>
"""
    return html_shell("MoCo Hiring Monitor — Daily", "daily", meta, body)


def build_weekly_page(db_path: str) -> str:
    now = utc_now_iso()
    meta = f'{pill("Last updated")} {now}'

    conn = connect_db(db_path)
    if not conn:
        body = "<h2>Weekly</h2><p>No database found yet. Run at least one daily scan.</p>"
        return html_shell("MoCo Hiring Monitor — Weekly", "weekly", meta, body)

    today = datetime.utcnow().date()  # instead of date.today()
    start, end = pick_week_range_with_fallback(conn, today)
    start_s = start.isoformat()
    end_excl = (end + timedelta(days=1)).isoformat()

    week_stats = sum_run_stats_over_range(db_path, start, end)

    excl_vals = sorted(EXCLUDE_WEEKLY_NORMS)
    excl_clause, excl_params = _sql_not_in_clause("employer_norm", excl_vals)

    new_cos = conn.execute(f"""
        SELECT employer_name, employer_norm, first_seen_run_date, places_verified, places_address
        FROM companies
        WHERE first_seen_run_date >= ?
          AND first_seen_run_date < ?
          {excl_clause}
        ORDER BY first_seen_run_date DESC, employer_name ASC
        LIMIT 250
    """, [start_s, end_excl] + excl_params).fetchall()

    norms = [r[1] for r in new_cos if r[1]]

    sample_job_by_norm: Dict[str, Tuple[str, str, str, str]] = {}

    if norms:
        placeholders = ",".join(["?"] * len(norms))
        rows = conn.execute(f"""
            SELECT employer_norm, job_title, apply_link, salary, job_requirements, first_seen_run_date
            FROM jobs
            WHERE first_seen_run_date >= ?
              AND first_seen_run_date < ?
              AND employer_norm IN ({placeholders})
            ORDER BY employer_norm ASC, first_seen_run_date DESC
        """, [start_s, end_excl] + norms).fetchall()

        for employer_norm, job_title, apply_link, salary, reqs, _d in rows:
            if employer_norm not in sample_job_by_norm:
                sample_job_by_norm[employer_norm] = (
                    job_title or "",
                    apply_link or "",
                    salary or "",
                    reqs or "",
                )

    raw_top = conn.execute(f"""
        SELECT employer_name, employer_norm, COUNT(DISTINCT job_id) AS cnt
        FROM jobs
        WHERE first_seen_run_date >= ?
          AND first_seen_run_date < ?
          AND employer_name IS NOT NULL
          AND TRIM(employer_name) <> ''
          {excl_clause}
        GROUP BY employer_norm
        ORDER BY cnt DESC
        LIMIT 30
    """, [start_s, end_excl] + excl_params).fetchall()

    top = [(company, cnt) for (company, _norm, cnt) in raw_top]

    conn.close()

    body = f"""
      <h2>Weekly</h2>
      <div class="meta">{pill("Week")} <b>{start_s}</b> (Sun) to <b>{end.isoformat()}</b> (Sat)</div>
    """

    body += render_stats_grid(
        week_stats,
        extra_note="Weekly tab stats are sums of daily run stats over the most recent completed week (true API-returned counts)."
    )

    body += """
      <h3 style="margin-top:22px;">New companies detected (week)</h3>
      <table>
        <thead>
          <tr>
            <th>Company</th>
            <th>First seen</th>
            <th>Verified by Places</th>
            <th>Sample job</th>
            <th>Salary</th>
            <th>Requirements</th>
          </tr>
        </thead>
        <tbody>
    """

    if new_cos:
        for (company, employer_norm, first_seen, places_verified, places_address) in new_cos:
            verified = (places_verified or 0) == 1
            badge = (
                f'<span class="badge {"verified" if verified else "unverified"}" '
                f'title="{places_address or ""}">'
                + ("Verified" if verified else "Unverified")
                + "</span>"
            )

            job_title, job_link, salary, reqs = sample_job_by_norm.get(employer_norm, ("", "", "", ""))

            body += "<tr>"
            body += f"<td>{company}</td>"
            body += f"<td>{first_seen}</td>"
            body += f"<td>{badge}</td>"
            body += f"<td>{link(job_title, job_link)}</td>"
            body += f"<td>{salary}</td>"
            body += f"<td>{reqs}</td>"
            body += "</tr>"
    else:
        body += "<tr><td colspan='6'>No new companies in the most recent completed week.</td></tr>"

    body += "</tbody></table>"

    body += """
      <h3 style="margin-top:22px;">Hiring companies (week)</h3>

      <table>
        <thead><tr><th>Rank</th><th>Company</th><th>Unique postings captured</th></tr></thead>
        <tbody>
    """
    if top:
        for i, (company, cnt) in enumerate(top, start=1):
            body += f"<tr><td>{i}</td><td>{company}</td><td>{cnt}</td></tr>"
    else:
        body += "<tr><td colspan='3'>No data for the most recent completed week.</td></tr>"
    body += "</tbody></table>"

    return html_shell("MoCo Hiring Monitor — Weekly", "weekly", meta, body)


def build_sector_weekly_page(db_path: str, title: str, active: str, field_tag: str) -> str:
    now = utc_now_iso()
    meta = f'{pill("Last updated")} {now} &nbsp; {pill("Sector")} <code>{field_tag}</code>'

    conn = connect_db(db_path)
    if not conn:
        body = f"<h2>{title}</h2><p>No database found yet. Run at least one daily scan.</p>"
        return html_shell(f"MoCo Hiring Monitor — {title}", active, meta, body)

    today = datetime.utcnow().date()  # instead of date.today()
    start, end = pick_week_range_with_fallback(conn, today)
    start_s = start.isoformat()
    end_excl = (end + timedelta(days=1)).isoformat()

    like_tag = f"%,{field_tag},%"

    stats = sector_week_stats(db_path, field_tag, start, end)

    excl_vals = sorted(EXCLUDE_WEEKLY_NORMS)
    excl_clause, excl_params = _sql_not_in_clause("employer_norm", excl_vals)

    top_cos = conn.execute(f"""
        SELECT employer_name, employer_norm, COUNT(DISTINCT job_id) AS cnt
        FROM jobs
        WHERE first_seen_run_date >= ?
          AND first_seen_run_date < ?
          AND fields LIKE ?
          AND employer_name IS NOT NULL AND TRIM(employer_name) <> ''
          {excl_clause}
        GROUP BY employer_norm
        ORDER BY cnt DESC
        LIMIT 3000
    """, [start_s, end_excl, like_tag] + excl_params).fetchall()

    new_cos = conn.execute(f"""
        SELECT DISTINCT c.employer_name, c.first_seen_run_date, c.places_verified, c.places_address, c.employer_norm
        FROM companies c
        JOIN jobs j ON j.employer_norm = c.employer_norm
        WHERE c.first_seen_run_date >= ?
          AND c.first_seen_run_date < ?
          AND j.first_seen_run_date >= ?
          AND j.first_seen_run_date < ?
          AND j.fields LIKE ?
          {excl_clause.replace("employer_norm", "c.employer_norm")}
        ORDER BY c.first_seen_run_date DESC, c.employer_name ASC
        LIMIT 250
    """, [start_s, end_excl, start_s, end_excl, like_tag] + excl_params).fetchall()

    jobs = conn.execute(f"""
        SELECT first_seen_run_date, employer_name, job_title, apply_link, salary, job_requirements, employer_norm
        FROM jobs
        WHERE first_seen_run_date >= ?
          AND first_seen_run_date < ?
          AND fields LIKE ?
          {excl_clause}
        ORDER BY first_seen_run_date DESC
        LIMIT 400
    """, [start_s, end_excl, like_tag] + excl_params).fetchall()

    conn.close()

    body = f"""
      <h2>{title}</h2>
      <div class="meta">{pill("Week")} <b>{start_s}</b> (Sun) to <b>{end.isoformat()}</b> (Sat)</div>
    """

    body += render_stats_grid(
        stats,
        extra_note="Sector tab stats count captured jobs tagged to this sector during the most recent completed week."
    )

    body += """
      <h3>Top hiring companies (sector, week)</h3>
      <table>
        <thead><tr><th>Rank</th><th>Company</th><th>Unique postings captured</th></tr></thead>
        <tbody>
    """
    if top_cos:
        for i, (company, _norm, cnt) in enumerate(top_cos, start=1):
            body += f"<tr><td>{i}</td><td>{company}</td><td>{cnt}</td></tr>"
    else:
        body += "<tr><td colspan='3'>No sector-tagged jobs in the most recent completed week.</td></tr>"
    body += "</tbody></table>"

    body += """
      <h3 style="margin-top:22px;">New companies (sector, week)</h3>
      <table>
        <thead><tr><th>Company</th><th>First seen</th><th>Verified by Places</th></tr></thead>
        <tbody>
    """
    if new_cos:
        for (company, first_seen, places_verified, places_address, _norm) in new_cos:
            verified = (places_verified or 0) == 1
            badge = f'<span class="badge {"verified" if verified else "unverified"}" title="{places_address or ""}">' + \
                    ("Verified" if verified else "Unverified") + "</span>"
            body += f"<tr><td>{company}</td><td>{first_seen}</td><td>{badge}</td></tr>"
    else:
        body += "<tr><td colspan='3'>No new sector companies in the most recent completed week.</td></tr>"
    body += "</tbody></table>"

    body += """
      <h3 style="margin-top:22px;">Jobs captured (sector, week)</h3>
      <table>
        <thead><tr><th>Date</th><th>Company</th><th>Title</th><th>Salary</th><th>Requirements</th></tr></thead>
        <tbody>
    """
    if jobs:
        for d, company, title_txt, url, salary, reqs, _norm in jobs:
            body += f"<tr><td>{d}</td><td>{company}</td><td>{link(title_txt, url)}</td><td>{salary or ''}</td><td>{reqs or ''}</td></tr>"
    else:
        body += "<tr><td colspan='5'>No jobs for this sector in the most recent completed week.</td></tr>"
    body += "</tbody></table>"

    return html_shell(f"MoCo Hiring Monitor — {title}", active, meta, body)


# ----------------------------
# Company indicators + STILL-OPEN metric (new proxy)
# ----------------------------

def build_company_indicators_page(db_path: str) -> str:
    now = utc_now_iso()
    meta = f'{pill("Last updated")} {now}'

    conn = connect_db(db_path)
    if not conn:
        body = "<h2>Company Indicators</h2><p>No database found yet. Run at least one daily scan.</p>"
        return html_shell("MoCo Hiring Monitor — Company Indicators", "indicators", meta, body)

    ensure_metrics_table(conn)
    ensure_weekly3_table(conn)

    rapidapi_key = os.getenv("RAPIDAPI_KEY", "").strip()
    rapidapi_host = os.getenv("RAPIDAPI_HOST", "jsearch.p.rapidapi.com").strip()
    session = build_retry_session()

    today = date.today()
    this_month = today.strftime("%Y-%m")
    first_this = today.replace(day=1)
    last_month_end = first_this - timedelta(days=1)
    last_month = last_month_end.strftime("%Y-%m")

    this_start, this_end = month_start_end(this_month)
    last_start, last_end = month_start_end(last_month)

    # window for "still open now" check (30-45 days ago)
    window_end = today - timedelta(days=30)
    window_start = today - timedelta(days=45)

    # simple exclusions for indicators page (keep if you want)
    EXCLUDE = set([
        "uber", "uber technologies", "walmart", "doordash", "lyft"
    ])

    def norm_simple(s: str) -> str:
        return (s or "").strip().lower()

    this_counts = conn.execute("""
        SELECT employer_name, employer_norm, COUNT(DISTINCT job_id) AS cnt
        FROM jobs
        WHERE first_seen_run_date >= ?
          AND first_seen_run_date < ?
          AND employer_name IS NOT NULL AND TRIM(employer_name) <> ''
        GROUP BY employer_norm
    """, (this_start.isoformat(), this_end.isoformat())).fetchall()

    last_counts = conn.execute("""
        SELECT employer_name, employer_norm, COUNT(DISTINCT job_id) AS cnt
        FROM jobs
        WHERE first_seen_run_date >= ?
          AND first_seen_run_date < ?
          AND employer_name IS NOT NULL AND TRIM(employer_name) <> ''
        GROUP BY employer_norm
    """, (last_start.isoformat(), last_end.isoformat())).fetchall()

    # maps by norm (stable)
    this_map = {}
    for name, en, cnt in this_counts:
        if norm_simple(name) in EXCLUDE:
            continue
        this_map[en] = {"name": name, "cnt": int(cnt or 0)}

    last_map = {}
    for name, en, cnt in last_counts:
        if norm_simple(name) in EXCLUDE:
            continue
        last_map[en] = int(cnt or 0)

    # top N by this month captured postings
    TOP_N = int(os.getenv("INDICATORS_TOP_N", "50"))
    companies_sorted = sorted(this_map.items(), key=lambda x: x[1]["cnt"], reverse=True)[:TOP_N]

    def top_titles_for_norm(employer_norm: str, start_d: date, end_d: date) -> List[str]:
        rows = conn.execute("""
            SELECT job_title, COUNT(*) AS c
            FROM jobs
            WHERE employer_norm = ?
              AND first_seen_run_date >= ?
              AND first_seen_run_date < ?
              AND job_title IS NOT NULL AND TRIM(job_title) <> ''
            GROUP BY job_title
            ORDER BY c DESC
            LIMIT 3
        """, (employer_norm, start_d.isoformat(), end_d.isoformat())).fetchall()
        return [r[0] for r in rows]

    table_rows = []

    weeks3 = last_n_completed_weeks(today, 3)  # newest first

    for employer_norm, obj in companies_sorted:
        company = obj["name"]
        this_cnt = obj["cnt"]
        last_cnt = last_map.get(employer_norm, 0)

        pct = "NA"
        if last_cnt and last_cnt > 0:
            pct = f"{((this_cnt - last_cnt) / last_cnt) * 100:.1f}%"

        titles_this = top_titles_for_norm(employer_norm, this_start, this_end)
        titles_last = top_titles_for_norm(employer_norm, last_start, last_end)

        # --- still-open metric ---
        window_ids = get_window_job_ids(conn, employer_norm, window_start, window_end)
        open_now_ids = fetch_open_job_ids_for_company(
            company,
            rapidapi_key,
            rapidapi_host,
            session=session,
            max_pages=6,
            per_call_sleep_s=0.25,
            connect_timeout_s=10,
            read_timeout_s=90,
        ) if rapidapi_key else set()

        still_open = window_ids.intersection(open_now_ids)
        still_open_count = len(still_open)
        window_total = len(window_ids)
        open_now_total = len(open_now_ids)
        still_open_rate = (still_open_count / window_total) if window_total > 0 else 0.0

        # persist month snapshot (updates daily within month)
        upsert_company_still_open_monthly(
            conn,
            metric_month=this_month,
            employer_norm=employer_norm,
            employer_name=company,
            window_start=window_start,
            window_end=window_end,
            window_jobs=window_total,
            open_now_jobs=open_now_total,
            still_open_jobs=still_open_count,
        )

        # persist last 3 completed week snapshots (updates daily)
        for w_start, w_end in weeks3:
            w_ids = get_window_job_ids(conn, employer_norm, w_start, w_end)
            w_still_open = w_ids.intersection(open_now_ids)
            upsert_company_still_open_weekly3(
                conn,
                week_end=w_end,
                employer_norm=employer_norm,
                employer_name=company,
                window_start=w_start,
                window_end=w_end,
                window_jobs=len(w_ids),
                open_now_jobs=open_now_total,
                still_open_jobs=len(w_still_open),
            )

        table_rows.append({
            "company": company,
            "this_cnt": this_cnt,
            "last_cnt": last_cnt if last_cnt else "NA",
            "pct": pct,
            "titles_this": ", ".join(titles_this) if titles_this else "",
            "titles_last": ", ".join(titles_last) if titles_last else "",
            "open_now": open_now_total,
            "window_total": window_total,
            "still_open": still_open_count,
            "still_open_rate": f"{still_open_rate*100:.1f}%" if window_total > 0 else "NA",
        })

    persisted_cnt = conn.execute("""
        SELECT COUNT(*)
        FROM company_still_open_monthly
        WHERE metric_month = ?
    """, (this_month,)).fetchone()[0] or 0

    conn.close()

    body = f"""
      <h2>Company Indicators</h2>
      <p class="muted">
        Comparison of unique postings captured in <b>{this_month}</b> vs <b>{last_month}</b>.
      </p>

      <div class="hint small">
        <b>Hard-to-fill proxy (updated):</b><br/>
        For each company, we take job IDs captured <b>{window_start.isoformat()}</b> to <b>{window_end.isoformat()}</b> (30–45 days ago),
        then query JSearch for the company’s open jobs “now” and count the overlap by <code>job_id</code>.<br/>
        Saved to DB table: <code>company_still_open_monthly</code> (rows this month so far: <b>{persisted_cnt}</b>).
      </div>

      <table>
        <thead>
          <tr>
            <th>Company</th>
            <th>{this_month} unique postings (captured)</th>
            <th>{last_month} unique postings (captured)</th>
            <th>% change</th>
            <th>Top titles ({this_month})</th>
            <th>Top titles ({last_month})</th>
            <th>Jobs captured 30 – 45d ago</th>
            <th>Jobs Still Open 30 - 45 days later (overlap)</th>
            <th>Jobs Still Open rate</th>
          </tr>
        </thead>
        <tbody>
    """

    if table_rows:
        for r in table_rows:
            body += (
                "<tr>"
                f"<td>{r['company']}</td>"
                f"<td>{r['this_cnt']}</td>"
                f"<td>{r['last_cnt']}</td>"
                f"<td>{r['pct']}</td>"
                f"<td>{r['titles_this']}</td>"
                f"<td>{r['titles_last']}</td>"
                f"<td>{r['window_total']}</td>"
                f"<td>{r['still_open']}</td>"
                f"<td>{r['still_open_rate']}</td>"
                "</tr>"
            )
    else:
        body += "<tr><td colspan='10'>No data for this month yet.</td></tr>"

    body += """
        </tbody>
      </table>

      <div class="hint small">
        Notes:
        <ul>
          <li>This measures “same posting still open” (same <code>job_id</code>). Re-posted roles with new IDs won’t count as still-open.</li>
          <li>“Open jobs now” is approximate; large employers may require more paging to fully capture.</li>
        </ul>
      </div>
    """

    return html_shell("MoCo Hiring Monitor — Company Indicators", "indicators", meta, body)


# ----------------------------
# Trends + Search
# ----------------------------

def pick_week_range_with_fallback(conn: sqlite3.Connection, today: date) -> Tuple[date, date]:
    # Prefer last completed Sun->Sat
    start, end = most_recent_completed_week(today)
    start_s = start.isoformat()
    end_excl = (end + timedelta(days=1)).isoformat()

    has_data = conn.execute("""
        SELECT 1
        FROM jobs
        WHERE first_seen_run_date >= ?
          AND first_seen_run_date < ?
        LIMIT 1
    """, (start_s, end_excl)).fetchone() is not None

    if has_data:
        return start, end

    # Fallback: current week-to-date (Sun -> today)
    cur_start, cur_end = sunday_to_saturday_range(today)
    return cur_start, today  # week-to-date




def build_trends_page(db_path: str) -> str:
    now = utc_now_iso()
    meta = f'{pill("Last updated")} {now}'

    conn = connect_db(db_path)
    if not conn:
        body = "<h2>Trends</h2><p>No database found yet. Run at least one daily scan.</p>"
        return html_shell("MoCo Hiring Monitor — Trends", "trends", meta, body)

    today = date.today()

    # --- NEW: Hiring delays tables ---
    this_month = today.strftime("%Y-%m")
    weeks3 = last_n_completed_weeks(today, 3)  # newest first
    week_ends = [w[1].isoformat() for w in weeks3]  # [newest, mid, oldest]

    def slowdown_flag(window_jobs: int, still_open_jobs: int, still_open_rate: float) -> bool:
        if window_jobs < 3:
            return False
        if still_open_jobs >= 5:
            return True
        if still_open_rate >= 0.50:
            return True
        return False

    month_rows = conn.execute("""
        SELECT employer_name, employer_norm, window_jobs, still_open_jobs, still_open_rate, window_start, window_end
        FROM company_still_open_monthly
        WHERE metric_month = ?
    """, (this_month,)).fetchall()

    month_candidates = []
    for name, _en, wjobs, so, rate, ws, we in month_rows:
        wjobs = int(wjobs or 0)
        so = int(so or 0)
        rate = float(rate or 0.0)
        if slowdown_flag(wjobs, so, rate):
            month_candidates.append({
                "company": name or "",
                "window_jobs": wjobs,
                "still_open": so,
                "rate": rate,
                "window": f"{ws} to {we}"
            })

    month_candidates.sort(key=lambda x: (x["still_open"], x["rate"]), reverse=True)
    month_candidates = month_candidates[:50]

    weekly_rows = []
    if len(week_ends) == 3:
        placeholders = ",".join(["?"] * 3)
        weekly_rows = conn.execute(f"""
            SELECT employer_name, employer_norm, week_end, window_jobs, still_open_jobs, still_open_rate
            FROM company_still_open_weekly3
            WHERE week_end IN ({placeholders})
        """, week_ends).fetchall()

    by_company: Dict[str, Dict[str, Any]] = {}
    for name, en, wend, wjobs, so, rate in weekly_rows:
        en = en or ""
        if en not in by_company:
            by_company[en] = {"company": name or "", "points": {}}
        by_company[en]["points"][wend] = {
            "window_jobs": int(wjobs or 0),
            "still_open": int(so or 0),
            "rate": float(rate or 0.0),
        }

    trend_candidates = []
    if len(week_ends) == 3:
        newest, mid, oldest = week_ends[0], week_ends[1], week_ends[2]
        for _en, obj in by_company.items():
            pts = obj["points"]
            if oldest not in pts or mid not in pts or newest not in pts:
                continue

            r0 = pts[oldest]["rate"]
            r1 = pts[mid]["rate"]
            r2 = pts[newest]["rate"]
            so_tot = pts[oldest]["still_open"] + pts[mid]["still_open"] + pts[newest]["still_open"]
            w_tot = pts[oldest]["window_jobs"] + pts[mid]["window_jobs"] + pts[newest]["window_jobs"]

            if w_tot < 6:
                continue

            avg_rate = (r0 + r1 + r2) / 3.0
            delta = r2 - r0

            is_flag = (avg_rate >= 0.45 and so_tot >= 6) or (delta >= 0.15 and so_tot >= 4) or (r2 >= 0.60 and pts[newest]["still_open"] >= 3)
            if is_flag:
                trend_candidates.append({
                    "company": obj["company"],
                    "avg_rate": avg_rate,
                    "delta": delta,
                    "so_tot": so_tot,
                    "w_tot": w_tot,
                    "r_old": r0,
                    "r_mid": r1,
                    "r_new": r2,
                    "so_new": pts[newest]["still_open"],
                    "w_new": pts[newest]["window_jobs"],
                    "week_old": oldest,
                    "week_mid": mid,
                    "week_new": newest,
                })

    trend_candidates.sort(key=lambda x: (x["so_new"], x["delta"], x["avg_rate"]), reverse=True)
    trend_candidates = trend_candidates[:50]

    def pct(x: float) -> str:
        return f"{x*100:.1f}%"

    hiring_tables = f"""
  <h2>Trends</h2>

  <h3 style="margin-top:10px;">Companies with hiring delays</h3>
  <p class="muted">
    “Hiring delays” = postings first seen earlier that appear to still be open now (same <code>job_id</code> overlap with current JSearch results).
    Computed for tracked companies (top N by monthly captured postings).
  </p>

  <div class="two-col">
    <div class="card">
      <h3>Month snapshot — {this_month}</h3>
      <div class="small muted">Window shown is 30–45 days ago relative to today.</div>
      <table style="margin-top:10px;">
        <thead>
          <tr>
            <th>Company</th>
            <th>Window</th>
            <th>Window jobs</th>
            <th>Still open</th>
            <th>Rate</th>
          </tr>
        </thead>
        <tbody>
          {''.join([
            f"<tr><td>{r['company']}</td><td>{r['window']}</td><td>{r['window_jobs']}</td><td>{r['still_open']}</td><td>{pct(r['rate'])}</td></tr>"
            for r in month_candidates
          ]) if month_candidates else "<tr><td colspan='5'>No hiring-delay companies flagged yet for this month (or metrics not computed yet).</td></tr>"}
        </tbody>
      </table>
    </div>

    <div class="card">
      <h3>3-week trend (completed weeks)</h3>
      <div class="small muted">
        Weeks end on Saturday. Shows rate change from oldest → newest week.
      </div>
      <table style="margin-top:10px;">
        <thead>
          <tr>
            <th>Company</th>
            <th>Newest week end</th>
            <th>Newest still open</th>
            <th>Newest rate</th>
            <th>Δ rate (old→new)</th>
            <th>Avg rate</th>
          </tr>
        </thead>
        <tbody>
          {''.join([
            f"<tr>"
            f"<td>{t['company']}</td>"
            f"<td>{t['week_new']}</td>"
            f"<td>{t['so_new']} / {t['w_new']}</td>"
            f"<td>{pct(t['r_new'])}</td>"
            f"<td>{pct(t['delta'])}</td>"
            f"<td>{pct(t['avg_rate'])}</td>"
            f"</tr>"
            for t in trend_candidates
          ]) if trend_candidates else "<tr><td colspan='6'>No 3-week hiring-delay trends flagged yet (or not enough weekly history).</td></tr>"}
        </tbody>
      </table>
    </div>
  </div>

  <hr style="margin:22px 0; border:none; border-top:1px solid #eee;" />
"""

    # --- existing trends calculations ---
    months = []
    d = today.replace(day=1)
    for _ in range(12):
        months.append(d.strftime("%Y-%m"))
        prev_end = d - timedelta(days=1)
        d = prev_end.replace(day=1)
    months.reverse()

    req_tags = [
        "no_experience",
        "under_3_years_experience",
        "more_than_3_years_experience",
        "no_degree",
    ]

    title_keywords = {
        "engineer": "engineer",
        "chemist": "chemist",
        "software_developer": "software developer",
        "data_scientist": "data scientist",
    }

    def month_range_iso(m: str):
        start_d, end_d = month_start_end(m)
        return start_d.isoformat(), end_d.isoformat()

    req_series: Dict[str, List[int]] = {t: [] for t in req_tags}
    for m in months:
        start_iso, end_iso = month_range_iso(m)
        for t in req_tags:
            cnt = conn.execute("""
                SELECT COUNT(DISTINCT job_id)
                FROM jobs
                WHERE first_seen_run_date >= ?
                  AND first_seen_run_date < ?
                  AND job_requirements LIKE ?
            """, (start_iso, end_iso, f"%{t}%")).fetchone()[0]
            req_series[t].append(int(cnt or 0))

    title_series: Dict[str, List[int]] = {k: [] for k in title_keywords.keys()}
    for m in months:
        start_iso, end_iso = month_range_iso(m)
        for key, needle in title_keywords.items():
            cnt = conn.execute("""
                SELECT COUNT(DISTINCT job_id)
                FROM jobs
                WHERE first_seen_run_date >= ?
                  AND first_seen_run_date < ?
                  AND job_title IS NOT NULL
                  AND LOWER(job_title) LIKE ?
            """, (start_iso, end_iso, f"%{needle.lower()}%")).fetchone()[0]
            title_series[key].append(int(cnt or 0))

    conn.close()

    selectable = {}
    for t in req_tags:
        selectable[f"req:{t}"] = {"label": f"Requirement: {t}", "values": req_series[t]}
    for k in title_keywords.keys():
        selectable[f"title:{k}"] = {"label": f"Title contains: {k}", "values": title_series[k]}

    default_key = "req:no_experience" if "req:no_experience" in selectable else list(selectable.keys())[0]
    data_obj = {"months": months, "series": selectable, "default": default_key}
    data_json = json.dumps(data_obj)

    req_cols = req_tags
    title_cols = list(title_keywords.keys())

    body = hiring_tables + f"""
      <p class="muted">
        Monthly counts of captured <b>unique job IDs</b>. Use the dropdown to change the chart variable.
        Title keyword counts are based on <code>job_title</code> contains-match.
      </p>

      <div class="card">
        <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap;">
          <h3 style="margin:0;">Trend chart</h3>
          <div class="small muted">Hover the line to see month/value.</div>
          <div style="margin-left:auto; min-width:260px;">
            <label class="small muted" for="seriesSelect">Graph variable</label><br/>
            <select id="seriesSelect" style="padding:8px 10px; border:1px solid #ddd; border-radius:10px; width:100%;"></select>
          </div>
        </div>

        <div style="position:relative; margin-top:10px;">
          <svg id="trendSvg" width="100%" viewBox="0 0 900 260" role="img" aria-label="Trend chart"></svg>
          <div id="tooltip" style="
                position:absolute; display:none; pointer-events:none;
                background:#111; color:#fff; padding:6px 8px; border-radius:10px;
                font-size:12px; transform: translate(-50%, -120%);
              "></div>
        </div>
      </div>

      <h3 style="margin-top:22px;">Monthly table — Requirements</h3>
      <p class="small muted">Counts where <code>job_requirements</code> contains the tag.</p>

      <table>
        <thead>
          <tr>
            <th>Month</th>
            {"".join([f"<th>{c}</th>" for c in req_cols])}
          </tr>
        </thead>
        <tbody>
    """

    for i, m in enumerate(months):
        body += "<tr><td>{}</td>{}</tr>".format(
            m,
            "".join([f"<td>{req_series[c][i]}</td>" for c in req_cols])
        )

    body += f"""
        </tbody>
      </table>

      <h3 style="margin-top:22px;">Monthly table — Title keywords</h3>
      <p class="small muted">Counts where the job title contains the keyword.</p>

      <table>
        <thead>
          <tr>
            <th>Month</th>
            {"".join([f"<th>{c}</th>" for c in title_cols])}
          </tr>
        </thead>
        <tbody>
    """

    for i, m in enumerate(months):
        body += "<tr><td>{}</td>{}</tr>".format(
            m,
            "".join([f"<td>{title_series[c][i]}</td>" for c in title_cols])
        )

    body += f"""
        </tbody>
      </table>

      <script>
        const DATA = {data_json};

        const svg = document.getElementById('trendSvg');
        const tooltip = document.getElementById('tooltip');
        const select = document.getElementById('seriesSelect');

        const W = 900, H = 260;
        const padL = 55, padR = 18, padT = 18, padB = 38;

        function clearSvg() {{
          while (svg.firstChild) svg.removeChild(svg.firstChild);
        }}

        function el(name, attrs = {{}}) {{
          const n = document.createElementNS('http://www.w3.org/2000/svg', name);
          for (const [k,v] of Object.entries(attrs)) n.setAttribute(k, String(v));
          return n;
        }}

        function xAt(i, n) {{
          if (n <= 1) return padL;
          return padL + (i * (W - padL - padR) / (n - 1));
        }}

        function yAt(v, maxY) {{
          return padT + (H - padT - padB) * (1 - (v / maxY));
        }}

        function render(key) {{
          const months = DATA.months;
          const s = DATA.series[key];
          const vals = (s && s.values) ? s.values : [];
          const label = (s && s.label) ? s.label : key;

          const maxY = Math.max(1, ...vals);
          clearSvg();

          svg.appendChild(el('rect', {{x:0, y:0, width:W, height:H, fill:'white'}}));
          svg.appendChild(el('line', {{x1:padL, y1:H-padB, x2:W-padR, y2:H-padB, stroke:'#ddd'}}));
          svg.appendChild(el('line', {{x1:padL, y1:padT, x2:padL, y2:H-padB, stroke:'#ddd'}}));

          svg.appendChild(el('text', {{x:padL, y:14, fill:'#111', 'font-size':'12'}})).textContent = label;

          for (let i=0; i<months.length; i+=2) {{
            const tx = xAt(i, months.length);
            const t = el('text', {{x:tx, y:H-14, fill:'#666', 'font-size':'10', 'text-anchor':'middle'}});
            t.textContent = months[i];
            svg.appendChild(t);
          }}

          const pts = vals.map((v,i) => `${{xAt(i, months.length).toFixed(1)}},${{yAt(v, maxY).toFixed(1)}}`).join(' ');
          svg.appendChild(el('polyline', {{points: pts, fill:'none', stroke:'#111', 'stroke-width':'2'}}));

          const vline = el('line', {{x1:padL, y1:padT, x2:padL, y2:H-padB, stroke:'#bbb', 'stroke-dasharray':'4 4', opacity:'0'}});
          const dot = el('circle', {{cx:padL, cy:padT, r:'4', fill:'#111', opacity:'0'}});
          svg.appendChild(vline);
          svg.appendChild(dot);

          function clamp(n, a, b){{ return Math.max(a, Math.min(b, n)); }}

          function onMove(evt) {{
            const rect = svg.getBoundingClientRect();
            const mx = evt.clientX - rect.left;
            const svgX = (mx / rect.width) * W;

            const n = months.length;
            const idxFloat = (n <= 1) ? 0 : ((svgX - padL) / (W - padL - padR)) * (n - 1);
            const idx = clamp(Math.round(idxFloat), 0, n-1);

            const x = xAt(idx, n);
            const v = vals[idx] ?? 0;
            const y = yAt(v, maxY);

            vline.setAttribute('x1', x);
            vline.setAttribute('x2', x);
            vline.setAttribute('opacity', '1');

            dot.setAttribute('cx', x);
            dot.setAttribute('cy', y);
            dot.setAttribute('opacity', '1');

            tooltip.style.display = 'block';
            tooltip.textContent = `${{months[idx]}} — ${{v}}`;
            tooltip.style.left = `${{(x / W) * rect.width}}px`;
            tooltip.style.top  = `${{(y / H) * rect.height}}px`;
          }}

          function onLeave() {{
            vline.setAttribute('opacity', '0');
            dot.setAttribute('opacity', '0');
            tooltip.style.display = 'none';
          }}

          svg.onmousemove = onMove;
          svg.onmouseleave = onLeave;
        }}

        const entries = Object.entries(DATA.series)
          .map(([k,v]) => ({{key:k, label:v.label}}))
          .sort((a,b) => a.label.localeCompare(b.label));

        for (const e of entries) {{
          const opt = document.createElement('option');
          opt.value = e.key;
          opt.textContent = e.label;
          select.appendChild(opt);
        }}

        select.value = DATA.default;
        select.onchange = () => render(select.value);

        render(DATA.default);
      </script>
    """

    return html_shell("MoCo Hiring Monitor — Trends", "trends", meta, body)


def build_search_assets(db_path: str) -> str:
    now = utc_now_iso()
    meta = f'{pill("Last updated")} {now}'

    conn = connect_db(db_path)
    if not conn:
        index = {"generated_utc": now, "jobs": []}
    else:
        rows = conn.execute("""
            SELECT employer_name, job_title, first_seen_run_date, apply_link, salary, job_requirements, fields
            FROM jobs
            ORDER BY first_seen_run_date DESC
            LIMIT 50000
        """).fetchall()
        conn.close()
        index = {
            "generated_utc": now,
            "jobs": [{
                "company": r[0],
                "title": r[1],
                "date": r[2],
                "link": r[3],
                "salary": r[4] or "",
                "reqs": r[5] or "",
                "fields": r[6] or "",
            } for r in rows]
        }

    with open(os.path.join(DOCS_DIR, "search_index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f)

    body = """
      <h2>Search</h2>
      <p class="muted">Type a company name or keywords to search across all captured companies or jobs.</p>
      <input id="q" type="text" placeholder="e.g., Lockheed, Bethesda, Engineer, Bioinformatics..." />
      <div class="meta" id="meta"></div>

      <table>
        <thead><tr><th>Date</th><th>Company</th><th>Title</th><th>Salary</th><th>Reqs</th><th>Fields</th></tr></thead>
        <tbody id="rows"></tbody>
      </table>

      <script>
        async function loadIndex() {
          const res = await fetch('search_index.json');
          return await res.json();
        }

        function esc(s){
          return (s||'')
            .replaceAll('&','&amp;')
            .replaceAll('<','&lt;')
            .replaceAll('>','&gt;');
        }

        function render(items) {
          const tbody = document.getElementById('rows');
          tbody.innerHTML = items.map(it => {
            const title = it.link ? `<a href="${it.link}" target="_blank" rel="noopener">${esc(it.title)}</a>` : esc(it.title);
            return `<tr>
              <td>${esc(it.date)}</td>
              <td>${esc(it.company)}</td>
              <td>${title}</td>
              <td>${esc(it.salary)}</td>
              <td>${esc(it.reqs)}</td>
              <td>${esc(it.fields)}</td>
            </tr>`;
          }).join('');
        }

        (async () => {
          const idx = await loadIndex();
          const all = idx.jobs || [];
          document.getElementById('meta').textContent = `Total Database Last Refreshed: ${idx.generated_utc}. Total Job Records: ${all.length}.`;

          const input = document.getElementById('q');
          function run() {
            const q = (input.value || '').toLowerCase().trim();
            if (!q) { render(all.slice(0,200)); return; }
            const terms = q.split(/\\s+/).filter(Boolean);
            const out = [];
            for (const it of all) {
              const hay = ((it.company||'') + ' ' + (it.title||'')).toLowerCase();
              let ok = true;
              for (const t of terms) { if (!hay.includes(t)) { ok = false; break; } }
              if (ok) out.push(it);
              if (out.length >= 500) break;
            }
            render(out);
          }
          input.addEventListener('input', run);
          run();
        })();
      </script>
    """
    return html_shell("MoCo Hiring Monitor — Search", "search", meta, body)


# ----------------------------
# Build all docs/*.html
# ----------------------------

def build():
    ensure_dirs()
    db_path = os.getenv("DB_PATH", DB_PATH_DEFAULT)

    daily_html = build_daily_page(db_path)
    weekly_html = build_weekly_page(db_path)

    tech_html = build_sector_weekly_page(db_path, "Technology", "tech", "technology")
    life_html = build_sector_weekly_page(db_path, "Life Sciences", "life", "life_sciences")
    aero_html = build_sector_weekly_page(db_path, "Aero/Defense/Satellite", "aero", "aero_defense_sat")

    indicators_html = build_company_indicators_page(db_path)
    trends_html = build_trends_page(db_path)
    search_html = build_search_assets(db_path)

    with open(os.path.join(DOCS_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(daily_html)
    with open(os.path.join(DOCS_DIR, "weekly.html"), "w", encoding="utf-8") as f:
        f.write(weekly_html)
    with open(os.path.join(DOCS_DIR, "technology.html"), "w", encoding="utf-8") as f:
        f.write(tech_html)
    with open(os.path.join(DOCS_DIR, "lifesciences.html"), "w", encoding="utf-8") as f:
        f.write(life_html)
    with open(os.path.join(DOCS_DIR, "aerodefense.html"), "w", encoding="utf-8") as f:
        f.write(aero_html)
    with open(os.path.join(DOCS_DIR, "indicators.html"), "w", encoding="utf-8") as f:
        f.write(indicators_html)
    with open(os.path.join(DOCS_DIR, "trends.html"), "w", encoding="utf-8") as f:
        f.write(trends_html)
    with open(os.path.join(DOCS_DIR, "search.html"), "w", encoding="utf-8") as f:
        f.write(search_html)


if __name__ == "__main__":
    build()