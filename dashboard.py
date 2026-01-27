import os
import csv
import sqlite3
from datetime import datetime

OUTPUT_DIR = "./outputs"
DOCS_DIR = "./docs"
DB_PATH_DEFAULT = "./data/moco_jobs.sqlite"


def utc_now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def find_latest(prefix: str, suffix: str = ".csv"):
    files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(prefix) and f.endswith(suffix)]
    if not files:
        return None
    files.sort()
    return files[-1]


def read_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_latest_run_stats(db_path: str, run_date: str):
    if not os.path.exists(db_path):
        return None
    conn = sqlite3.connect(db_path)
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
            "jobs_scanned_count": row[0] or 0,
            "jobs_in_moco_count": row[1] or 0,
            "new_jobs_count": row[2] or 0,
            "new_companies_count": row[3] or 0,
            "started_utc": row[4] or "",
            "finished_utc": row[5] or ""
        }
    finally:
        conn.close()


def pill(text, cls=""):
    return f'<span class="pill {cls}">{text}</span>'


def build():
    os.makedirs(DOCS_DIR, exist_ok=True)

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

    stats = read_latest_run_stats(DB_PATH_DEFAULT, run_date) if run_date else None

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>MoCo Hiring Monitor</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px; }}
    .meta {{ color: #444; margin-bottom: 16px; }}
    .pill {{ display:inline-block; padding:2px 10px; border:1px solid #ddd; border-radius: 999px; font-size: 12px; margin-right: 8px; }}
    .pill.ok {{ border-color: #9ad29a; background: #f2fbf2; }}
    .pill.warn {{ border-color: #f0d090; background: #fff8e6; }}
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
  </style>
</head>
<body>
  <h1>MoCo Hiring Monitor</h1>

  <div class="meta">
    {pill("Last updated")} {now}<br/>
    {pill("Latest daily run")} {run_date or "No runs yet"} &nbsp;
    {pill("Latest monthly report")} {month_label or "None yet"}
  </div>
"""

    # Coverage stats tile
    if stats:
        html += f"""
  <div class="grid">
    <div class="card">
      <h3>Jobs scanned</h3>
      <div class="big">{stats["jobs_scanned_count"]}</div>
      <div class="small">API results returned today (before MoCo filter)</div>
    </div>
    <div class="card">
      <h3>Jobs inside MoCo</h3>
      <div class="big">{stats["jobs_in_moco_count"]}</div>
      <div class="small">After boundary filter (lat/lon → polygon)</div>
    </div>
    <div class="card">
      <h3>New companies</h3>
      <div class="big">{stats["new_companies_count"]}</div>
      <div class="small">First time seen in monitor today</div>
    </div>
    <div class="card">
      <h3>New unique jobs</h3>
      <div class="big">{stats["new_jobs_count"]}</div>
      <div class="small">Unique job IDs newly inserted today</div>
    </div>
  </div>
"""
    else:
        html += """
  <div class="card">
    <h3>Coverage stats</h3>
    <div class="small">Run at least one daily scan to populate coverage stats.</div>
  </div>
"""

    # New companies table
    html += """
  <h2>New companies detected (latest daily run)</h2>
  <p>This list shows companies first seen on the latest daily run. “Verified by Places” means a Google Places match was found whose coordinates fall inside the Montgomery County boundary.</p>

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

    def link(title, url):
        if url and title:
            return f'<a href="{url}" target="_blank" rel="noopener">{title}</a>'
        return title or ""

    if daily_rows:
        for r in daily_rows:
            verified = (r.get("places_verified", "") or "").strip() in ("1", "true", "True")
            addr = (r.get("places_address", "") or "").strip()
            badge = f'<span class="badge {"verified" if verified else "unverified"}" title="{addr}">' + \
                    ("Verified" if verified else "Unverified") + "</span>"

            html += "<tr>"
            html += f"<td>{r.get('company','')}</td>"
            html += f"<td>{badge}</td>"
            html += f"<td>{r.get('unique_jobs_found_today','')}</td>"
            html += f"<td>{link(r.get('sample_1_title',''), r.get('sample_1_link',''))}</td>"
            html += f"<td>{link(r.get('sample_2_title',''), r.get('sample_2_link',''))}</td>"
            html += f"<td>{link(r.get('sample_3_title',''), r.get('sample_3_link',''))}</td>"
            md_link = r.get("md_entity_search_link", "https://egov.maryland.gov/businessexpress/entitysearch")
            html += f"<td><a href='{md_link}' target='_blank' rel='noopener'>MD Business Express</a></td>"
            html += "</tr>"
    else:
        html += "<tr><td colspan='7'>No new companies found (or no daily runs yet).</td></tr>"

    html += """
    </tbody>
  </table>
"""

    # Monthly Top Hiring section (reads latest top_companies_YYYY-MM.csv)
    html += """
  <h2>Top companies hiring (latest monthly report)</h2>
  <p>Ranked by unique job IDs captured in that month.</p>

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
            html += "<tr>"
            html += f"<td>{i}</td>"
            html += f"<td>{r.get('company','')}</td>"
            html += f"<td>{r.get('unique_job_postings_found','')}</td>"
            html += "</tr>"
    else:
        html += "<tr><td colspan='3'>No monthly report found yet. Run <code>python monitor.py monthly --month YYYY-MM</code> to generate one.</td></tr>"

    html += """
    </tbody>
  </table>

  <hr style="margin: 28px 0; border: none; border-top: 1px solid #eee;" />
  <div class="small">
    Tip: In GitHub Actions, run daily scans and build this page automatically. Host <code>docs/</code> on GitHub Pages.
  </div>
</body>
</html>
"""

    with open(os.path.join(DOCS_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    build()
