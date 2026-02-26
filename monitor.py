# monitor.py
import os
import re
import json
import csv
import time
import sqlite3
import argparse
import random
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import yaml
from dotenv import load_dotenv
from shapely.geometry import shape, Point

JSEARCH_SEARCH_URL = "https://jsearch.p.rapidapi.com/search"

# Put your file at one of these paths in the repo:
LOCAL_MD_COUNTIES_PATHS = [
    "./data/maryland-counties.geojson",
    "./maryland-counties.geojson",
]

# Cache just Montgomery County geometry here (created automatically)
MOCO_CACHE_PATH = "./data/moco_boundary.geojson"


def utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def ensure_dirs():
    os.makedirs("./data", exist_ok=True)
    os.makedirs("./outputs", exist_ok=True)
    os.makedirs("./docs", exist_ok=True)


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


def safe_get(d: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def parse_job_posted_at(job: Dict[str, Any]) -> Optional[str]:
    v = safe_get(
        job,
        keys=[
            "job_posted_at_datetime_utc",
            "job_posted_at_datetime",
            "job_posted_at",
            "job_posted_at_timestamp",
        ],
        default=None,
    )
    if v is None:
        return None
    return str(v)


def _find_local_md_geojson_path() -> Optional[str]:
    for p in LOCAL_MD_COUNTIES_PATHS:
        if os.path.exists(p):
            return p
    return None


def _extract_moco_feature(gj: Dict[str, Any]) -> Dict[str, Any]:
    moco_feat = None
    for feat in gj.get("features", []):
        props = feat.get("properties", {}) or {}
        candidate = (
            props.get("NAME")
            or props.get("name")
            or props.get("County")
            or props.get("county")
            or ""
        )
        name = str(candidate).strip().lower()
        if name == "montgomery":
            moco_feat = feat
            break

    if not moco_feat:
        sample = gj.get("features", [{}])[0].get("properties", {}) if gj.get("features") else {}
        raise RuntimeError(
            "Could not find Montgomery County in your maryland-counties.geojson.\n"
            f"Checked keys NAME/name/County/county. Sample property keys: {list(sample.keys())}"
        )
    return moco_feat


def load_moco_polygon() -> Any:
    """
    Loads LOCAL Maryland counties GeoJSON, extracts Montgomery County,
    caches it to ./data/moco_boundary.geojson, and returns Shapely geometry.
    """
    ensure_dirs()

    # If cached, load cache
    if os.path.exists(MOCO_CACHE_PATH):
        with open(MOCO_CACHE_PATH, "r", encoding="utf-8") as f:
            feat = json.load(f)
        return shape(feat["geometry"])

    local_path = _find_local_md_geojson_path()
    if not local_path:
        raise RuntimeError(
            "Missing local Maryland counties GeoJSON.\n"
            "Place it at one of:\n"
            "  - ./data/maryland-counties.geojson (recommended)\n"
            "  - ./maryland-counties.geojson\n"
        )

    with open(local_path, "r", encoding="utf-8") as f:
        gj = json.load(f)

    moco_feat = _extract_moco_feature(gj)

    with open(MOCO_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(moco_feat, f)

    return shape(moco_feat["geometry"])


def is_job_in_moco(job: Dict[str, Any], moco_geom: Any) -> bool:
    lat = job.get("job_latitude")
    lon = job.get("job_longitude")

    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
        pt = Point(float(lon), float(lat))
        return moco_geom.contains(pt)

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


def places_text_search(company_name: str, google_places_key: str) -> Optional[Dict[str, Any]]:
    if not google_places_key:
        return None

    q = f"{company_name} Montgomery County MD"
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": q, "key": google_places_key}

    try:
        # keep places timeouts reasonable
        r = requests.get(url, params=params, timeout=(10, 20))
        r.raise_for_status()
        payload = r.json()
        results = payload.get("results") or []
        if not results:
            return None
        return results[0]
    except Exception:
        return None


def verify_company_with_places(company_name: str, moco_geom: Any, google_places_key: str) -> Dict[str, Any]:
    if not google_places_key:
        return {"verified": False, "place_id": "", "address": "", "lat": None, "lon": None, "reason": "no_places_key"}

    top = places_text_search(company_name, google_places_key)
    if not top:
        return {"verified": False, "place_id": "", "address": "", "lat": None, "lon": None, "reason": "no_places_match"}

    place_id = str(top.get("place_id") or "")
    address = str(top.get("formatted_address") or "")
    geom = (top.get("geometry") or {}).get("location") or {}
    lat = geom.get("lat")
    lon = geom.get("lng")

    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
        pt = Point(float(lon), float(lat))
        if moco_geom.contains(pt):
            return {"verified": True, "place_id": place_id, "address": address, "lat": float(lat), "lon": float(lon), "reason": "inside_moco"}
        return {"verified": False, "place_id": place_id, "address": address, "lat": float(lat), "lon": float(lon), "reason": "outside_moco"}

    return {"verified": False, "place_id": place_id, "address": address, "lat": None, "lon": None, "reason": "no_latlon"}


# ----------------------------
# Reliability: retries + bumps
# ----------------------------

def build_retry_session() -> requests.Session:
    """
    Requests session with retries + exponential backoff for transient RapidAPI issues.
    Retries on: timeouts, 429, and common 5xx.
    """
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=1.5,  # 0s, 1.5s, 3s, 6s, 12s...
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


# ----------------------------
# NEW: derived metadata fields
# ----------------------------

def extract_salary_text(job: Dict[str, Any]) -> str:
    """
    Attempts to extract a human-readable salary string from common fields.
    If missing, returns empty string.
    """
    formatted = safe_get(job, ["job_salary", "salary", "job_salary_range", "job_salary_formatted"], "")
    if formatted:
        return str(formatted).strip()

    min_sal = safe_get(job, ["job_min_salary", "job_salary_min", "min_salary"], None)
    max_sal = safe_get(job, ["job_max_salary", "job_salary_max", "max_salary"], None)
    currency = str(safe_get(job, ["job_salary_currency", "salary_currency", "currency"], "") or "").strip()
    period = str(safe_get(job, ["job_salary_period", "salary_period"], "") or "").strip()  # YEAR, HOUR, etc.

    if min_sal is None and max_sal is None:
        return ""

    core = ""
    if min_sal is not None and max_sal is not None:
        core = f"{min_sal}-{max_sal}"
    elif min_sal is not None:
        core = f"{min_sal}+"
    else:
        core = f"up to {max_sal}"

    tail = " ".join([currency, period]).strip()
    return f"{core} {tail}".strip()


def derive_job_requirements(job: Dict[str, Any]) -> str:
    """
    Returns comma-delimited list of:
      under_3_years_experience, more_than_3_years_experience, no_experience, no_degree
    Uses simple heuristics on title/description/highlights if present.
    """
    title = str(job.get("job_title") or "").lower()
    desc = str(safe_get(job, ["job_description", "job_highlights", "job_summary"], "") or "").lower()
    text = f"{title}\n{desc}"

    tags = set()

    if any(k in text for k in [
        "no degree", "no-degree", "no diploma", "no diploma required",
        "high school or equivalent", "high school diploma or equivalent"
    ]):
        tags.add("no_degree")

    if any(k in text for k in [
        "no experience", "entry level", "entry-level", "0 years", "zero years",
        "training provided", "no prior experience"
    ]):
        tags.add("no_experience")

    if any(k in text for k in [
        "1 year", "one year", "2 years", "two years", "1-2 years", "2+ years"
    ]):
        tags.add("under_3_years_experience")

    if any(k in text for k in [
        "3 years", "3+ years", "four years", "4 years", "5 years", "6 years",
        "7 years", "8 years", "10 years", "5+ years"
    ]):
        tags.add("more_than_3_years_experience")

    if "more_than_3_years_experience" in tags and "under_3_years_experience" in tags:
        tags.discard("under_3_years_experience")

    return ",".join(sorted(tags))


def derive_fields(job: Dict[str, Any]) -> str:
    title = str(job.get("job_title") or "").lower()
    desc_raw = safe_get(job, ["job_description", "job_highlights", "job_summary"], "")
    desc = str(desc_raw or "").lower()
    employer = str(job.get("employer_name") or "").lower()

    text = f"{title}\n{desc}\n{employer}"
    text = re.sub(r"[^a-z0-9+\s/.-]", " ", text.lower())
    text = re.sub(r"\s+", " ", text).strip()

    tags = set()

    def has_phrase(p: str) -> bool:
        return p in text

    def has_word(w: str) -> bool:
        return re.search(rf"(?<![a-z0-9]){re.escape(w)}(?![a-z0-9])", text) is not None

    # ----------------------------
    # TECHNOLOGY
    # ----------------------------

    tech_phrases = [
        "software", "developer", "machine learning", "cloud",
        "cybersecurity", "devops", "full stack", "fullstack",
        "backend", "frontend", "data engineer", "data scientist",
        "database", "network engineer", "systems engineer",
        "sre", "programmer",
        "aws", "azure", "gcp", "cyber",
        "python", "java", "javascript", "typescript", "react", "sql",
        "system administrator"
    ]

    tech_words = ["ai", "ml", "api", "etl"]

    tech_exclude_phrases = [
        "automotive technician",
        "maintenance technician",
        "service technician",
        "hvac technician",
        "repair technician",
        "field technician",
        "pharmacy technician",
        "nail technician",
        "behavior technician",
        "veterinary technician",
        "medical technician",
        "lab technician",
        "manufacturing technician",
        "it support",
        "help desk",
        "desktop support",
        "computer teacher",
        "technology teacher",
        "educational technology",
    ]

    tech_exclude_words = [
        "custodian",
        "janitor",
        "plumber",
        "electrician",
        "mechanic",
        "installer",
        "health",
    ]

    tech_excluded = (
        any(has_phrase(p) for p in tech_exclude_phrases)
        or any(has_word(w) for w in tech_exclude_words)
    )

    tech_hit = (
        (any(has_phrase(p) for p in tech_phrases) or any(has_word(w) for w in tech_words))
        and not tech_excluded
    )

    # ----------------------------
    # LIFE SCIENCES
    # ----------------------------

    life_phrases = [
        "biotech", "bioinformatics", "laboratory", "clinical", "pharma",
        "assay", "regulatory", "molecular", "genomics", "microbiology",
        "biologist", "scientist", "chemist", "gene",
        "therapeutics", "biosciences", "biologics", "diagnostics"
    ]

    life_words = ["lab", "qc", "qa"]

    life_exclude_phrases = [
        "nurse", "doctor", "physician", "dentist",
        "dental", "hygienist", "hospital"
    ]

    life_exclude_words = ["rn", "cna"]

    life_excluded = (
        any(has_phrase(p) for p in life_exclude_phrases)
        or any(has_word(w) for w in life_exclude_words)
    )

    life_hit = (
        (any(has_phrase(p) for p in life_phrases) or any(has_word(w) for w in life_words))
        and not life_excluded
    )

    # ----------------------------
    # AERO / DEFENSE / SATELLITE
    # ----------------------------

    aero_phrases = [
        "aerospace", "satellite", "space", "radar", "defense",
        "spacecraft", "ground station", "communications satellite",
        "sigint", "clearance", "cleared",
        "aeronautics", "space systems", "defense systems", "satcom",
        "navy", "army"
    ]

    aero_words = ["rf", "dod"]

    aero_tokens = [
        "ts/sci", "ts sci",
        "secret clearance", "top secret"
    ]

    aero_hit = (
        any(has_phrase(p) for p in aero_phrases)
        or any(has_word(w) for w in aero_words)
        or any(has_phrase(t) for t in aero_tokens)
    )

    # ----------------------------
    # Retail / noise blockers
    # ----------------------------

    retail_blockers = [
        "cashier", "barista", "server", "waiter", "waitress",
        "crew member", "store associate", "retail associate",
        "stock associate", "teacher"
    ]

    is_retailish = any(b in text for b in retail_blockers)

    # ----------------------------
    # Apply tags
    # ----------------------------

    if tech_hit and not is_retailish:
        tags.add("technology")

    if aero_hit and not is_retailish:
        tags.add("aero_defense_sat")

    if life_hit:
        tags.add("life_sciences")

    return ",".join(sorted(tags))


@dataclass
class JSearchClient:
    rapidapi_key: str
    rapidapi_host: str = "jsearch.p.rapidapi.com"
    timeout_s: int = 90               # ⬅️ bump read timeout
    connect_timeout_s: int = 10       # ⬅️ separate connect timeout
    session: Optional[requests.Session] = None

    def __post_init__(self):
        if self.session is None:
            self.session = build_retry_session()

    def search(
        self,
        query: str,
        page: int = 1,
        num_pages: int = 1,
        date_posted: str = "today",
        country: str = "us"
    ) -> List[Dict[str, Any]]:
        headers = {"X-RapidAPI-Key": self.rapidapi_key, "X-RapidAPI-Host": self.rapidapi_host}
        params = {"query": query, "page": str(page), "num_pages": str(num_pages), "date_posted": date_posted, "country": country}
        timeout = (self.connect_timeout_s, self.timeout_s)

        resp = self.session.get(JSEARCH_SEARCH_URL, headers=headers, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data") or []
        return data if isinstance(data, list) else []


def init_db(conn: sqlite3.Connection):
    # jobs table (now includes job_requirements, fields, salary)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        job_id TEXT PRIMARY KEY,
        employer_name TEXT,
        employer_norm TEXT,
        job_title TEXT,
        job_publisher TEXT,
        job_employment_type TEXT,
        job_city TEXT,
        job_state TEXT,
        job_country TEXT,
        job_posted_at TEXT,
        apply_link TEXT,

        job_requirements TEXT DEFAULT '',
        fields TEXT DEFAULT '',
        salary TEXT DEFAULT '',

        search_query TEXT,
        first_seen_run_date TEXT,
        first_seen_utc TEXT
    );
    """)

    # companies table
    conn.execute("""
    CREATE TABLE IF NOT EXISTS companies (
        employer_norm TEXT PRIMARY KEY,
        employer_name TEXT,
        first_seen_run_date TEXT,
        first_seen_utc TEXT,
        places_verified INTEGER DEFAULT 0,
        places_reason TEXT DEFAULT '',
        places_place_id TEXT DEFAULT '',
        places_address TEXT DEFAULT '',
        places_verified_utc TEXT DEFAULT ''
    );
    """)

    # runs table
    conn.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date TEXT,
        started_utc TEXT,
        finished_utc TEXT,
        queries_json TEXT,
        jobs_scanned_count INTEGER DEFAULT 0,
        jobs_in_moco_count INTEGER DEFAULT 0,
        new_jobs_count INTEGER DEFAULT 0,
        new_companies_count INTEGER DEFAULT 0
    );
    """)
    conn.commit()

    # migrations
    _ensure_column(conn, "companies", "places_verified", "INTEGER DEFAULT 0")
    _ensure_column(conn, "companies", "places_reason", "TEXT DEFAULT ''")
    _ensure_column(conn, "companies", "places_place_id", "TEXT DEFAULT ''")
    _ensure_column(conn, "companies", "places_address", "TEXT DEFAULT ''")
    _ensure_column(conn, "companies", "places_verified_utc", "TEXT DEFAULT ''")

    _ensure_column(conn, "runs", "jobs_scanned_count", "INTEGER DEFAULT 0")
    _ensure_column(conn, "runs", "jobs_in_moco_count", "INTEGER DEFAULT 0")
    _ensure_column(conn, "runs", "new_jobs_count", "INTEGER DEFAULT 0")
    _ensure_column(conn, "runs", "new_companies_count", "INTEGER DEFAULT 0")

    # NEW job columns
    _ensure_column(conn, "jobs", "job_requirements", "TEXT DEFAULT ''")
    _ensure_column(conn, "jobs", "fields", "TEXT DEFAULT ''")
    _ensure_column(conn, "jobs", "salary", "TEXT DEFAULT ''")

    conn.commit()


def _ensure_column(conn: sqlite3.Connection, table: str, col: str, col_def: str):
    existing = conn.execute(f"PRAGMA table_info({table});").fetchall()
    cols = {r[1] for r in existing}
    if col not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_def};")


def upsert_company_if_missing(conn: sqlite3.Connection, employer_name: str, run_date: str):
    norm = normalize_company(employer_name)
    if not norm:
        return
    conn.execute("""
    INSERT OR IGNORE INTO companies (employer_norm, employer_name, first_seen_run_date, first_seen_utc)
    VALUES (?, ?, ?, ?)
    """, (norm, employer_name, run_date, utc_now_iso()))


def update_company_places_verification(conn: sqlite3.Connection, employer_norm: str, v: Dict[str, Any]):
    conn.execute("""
    UPDATE companies
    SET places_verified = ?,
        places_reason = ?,
        places_place_id = ?,
        places_address = ?,
        places_verified_utc = ?
    WHERE employer_norm = ?
    """, (
        1 if v.get("verified") else 0,
        str(v.get("reason") or ""),
        str(v.get("place_id") or ""),
        str(v.get("address") or ""),
        utc_now_iso(),
        employer_norm
    ))


def insert_job_if_new(conn: sqlite3.Connection, job: Dict[str, Any], search_query: str, run_date: str) -> bool:
    job_id = str(job.get("job_id") or "").strip()
    if not job_id:
        return False

    employer_name = str(job.get("employer_name") or "").strip()
    employer_norm = normalize_company(employer_name)

    job_title = str(job.get("job_title") or "").strip()
    job_publisher = str(job.get("job_publisher") or "").strip()
    job_employment_type = str(job.get("job_employment_type") or "").strip()

    city = safe_get(job, ["job_city", "job_location_city"], "")
    state = safe_get(job, ["job_state", "job_location_state"], "")
    country = safe_get(job, ["job_country"], "")

    posted_at = parse_job_posted_at(job)
    apply_link = str(job.get("job_apply_link") or "").strip()

    # NEW derived fields
    job_requirements = derive_job_requirements(job)
    raw_fields = derive_fields(job)                 # "technology,life_sciences"
    fields = f",{raw_fields}," if raw_fields else ""  # stored for LIKE '%,tag,%'
    salary = extract_salary_text(job)

    cur = conn.execute("""
    INSERT OR IGNORE INTO jobs (
        job_id, employer_name, employer_norm, job_title, job_publisher, job_employment_type,
        job_city, job_state, job_country, job_posted_at, apply_link,
        job_requirements, fields, salary,
        search_query, first_seen_run_date, first_seen_utc
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        job_id, employer_name, employer_norm, job_title, job_publisher, job_employment_type,
        city, state, country, posted_at, apply_link,
        job_requirements, fields, salary,
        search_query, run_date, utc_now_iso()
    ))
    return cur.rowcount == 1


def run_daily(config_path: str):
    load_dotenv()
    ensure_dirs()

    rapidapi_key = os.getenv("RAPIDAPI_KEY", "").strip()
    rapidapi_host = os.getenv("RAPIDAPI_HOST", "jsearch.p.rapidapi.com").strip()
    google_places_key = os.getenv("GOOGLE_PLACES_KEY", "").strip()
    db_path = os.getenv("DB_PATH", "./data/moco_jobs.sqlite").strip()

    # Optional: tune delay without code changes
    api_delay_s = float(os.getenv("JSEARCH_DELAY_SECONDS", "0.25"))

    if not rapidapi_key:
        raise SystemExit("Missing RAPIDAPI_KEY. Copy .env.example to .env and fill it in.")

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    daily_cfg = cfg["daily"]
    date_posted = daily_cfg.get("date_posted", "today")
    num_pages = int(daily_cfg.get("num_pages", 1))
    queries = daily_cfg.get("queries", [])

    run_date = date.today().isoformat()
    started = utc_now_iso()

    moco_geom = load_moco_polygon()
    client = JSearchClient(rapidapi_key=rapidapi_key, rapidapi_host=rapidapi_host)

    conn = sqlite3.connect(db_path)
    init_db(conn)

    jobs_scanned_count = 0
    jobs_in_moco_count = 0
    new_jobs_count = 0
    new_companies_today = set()

    for q in queries:
        q = str(q).strip()
        if not q:
            continue

        # Delay (with a tiny jitter to be nicer to the API + reduce thundering herd)
        time.sleep(api_delay_s + random.uniform(0.0, 0.15))

        try:
            jobs = client.search(query=q, page=1, num_pages=num_pages, date_posted=date_posted, country="us")
        except requests.exceptions.RequestException as e:
            print(f"[WARN] JSearch failed for query={q!r}: {e}. Skipping this query for today.")
            jobs = []

        jobs_scanned_count += len(jobs)

        for job in jobs:
            if not is_job_in_moco(job, moco_geom):
                continue

            jobs_in_moco_count += 1

            employer_name = str(job.get("employer_name") or "").strip()
            employer_norm = normalize_company(employer_name)

            if employer_norm:
                existed = conn.execute("SELECT 1 FROM companies WHERE employer_norm = ?", (employer_norm,)).fetchone()
                upsert_company_if_missing(conn, employer_name, run_date)
                now_row = conn.execute("SELECT first_seen_run_date FROM companies WHERE employer_norm = ?", (employer_norm,)).fetchone()

                if existed is None and now_row and now_row[0] == run_date:
                    new_companies_today.add(employer_norm)

                    # Places verification only for new companies (keeps API usage low)
                    if google_places_key:
                        v = verify_company_with_places(employer_name, moco_geom, google_places_key)
                        update_company_places_verification(conn, employer_norm, v)

            if insert_job_if_new(conn, job, search_query=q, run_date=run_date):
                new_jobs_count += 1

    finished = utc_now_iso()

    conn.execute("""
    INSERT INTO runs (run_date, started_utc, finished_utc, queries_json, jobs_scanned_count, jobs_in_moco_count, new_jobs_count, new_companies_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        run_date, started, finished,
        json.dumps({"queries": queries, "date_posted": date_posted, "num_pages": num_pages}),
        jobs_scanned_count, jobs_in_moco_count, new_jobs_count, len(new_companies_today)
    ))
    conn.commit()

    # Build daily new-company CSV including Places verification info
    rows = []
    for employer_norm in sorted(new_companies_today):
        comp = conn.execute("""
            SELECT employer_name, places_verified, places_reason, places_address, places_place_id
            FROM companies
            WHERE employer_norm = ?
        """, (employer_norm,)).fetchone()
        if not comp:
            continue

        employer_name, places_verified, places_reason, places_address, places_place_id = comp

        job_count = conn.execute("""
            SELECT COUNT(DISTINCT job_id)
            FROM jobs
            WHERE employer_norm = ?
              AND first_seen_run_date = ?
        """, (employer_norm, run_date)).fetchone()[0]

        samples = conn.execute("""
            SELECT job_title, apply_link
            FROM jobs
            WHERE employer_norm = ?
              AND first_seen_run_date = ?
            LIMIT 3
        """, (employer_norm, run_date)).fetchall()

        rows.append({
            "run_date": run_date,
            "company": employer_name,
            "unique_jobs_found_today": job_count,
            "places_verified": int(places_verified or 0),
            "places_reason": places_reason or "",
            "places_address": places_address or "",
            "places_place_id": places_place_id or "",
            "sample_1_title": samples[0][0] if len(samples) > 0 else "",
            "sample_1_link": samples[0][1] if len(samples) > 0 else "",
            "sample_2_title": samples[1][0] if len(samples) > 1 else "",
            "sample_2_link": samples[1][1] if len(samples) > 1 else "",
            "sample_3_title": samples[2][0] if len(samples) > 2 else "",
            "sample_3_link": samples[2][1] if len(samples) > 2 else "",
            "md_entity_search_link": "https://egov.maryland.gov/businessexpress/entitysearch"
        })

    out_path = f"./outputs/new_companies_{run_date}.csv"
    fieldnames = list(rows[0].keys()) if rows else [
        "run_date", "company", "unique_jobs_found_today", "places_verified", "places_reason", "places_address",
        "sample_1_title", "sample_1_link", "md_entity_search_link"
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    conn.close()

    print("Daily run complete.")
    print(f"  Date: {run_date}")
    print(f"  Jobs scanned (API results): {jobs_scanned_count}")
    print(f"  Jobs inside MoCo (filtered): {jobs_in_moco_count}")
    print(f"  New unique jobs inserted: {new_jobs_count}")
    print(f"  New companies detected today: {len(new_companies_today)}")
    print(f"  Output: {out_path}")


def run_monthly(month: str):
    load_dotenv()
    ensure_dirs()

    db_path = os.getenv("DB_PATH", "./data/moco_jobs.sqlite").strip()
    top_n = 30

    try:
        start = datetime.strptime(month + "-01", "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit("Month must be YYYY-MM")

    if start.month == 12:
        end = date(start.year + 1, 1, 1)
    else:
        end = date(start.year, start.month + 1, 1)

    cfg_path = "./config.yaml"
    if os.path.exists(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        top_n = int(cfg.get("monthly", {}).get("top_n", top_n))

    conn = sqlite3.connect(db_path)
    init_db(conn)

    rows = conn.execute("""
        SELECT employer_name, COUNT(DISTINCT job_id) AS unique_jobs
        FROM jobs
        WHERE first_seen_run_date >= ?
          AND first_seen_run_date < ?
          AND employer_name IS NOT NULL
          AND TRIM(employer_name) <> ''
        GROUP BY employer_name
        ORDER BY unique_jobs DESC
        LIMIT ?
    """, (start.isoformat(), end.isoformat(), top_n)).fetchall()

    out_path = f"./outputs/top_companies_{month}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["month", "company", "unique_job_postings_found"])
        for company, cnt in rows:
            w.writerow([month, company, cnt])

    conn.close()

    print("Monthly report complete.")
    print(f"  Month: {month}")
    print(f"  Output: {out_path}")


def retag_fields_in_db(db_path: str, days_back: int = 180):
    """
    One-time repair:
      - recompute derive_fields() for past jobs
      - store fields wrapped with commas so dashboard LIKE '%,tag,%' works
    """
    ensure_dirs()
    conn = sqlite3.connect(db_path)
    init_db(conn)

    cutoff = (date.today() - timedelta(days=days_back)).isoformat()

    rows = conn.execute("""
        SELECT job_id, job_title, employer_name
        FROM jobs
        WHERE first_seen_run_date >= ?
    """, (cutoff,)).fetchall()

    updated = 0
    for job_id, job_title, employer_name in rows:
        job = {
            "job_title": job_title,
            "employer_name": employer_name,
            # We don't have description columns saved in DB currently, so we retag from title+employer.
        }
        raw = derive_fields(job)
        wrapped = f",{raw}," if raw else ""
        conn.execute("UPDATE jobs SET fields = ? WHERE job_id = ?", (wrapped, job_id))
        updated += 1

    conn.commit()
    conn.close()
    print(f"Retag complete: updated {updated} jobs since {cutoff}.")


def main():
    ap = argparse.ArgumentParser(description="MoCo hiring monitor using JSearch (RapidAPI).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("daily", help="Run daily new-company detection.")
    d.add_argument("--config", default="./config.yaml", help="Path to config.yaml")

    m = sub.add_parser("monthly", help="Generate monthly top-companies report from stored data.")
    m.add_argument("--month", required=True, help="Month in YYYY-MM format (e.g., 2026-01)")

    # Optional maintenance command
    r = sub.add_parser("retag", help="Recompute job fields tags in DB for the last N days.")
    r.add_argument("--db", default=os.getenv("DB_PATH", "./data/moco_jobs.sqlite"), help="DB path")
    r.add_argument("--days-back", type=int, default=180, help="How many days back to retag")

    args = ap.parse_args()
    if args.cmd == "daily":
        run_daily(args.config)
    elif args.cmd == "monthly":
        run_monthly(args.month)
    elif args.cmd == "retag":
        retag_fields_in_db(args.db, args.days_back)


if __name__ == "__main__":

    main()

