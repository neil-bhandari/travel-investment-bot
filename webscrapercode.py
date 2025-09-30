import os
import csv
import time
import datetime as dt
from typing import Dict, Any, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEY = os.environ.get("PILOTERR_API_KEY")
BASE = "https://piloterr.com/api/v2"

# Tunables for low usage during testing
DAYS_SINCE = int(os.environ.get("DAYS_SINCE", "1"))                 # lookback window (days)
ROUND_LIMIT_PER_CALL = int(os.environ.get("ROUND_LIMIT_PER_CALL", "50"))
MAX_COMPANY_LOOKUPS = int(os.environ.get("MAX_COMPANY_LOOKUPS", "20"))  # safety cap on lookups
SLEEP_SEC = float(os.environ.get("SLEEP_SEC", "0.2"))                # tiny pause between calls

CRUNCHBASE_TRAVEL_LABEL = "hospitality, travel and tourism"
TRAVEL_KEYWORDS = {
    # Core sectors
    "hospitality",
    "travel",
    "tourism",

    # Lodging & stays
    "hotel",
    "lodging",
    "resort",
    "vacation rental",
    "short term rental",
    "hostel",
    "bnb",
    "bed and breakfast",

    # Booking & platforms
    "ota",            # Online travel agency
    "booking",
    "expedia",        # common OTA brand
    "tripadvisor",    # common OTA brand

    # Transport modes (traveler-facing only)
    "airline",
    "airport",
    "cruise",
    "ferry",
    "rideshare",

    # Travel services
    "tour operator",
    "tourism board",
    "travel agency"
}

# ----- HTTP session with retries & separate connect/read timeouts -----
def make_session() -> requests.Session:
    if not API_KEY:
        raise SystemExit("Set PILOTERR_API_KEY in your environment or PyCharm Run configuration.")
    sess = requests.Session()
    sess.headers.update({
        "x-api-key": API_KEY,
        "Accept": "application/json",
        "User-Agent": "travel-digest/0.1 (+PyRequests)"
    })
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"},
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

SESSION = make_session()

def http_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE}{path}"
    # tuple timeout: (connect, read)
    resp = SESSION.get(url, params=params, timeout=(5, 120))
    resp.raise_for_status()
    return resp.json()

# ----- Helpers -----
def norm_list_to_strings(val):
    """
    Accepts str | list[str] | list[dict] and returns a list[str].
    If dicts, tries 'name' or 'value'; else str(item).
    """
    if not val:
        return []
    if isinstance(val, str):
        return [val]
    out = []
    if isinstance(val, list):
        for item in val:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                out.append(str(item.get("name") or item.get("value") or item))
            else:
                out.append(str(item))
    else:
        out.append(str(val))
    return out

def fetch_recent_rounds(days_since: int) -> List[Dict[str, Any]]:
    print(f"[info] Fetching funding rounds: last {days_since} day(s), limit={ROUND_LIMIT_PER_CALL}", flush=True)
    params = {"days_since_announcement": days_since, "limit": ROUND_LIMIT_PER_CALL}
    data = http_get("/crunchbase/funding_rounds", params)
    items = data.get("results") or data.get("data") or data.get("items") or []
    items.sort(key=lambda x: x.get("announced_on") or "", reverse=True)
    print(f"[info] Retrieved {len(items)} rounds (all industries)", flush=True)
    return items

def extract_company_uuid(rd: Dict[str, Any]) -> str:
    for k in ("funded_organization_identifier", "funded_organization_uuid", "organization_uuid", "company_uuid"):
        v = rd.get(k)
        if isinstance(v, str) and v:
            return v
        if isinstance(v, dict) and v.get("uuid"):
            return v["uuid"]
    return ""

def get_company(query_value: str) -> Dict[str, Any]:
    # Piloterr expects `query=` or `domain=`, not `uuid=`
    data = http_get("/crunchbase/company/info", {"query": query_value})
    if isinstance(data, dict) and data.get("name"):
        return data
    if isinstance(data, dict) and isinstance(data.get("data"), list) and data["data"]:
        return data["data"][0]
    return {}

def is_travel_company(co: Dict[str, Any]) -> bool:
    hay = []
    for k in ("categories", "industries", "tags", "short_description", "description"):
        v = co.get(k)
        if not v:
            continue
        if k in ("categories", "industries", "tags"):
            hay += [s.lower() for s in norm_list_to_strings(v)]
        else:
            hay.append(str(v).lower())
    blob = " | ".join(hay)
    if CRUNCHBASE_TRAVEL_LABEL in blob:
        return True
    return any(term in blob for term in TRAVEL_KEYWORDS)

def safe_amount(rd: Dict[str, Any]) -> str:
    money = rd.get("money_raised_usd") or rd.get("money_raised")
    if isinstance(money, dict):
        val = money.get("value_usd") or money.get("value")
    else:
        val = money
    try:
        return f"${int(float(val)):,}"
    except Exception:
        return "Undisclosed"

# ----- Core flow -----
def pick_two_latest_travel() -> List[Dict[str, Any]]:
    rounds = fetch_recent_rounds(DAYS_SINCE)
    picked: List[Dict[str, Any]] = []
    seen = set()
    lookups = 0

    for idx, rd in enumerate(rounds, start=1):
        if len(picked) >= 2 or lookups >= MAX_COMPANY_LOOKUPS:
            break

        uuid = extract_company_uuid(rd)
        if not uuid or uuid in seen:
            continue
        seen.add(uuid)

        print(f"[progress] ({idx}/{len(rounds)}) Checking company uuid={uuid} ...", flush=True)
        co = get_company(uuid)
        lookups += 1

        if not co:
            print("  -> no company data returned, skipping.", flush=True)
            continue

        if not is_travel_company(co):
            print(f"  -> not travel/hospitality, skipping.", flush=True)
            continue

        row = {
            "announced_on": rd.get("announced_on"),  # may be None; that's ok for now
            "investment_type": rd.get("investment_type"),
            "amount_usd": safe_amount(rd),
            "company_name": co.get("name"),
            "website": co.get("website") or co.get("homepage_url") or "",
            "location": co.get("location") or co.get("country") or co.get("country_code") or "",
            "categories": ", ".join(
                norm_list_to_strings(co.get("categories")) or
                norm_list_to_strings(co.get("industries"))
            ),
            "crunchbase_url": co.get("permalink") or co.get("cb_url") or "",
            "description": co.get("short_description") or co.get("description") or "",
        }
        picked.append(row)
        print(f"  -> ✅ travel match: {row['company_name']} ({row['investment_type']}, {row['amount_usd']})", flush=True)

        time.sleep(SLEEP_SEC)

    picked.sort(key=lambda r: r["announced_on"] or "", reverse=True)
    return picked

def save_csv(rows: List[Dict[str, Any]]) -> str:
    today = dt.date.today().isoformat()
    out = f"funded_travel_top2_{today}.csv"
    fields = ["announced_on", "investment_type", "amount_usd", "company_name",
              "website", "location", "categories", "crunchbase_url", "description"]
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return out

def post_to_slack(rows: List[Dict[str, Any]]):
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        return  # optional; only post if provided
    if not rows:
        text = ":no_entry: No travel/hospitality fundings found today."
    else:
        blocks = []
        for r in rows:
            blocks.append(
                f"*Company:* {r['company_name']}\n"
                f"*Categories:* {r['categories'] or '—'}\n"
                f"*Announced:* {r['announced_on'] or 'None'}\n"
                f"*Type:* {r['investment_type'] or '—'}\n"
                f"*Amount:* {r['amount_usd']}\n"
                f"*Website:* {r['website'] or '—'}\n"
                f"*Description:* {r['description'] or '—'}\n"
                f"*Crunchbase:* {r['crunchbase_url'] or '—'}"
            )
        text = "*Latest Travel/Hospitality Investments:*\n\n" + "\n\n".join(blocks)
    try:
        requests.post(webhook, json={"text": text}, timeout=10)
        print("[info] Posted to Slack.", flush=True)
    except Exception as e:
        print(f"[warn] Slack post failed: {e}", flush=True)

def main():
    rows = pick_two_latest_travel()
    if not rows:
        print("[result] No travel or hospitality rounds found in the current window.", flush=True)
        post_to_slack(rows)
        return

    print("\n[result] Top 2 latest travel/hospitality fundings:")
    for r in rows:
        print(
            f"Company: {r['company_name']}\n"
            f"Categories: {r['categories'] or '—'}\n"
            f"Announced: {r['announced_on'] or 'None'}\n"
            f"Type: {r['investment_type'] or '—'}\n"
            f"Amount: {r['amount_usd']}\n"
            f"Website: {r['website'] or '—'}\n"
            f"Description: {r['description'] or '—'}\n"
            f"Crunchbase: {r['crunchbase_url'] or '—'}\n"
            "—"
        )
    fname = save_csv(rows)
    print(f"[result] Saved {len(rows)} rows to {fname}", flush=True)

    # Optional Slack post
    post_to_slack(rows)

if __name__ == "__main__":
    main()
