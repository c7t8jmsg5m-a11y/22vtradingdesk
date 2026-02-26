"""
FINRA Margin Debt Collector
Fetches: FINRA margin debt (monthly), GDP (quarterly), SPX YoY return
Sources: FINRA website, FRED API, Yahoo Finance
Schedule: Daily (data updates monthly for FINRA, quarterly for GDP)
"""

import os
import json
import requests
from datetime import datetime, timedelta, date
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

# FRED series for fallback margin debt and GDP
FRED_MARGIN_SERIES = "BOGZ1FL663067003Q"  # Security credit from broker-dealers (quarterly, millions)
FRED_GDP_SERIES = "GDP"  # Nominal GDP (quarterly, billions)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/json,text/csv",
}

MONTH_NAMES = [
    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]


def get_fred_api_key():
    """Load FRED API key from environment or .env file."""
    key = os.environ.get("FRED_API_KEY")
    if key:
        return key

    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith("FRED_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def fetch_fred_series(series_id, api_key, lookback_days=730):
    """Fetch a FRED series with 2-year lookback for YoY computation."""
    start_date = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "sort_order": "asc",
    }

    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    observations = data.get("observations", [])
    valid = [
        {"date": obs["date"], "value": float(obs["value"])}
        for obs in observations
        if obs["value"] != "."
    ]
    return valid


def fetch_finra_margin_web():
    """
    Try to fetch FINRA margin statistics from their website.
    FINRA publishes monthly margin data at:
    https://www.finra.org/finra-data/browse-catalog/margin-statistics

    Returns list of monthly observations or None if scraping fails.
    """
    try:
        from bs4 import BeautifulSoup

        # FINRA margin statistics page — try the data API endpoint
        # FINRA has a REST API at their data gateway
        api_url = "https://api.finra.org/data/group/otcMarket/name/monthlyShortInterest"
        margin_url = "https://www.finra.org/finra-data/browse-catalog/margin-statistics"

        # Try the main page to find a data download link
        resp = requests.get(margin_url, timeout=15, headers=HEADERS)
        if resp.status_code != 200:
            print(f"  [FINRA] Website returned HTTP {resp.status_code}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for CSV or data links
        links = soup.find_all("a", href=True)
        csv_link = None
        for link in links:
            href = link["href"]
            if "margin" in href.lower() and (".csv" in href.lower() or "download" in href.lower()):
                csv_link = href
                break

        if csv_link:
            if not csv_link.startswith("http"):
                csv_link = "https://www.finra.org" + csv_link
            csv_resp = requests.get(csv_link, timeout=15, headers=HEADERS)
            if csv_resp.status_code == 200:
                return parse_finra_csv(csv_resp.text)

        # Try parsing tables on the page
        tables = soup.find_all("table")
        for table in tables:
            result = parse_finra_table(table)
            if result:
                return result

        print("  [FINRA] Could not find margin data on page")
        return None

    except ImportError:
        print("  [FINRA] BeautifulSoup not installed — skipping web scrape")
        return None
    except Exception as e:
        print(f"  [FINRA] Web scrape error: {e}")
        return None


def parse_finra_csv(csv_text):
    """Parse FINRA margin statistics CSV."""
    import csv
    import io

    reader = csv.DictReader(io.StringIO(csv_text))
    observations = []

    for row in reader:
        # Look for margin debt column (various possible names)
        margin_val = None
        date_val = None

        for key, val in row.items():
            key_lower = key.lower()
            if "debit" in key_lower or "margin" in key_lower:
                try:
                    margin_val = float(val.replace(",", "").replace("$", ""))
                except (ValueError, AttributeError):
                    continue
            if "date" in key_lower or "month" in key_lower or "period" in key_lower:
                date_val = val

        if margin_val and date_val:
            observations.append({"date": date_val, "value_mn": margin_val})

    if observations:
        print(f"  [FINRA] Parsed {len(observations)} monthly observations from CSV")
    return observations if observations else None


def parse_finra_table(table):
    """Parse an HTML table for margin debt data."""
    rows = table.find_all("tr")
    if len(rows) < 2:
        return None

    observations = []
    headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

    # Find relevant column indices
    date_col = None
    margin_col = None
    for i, h in enumerate(headers):
        if "date" in h or "month" in h or "period" in h:
            date_col = i
        if "debit" in h or "margin" in h:
            margin_col = i

    if date_col is None or margin_col is None:
        return None

    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) > max(date_col, margin_col):
            try:
                date_str = cells[date_col].get_text(strip=True)
                val_str = cells[margin_col].get_text(strip=True).replace(",", "").replace("$", "")
                margin_val = float(val_str)
                observations.append({"date": date_str, "value_mn": margin_val})
            except (ValueError, IndexError):
                continue

    return observations if observations else None


def fetch_margin_from_fred(api_key):
    """
    Fetch margin debt proxy from FRED as fallback.
    Series: BOGZ1FL663067003Q — security credit from brokers (quarterly, in millions).
    """
    try:
        obs = fetch_fred_series(FRED_MARGIN_SERIES, api_key, lookback_days=730)
        if obs:
            # Convert millions to billions
            for o in obs:
                o["value_bn"] = round(o["value"] / 1000, 1)
            print(f"  [FRED] Margin debt proxy: {len(obs)} quarterly observations")
            return obs
        else:
            print("  [FRED] No margin debt proxy data")
            return None
    except Exception as e:
        print(f"  [FRED] Margin debt fetch error: {e}")
        return None


def fetch_gdp_from_fred(api_key):
    """Fetch nominal GDP from FRED for debt/GDP ratio."""
    try:
        obs = fetch_fred_series(FRED_GDP_SERIES, api_key, lookback_days=400)
        if obs:
            print(f"  [FRED] GDP: latest ${obs[-1]['value']:.0f}B as of {obs[-1]['date']}")
            return obs
        return None
    except Exception as e:
        print(f"  [FRED] GDP fetch error: {e}")
        return None


def fetch_spx_yoy():
    """Compute SPX year-over-year return using yfinance."""
    try:
        import yfinance as yf
        spx = yf.Ticker("^GSPC")
        hist = spx.history(period="13mo")
        if len(hist) >= 2:
            current = hist["Close"].iloc[-1]
            # Find the value closest to 12 months ago
            oldest = hist["Close"].iloc[0]
            yoy_pct = round(((current - oldest) / oldest) * 100, 1)
            print(f"  [Yahoo] SPX YoY: {yoy_pct:+.1f}% (current: {current:,.0f})")
            return yoy_pct
        return None
    except ImportError:
        print("  [FINRA] yfinance not installed — skipping SPX YoY")
        return None
    except Exception as e:
        print(f"  [FINRA] SPX YoY failed: {e}")
        return None


def compute_streak(observations):
    """
    Count consecutive monthly increases from the most recent observation backwards.
    Returns (streak_count, streak_label).
    """
    if len(observations) < 2:
        return 0, ""

    streak = 0
    for i in range(len(observations) - 1, 0, -1):
        if observations[i]["value"] > observations[i - 1]["value"]:
            streak += 1
        else:
            break

    if streak == 0:
        return 0, ""

    suffix = "th"
    if streak % 10 == 1 and streak != 11:
        suffix = "st"
    elif streak % 10 == 2 and streak != 12:
        suffix = "nd"
    elif streak % 10 == 3 and streak != 13:
        suffix = "rd"

    return streak, f"{streak}{suffix} consecutive record"


def collect_all():
    """Run FINRA margin debt collection."""
    print("[FINRA Collector] Starting...")

    api_key = get_fred_api_key()
    results = {
        "timestamp": datetime.now().isoformat(),
        "source": "finra_collector",
        "margin_debt": {
            "current_bn": None,
            "current_date": None,
            "date_label": None,
            "history": [],
            "yoy_pct": None,
            "streak_months": 0,
            "streak_label": "",
            "source": None,
        },
        "gdp": {
            "current_bn": None,
            "as_of": None,
        },
        "debt_gdp_pct": None,
        "spx_yoy_pct": None,
    }

    # Step 1: Try FINRA website first
    finra_data = fetch_finra_margin_web()

    # Step 2: Fall back to FRED
    if not finra_data and api_key:
        fred_data = fetch_margin_from_fred(api_key)
        if fred_data:
            latest = fred_data[-1]
            results["margin_debt"]["current_bn"] = latest["value_bn"]
            results["margin_debt"]["current_date"] = latest["date"]
            results["margin_debt"]["source"] = "fred_quarterly"

            # Date label
            try:
                dt = datetime.strptime(latest["date"], "%Y-%m-%d")
                quarter = (dt.month - 1) // 3 + 1
                results["margin_debt"]["date_label"] = f"Q{quarter} {dt.year}"
            except ValueError:
                results["margin_debt"]["date_label"] = latest["date"]

            # History as bn
            results["margin_debt"]["history"] = [
                {"date": o["date"], "value_bn": o["value_bn"]}
                for o in fred_data
            ]

            # YoY computation
            if len(fred_data) >= 5:  # Need at least 4 quarters + current
                current_val = fred_data[-1]["value"]
                yoy_val = fred_data[-5]["value"] if len(fred_data) >= 5 else fred_data[0]["value"]
                if yoy_val > 0:
                    yoy_pct = round(((current_val - yoy_val) / yoy_val) * 100, 1)
                    results["margin_debt"]["yoy_pct"] = yoy_pct

            # Streak
            streak, label = compute_streak(
                [{"value": o["value"]} for o in fred_data]
            )
            results["margin_debt"]["streak_months"] = streak
            results["margin_debt"]["streak_label"] = label

    elif finra_data:
        # Process FINRA web data
        # Convert to a standard format
        # The FINRA data might be in millions — convert to billions
        for obs in finra_data:
            if obs.get("value_mn"):
                obs["value_bn"] = round(obs["value_mn"] / 1000, 1)
                obs["value"] = obs["value_mn"]

        if finra_data:
            latest = finra_data[-1]
            results["margin_debt"]["current_bn"] = latest.get("value_bn")
            results["margin_debt"]["current_date"] = latest.get("date")
            results["margin_debt"]["source"] = "finra_web"

            # History
            results["margin_debt"]["history"] = [
                {"date": o["date"], "value_bn": o.get("value_bn")}
                for o in finra_data
            ]

            # YoY
            if len(finra_data) >= 13:
                current_val = finra_data[-1]["value"]
                yoy_val = finra_data[-13]["value"]
                if yoy_val > 0:
                    results["margin_debt"]["yoy_pct"] = round(
                        ((current_val - yoy_val) / yoy_val) * 100, 1
                    )

            # Streak
            streak, label = compute_streak(finra_data)
            results["margin_debt"]["streak_months"] = streak
            results["margin_debt"]["streak_label"] = label

            # Date label
            results["margin_debt"]["date_label"] = latest.get("date", "")

    elif not api_key:
        print("  [FINRA] No FRED API key — cannot fetch margin data")
        print("  [FINRA] Set FRED_API_KEY in .env or environment")

    # Step 3: GDP from FRED
    if api_key:
        gdp_data = fetch_gdp_from_fred(api_key)
        if gdp_data:
            latest_gdp = gdp_data[-1]
            results["gdp"]["current_bn"] = latest_gdp["value"]
            results["gdp"]["as_of"] = latest_gdp["date"]

            # Compute debt/GDP ratio
            margin_bn = results["margin_debt"]["current_bn"]
            if margin_bn and latest_gdp["value"] > 0:
                results["debt_gdp_pct"] = round(
                    (margin_bn / latest_gdp["value"]) * 100, 2
                )

    # Step 4: SPX YoY
    results["spx_yoy_pct"] = fetch_spx_yoy()

    # Save raw
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_file = DATA_DIR / "raw_finra.json"
    with open(raw_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    margin_bn = results["margin_debt"]["current_bn"]
    if margin_bn:
        print(f"[FINRA Collector] Done. Margin: ${margin_bn}B "
              f"({results['margin_debt']['source']}). Saved to {raw_file}")
    else:
        print(f"[FINRA Collector] Done. No margin data available. Saved to {raw_file}")

    return results


if __name__ == "__main__":
    collect_all()
