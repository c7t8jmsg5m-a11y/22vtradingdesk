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


def parse_finra_date(date_str):
    """
    Parse FINRA date format 'Mon-YY' (e.g., 'Jan-26') to ISO date string.
    Returns first day of the month as 'YYYY-MM-01'.
    """
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    parts = date_str.strip().split("-")
    if len(parts) != 2:
        return None
    month_str = parts[0].lower().strip()
    year_str = parts[1].strip()
    month = month_map.get(month_str)
    if month is None:
        return None
    # 2-digit year: 00-69 → 2000s, 70-99 → 1900s
    try:
        yr = int(year_str)
        year = 2000 + yr if yr < 70 else 1900 + yr
    except ValueError:
        return None
    return f"{year}-{month:02d}-01"


def fetch_finra_margin_web():
    """
    Fetch FINRA margin statistics from their official page.
    FINRA publishes monthly margin data at:
    https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics

    Table has columns:
    - Month/Year (format: 'Mon-YY', e.g., 'Jan-26')
    - Debit Balances in Customers' Securities Margin Accounts (millions)
    - Free Credit Balances in Customers' Cash Accounts (millions)
    - Free Credit Balances in Customers' Securities Margin Accounts (millions)

    Returns list of monthly observations sorted oldest→newest, or None if scraping fails.
    """
    try:
        from bs4 import BeautifulSoup

        margin_url = "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics"

        resp = requests.get(margin_url, timeout=20, headers=HEADERS)
        if resp.status_code != 200:
            print(f"  [FINRA] Website returned HTTP {resp.status_code}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find the table with margin data — look for a table that has
        # 'Debit' in one of its header cells
        tables = soup.find_all("table")
        target_table = None
        date_col = None
        debit_col = None

        for table in tables:
            header_row = table.find("tr")
            if not header_row:
                continue
            headers = [
                cell.get_text(strip=True).lower()
                for cell in header_row.find_all(["th", "td"])
            ]
            for i, h in enumerate(headers):
                if "month" in h or "year" in h:
                    date_col = i
                if "debit" in h:
                    debit_col = i
            if date_col is not None and debit_col is not None:
                target_table = table
                break

        if target_table is None:
            # Fallback: try any table with Mon-YY pattern in first column
            for table in tables:
                rows = table.find_all("tr")
                if len(rows) < 3:
                    continue
                first_data_row = rows[1]
                cells = first_data_row.find_all("td")
                if cells:
                    text = cells[0].get_text(strip=True)
                    if parse_finra_date(text):
                        target_table = table
                        date_col = 0
                        debit_col = 1
                        break

        if target_table is None:
            print("  [FINRA] Could not find margin data table on page")
            return None

        # Parse all data rows
        observations = []
        rows = target_table.find_all("tr")
        for row in rows[1:]:  # Skip header row
            cells = row.find_all(["td", "th"])
            if len(cells) <= max(date_col, debit_col):
                continue
            raw_date = cells[date_col].get_text(strip=True)
            raw_debit = cells[debit_col].get_text(strip=True)

            iso_date = parse_finra_date(raw_date)
            if not iso_date:
                continue

            try:
                debit_mn = float(raw_debit.replace(",", "").replace("$", ""))
            except (ValueError, AttributeError):
                continue

            observations.append({
                "date": iso_date,
                "value_mn": debit_mn,
                "date_raw": raw_date,
            })

        if not observations:
            print("  [FINRA] Table found but no data rows parsed")
            return None

        # Sort oldest → newest
        observations.sort(key=lambda x: x["date"])
        print(f"  [FINRA] Parsed {len(observations)} monthly observations from website")
        print(f"  [FINRA] Latest: {observations[-1]['date_raw']} — ${observations[-1]['value_mn']:,.0f}M")
        return observations

    except ImportError:
        print("  [FINRA] BeautifulSoup not installed — skipping web scrape")
        return None
    except Exception as e:
        print(f"  [FINRA] Web scrape error: {e}")
        return None


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
    Count consecutive all-time-high months from the most recent observation backwards.
    A month is a "record high" if its value exceeds all prior months' values.
    Returns (streak_count, streak_label).
    """
    if len(observations) < 2:
        return 0, ""

    streak = 0
    # Walk backwards and check if each month was a new all-time high
    for i in range(len(observations) - 1, 0, -1):
        prior_max = max(obs["value"] for obs in observations[:i])
        if observations[i]["value"] > prior_max:
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

    return streak, f"{streak}{suffix} consecutive record high"


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

    # Step 1: Try FINRA website first (monthly data, most current)
    finra_data = fetch_finra_margin_web()

    if finra_data:
        # Process FINRA web data (monthly, in millions)
        for obs in finra_data:
            if obs.get("value_mn"):
                obs["value_bn"] = round(obs["value_mn"] / 1000, 1)
                obs["value"] = obs["value_mn"]

        latest = finra_data[-1]
        results["margin_debt"]["current_bn"] = latest.get("value_bn")
        results["margin_debt"]["current_date"] = latest.get("date")
        results["margin_debt"]["source"] = "finra_monthly"

        # Monthly date label: "Jan 2026" from ISO date "2026-01-01"
        try:
            dt = datetime.strptime(latest["date"], "%Y-%m-%d")
            results["margin_debt"]["date_label"] = f"{MONTH_NAMES[dt.month]} {dt.year}"
        except (ValueError, IndexError):
            results["margin_debt"]["date_label"] = latest.get("date_raw", latest["date"])

        # History as bn
        results["margin_debt"]["history"] = [
            {"date": o["date"], "value_bn": o.get("value_bn")}
            for o in finra_data
        ]

        # YoY: compare current month to same month 12 months ago
        if len(finra_data) >= 13:
            current_val = finra_data[-1]["value"]
            yoy_val = finra_data[-13]["value"]
            if yoy_val > 0:
                results["margin_debt"]["yoy_pct"] = round(
                    ((current_val - yoy_val) / yoy_val) * 100, 1
                )

        # Streak (consecutive monthly increases)
        streak, label = compute_streak(finra_data)
        results["margin_debt"]["streak_months"] = streak
        results["margin_debt"]["streak_label"] = label

        # MoM change
        if len(finra_data) >= 2:
            prev_val = finra_data[-2]["value"]
            current_val = finra_data[-1]["value"]
            if prev_val > 0:
                results["margin_debt"]["mom_pct"] = round(
                    ((current_val - prev_val) / prev_val) * 100, 1
                )

    # Step 2: Fall back to FRED quarterly if FINRA scrape failed
    elif api_key:
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
            if len(fred_data) >= 5:
                current_val = fred_data[-1]["value"]
                yoy_val = fred_data[-5]["value"]
                if yoy_val > 0:
                    yoy_pct = round(((current_val - yoy_val) / yoy_val) * 100, 1)
                    results["margin_debt"]["yoy_pct"] = yoy_pct

            # Streak
            streak, label = compute_streak(
                [{"value": o["value"]} for o in fred_data]
            )
            results["margin_debt"]["streak_months"] = streak
            results["margin_debt"]["streak_label"] = label

    else:
        print("  [FINRA] No FRED API key and web scrape failed — no margin data")
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
