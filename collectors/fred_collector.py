"""
FRED API Data Collector
Fetches: NFCI, HY OAS, SOFR, VIX, BBB OAS, CCC OAS, NFCI Leverage Subindex
Source: Federal Reserve Economic Data (FRED) — free API
Schedule: Daily at 6:00 AM EST + 1:00 PM EST

Requires FRED_API_KEY in environment or .env file.
Get a free key at: https://fredaccount.stlouisfed.org
"""

import os
import json
import requests
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

# FRED series relevant to the leverage monitor
SERIES = {
    "nfci": {
        "id": "NFCI",
        "name": "National Financial Conditions Index",
        "frequency": "weekly",
    },
    "nfci_leverage": {
        "id": "NFCILEVERAGE",
        "name": "NFCI Leverage Subindex",
        "frequency": "weekly",
    },
    "hy_oas": {
        "id": "BAMLH0A0HYM2",
        "name": "ICE BofA US High Yield Index OAS",
        "frequency": "daily",
    },
    "ccc_oas": {
        "id": "BAMLH0A3HYC",
        "name": "ICE BofA CCC & Lower US HY Index OAS",
        "frequency": "daily",
    },
    "bbb_oas": {
        "id": "BAMLC0A4CBBB",
        "name": "ICE BofA BBB US Corporate Index OAS",
        "frequency": "daily",
    },
    "sofr": {
        "id": "SOFR",
        "name": "Secured Overnight Financing Rate",
        "frequency": "daily",
    },
    "vix": {
        "id": "VIXCLS",
        "name": "CBOE Volatility Index: VIX",
        "frequency": "daily",
    },
}


def get_api_key():
    """Load FRED API key from environment or .env file."""
    key = os.environ.get("FRED_API_KEY")
    if key:
        return key

    # Try loading from .env file
    env_file = Path(__file__).parent.parent / ".env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith("FRED_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")

    return None


def fetch_series(series_id, api_key, lookback_days=90):
    """
    Fetch a single FRED series.
    Returns the most recent value and history.
    """
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

    # Filter out missing values (FRED uses "." for missing)
    valid = [
        {"date": obs["date"], "value": float(obs["value"])}
        for obs in observations
        if obs["value"] != "."
    ]

    if not valid:
        return {"current": None, "prev": None, "history": []}

    current = valid[-1]["value"]
    prev = valid[-2]["value"] if len(valid) > 1 else None

    return {
        "current": round(current, 4),
        "prev": round(prev, 4) if prev is not None else None,
        "change": round(current - prev, 4) if prev is not None else None,
        "as_of": valid[-1]["date"],
        "history": valid,
    }


def classify_financing_signal(nfci, hy_oas, sofr):
    """Classify overall financing conditions."""
    if nfci is None and hy_oas is None:
        return "UNKNOWN"

    stress_points = 0

    if nfci is not None:
        if nfci > 0:
            stress_points += 3  # Tighter than average
        elif nfci > -0.30:
            stress_points += 2  # Tightening
        elif nfci > -0.50:
            stress_points += 1  # Mildly loose

    if hy_oas is not None:
        if hy_oas > 500:
            stress_points += 3  # Crisis
        elif hy_oas > 350:
            stress_points += 2  # Stress
        elif hy_oas > 300:
            stress_points += 1  # Elevated

    if sofr is not None and sofr > 4.50:
        stress_points += 1

    if stress_points >= 5:
        return "STRESS"
    elif stress_points >= 3:
        return "TIGHTENING"
    elif stress_points >= 1:
        return "NEUTRAL"
    else:
        return "LOOSE"


def collect_all():
    """Run all FRED collectors and return combined data."""
    print("[FRED Collector] Starting...")

    api_key = get_api_key()
    if not api_key:
        print("  [FRED] No API key found. Set FRED_API_KEY in environment or .env file.")
        print("  [FRED] Get a free key at: https://fredaccount.stlouisfed.org")
        return {
            "error": "No FRED API key",
            "timestamp": datetime.now().isoformat(),
            "source": "fred_collector",
        }

    results = {
        "timestamp": datetime.now().isoformat(),
        "source": "fred_collector",
    }

    for key, series_info in SERIES.items():
        try:
            data = fetch_series(series_info["id"], api_key)
            results[key] = {
                **data,
                "series_id": series_info["id"],
                "name": series_info["name"],
                "frequency": series_info["frequency"],
            }
            if data["current"] is not None:
                print(f"  [FRED] {series_info['name']}: {data['current']}"
                      f" (as of {data['as_of']})")
            else:
                print(f"  [FRED] {series_info['name']}: no data")
        except Exception as e:
            print(f"  [FRED] {series_info['name']} failed: {e}")
            results[key] = {
                "current": None,
                "error": str(e),
                "series_id": series_info["id"],
            }

    # Compute financing signal
    nfci_val = results.get("nfci", {}).get("current")
    hy_val = results.get("hy_oas", {}).get("current")
    sofr_val = results.get("sofr", {}).get("current")
    results["financing_signal"] = classify_financing_signal(nfci_val, hy_val, sofr_val)
    print(f"  [FRED] Financing signal: {results['financing_signal']}")

    # Save raw data
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_file = DATA_DIR / "raw_fred.json"
    with open(raw_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"[FRED Collector] Done. Saved to {raw_file}")
    return results


if __name__ == "__main__":
    collect_all()
