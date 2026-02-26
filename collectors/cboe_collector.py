"""
CBOE Data Collector
Fetches: Put/Call ratios (equity, index, total), SKEW index, VIX
Source: cboe.com public data pages
Schedule: Daily at 6:00 AM EST (previous close data)
"""

import requests
import json
import csv
import io
import os
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
HISTORY_DIR = DATA_DIR / "history"


def fetch_put_call_ratios():
    """
    Fetch daily P/C ratios from CBOE.

    Tries CBOE's delayed quote JSON API first, then falls back to
    scraping the CBOE daily statistics pages with BeautifulSoup.
    """
    ratios = {
        "equity": None,
        "index": None,
        "total": None,
        "timestamp": None,
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/json",
    }

    # Primary: CBOE delayed quote JSON API
    json_endpoints = {
        "total": "https://cdn.cboe.com/api/global/delayed_quotes/options/_PCR.json",
        "equity": "https://cdn.cboe.com/api/global/delayed_quotes/options/_EPCR.json",
        "index": "https://cdn.cboe.com/api/global/delayed_quotes/options/_IPCR.json",
    }

    for ratio_type, url in json_endpoints.items():
        try:
            resp = requests.get(url, timeout=15, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                # CBOE delayed quote JSON typically has "data" with "close" or "current_price"
                if "data" in data:
                    val = data["data"].get("close") or data["data"].get("current_price")
                    if val is not None:
                        ratios[ratio_type] = round(float(val), 4)
                        print(f"  [CBOE] {ratio_type} P/C (JSON): {ratios[ratio_type]}")
                        continue
                # Try alternative structure
                for key in ["close", "last", "current_price", "previousClose"]:
                    if key in data:
                        ratios[ratio_type] = round(float(data[key]), 4)
                        print(f"  [CBOE] {ratio_type} P/C (JSON/{key}): {ratios[ratio_type]}")
                        break
                if ratios[ratio_type] is not None:
                    continue
                print(f"  [CBOE] {ratio_type} JSON keys: {list(data.keys())[:8]} — could not extract value")
        except Exception as e:
            print(f"  [CBOE] {ratio_type} JSON endpoint failed: {e}")

    # Fallback: Scrape CBOE daily statistics HTML pages
    csv_urls = {
        "equity": "https://www.cboe.com/us/options/market_statistics/daily/equity-put-call-ratio/",
        "index": "https://www.cboe.com/us/options/market_statistics/daily/index-put-call-ratio/",
        "total": "https://www.cboe.com/us/options/market_statistics/daily/total-put-call-ratio/",
    }

    for ratio_type, url in csv_urls.items():
        if ratios[ratio_type] is not None:
            continue  # Already got it from JSON API

        try:
            resp = requests.get(url, timeout=15, headers=headers)
            if resp.status_code == 200:
                try:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(resp.text, "html.parser")

                    # CBOE pages typically have a data table with date and ratio columns
                    tables = soup.find_all("table")
                    for table in tables:
                        rows = table.find_all("tr")
                        if len(rows) > 1:
                            # Get the last data row (most recent date)
                            last_row = rows[-1]
                            cells = last_row.find_all("td")
                            if len(cells) >= 2:
                                # The ratio is typically in the last cell
                                for cell in reversed(cells):
                                    try:
                                        val = float(cell.get_text(strip=True))
                                        if 0.1 < val < 5.0:  # Sanity check for P/C ratio
                                            ratios[ratio_type] = round(val, 4)
                                            print(f"  [CBOE] {ratio_type} P/C (HTML): {ratios[ratio_type]}")
                                            break
                                    except ValueError:
                                        continue
                            if ratios[ratio_type] is not None:
                                break
                except ImportError:
                    print(f"  [CBOE] BeautifulSoup not installed, cannot parse HTML for {ratio_type}")
                except Exception as e:
                    print(f"  [CBOE] HTML parse error for {ratio_type}: {e}")

                if ratios[ratio_type] is None:
                    print(f"  [CBOE] {ratio_type} P/C page fetched ({len(resp.text)} bytes) but could not parse value")
        except Exception as e:
            print(f"  [CBOE] {ratio_type} fetch failed: {e}")

    ratios["timestamp"] = datetime.now().isoformat()
    return ratios


def fetch_skew_index():
    """
    Fetch CBOE SKEW index.
    
    Available at: https://www.cboe.com/tradable_products/vix/vix_historical_data/
    Also via: ^SKEW on Yahoo Finance as a backup
    """
    skew_data = {
        "current": None,
        "history": [],
        "timestamp": None,
    }
    
    # Primary: Try CBOE direct
    try:
        url = "https://cdn.cboe.com/api/global/delayed_quotes/options/_SKEW.json"
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            data = resp.json()
            print(f"  [CBOE] SKEW response keys: {list(data.keys())[:5]}")
    except Exception as e:
        print(f"  [CBOE] SKEW primary failed: {e}")
    
    # Fallback: Yahoo Finance
    try:
        import yfinance as yf
        skew = yf.Ticker("^SKEW")
        hist = skew.history(period="6mo")
        if not hist.empty:
            skew_data["current"] = round(hist["Close"].iloc[-1], 2)
            skew_data["history"] = [
                {"date": str(d.date()), "value": round(v, 2)}
                for d, v in hist["Close"].items()
            ]
            print(f"  [Yahoo] SKEW current: {skew_data['current']}")
    except Exception as e:
        print(f"  [Yahoo] SKEW fallback failed: {e}")
    
    skew_data["timestamp"] = datetime.now().isoformat()
    return skew_data


def fetch_vix():
    """Fetch VIX level and recent history."""
    vix_data = {
        "current": None,
        "prev_close": None,
        "change": None,
        "history_30d": [],
        "timestamp": None,
    }
    
    try:
        import yfinance as yf
        vix = yf.Ticker("^VIX")
        hist = vix.history(period="1mo")
        if not hist.empty:
            vix_data["current"] = round(hist["Close"].iloc[-1], 2)
            if len(hist) > 1:
                vix_data["prev_close"] = round(hist["Close"].iloc[-2], 2)
                vix_data["change"] = round(
                    vix_data["current"] - vix_data["prev_close"], 2
                )
            vix_data["history_30d"] = [
                {"date": str(d.date()), "value": round(v, 2)}
                for d, v in hist["Close"].items()
            ]
            print(f"  [Yahoo] VIX current: {vix_data['current']} ({vix_data['change']:+.2f})")
    except Exception as e:
        print(f"  [Yahoo] VIX failed: {e}")
    
    vix_data["timestamp"] = datetime.now().isoformat()
    return vix_data


def fetch_pc_ratio_yfinance():
    """
    Alternative P/C ratio calculation from Yahoo Finance options data.
    Pulls SPY/SPX options chains and calculates put/call volume ratio.
    """
    pc_data = {
        "spy_pc_ratio": None,
        "spx_pc_ratio": None,
        "spy_call_volume": 0,
        "spy_put_volume": 0,
        "timestamp": None,
    }
    
    try:
        import yfinance as yf
        spy = yf.Ticker("SPY")
        
        # Get nearest expiration
        expirations = spy.options
        if expirations:
            nearest = expirations[0]
            calls = spy.option_chain(nearest).calls
            puts = spy.option_chain(nearest).puts
            
            total_call_vol = calls["volume"].sum()
            total_put_vol = puts["volume"].sum()
            
            if total_call_vol > 0:
                pc_data["spy_pc_ratio"] = round(total_put_vol / total_call_vol, 4)
                pc_data["spy_call_volume"] = int(total_call_vol)
                pc_data["spy_put_volume"] = int(total_put_vol)
                print(f"  [Yahoo] SPY P/C: {pc_data['spy_pc_ratio']} "
                      f"(calls: {total_call_vol:,.0f}, puts: {total_put_vol:,.0f})")
    except Exception as e:
        print(f"  [Yahoo] SPY P/C failed: {e}")
    
    pc_data["timestamp"] = datetime.now().isoformat()
    return pc_data


def collect_all():
    """Run all CBOE collectors and return combined data."""
    print("[CBOE Collector] Starting...")
    
    results = {
        "pc_ratios_cboe": fetch_put_call_ratios(),
        "pc_ratios_yf": fetch_pc_ratio_yfinance(),
        "skew": fetch_skew_index(),
        "vix": fetch_vix(),
        "collected_at": datetime.now().isoformat(),
        "source": "cboe_collector",
    }
    
    # Save raw data
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_file = DATA_DIR / "raw_cboe.json"
    with open(raw_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"[CBOE Collector] Done. Saved to {raw_file}")
    return results


if __name__ == "__main__":
    collect_all()
