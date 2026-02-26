"""
SqueezeMetrics Data Collector
Fetches: GEX (Gamma Exposure), DIX (Dark Index)
Source: squeezemetrics.com/monitor/dix
Schedule: Daily at 6:00 AM EST
"""

import requests
import json
import os
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"


def fetch_gex_dix():
    """
    Fetch GEX and DIX from SqueezeMetrics.
    
    SqueezeMetrics publishes daily GEX and DIX at:
    https://squeezemetrics.com/monitor/dix
    
    They expose a JSON API that the chart on their page uses.
    The endpoint returns historical daily data.
    """
    result = {
        "gex_current": None,
        "dix_current": None,
        "gex_history": [],
        "dix_history": [],
        "timestamp": None,
    }
    
    # SqueezeMetrics API endpoint (used by their frontend chart)
    # This serves JSON with columns: date, dix, gex
    url = "https://squeezemetrics.com/monitor/dix"
    api_url = "https://squeezemetrics.com/monitor/dix.json"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/html",
        "Referer": "https://squeezemetrics.com/monitor/dix",
    }
    
    # Try JSON API first
    try:
        resp = requests.get(api_url, timeout=15, headers=headers)
        print(f"  [SQM] JSON API status: {resp.status_code}")

        if resp.status_code == 200:
            data = resp.json()

            # Log the full structure for debugging
            if isinstance(data, dict):
                print(f"  [SQM] JSON keys: {list(data.keys())}")
                # Log a sample of each top-level key's type and length
                for k, v in data.items():
                    if isinstance(v, list):
                        print(f"  [SQM]   {k}: list[{len(v)}]"
                              f"{' first=' + repr(v[0]) if v else ''}")
                    elif isinstance(v, dict):
                        print(f"  [SQM]   {k}: dict keys={list(v.keys())[:5]}")
                    else:
                        print(f"  [SQM]   {k}: {type(v).__name__} = {repr(v)[:80]}")
            elif isinstance(data, list):
                print(f"  [SQM] Response is list[{len(data)}]"
                      f"{' first=' + repr(data[0]) if data else ''}")

            # Parse known structures
            parsed = False

            if isinstance(data, dict):
                # Structure A: {"data": [[date, dix, gex], ...]}
                if "data" in data and isinstance(data["data"], list):
                    rows = data["data"]
                    if rows and isinstance(rows[-1], (list, tuple)) and len(rows[-1]) >= 3:
                        latest = rows[-1]
                        result["dix_current"] = latest[1]
                        result["gex_current"] = latest[2]
                        for row in rows[-30:]:
                            result["gex_history"].append({"date": row[0], "value": row[2]})
                            result["dix_history"].append({"date": row[0], "value": row[1]})
                        parsed = True

                # Structure B: {"dates": [...], "dix": [...], "gex": [...]}
                if not parsed and "dates" in data:
                    dates = data.get("dates", [])
                    gex_vals = data.get("gex", [])
                    dix_vals = data.get("dix", [])
                    if gex_vals:
                        result["gex_current"] = gex_vals[-1]
                    if dix_vals:
                        result["dix_current"] = dix_vals[-1]
                    for i in range(max(0, len(dates) - 30), len(dates)):
                        if i < len(gex_vals):
                            result["gex_history"].append({"date": dates[i], "value": gex_vals[i]})
                        if i < len(dix_vals):
                            result["dix_history"].append({"date": dates[i], "value": dix_vals[i]})
                    parsed = True

                # Structure C: {"values": {"dix": ..., "gex": ...}} or similar nested
                if not parsed:
                    for key in ["values", "latest", "current"]:
                        if key in data and isinstance(data[key], dict):
                            sub = data[key]
                            if "gex" in sub:
                                result["gex_current"] = sub["gex"]
                            if "dix" in sub:
                                result["dix_current"] = sub["dix"]
                            if result["gex_current"] is not None:
                                parsed = True
                                break

            if result["gex_current"] is not None:
                gex_fmt = f"{result['gex_current']:.2f}" if isinstance(result['gex_current'], float) else str(result['gex_current'])
                dix_fmt = f"{result['dix_current']:.4f}" if isinstance(result['dix_current'], float) else str(result['dix_current'])
                print(f"  [SQM] GEX: {gex_fmt}B, DIX: {dix_fmt}")
            else:
                print("  [SQM] JSON parsed but values not extracted — response structure unrecognized")

        elif resp.status_code == 403:
            print("  [SQM] JSON API returned 403 Forbidden — may require auth or be rate-limited")
        else:
            print(f"  [SQM] JSON API returned {resp.status_code}")

    except requests.exceptions.JSONDecodeError:
        print("  [SQM] JSON endpoint returned non-JSON, trying HTML scrape...")
        _try_html_scrape(url, headers, result)
    except Exception as e:
        print(f"  [SQM] JSON API failed: {e}")
        _try_html_scrape(url, headers, result)
    
    result["timestamp"] = datetime.now().isoformat()
    return result


def _try_html_scrape(url, headers, result):
    """Fallback: scrape the SqueezeMetrics page for embedded chart data."""
    try:
        resp = requests.get(url, timeout=15, headers=headers)
        if resp.status_code == 200:
            # The page includes inline JS with chart data
            # Look for the data payload in script tags
            text = resp.text
            
            # Common patterns to look for:
            # var chartData = {...}
            # or data embedded in a <script> tag
            import re
            
            # Try to find GEX/DIX values in the page
            gex_match = re.search(r'gex["\s:]+([0-9.-]+)', text, re.IGNORECASE)
            dix_match = re.search(r'dix["\s:]+([0-9.]+)', text, re.IGNORECASE)
            
            if gex_match:
                result["gex_current"] = float(gex_match.group(1))
            if dix_match:
                result["dix_current"] = float(dix_match.group(1))
            
            if result["gex_current"] is not None:
                print(f"  [SQM] HTML scrape: GEX={result['gex_current']}, DIX={result['dix_current']}")
            else:
                print("  [SQM] HTML scrape: couldn't extract values")
    except Exception as e:
        print(f"  [SQM] HTML scrape failed: {e}")


def compute_gex_signal(gex_value):
    """Classify GEX into a regime signal."""
    if gex_value is None:
        return "UNKNOWN"
    if gex_value > 3:
        return "STRONG_POSITIVE"  # Dealers long gamma, vol suppressed
    elif gex_value > 0:
        return "POSITIVE"         # Mild vol suppression
    elif gex_value > -2:
        return "NEGATIVE"         # Mild vol amplification
    else:
        return "DEEP_NEGATIVE"    # Dealers short gamma, vol amplified


def compute_dix_signal(dix_value):
    """Classify DIX into a positioning signal."""
    if dix_value is None:
        return "UNKNOWN"
    if dix_value > 0.47:
        return "BULLISH"          # Dark pools buying
    elif dix_value > 0.42:
        return "NEUTRAL"          # Normal range
    elif dix_value > 0.38:
        return "CAUTIOUS"         # Some selling
    else:
        return "BEARISH"          # Dark pools selling heavily


def collect_all():
    """Run all SqueezeMetrics collectors."""
    print("[SqueezeMetrics Collector] Starting...")
    
    raw = fetch_gex_dix()
    
    results = {
        **raw,
        "gex_signal": compute_gex_signal(raw["gex_current"]),
        "dix_signal": compute_dix_signal(raw["dix_current"]),
        "source": "squeeze_collector",
    }
    
    # Save raw data
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_file = DATA_DIR / "raw_squeeze.json"
    with open(raw_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"[SqueezeMetrics Collector] Done. Saved to {raw_file}")
    return results


if __name__ == "__main__":
    collect_all()
