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
        if resp.status_code == 200:
            data = resp.json()
            
            # SqueezeMetrics returns data in columnar format
            # Typical structure: {"data": [[date, dix, gex], ...]}
            # or {"dates": [...], "dix": [...], "gex": [...]}
            if isinstance(data, dict):
                print(f"  [SQM] JSON keys: {list(data.keys())[:5]}")
                
                # Handle different response structures
                if "data" in data:
                    rows = data["data"]
                    if rows:
                        latest = rows[-1]
                        result["gex_current"] = latest[-1] if len(latest) > 2 else None
                        result["dix_current"] = latest[1] if len(latest) > 1 else None
                        
                        # Last 30 days of history
                        for row in rows[-30:]:
                            result["gex_history"].append({
                                "date": row[0],
                                "value": row[-1] if len(row) > 2 else None,
                            })
                            result["dix_history"].append({
                                "date": row[0],
                                "value": row[1] if len(row) > 1 else None,
                            })
                
                elif "dates" in data:
                    dates = data.get("dates", [])
                    gex_vals = data.get("gex", [])
                    dix_vals = data.get("dix", [])
                    
                    if gex_vals:
                        result["gex_current"] = gex_vals[-1]
                    if dix_vals:
                        result["dix_current"] = dix_vals[-1]
                    
                    for i in range(max(0, len(dates) - 30), len(dates)):
                        result["gex_history"].append({
                            "date": dates[i],
                            "value": gex_vals[i] if i < len(gex_vals) else None,
                        })
                        result["dix_history"].append({
                            "date": dates[i],
                            "value": dix_vals[i] if i < len(dix_vals) else None,
                        })
            
            if result["gex_current"] is not None:
                print(f"  [SQM] GEX: {result['gex_current']:.2f}B, DIX: {result['dix_current']:.4f}")
            else:
                print("  [SQM] JSON parsed but values not extracted — check response structure")
                
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
