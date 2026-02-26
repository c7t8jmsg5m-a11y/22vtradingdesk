"""
Options Activity Processor
Normalizes data from all collectors into the unified schema
consumed by the React dashboard and morning briefing.
"""

import json
import glob
from datetime import datetime, timedelta
from pathlib import Path


def fetch_live_vix():
    """
    Fetch live VIX directly from yfinance.
    Called by the processor to ensure VIX is never stale.
    Returns dict with current, change, history_30d or empty dict on failure.
    """
    try:
        import yfinance as yf
        vix = yf.Ticker("^VIX")
        hist = vix.history(period="1mo")
        if hist.empty:
            return {}
        current = round(hist["Close"].iloc[-1], 2)
        prev = round(hist["Close"].iloc[-2], 2) if len(hist) > 1 else None
        change = round(current - prev, 2) if prev else None
        history = [
            {"date": str(d.date()), "value": round(v, 2)}
            for d, v in hist["Close"].items()
        ]
        print(f"[Processor] Live VIX from yfinance: {current} ({change:+.2f})" if change else
              f"[Processor] Live VIX from yfinance: {current}")
        return {
            "current": current,
            "prev_close": prev,
            "change": change,
            "history_30d": history,
        }
    except Exception as e:
        print(f"[Processor] Live VIX fetch failed: {e}")
        return {}

DATA_DIR = Path(__file__).parent.parent / "data"
HISTORY_DIR = DATA_DIR / "history"


def load_raw(filename):
    """Load raw collector output."""
    path = DATA_DIR / filename
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def load_history(days=30):
    """
    Load recent daily history files to compute moving averages.
    Returns a list of (date_str, data_dict) sorted by date ascending.
    """
    history = []
    if not HISTORY_DIR.exists():
        return history

    # Get all history files, sorted by name (date)
    files = sorted(HISTORY_DIR.glob("*.json"))

    # Only load the most recent N files
    for f in files[-days:]:
        try:
            with open(f) as fh:
                data = json.load(fh)
            date_str = f.stem  # filename is YYYY-MM-DD
            history.append((date_str, data))
        except (json.JSONDecodeError, IOError):
            continue

    return history


def compute_moving_averages(history, field_path, windows=(5, 20)):
    """
    Compute rolling moving averages from history data.

    field_path: dot-separated path to the value, e.g. "pc_ratio.equity"
    windows: tuple of window sizes to compute

    Returns dict of {window_size: ma_value} and a list of {date, value} history.
    """
    values = []
    history_series = []

    for date_str, data in history:
        # Navigate the dot path
        val = data
        for key in field_path.split("."):
            if isinstance(val, dict):
                val = val.get(key)
            else:
                val = None
                break

        if val is not None and isinstance(val, (int, float)):
            values.append(val)
            history_series.append({"date": date_str, "value": round(val, 4)})

    mas = {}
    for w in windows:
        if len(values) >= w:
            ma = sum(values[-w:]) / w
            mas[w] = round(ma, 4)
        else:
            mas[w] = None

    return mas, history_series


def classify_pc_signal(equity_pc, ma_5d=None):
    """Classify P/C ratio into a sentiment signal."""
    val = ma_5d if ma_5d else equity_pc
    if val is None:
        return "UNKNOWN"
    if val > 0.90:
        return "EXTREME_FEAR"
    elif val > 0.75:
        return "ELEVATED"
    elif val > 0.55:
        return "NEUTRAL"
    elif val > 0.40:
        return "COMPLACENT"
    else:
        return "EXTREME_GREED"


def classify_skew_signal(skew_val):
    if skew_val is None:
        return "UNKNOWN"
    if skew_val > 155:
        return "EXTREME"
    elif skew_val > 145:
        return "ELEVATED"
    elif skew_val > 125:
        return "MODERATE"
    else:
        return "LOW"


def classify_gex_signal(gex_val):
    if gex_val is None:
        return "UNKNOWN"
    if gex_val > 3:
        return "STRONG_POSITIVE"
    elif gex_val > 0:
        return "POSITIVE"
    elif gex_val > -2:
        return "NEGATIVE"
    else:
        return "DEEP_NEGATIVE"


def classify_dix_signal(dix_val):
    if dix_val is None:
        return "UNKNOWN"
    if dix_val > 0.47:
        return "BULLISH"
    elif dix_val > 0.42:
        return "NEUTRAL"
    elif dix_val > 0.38:
        return "CAUTIOUS"
    else:
        return "BEARISH"


def check_alerts(output):
    """Generate alerts based on threshold breaches."""
    alerts = []
    
    # P/C Ratio alerts
    pc = output.get("pc_ratio", {})
    eq_pc = pc.get("equity")
    if eq_pc and eq_pc > 0.90:
        alerts.append({
            "type": "PC_EXTREME_FEAR",
            "message": f"Equity P/C at {eq_pc:.2f} — extreme fear, contrarian buy zone",
            "severity": "critical",
        })
    elif eq_pc and eq_pc > 0.75:
        alerts.append({
            "type": "PC_ELEVATED",
            "message": f"Equity P/C at {eq_pc:.2f} — elevated hedging demand",
            "severity": "warning",
        })
    elif eq_pc and eq_pc < 0.40:
        alerts.append({
            "type": "PC_EXTREME_GREED",
            "message": f"Equity P/C at {eq_pc:.2f} — extreme complacency",
            "severity": "critical",
        })
    
    # GEX alerts
    gex = output.get("gex", {})
    gex_val = gex.get("estimate_bn")
    if gex_val is not None and gex_val < -2:
        alerts.append({
            "type": "GEX_DEEP_NEGATIVE",
            "message": f"GEX at {gex_val:.1f}B — dealers short gamma, vol amplification",
            "severity": "critical",
        })
    elif gex_val is not None and gex_val < 0:
        alerts.append({
            "type": "GEX_NEGATIVE",
            "message": f"GEX flipped negative at {gex_val:.1f}B",
            "severity": "warning",
        })
    
    # Skew alerts
    skew = output.get("skew", {})
    skew_val = skew.get("cboe_skew")
    if skew_val and skew_val > 155:
        alerts.append({
            "type": "SKEW_EXTREME",
            "message": f"CBOE SKEW at {skew_val:.0f} — elevated tail risk hedging",
            "severity": "critical",
        })
    elif skew_val and skew_val > 145:
        alerts.append({
            "type": "SKEW_ELEVATED",
            "message": f"CBOE SKEW at {skew_val:.0f} — above average tail demand",
            "severity": "warning",
        })
    
    # DIX alerts
    dix = output.get("dix", {})
    dix_val = dix.get("current")
    if dix_val and dix_val < 0.38:
        alerts.append({
            "type": "DIX_BEARISH",
            "message": f"DIX at {dix_val:.3f} — dark pools selling aggressively",
            "severity": "critical",
        })
    
    # 0DTE concentration
    vol = output.get("volume", {})
    zero_dte_pct = vol.get("zero_dte_call_pct", 0)
    if zero_dte_pct and zero_dte_pct > 55:
        alerts.append({
            "type": "ZERO_DTE_HIGH",
            "message": f"0DTE at {zero_dte_pct:.0f}% of total — structural fragility flag",
            "severity": "warning",
        })
    
    return alerts


def process():
    """
    Main processor: reads raw collector outputs, normalizes into dashboard schema.
    """
    print("[Processor] Loading raw data...")
    
    cboe = load_raw("raw_cboe.json")
    squeeze = load_raw("raw_squeeze.json")
    yahoo = load_raw("raw_yahoo.json")
    fred = load_raw("raw_fred.json")
    finra = load_raw("raw_finra.json")
    edgar = load_raw("raw_edgar.json")
    calendar = load_raw("raw_calendar.json")
    overrides = load_raw("manual_overrides.json")
    gex_manual = overrides.get("gex_manual", {})
    
    # Build unified output
    spy_chain = yahoo.get("spy_chain", {})
    summary = spy_chain.get("summary", {})
    vol_surface = yahoo.get("vol_surface", {})
    premium = yahoo.get("net_premium", {})
    vix = cboe.get("vix", {})
    # If CBOE collector didn't provide VIX, fetch live from yfinance
    if not vix.get("current"):
        vix = fetch_live_vix()
    # Last resort: FRED VIX (lags 1 day)
    if not vix.get("current") and fred.get("vix", {}).get("current"):
        print("[Processor] WARNING: Using FRED VIX (1-day lag)")
        vix = {
            "current": fred["vix"]["current"],
            "change": fred["vix"].get("change"),
            "history_30d": [
                {"date": h["date"], "value": h["value"]}
                for h in fred["vix"].get("history", [])[-30:]
            ],
        }
    skew_data = cboe.get("skew", {})
    
    # P/C ratio — prefer CBOE official, fallback to Yahoo-derived
    pc_yf = cboe.get("pc_ratios_yf", {})
    pc_cboe = cboe.get("pc_ratios_cboe", {})
    equity_pc = pc_yf.get("spy_pc_ratio") or summary.get("blended_pc_ratio")

    # Use CBOE official index/total P/C if available
    index_pc = pc_cboe.get("index") if isinstance(pc_cboe.get("index"), (int, float)) else None
    total_pc = pc_cboe.get("total") if isinstance(pc_cboe.get("total"), (int, float)) else None

    # Load history for moving averages
    history = load_history(30)
    pc_mas, pc_history = compute_moving_averages(history, "pc_ratio.equity", windows=(5, 20))
    print(f"[Processor] Loaded {len(history)} history files for MAs")

    output = {
        "timestamp": datetime.now().isoformat(),
        "source_tier": "free",

        "pc_ratio": {
            "equity": equity_pc,
            "index": index_pc,
            "total": total_pc,
            "equity_5d_ma": pc_mas.get(5),
            "equity_20d_ma": pc_mas.get(20),
            "signal": classify_pc_signal(equity_pc, ma_5d=pc_mas.get(5)),
            "history_30d": pc_history,
        },
        
        "gex": {
            "estimate_bn": squeeze.get("gex_current"),
            "level": classify_gex_signal(squeeze.get("gex_current")),
            "flip_point": gex_manual.get("flip_point"),
            "call_wall": gex_manual.get("call_wall"),
            "put_wall": gex_manual.get("put_wall"),
            "last_price": gex_manual.get("last_price"),
            "history_5d": squeeze.get("gex_history", [])[-5:],
        },
        
        "skew": {
            "cboe_skew": skew_data.get("current"),
            "put_25d_vol": vol_surface.get("put_25d_iv"),
            "call_25d_vol": vol_surface.get("call_25d_iv"),
            "risk_reversal": vol_surface.get("risk_reversal"),
            "atm_iv": vol_surface.get("atm_iv"),
            "signal": classify_skew_signal(skew_data.get("current")),
            "history_monthly": skew_data.get("history", []),
        },
        
        "volume": {
            "total_calls": summary.get("total_call_volume", 0),
            "total_puts": summary.get("total_put_volume", 0),
            "zero_dte_calls": summary.get("zero_dte_call_volume", 0),
            "zero_dte_puts": summary.get("zero_dte_put_volume", 0),
            "zero_dte_call_pct": summary.get("zero_dte_call_pct", 0),
            "zero_dte_put_pct": summary.get("zero_dte_put_pct", 0),
            "net_premium_calls_bn": premium.get("call_premium_bn", 0),
            "net_premium_puts_bn": premium.get("put_premium_bn", 0),
            "top_call_strikes": spy_chain.get("top_call_strikes", [])[:5],
            "top_put_strikes": spy_chain.get("top_put_strikes", [])[:5],
        },
        
        "vix": {
            "current": vix.get("current"),
            "change": vix.get("change"),
            "history_30d": vix.get("history_30d", []),
        },
        
        "dix": {
            "current": squeeze.get("dix_current"),
            "signal": classify_dix_signal(squeeze.get("dix_current")),
            "history_30d": squeeze.get("dix_history", [])[-30:],
        },

        "financing": {
            "nfci": {
                "current": fred.get("nfci", {}).get("current"),
                "prev": fred.get("nfci", {}).get("prev"),
                "change": fred.get("nfci", {}).get("change"),
                "as_of": fred.get("nfci", {}).get("as_of"),
                "history_90d": fred.get("nfci", {}).get("history", []),
            },
            "nfci_leverage": {
                "current": fred.get("nfci_leverage", {}).get("current"),
                "as_of": fred.get("nfci_leverage", {}).get("as_of"),
                "history_90d": fred.get("nfci_leverage", {}).get("history", []),
            },
            "hy_oas": {
                "current": fred.get("hy_oas", {}).get("current"),
                "prev": fred.get("hy_oas", {}).get("prev"),
                "change": fred.get("hy_oas", {}).get("change"),
                "as_of": fred.get("hy_oas", {}).get("as_of"),
                "history_90d": fred.get("hy_oas", {}).get("history", []),
            },
            "ccc_oas": {
                "current": fred.get("ccc_oas", {}).get("current"),
                "as_of": fred.get("ccc_oas", {}).get("as_of"),
                "history_90d": fred.get("ccc_oas", {}).get("history", []),
            },
            "sofr": {
                "current": fred.get("sofr", {}).get("current"),
                "prev": fred.get("sofr", {}).get("prev"),
                "change": fred.get("sofr", {}).get("change"),
                "as_of": fred.get("sofr", {}).get("as_of"),
                "history_90d": fred.get("sofr", {}).get("history", []),
            },
            "bbb_oas": {
                "current": fred.get("bbb_oas", {}).get("current"),
                "as_of": fred.get("bbb_oas", {}).get("as_of"),
                "history_90d": fred.get("bbb_oas", {}).get("history", []),
            },
            "signal": fred.get("financing_signal", "UNKNOWN"),
        },

        "margin_debt": {
            "current_bn": finra.get("margin_debt", {}).get("current_bn"),
            "date_label": finra.get("margin_debt", {}).get("date_label"),
            "yoy_pct": finra.get("margin_debt", {}).get("yoy_pct"),
            "streak_months": finra.get("margin_debt", {}).get("streak_months"),
            "streak_label": finra.get("margin_debt", {}).get("streak_label"),
            "source": finra.get("margin_debt", {}).get("source") or "manual",
            "history": finra.get("margin_debt", {}).get("history", []),
        },

        "debt_gdp_pct": finra.get("debt_gdp_pct"),
        "spx_yoy_pct": finra.get("spx_yoy_pct"),

        "crowding": {
            "themes": edgar.get("crowding_themes", []),
            "headline": edgar.get("crowding_headline", ""),
            "mag7_concentration": edgar.get("mag7_concentration", {}),
            "sector_concentration": edgar.get("sector_concentration", {}),
            "filing_quarter": edgar.get("filing_quarter"),
        },

        "calendar_events": calendar.get("events", []),

        "alerts": [],
    }
    
    # Generate alerts
    output["alerts"] = check_alerts(output)
    
    # Save latest.json
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    latest_file = DATA_DIR / "latest.json"
    with open(latest_file, "w") as f:
        json.dump(output, f, indent=2)
    
    # Save to history
    history_dir = DATA_DIR / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    history_file = history_dir / f"{datetime.now().strftime('%Y-%m-%d')}.json"
    with open(history_file, "w") as f:
        json.dump(output, f, indent=2)
    
    # Save alerts separately
    if output["alerts"]:
        alerts_file = DATA_DIR / "alerts.json"
        with open(alerts_file, "w") as f:
            json.dump(output["alerts"], f, indent=2)
    
    print(f"[Processor] Output saved to {latest_file}")
    print(f"[Processor] {len(output['alerts'])} alert(s) generated")
    
    for alert in output["alerts"]:
        icon = "🔴" if alert["severity"] == "critical" else "🟡"
        print(f"  {icon} {alert['type']}: {alert['message']}")
    
    return output


if __name__ == "__main__":
    process()
