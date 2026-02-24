"""
Options Activity Processor
Normalizes data from all collectors into the unified schema
consumed by the React dashboard and morning briefing.
"""

import json
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"


def load_raw(filename):
    """Load raw collector output."""
    path = DATA_DIR / filename
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


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
    
    # Build unified output
    spy_chain = yahoo.get("spy_chain", {})
    summary = spy_chain.get("summary", {})
    vol_surface = yahoo.get("vol_surface", {})
    premium = yahoo.get("net_premium", {})
    vix = cboe.get("vix", {})
    skew_data = cboe.get("skew", {})
    
    # P/C ratio — prefer CBOE official, fallback to Yahoo-derived
    pc_yf = cboe.get("pc_ratios_yf", {})
    equity_pc = pc_yf.get("spy_pc_ratio") or summary.get("blended_pc_ratio")
    
    output = {
        "timestamp": datetime.now().isoformat(),
        "source_tier": "free",
        
        "pc_ratio": {
            "equity": equity_pc,
            "index": None,  # Need CBOE HTML parse for this
            "total": None,
            "equity_5d_ma": None,  # Calculated from history DB
            "equity_20d_ma": None,
            "signal": classify_pc_signal(equity_pc),
            "history_30d": [],  # Populated from history archive
        },
        
        "gex": {
            "estimate_bn": squeeze.get("gex_current"),
            "level": classify_gex_signal(squeeze.get("gex_current")),
            "flip_point": None,  # Requires chain-level GEX calc
            "call_wall": None,
            "put_wall": None,
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
