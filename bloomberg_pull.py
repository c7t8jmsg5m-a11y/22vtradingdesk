"""
Bloomberg Unified Data Pull
Runs all BQL queries for both the Options Monitor and Leverage Monitor.
Execute on the 22V workstation (Windows PC) where Bloomberg Terminal is running.

Requirements:
    pip install xbbg pandas

Usage:
    python bloomberg_pull.py              # Full pull, save to JSON
    python bloomberg_pull.py --push       # Pull + push JSON to GitHub for Mac mini consumption
    python bloomberg_pull.py --quick      # Snapshot only (no history)

Schedule via Windows Task Scheduler:
    6:00 AM  — Pre-market pull (full history + snapshot)
    12:00 PM — Midday refresh (snapshot only)
    4:15 PM  — Post-close pull (full history + snapshot)
"""

import json
import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_DIR = Path(__file__).parent / "data"


# ============================================================
# TICKER DEFINITIONS
# ============================================================

# Options Monitor tickers
OPTIONS_TICKERS = {
    # Put/Call Ratios
    "PCUSEQTR Index": "equity_pc_ratio",
    "PCUSIDXT Index": "index_pc_ratio",
    "PCUSTOTT Index": "total_pc_ratio",
    # Volatility
    "VIX Index": "vix",
    "VVIX Index": "vvix",
    "SKEW Index": "skew",
    # VIX Term Structure
    "UX1 Index": "vix_fut_1m",
    "UX2 Index": "vix_fut_2m",
    "UX3 Index": "vix_fut_3m",
    "UX4 Index": "vix_fut_4m",
}

# Leverage Monitor tickers
LEVERAGE_TICKERS = {
    # Financing Conditions
    "GSUSFCI Index": "gs_fci",
    "NFCIINDX Index": "nfci",
    "BAMLHYSP Index": "hy_oas",
    "SOFRRATE Index": "sofr",
    "USYC2Y10 Index": "yield_curve_2s10s",
    # Leverage Levels
    "FINRMRGD Index": "finra_margin_debt",
    # Positioning
    "GSTHHVIP Index": "gs_hf_vip",
    "GSTHHFML Index": "gs_hf_crowded_longs",
    "GSTHSMS Index": "gs_most_shorted",
}

# Cross-Asset (shared)
CROSS_ASSET_TICKERS = {
    "SPX Index": "spx",
    "HYG US Equity": "hyg",
    "DXY Curncy": "dxy",
    "GC1 Comdty": "gold",
    "USGG2YR Index": "ust_2y",
    "USGG10YR Index": "ust_10y",
    "USGG30YR Index": "ust_30y",
}

ALL_TICKERS = {**OPTIONS_TICKERS, **LEVERAGE_TICKERS, **CROSS_ASSET_TICKERS}

# Tickers that need historical data for charts
HISTORY_TICKERS = [
    "PCUSEQTR Index", "PCUSIDXT Index", "PCUSTOTT Index",
    "VIX Index", "SKEW Index",
    "GSUSFCI Index", "NFCIINDX Index", "BAMLHYSP Index",
    "FINRMRGD Index", "USYC2Y10 Index",
    "SPX Index",
]


def pull_snapshot():
    """Pull current values for all tickers."""
    print("[BBG] Pulling snapshot...")

    try:
        from xbbg import blp

        tickers = list(ALL_TICKERS.keys())
        fields = ["px_last", "chg_net_1d", "chg_pct_1d"]

        df = blp.bdp(tickers, fields)

        snapshot = {}
        for ticker, row in df.iterrows():
            key = ALL_TICKERS.get(ticker, ticker)
            snapshot[key] = {
                "ticker": ticker,
                "value": row.get("px_last"),
                "change": row.get("chg_net_1d"),
                "change_pct": row.get("chg_pct_1d"),
                "timestamp": datetime.now().isoformat(),
            }
            print(f"  {key}: {row.get('px_last')} ({row.get('chg_pct_1d', 0):+.2f}%)")

        return snapshot

    except ImportError:
        print("[BBG] xbbg not installed. Run: pip install xbbg")
        print("[BBG] Generating template output for testing...")
        return _mock_snapshot()
    except Exception as e:
        print(f"[BBG] Snapshot failed: {e}")
        print("[BBG] Is Bloomberg Terminal running?")
        return _mock_snapshot()


def pull_history(lookback_days=180):
    """Pull historical data for chart-worthy tickers."""
    print(f"[BBG] Pulling {lookback_days}d history...")

    try:
        from xbbg import blp

        start = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

        df = blp.bdh(HISTORY_TICKERS, "px_last", start_date=start)

        history = {}
        for ticker in HISTORY_TICKERS:
            key = ALL_TICKERS.get(ticker, ticker)
            if ticker in df.columns.get_level_values(0):
                series = df[ticker]["px_last"].dropna()
                history[key] = [
                    {"date": str(d.date()), "value": round(v, 4)}
                    for d, v in series.items()
                ]
                print(f"  {key}: {len(history[key])} data points")

        return history

    except ImportError:
        print("[BBG] xbbg not available, skipping history")
        return {}
    except Exception as e:
        print(f"[BBG] History failed: {e}")
        return {}


def pull_vol_surface():
    """Pull SPX implied vol surface for options monitor."""
    print("[BBG] Pulling vol surface...")

    try:
        from xbbg import blp

        # BQL for vol surface data
        # Note: exact BQL syntax may need adjustment per your terminal version
        fields = [
            "ivol_delta_put(delta=25,period=30d)",
            "ivol_delta_call(delta=25,period=30d)",
            "ivol_atm(period=30d)",
        ]

        # Try BDP approach
        df = blp.bdp(["SPX Index"], ["PUT_IMPVOL_25DELTA_MID", "CALL_IMPVOL_25DELTA_MID", "AT_THE_MONEY_IMPVOL_MID"])

        surface = {
            "put_25d_iv": None,
            "call_25d_iv": None,
            "atm_iv": None,
            "risk_reversal": None,
        }

        if not df.empty:
            row = df.iloc[0]
            p25 = row.get("PUT_IMPVOL_25DELTA_MID")
            c25 = row.get("CALL_IMPVOL_25DELTA_MID")
            atm = row.get("AT_THE_MONEY_IMPVOL_MID")

            surface["put_25d_iv"] = round(p25, 2) if p25 else None
            surface["call_25d_iv"] = round(c25, 2) if c25 else None
            surface["atm_iv"] = round(atm, 2) if atm else None

            if p25 and c25:
                surface["risk_reversal"] = round(c25 - p25, 2)

            print(f"  25d Put: {surface['put_25d_iv']}, ATM: {surface['atm_iv']}, "
                  f"25d Call: {surface['call_25d_iv']}, RR: {surface['risk_reversal']}")

        return surface

    except Exception as e:
        print(f"[BBG] Vol surface failed: {e}")
        return {}


def pull_options_volume():
    """Pull SPX/SPY options volume breakdown."""
    print("[BBG] Pulling options volume...")

    try:
        from xbbg import blp

        df = blp.bdp(
            ["SPX Index", "SPY US Equity"],
            ["OPT_PUT_VOLUME", "OPT_CALL_VOLUME", "OPT_PUT_CALL_RATIO"]
        )

        volume = {}
        for ticker, row in df.iterrows():
            key = "spx" if "SPX" in ticker else "spy"
            volume[key] = {
                "put_volume": row.get("OPT_PUT_VOLUME"),
                "call_volume": row.get("OPT_CALL_VOLUME"),
                "pc_ratio": row.get("OPT_PUT_CALL_RATIO"),
            }
            print(f"  {key}: Puts={volume[key]['put_volume']}, "
                  f"Calls={volume[key]['call_volume']}, "
                  f"P/C={volume[key]['pc_ratio']}")

        return volume

    except Exception as e:
        print(f"[BBG] Options volume failed: {e}")
        return {}


def compute_derived_metrics(snapshot, history):
    """Calculate derived metrics for both systems."""
    derived = {}

    # VIX term structure
    vix_spot = snapshot.get("vix", {}).get("value")
    vix_1m = snapshot.get("vix_fut_1m", {}).get("value")
    vix_2m = snapshot.get("vix_fut_2m", {}).get("value")

    if vix_spot and vix_1m:
        derived["vix_term_structure"] = "contango" if vix_1m > vix_spot else "backwardation"
        derived["vix_contango_pct"] = round((vix_1m / vix_spot - 1) * 100, 2)

    # Margin debt ratios (if available)
    margin = snapshot.get("finra_margin_debt", {}).get("value")
    spx_mktcap = snapshot.get("spx", {}).get("value")  # This is price, need actual mkt cap

    # Margin debt YoY from history
    margin_hist = history.get("finra_margin_debt", [])
    if margin and len(margin_hist) >= 12:
        margin_12m_ago = margin_hist[-12]["value"] if len(margin_hist) >= 12 else None
        if margin_12m_ago and margin_12m_ago > 0:
            derived["margin_debt_yoy"] = round((margin / margin_12m_ago - 1) * 100, 2)

    # P/C ratio 5-day and 20-day MAs from history
    pc_hist = history.get("equity_pc_ratio", [])
    if len(pc_hist) >= 5:
        derived["equity_pc_5d_ma"] = round(
            sum(p["value"] for p in pc_hist[-5:]) / 5, 4
        )
    if len(pc_hist) >= 20:
        derived["equity_pc_20d_ma"] = round(
            sum(p["value"] for p in pc_hist[-20:]) / 20, 4
        )

    # HY spread direction (30d change)
    hy_hist = history.get("hy_oas", [])
    if len(hy_hist) >= 22:
        hy_now = hy_hist[-1]["value"]
        hy_30d_ago = hy_hist[-22]["value"]
        derived["hy_spread_30d_change"] = round(hy_now - hy_30d_ago, 1)
        derived["hy_spread_direction"] = "widening" if hy_now > hy_30d_ago else "tightening"

    # FCI direction
    fci_hist = history.get("gs_fci", [])
    if len(fci_hist) >= 22:
        fci_now = fci_hist[-1]["value"]
        fci_30d_ago = fci_hist[-22]["value"]
        derived["fci_30d_change"] = round(fci_now - fci_30d_ago, 4)
        derived["fci_direction"] = "tightening" if fci_now > fci_30d_ago else "loosening"

    return derived


def assemble_output(snapshot, history, vol_surface, options_volume, derived):
    """Assemble the unified JSON output consumed by both systems."""
    output = {
        "timestamp": datetime.now().isoformat(),
        "source": "bloomberg",

        # Options Monitor section
        "options_monitor": {
            "pc_ratio": {
                "equity": snapshot.get("equity_pc_ratio", {}).get("value"),
                "index": snapshot.get("index_pc_ratio", {}).get("value"),
                "total": snapshot.get("total_pc_ratio", {}).get("value"),
                "equity_5d_ma": derived.get("equity_pc_5d_ma"),
                "equity_20d_ma": derived.get("equity_pc_20d_ma"),
                "history_30d": history.get("equity_pc_ratio", [])[-30:],
            },
            "volatility": {
                "vix": snapshot.get("vix", {}).get("value"),
                "vix_change": snapshot.get("vix", {}).get("change"),
                "vvix": snapshot.get("vvix", {}).get("value"),
                "skew": snapshot.get("skew", {}).get("value"),
                "term_structure": derived.get("vix_term_structure"),
                "contango_pct": derived.get("vix_contango_pct"),
                "vix_futures": {
                    "1m": snapshot.get("vix_fut_1m", {}).get("value"),
                    "2m": snapshot.get("vix_fut_2m", {}).get("value"),
                    "3m": snapshot.get("vix_fut_3m", {}).get("value"),
                    "4m": snapshot.get("vix_fut_4m", {}).get("value"),
                },
                "skew_history": history.get("skew", []),
                "vix_history": history.get("vix", []),
            },
            "vol_surface": vol_surface,
            "options_volume": options_volume,
        },

        # Leverage Monitor section
        "leverage_monitor": {
            "layer1_levels": {
                "finra_margin_debt": snapshot.get("finra_margin_debt", {}).get("value"),
                "margin_debt_yoy": derived.get("margin_debt_yoy"),
                "margin_debt_history": history.get("finra_margin_debt", []),
            },
            "layer3_financing": {
                "gs_fci": snapshot.get("gs_fci", {}).get("value"),
                "gs_fci_direction": derived.get("fci_direction"),
                "gs_fci_30d_change": derived.get("fci_30d_change"),
                "nfci": snapshot.get("nfci", {}).get("value"),
                "hy_oas": snapshot.get("hy_oas", {}).get("value"),
                "hy_spread_direction": derived.get("hy_spread_direction"),
                "hy_spread_30d_change": derived.get("hy_spread_30d_change"),
                "sofr": snapshot.get("sofr", {}).get("value"),
                "yield_curve_2s10s": snapshot.get("yield_curve_2s10s", {}).get("value"),
                "fci_history": history.get("gs_fci", []),
                "hy_history": history.get("hy_oas", []),
            },
            "layer4_positioning": {
                "gs_vip": snapshot.get("gs_hf_vip", {}),
                "gs_crowded_longs": snapshot.get("gs_hf_crowded_longs", {}),
                "gs_most_shorted": snapshot.get("gs_most_shorted", {}),
            },
            "layer5_vol": "see options_monitor.volatility",
        },

        # Cross-asset context
        "cross_asset": {
            "spx": snapshot.get("spx", {}),
            "dxy": snapshot.get("dxy", {}),
            "gold": snapshot.get("gold", {}),
            "hyg": snapshot.get("hyg", {}),
            "ust_2y": snapshot.get("ust_2y", {}).get("value"),
            "ust_10y": snapshot.get("ust_10y", {}).get("value"),
            "ust_30y": snapshot.get("ust_30y", {}).get("value"),
        },
    }

    return output


def _mock_snapshot():
    """Generate mock data for testing without Bloomberg."""
    mock = {}
    for ticker, key in ALL_TICKERS.items():
        mock[key] = {
            "ticker": ticker,
            "value": None,
            "change": None,
            "change_pct": None,
            "timestamp": datetime.now().isoformat(),
            "note": "MOCK — Bloomberg not connected",
        }
    return mock


def run(full=True, push=False):
    """Main execution."""
    print("=" * 60)
    print(f"BLOOMBERG UNIFIED PULL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Pull data
    snapshot = pull_snapshot()
    history = pull_history() if full else {}
    vol_surface = pull_vol_surface()
    options_volume = pull_options_volume()

    # Compute derived metrics
    derived = compute_derived_metrics(snapshot, history)

    # Assemble output
    output = assemble_output(snapshot, history, vol_surface, options_volume, derived)

    # Save
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    latest_file = OUTPUT_DIR / "bloomberg_latest.json"
    with open(latest_file, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n[BBG] Saved to {latest_file}")

    # Archive
    archive_file = OUTPUT_DIR / f"bloomberg_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    with open(archive_file, "w") as f:
        json.dump(output, f, indent=2, default=str)

    if push:
        _push_to_github()

    return output


def _push_to_github():
    """Push bloomberg_latest.json to GitHub for Mac mini consumption."""
    import subprocess
    try:
        subprocess.run(["git", "add", "data/bloomberg_latest.json"], cwd=str(Path(__file__).parent))
        subprocess.run(["git", "commit", "-m", f"BBG pull {datetime.now().strftime('%Y-%m-%d %H:%M')}"],
                       cwd=str(Path(__file__).parent))
        subprocess.run(["git", "push"], cwd=str(Path(__file__).parent))
        print("[BBG] Pushed to GitHub")
    except Exception as e:
        print(f"[BBG] GitHub push failed: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bloomberg Unified Data Pull")
    parser.add_argument("--push", action="store_true", help="Push to GitHub after pull")
    parser.add_argument("--quick", action="store_true", help="Snapshot only, no history")
    args = parser.parse_args()

    run(full=not args.quick, push=args.push)
