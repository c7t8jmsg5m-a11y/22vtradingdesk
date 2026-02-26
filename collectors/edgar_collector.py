"""
Market Crowding / Concentration Collector
Fetches: Mag-7 market cap concentration, sector weights, AI/semi exposure
Source: yfinance (market cap data)
Schedule: Daily

Measures WHERE exposure is concentrated in equity markets.
Answers: How crowded is Mag-7? How heavy is tech/semis? Is concentration rising?
"""

import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(__file__).parent.parent / "data"

# Magnificent 7
MAG7 = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "GOOGL": "Alphabet",
    "AMZN": "Amazon",
    "NVDA": "NVIDIA",
    "META": "Meta",
    "TSLA": "Tesla",
}

# AI infrastructure / semiconductor names
AI_INFRA = {
    "NVDA": "NVIDIA",
    "AMD": "AMD",
    "MU": "Micron",
    "LRCX": "Lam Research",
    "AMAT": "Applied Materials",
    "KLAC": "KLA Corp",
    "ASML": "ASML",
    "AVGO": "Broadcom",
    "TSM": "TSMC",
    "NOW": "ServiceNow",
}

# SPY top ~50 holdings approximate weights (updated periodically)
# Used as a reference for concentration vs equal-weight
SPY_TOTAL_STOCKS = 503


def fetch_market_caps(tickers):
    """Fetch market cap for a list of tickers."""
    import yfinance as yf

    caps = {}
    for ticker in tickers:
        try:
            tk = yf.Ticker(ticker)
            info = tk.info
            cap = info.get("marketCap")
            if cap:
                caps[ticker] = {
                    "market_cap_bn": round(cap / 1e9, 1),
                    "name": info.get("shortName", ticker),
                }
        except Exception as e:
            print(f"  [Crowding] {ticker} failed: {e}")
    return caps


def fetch_spx_cap():
    """Fetch total S&P 500 market cap via SPY."""
    import yfinance as yf

    try:
        spy = yf.Ticker("SPY")
        info = spy.info
        # SPY market cap * ~10 ≈ SPX total (SPY is 1/10 of SPX)
        # Actually, use the sum approach: SPY AUM * shares outstanding is ETF cap
        # Better: just sum the Mag-7 and compare to total from info
        spy_price = info.get("regularMarketPrice") or info.get("previousClose")
        # SPX total market cap ~$50T — use a known reference
        # We'll compute Mag-7 as % of the Mag-7+rest, using SPY weight data
        return None
    except Exception:
        return None


def compute_concentration(mag7_caps, ai_caps):
    """
    Compute concentration metrics.
    Returns themes in the format expected by the scoring engine.
    """
    themes = []

    # Total Mag-7 market cap
    mag7_total = sum(d["market_cap_bn"] for d in mag7_caps.values())

    # Approximate SPX total market cap (~$50T as of early 2026)
    # We'll use the ratio of Mag-7 to SPX
    # More precisely: fetch SPX cap-weighted % from yfinance
    # For now, estimate from known: Mag-7 ≈ 30-35% of SPX
    # We'll use market caps directly for relative comparison

    # Theme 1: Mag-7 as % of total watched universe
    all_unique = dict(mag7_caps)
    for t, d in ai_caps.items():
        if t not in all_unique:
            all_unique[t] = d
    universe_total = sum(d["market_cap_bn"] for d in all_unique.values())

    if universe_total > 0:
        mag7_pct_universe = round((mag7_total / universe_total) * 100, 1)

    # Mag-7 concentration score
    # Historical: Mag-7 = 25-35% of S&P 500
    # Above 30% is elevated, above 33% is extreme
    # Estimate: Mag-7 total cap / approximate SPX total cap
    # SPX ~$50T in early 2026
    approx_spx_total = 50000  # $50T in billions
    mag7_pct_spx = round((mag7_total / approx_spx_total) * 100, 1) if approx_spx_total else 0

    # Map to percentile: 20% = 30th pctl, 25% = 60th, 30% = 80th, 33% = 90th, 35% = 95th
    if mag7_pct_spx >= 33:
        mag7_score = 95
    elif mag7_pct_spx >= 30:
        mag7_score = 85
    elif mag7_pct_spx >= 27:
        mag7_score = 70
    elif mag7_pct_spx >= 24:
        mag7_score = 55
    else:
        mag7_score = 35

    themes.append({
        "name": "Mag 7 % of S&P 500",
        "pct": mag7_score,
        "detail": f"~{mag7_pct_spx}% of SPX market cap (${mag7_total:,.0f}B)",
    })

    # Theme 2: AI/Semiconductor concentration
    ai_total = sum(d["market_cap_bn"] for d in ai_caps.values())
    ai_pct_spx = round((ai_total / approx_spx_total) * 100, 1) if approx_spx_total else 0

    # AI/semi historically ~8-15% of SPX. Above 12% = elevated, above 15% = crowded
    if ai_pct_spx >= 15:
        ai_score = 95
    elif ai_pct_spx >= 12:
        ai_score = 85
    elif ai_pct_spx >= 10:
        ai_score = 70
    elif ai_pct_spx >= 8:
        ai_score = 55
    else:
        ai_score = 35

    themes.append({
        "name": "AI Infra Concentration",
        "pct": ai_score,
        "detail": f"~{ai_pct_spx}% of SPX (${ai_total:,.0f}B across {len(ai_caps)} names)",
    })

    # Theme 3: Single-stock dominance (largest name)
    all_caps = {**mag7_caps, **ai_caps}
    if all_caps:
        top_stock = max(all_caps.items(), key=lambda x: x[1]["market_cap_bn"])
        top_pct = round((top_stock[1]["market_cap_bn"] / approx_spx_total) * 100, 1)

        # Single stock > 6% of SPX is historically extreme
        if top_pct >= 7:
            single_score = 95
        elif top_pct >= 5:
            single_score = 80
        elif top_pct >= 4:
            single_score = 60
        else:
            single_score = 40

        themes.append({
            "name": f"{top_stock[0]} Single-Stock Weight",
            "pct": single_score,
            "detail": f"~{top_pct}% of SPX (${top_stock[1]['market_cap_bn']:,.0f}B)",
        })

    return themes, {
        "mag7_total_bn": mag7_total,
        "mag7_pct_spx": mag7_pct_spx,
        "ai_total_bn": ai_total,
        "ai_pct_spx": ai_pct_spx,
    }


def collect_all():
    """Run market concentration analysis."""
    print("[Crowding Collector] Starting...")

    results = {
        "timestamp": datetime.now().isoformat(),
        "source": "edgar_collector",
        "filing_quarter": None,
        "funds_analyzed": 0,
        "funds_total": 0,
        "crowding_themes": [],
        "crowding_headline": "",
        "mag7_concentration": {},
        "sector_concentration": {},
    }

    try:
        import yfinance as yf
    except ImportError:
        print("  [Crowding] yfinance not installed. Run: pip install yfinance")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(DATA_DIR / "raw_edgar.json", "w") as f:
            json.dump(results, f, indent=2)
        return results

    # Fetch Mag-7 market caps
    print("  [Crowding] Fetching Mag-7 market caps...")
    mag7_caps = fetch_market_caps(MAG7.keys())

    # Fetch AI infra market caps (skip duplicates with Mag-7)
    extra_ai = {t: n for t, n in AI_INFRA.items() if t not in MAG7}
    print(f"  [Crowding] Fetching {len(extra_ai)} AI infra names...")
    ai_extra_caps = fetch_market_caps(extra_ai.keys())

    # Combine AI infra (Mag-7 members + extras)
    ai_caps = {}
    for t in AI_INFRA:
        if t in mag7_caps:
            ai_caps[t] = mag7_caps[t]
        elif t in ai_extra_caps:
            ai_caps[t] = ai_extra_caps[t]

    if not mag7_caps:
        print("[Crowding Collector] No market cap data. Saved empty result.")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(DATA_DIR / "raw_edgar.json", "w") as f:
            json.dump(results, f, indent=2)
        return results

    # Determine quarter
    today = datetime.now()
    q = (today.month - 1) // 3 + 1
    results["filing_quarter"] = f"{today.year}-Q{q}"

    # Compute concentration
    themes, metrics = compute_concentration(mag7_caps, ai_caps)
    results["crowding_themes"] = themes

    # Mag-7 detail
    results["mag7_concentration"] = {
        "holdings": {
            t: {"market_cap_bn": d["market_cap_bn"], "name": d["name"]}
            for t, d in sorted(mag7_caps.items(), key=lambda x: x[1]["market_cap_bn"], reverse=True)
        },
        "total_bn": metrics["mag7_total_bn"],
        "pct_spx": metrics["mag7_pct_spx"],
    }

    # Sector concentration
    sector_values = defaultdict(float)
    sector_map = {
        "AAPL": "Tech", "MSFT": "Tech", "NVDA": "Semis", "AVGO": "Semis",
        "AMD": "Semis", "MU": "Semis", "LRCX": "Semi Equip", "AMAT": "Semi Equip",
        "KLAC": "Semi Equip", "ASML": "Semi Equip", "TSM": "Semis",
        "GOOGL": "Internet", "META": "Internet", "AMZN": "Internet",
        "TSLA": "EV/Auto", "NOW": "Software",
    }
    all_caps = {**mag7_caps, **ai_extra_caps}
    for t, d in all_caps.items():
        sector = sector_map.get(t, "Other")
        sector_values[sector] += d["market_cap_bn"]

    total_cap = sum(sector_values.values())
    if total_cap > 0:
        results["sector_concentration"] = {
            s: round((v / total_cap) * 100, 1)
            for s, v in sorted(sector_values.items(), key=lambda x: x[1], reverse=True)
        }

    # Generate headline
    above_80 = [t for t in themes if t["pct"] > 80]
    if above_80:
        results["crowding_headline"] = (
            f"Concentration elevated: "
            f"{', '.join(t['name'] for t in above_80[:2])}"
        )
    else:
        results["crowding_headline"] = f"Market concentration within normal ranges"

    # Save raw
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_file = DATA_DIR / "raw_edgar.json"
    with open(raw_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print(f"[Crowding Collector] Done. {len(themes)} themes. Saved to {raw_file}")
    for t in themes:
        print(f"  [{t['pct']:3d}] {t['name']}: {t['detail']}")

    return results


if __name__ == "__main__":
    collect_all()
