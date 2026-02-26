#!/usr/bin/env python3
"""
Leverage Monitor Scoring Engine

Reads: data/latest.json + data/manual_overrides.json + config/thresholds.yaml
Outputs: data/leverage_monitor.json

Computes all 7 layer scores, composite 0-10, regime classification,
cascade analysis, and alert generation.
"""

import json
import yaml
from datetime import datetime, date
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
CONFIG_DIR = BASE_DIR / "config"

REGIMES = {
    (0, 2): ("LOW_RISK", "Low Risk"),
    (3, 4): ("ELEVATED", "Elevated"),
    (5, 6): ("FRAGILE_EQUILIBRIUM", "Fragile Equilibrium"),
    (7, 8): ("ACTIVE_DETERIORATION", "Active Deterioration"),
    (9, 10): ("CASCADE_RISK", "Cascade Risk"),
}


def load_json(path):
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def load_thresholds():
    path = CONFIG_DIR / "thresholds.yaml"
    if path.exists():
        with open(path) as f:
            return yaml.safe_load(f)
    return {}


def classify_regime(score):
    score_int = int(round(score))
    for (lo, hi), (code, label) in REGIMES.items():
        if lo <= score_int <= hi:
            return code, label
    return "UNKNOWN", "Unknown"


def score_l1(latest, overrides):
    """L1: Leverage Levels (0-2) — margin from automated, HF from manual"""
    l1 = overrides.get("l1_leverage", {})
    score = 0
    details = []

    # Prefer automated margin data, fall back to manual
    margin_data = latest.get("margin_debt", {})
    margin = margin_data.get("current_bn") or l1.get("finra_margin_bn", 0)
    margin_date = margin_data.get("date_label") or l1.get("finra_margin_date", "")
    margin_streak = margin_data.get("streak_label") or l1.get("finra_margin_streak", "")
    margin_source = margin_data.get("source", "manual")

    # Threshold depends on data source:
    # - FINRA monthly margin debt: $1000B (the $1T Jan-2021 peak)
    # - FRED quarterly proxy (BOGZ1FL663067003Q): $500B (scaled equivalent)
    if margin_source == "fred_quarterly":
        margin_threshold = 500
        threshold_label = "FRED proxy $500B (scaled from $1T FINRA)"
    else:
        margin_threshold = 1000
        threshold_label = "$1T Jan-2021 peak"

    if margin > margin_threshold:
        score += 1
        details.append(f"Margin ${margin}B above {threshold_label}")

    hf_gs = l1.get("hf_gross_gs", 0)
    hf_jpm = l1.get("hf_gross_jpm", 0)
    if hf_gs > 275 or hf_jpm > 290:
        score += 1
        details.append(f"HF gross: GS {hf_gs}%, JPM {hf_jpm}%")

    data = dict(l1)
    data.update({
        "finra_margin_bn": margin,
        "finra_margin_date": margin_date,
        "finra_margin_streak": margin_streak,
        "margin_history": margin_data.get("history", []),
        "margin_yoy_pct": margin_data.get("yoy_pct"),
        "debt_gdp_pct": latest.get("debt_gdp_pct"),
        "spx_yoy_pct": latest.get("spx_yoy_pct"),
    })

    return {
        "score": min(score, 2),
        "max": 2,
        "label": "Leverage Levels",
        "details": " | ".join(details) if details else "Within normal range",
        "data": data,
    }


def score_l2(latest, overrides):
    """L2: Momentum (0-2) — prefer automated from latest, fall back to manual"""
    l2 = overrides.get("l2_momentum", {})
    score = 0
    details = []

    # Prefer automated margin YoY and debt/GDP from latest
    margin_data = latest.get("margin_debt", {})
    yoy = margin_data.get("yoy_pct") or l2.get("margin_yoy_pct", 0)
    debt_gdp = latest.get("debt_gdp_pct") or l2.get("debt_gdp_pct", 0)
    spx_yoy = latest.get("spx_yoy_pct") or l2.get("spx_yoy_pct", 0)
    margin_source = margin_data.get("source", "manual")

    if yoy > 20:
        score += 1
        details.append(f"Margin YoY +{yoy}% (>20% threshold)")

    # Debt/GDP threshold depends on data source:
    # - FINRA monthly margin debt: 3.5% (~$1.1T / $31.5T GDP)
    # - FRED quarterly proxy: 1.5% (scaled — different measure, ~0.44x of FINRA)
    if margin_source == "fred_quarterly":
        gdp_threshold = 1.5
    else:
        gdp_threshold = 3.5

    if debt_gdp > gdp_threshold:
        score += 1
        details.append(f"Debt/GDP {debt_gdp}% above {gdp_threshold}% threshold")

    data = {
        "margin_yoy_pct": yoy,
        "debt_gdp_pct": debt_gdp,
        "spx_yoy_pct": spx_yoy,
        "notes": l2.get("notes", ""),
    }

    return {
        "score": min(score, 2),
        "max": 2,
        "label": "Momentum",
        "details": " | ".join(details) if details else "Moderate pace",
        "data": data,
    }


def score_l3(latest):
    """L3: Financing (0-2) — AUTOMATED via FRED"""
    fin = latest.get("financing", {})
    score = 0
    details = []

    nfci = fin.get("nfci", {}).get("current")
    hy_oas = fin.get("hy_oas", {}).get("current")
    sofr = fin.get("sofr", {}).get("current")

    if nfci is not None:
        if nfci > -0.30:
            score += 1
            details.append(f"NFCI {nfci:+.3f} — tightening")
        else:
            details.append(f"NFCI {nfci:+.3f} — loose")

    if hy_oas is not None:
        if hy_oas > 350:
            score += 1
            details.append(f"HY OAS {hy_oas:.0f}bp — credit stress")
        else:
            details.append(f"HY OAS {hy_oas:.0f}bp")

    # SOFR as additional trigger (capped at layer max of 2)
    if sofr is not None and sofr > 4.50 and score < 2:
        score += 1
        details.append(f"SOFR {sofr:.2f}% — elevated")
    elif sofr is not None:
        details.append(f"SOFR {sofr:.2f}%")

    return {
        "score": min(score, 2),
        "max": 2,
        "label": "Financing",
        "details": " | ".join(details) if details else "Data unavailable",
        "data": {
            "nfci": nfci,
            "hy_oas": hy_oas,
            "sofr": sofr,
            "signal": fin.get("signal", "UNKNOWN"),
        },
    }


def score_l4(latest, overrides):
    """L4: Crowding (0-1) — automated concentration + manual narrative themes"""
    l4_manual = overrides.get("l4_crowding", {})
    l4_auto = latest.get("crowding", {})
    score = 0
    details = []

    # Merge themes: manual themes take priority (narrative), auto themes fill gaps
    manual_themes = l4_manual.get("themes", [])
    auto_themes = l4_auto.get("themes", [])

    # Use manual themes as primary if present, otherwise auto
    themes = manual_themes if manual_themes else auto_themes

    high_concentration = [t for t in themes if t.get("pct", 0) > 80]

    if high_concentration:
        score = 1
        names = [t["name"] for t in high_concentration]
        details.append(f"High concentration: {', '.join(names)}")
    else:
        details.append("No extreme crowding detected")

    headline = l4_manual.get("headline") or l4_auto.get("headline", "")
    if headline:
        details.append(headline)

    # Build combined data for template
    data = {
        "themes": themes,
        "headline": headline,
        "notes": l4_manual.get("notes", ""),
        "mag7_concentration": l4_auto.get("mag7_concentration", {}),
        "sector_concentration": l4_auto.get("sector_concentration", {}),
        "filing_quarter": l4_auto.get("filing_quarter"),
    }

    return {
        "score": min(score, 1),
        "max": 1,
        "label": "Crowding",
        "details": " | ".join(details) if details else "Normal positioning",
        "data": data,
    }


def score_l5a(latest, overrides):
    """L5A: Vol Structure (0-1) — AUTOMATED"""
    score = 0.0
    details = []

    vix = latest.get("vix", {}).get("current")
    if vix is not None:
        if vix > 22:
            score += 0.5
            details.append(f"VIX {vix:.2f} above 22")
        else:
            details.append(f"VIX {vix:.2f}")

    # VVIX from manual overrides (not available in free pipeline)
    gex_manual = overrides.get("gex_manual", {})
    iv_rank = gex_manual.get("iv_rank")
    ivol = gex_manual.get("ivol")
    hvol = gex_manual.get("hvol")

    if iv_rank is not None and iv_rank > 50:
        score += 0.5
        details.append(f"IV Rank {iv_rank:.1f}% elevated")
    elif ivol is not None and hvol is not None and ivol > hvol * 1.3:
        score += 0.5
        details.append(f"IV/HV divergence: {ivol:.1f}/{hvol:.1f}")

    return {
        "score": min(score, 1),
        "max": 1,
        "label": "Vol Structure",
        "details": " | ".join(details) if details else "Normal vol regime",
        "data": {
            "vix": vix,
            "ivol": ivol,
            "hvol": hvol,
            "iv_rank": iv_rank,
        },
    }


def score_l5b(latest):
    """L5B: Options Sentiment (0-1) — AUTOMATED"""
    score = 0.0
    details = []

    pc = latest.get("pc_ratio", {})
    equity_pc = pc.get("equity")

    if equity_pc is not None:
        if equity_pc > 0.85:
            score += 0.5
            details.append(f"P/C {equity_pc:.2f} — elevated fear")
        elif equity_pc < 0.45:
            score += 0.5
            details.append(f"P/C {equity_pc:.2f} — extreme complacency")
        else:
            details.append(f"P/C {equity_pc:.2f}")

    gex = latest.get("gex", {})
    gex_level = gex.get("level", "UNKNOWN")
    if gex_level in ("NEGATIVE", "DEEP_NEGATIVE"):
        score += 0.25
        details.append(f"GEX {gex_level}")

    # VIX rate of change over 30 days
    vix_hist = latest.get("vix", {}).get("history_30d", [])
    if len(vix_hist) >= 2:
        first_val = vix_hist[0].get("value")
        last_val = vix_hist[-1].get("value")
        if first_val and last_val and first_val > 0:
            vix_roc = ((last_val - first_val) / first_val) * 100
            if vix_roc > 25:
                score += 0.25
                details.append(f"VIX 30d RoC {vix_roc:+.0f}%")

    return {
        "score": min(score, 1),
        "max": 1,
        "label": "Options Sentiment",
        "details": " | ".join(details) if details else "Neutral sentiment",
        "data": {
            "equity_pc": equity_pc,
            "gex_level": gex_level,
            "skew": latest.get("skew", {}).get("cboe_skew"),
        },
    }


def score_l6(latest, overrides):
    """L6: Calendar (0-1) — prefer automated events, merge with manual additions"""
    # Prefer automated calendar events from latest
    auto_events = latest.get("calendar_events", [])
    # Fall back to manual events from overrides
    manual_events = overrides.get("l6_calendar", {}).get("events", [])

    # Use automated if available, otherwise manual
    events = auto_events if auto_events else manual_events

    score = 0
    details = []

    today = date.today()
    upcoming_high = []

    for event in events:
        try:
            event_date = datetime.strptime(event["date"], "%Y-%m-%d").date()
            days_away = (event_date - today).days
            if 0 <= days_away <= 30 and event.get("severity") == "HIGH":
                upcoming_high.append(f"{event['name']} ({days_away}d)")
        except (ValueError, KeyError):
            continue

    if upcoming_high:
        score = 1
        details.append(f"HIGH events within 30d: {', '.join(upcoming_high[:3])}")
    else:
        details.append("No HIGH-severity catalysts within 30 days")

    return {
        "score": min(score, 1),
        "max": 1,
        "label": "Calendar",
        "details": " | ".join(details),
        "data": {"upcoming_high": upcoming_high, "all_events": events},
    }


def assess_cascade(layers, composite):
    """
    Assess the cascade vulnerability chain.
    Steps: Record Leverage → Acceleration → Financing Tightens →
           Vol Regime Shift → Forced Unwind → Contagion
    """
    l1_score = layers["l1"]["score"]
    l2_score = layers["l2"]["score"]
    l3_score = layers["l3"]["score"]
    l5a_score = layers["l5a"]["score"]

    steps_active = 0
    narrative_parts = []

    # Step 1: Record leverage loaded
    if l1_score >= 2:
        steps_active += 1
        narrative_parts.append("Step 1 ACTIVE: Record leverage loaded")
    else:
        narrative_parts.append("Step 1 INACTIVE: Leverage below threshold")

    # Step 2: Acceleration
    if l2_score >= 2:
        steps_active += 1
        narrative_parts.append("Step 2 ACTIVE: Momentum accelerating")

    # Step 3: Financing tightens
    if l3_score >= 1:
        steps_active += 1
        narrative_parts.append("Step 3 ACTIVE: Financing conditions tightening")
    else:
        narrative_parts.append(f"Step 3 BLOCKED: Financing still loose (score {l3_score}/2)")

    # Step 4: Vol regime shift
    if l5a_score >= 0.5:
        steps_active += 1
        narrative_parts.append("Step 4 ACTIVE: Vol regime elevated")
    else:
        narrative_parts.append("Step 4 BLOCKED: Dealers long gamma, vol suppressed")

    # Probability assessment
    if steps_active >= 4:
        prob = "HIGH"
    elif steps_active >= 3:
        prob = "MODERATE-HIGH"
    elif steps_active >= 2:
        prob = "MODERATE"
    elif steps_active >= 1:
        prob = "LOW-MODERATE"
    else:
        prob = "LOW"

    blocked_at = None
    for i, part in enumerate(narrative_parts):
        if "BLOCKED" in part:
            blocked_at = i + 1
            break

    return {
        "activation_probability": prob,
        "steps_active": steps_active,
        "blocked_at_step": blocked_at,
        "narrative": " | ".join(narrative_parts),
    }


def generate_commentary(layers, composite, regime_code, cascade):
    """Generate structured commentary triggers for the template."""
    tensions = []
    bottom_line_parts = []

    l1 = layers["l1"]
    l3 = layers["l3"]
    l5b = layers["l5b"]

    # Detect divergences
    if l1["score"] >= 2 and l3["score"] == 0:
        tensions.append("protection buying vs loose financing")
        headline_type = "SENTIMENT_STRUCTURE_DIVERGENCE"
    elif composite >= 7:
        headline_type = "ACTIVE_STRESS"
        tensions.append("multiple layers firing simultaneously")
    elif composite >= 5:
        headline_type = "FRAGILE_EQUILIBRIUM"
        tensions.append("leverage at records but key dampeners holding")
    else:
        headline_type = "LOW_RISK"
        tensions.append("no significant tensions detected")

    l3_data = l3.get("data", {})
    nfci = l3_data.get("nfci")
    if nfci is not None:
        l3_direction = "tightening" if nfci > -0.30 else "loosening"
    else:
        l3_direction = "unknown"

    if l1["score"] >= 2:
        bottom_line_parts.append("Structural leverage at records")
    if l3["score"] == 0:
        bottom_line_parts.append("financing actively easing")
    elif l3["score"] >= 1:
        bottom_line_parts.append("financing beginning to tighten")

    return {
        "headline_type": headline_type,
        "l3_direction": l3_direction,
        "key_tension": " | ".join(tensions),
        "bottom_line": " but ".join(bottom_line_parts) if bottom_line_parts else "Monitoring",
    }


def generate_pulse(layers, latest, composite):
    """Generate the real-time pulse section."""
    vix = latest.get("vix", {}).get("current")
    pc = latest.get("pc_ratio", {}).get("equity")
    gex_level = latest.get("gex", {}).get("level", "UNKNOWN")

    # Turbulence
    if vix and vix > 25:
        turbulence = "ELEVATED"
    elif vix and vix > 20:
        turbulence = "MODERATE"
    else:
        turbulence = "NORMAL"

    # Regime shorthand
    if composite >= 7:
        regime = "ACTIVE_DETERI"
    elif composite >= 5:
        regime = "FRAGILE_EQ"
    else:
        regime = "STABLE"

    # Synthesis
    if pc and pc > 0.85 and layers["l3"]["score"] == 0:
        synthesis = "TAPE DIVERGING"
        note = "Calm surface - loaded"
    elif composite >= 7:
        synthesis = "STRESS BUILDING"
        note = "Multiple layers active"
    elif composite >= 5:
        synthesis = "WATCHING"
        note = "Key levels holding"
    else:
        synthesis = "STABLE"
        note = "No immediate concerns"

    return {
        "turbulence": turbulence,
        "regime": regime,
        "synthesis": synthesis,
        "synthesis_note": note,
    }


def run():
    """Main scoring engine entry point."""
    print("[Scoring Engine] Starting...")

    latest = load_json(DATA_DIR / "latest.json")
    overrides = load_json(DATA_DIR / "manual_overrides.json")

    if not latest:
        print("[Scoring Engine] No latest.json found — run the pipeline first")
        return None

    # Load previous score for comparison
    prev_output = load_json(DATA_DIR / "leverage_monitor.json")
    prev_composite = prev_output.get("composite_score")

    # Score all layers
    layers = {
        "l1": score_l1(latest, overrides),
        "l2": score_l2(latest, overrides),
        "l3": score_l3(latest),
        "l4": score_l4(latest, overrides),
        "l5a": score_l5a(latest, overrides),
        "l5b": score_l5b(latest),
        "l6": score_l6(latest, overrides),
    }

    composite = sum(layer["score"] for layer in layers.values())
    regime_code, regime_label = classify_regime(composite)

    # Cascade analysis
    cascade = assess_cascade(layers, composite)

    # Commentary
    commentary_override = overrides.get("commentary_override")
    commentary = commentary_override or generate_commentary(layers, composite, regime_code, cascade)

    # Real-time pulse
    pulse = generate_pulse(layers, latest, composite)

    # Alerts specific to leverage monitor
    alerts = []
    if composite >= 8 and (prev_composite is None or prev_composite < 8):
        alerts.append({
            "type": "COMPOSITE_CRITICAL",
            "message": f"Composite score rose to {composite:.1f} — Active Deterioration",
            "severity": "critical",
        })
    if cascade["steps_active"] >= 3:
        alerts.append({
            "type": "CASCADE_WARNING",
            "message": f"Cascade chain: {cascade['steps_active']}/4 steps active",
            "severity": "critical" if cascade["steps_active"] >= 4 else "warning",
        })

    output = {
        "timestamp": datetime.now().isoformat(),
        "composite_score": round(composite, 1),
        "composite_prev": prev_composite,
        "regime": regime_code,
        "regime_label": regime_label,
        "layers": layers,
        "alerts": alerts,
        "cascade": cascade,
        "real_time_pulse": pulse,
        "commentary": commentary,
        "options_data_timestamp": latest.get("timestamp"),
    }

    # Save
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_file = DATA_DIR / "leverage_monitor.json"
    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[Scoring Engine] Composite: {composite:.1f}/10 — {regime_label}")
    for layer_key, layer in layers.items():
        print(f"  {layer['label']}: {layer['score']}/{layer['max']} — {layer['details'][:60]}")

    if alerts:
        for a in alerts:
            icon = "!!" if a["severity"] == "critical" else "!"
            print(f"  [{icon}] {a['message']}")

    print(f"[Scoring Engine] Saved to {out_file}")
    return output


if __name__ == "__main__":
    run()
