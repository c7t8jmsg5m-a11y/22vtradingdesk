#!/usr/bin/env python3
"""
Leverage Monitor Renderer

Reads: data/leverage_monitor.json + data/latest.json + data/manual_overrides.json
Template: templates/leverage_monitor.html.j2
Outputs: output/leverage/index.html

Uses Jinja2 to render the leverage monitor dashboard from data.
"""

import json
from datetime import datetime
from pathlib import Path

try:
    from jinja2 import Environment, FileSystemLoader
except ImportError:
    print("[Renderer] Jinja2 not installed. Run: pip install jinja2")
    raise

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
TEMPLATE_DIR = BASE_DIR / "templates"
OUTPUT_DIR = BASE_DIR / "output" / "leverage"


def load_json(path):
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def format_number(value, fmt="{:.2f}"):
    """Format a number, returning 'N/A' if None."""
    if value is None:
        return "N/A"
    try:
        return fmt.format(float(value))
    except (ValueError, TypeError):
        return str(value)


def format_bp(value):
    """Format basis points."""
    if value is None:
        return "N/A"
    return f"{value:.0f}bp"


def format_pct(value):
    """Format as percentage."""
    if value is None:
        return "N/A"
    return f"{value:.2f}%"


def format_bn(value):
    """Format as billions."""
    if value is None:
        return "N/A"
    return f"${value:,.0f}B"


def score_color(score, max_score):
    """Return CSS color var based on score ratio."""
    if max_score == 0:
        return "var(--dim)"
    ratio = score / max_score
    if ratio >= 0.75:
        return "var(--red)"
    elif ratio >= 0.25:
        return "var(--amber)"
    else:
        return "var(--green)"


def regime_pill_class(regime):
    """Return pill CSS class based on regime."""
    if regime in ("CASCADE_RISK",):
        return "p-c"
    elif regime in ("ACTIVE_DETERIORATION",):
        return "p-r"
    elif regime in ("FRAGILE_EQUILIBRIUM", "ELEVATED"):
        return "p-a"
    else:
        return "p-g"


def render():
    """Main render function."""
    print("[Renderer] Starting...")

    # Load data
    monitor = load_json(DATA_DIR / "leverage_monitor.json")
    latest = load_json(DATA_DIR / "latest.json")
    overrides = load_json(DATA_DIR / "manual_overrides.json")

    if not monitor:
        print("[Renderer] No leverage_monitor.json found — run the scoring engine first")
        return None

    # Set up Jinja2
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=False,  # HTML template, we control the content
    )

    # Register custom filters
    env.filters["fmt"] = format_number
    env.filters["bp"] = format_bp
    env.filters["pct"] = format_pct
    env.filters["bn"] = format_bn
    env.filters["score_color"] = lambda s, m: score_color(s, m)

    # Load template
    template = env.get_template("leverage_monitor.html.j2")

    # Render
    now = datetime.now()
    html = template.render(
        data=monitor,
        latest=latest,
        overrides=overrides,
        now=now,
        date_str=now.strftime("%b %d, %Y"),
        regime_pill_class=regime_pill_class(monitor.get("regime", "")),
        int=int,
        round=round,
        abs=abs,
        min=min,
        max=max,
    )

    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / "index.html"
    with open(output_file, "w") as f:
        f.write(html)

    print(f"[Renderer] Output saved to {output_file}")
    print(f"[Renderer] Score: {monitor.get('composite_score')}/10 — {monitor.get('regime_label')}")
    return str(output_file)


if __name__ == "__main__":
    render()
