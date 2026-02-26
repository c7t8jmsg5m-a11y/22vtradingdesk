#!/usr/bin/env python3
"""
Manual Override Update Helper
Quick CLI for updating manual_overrides.json without hand-editing JSON.

Now simplified — margin debt, momentum, and calendar events are automated.
Only HF leverage, GEX (from Barchart), crowding themes, and one-off events remain manual.

Usage:
    python update_manual.py hf --gs 285 --jpm 298 --ms-pctl 99
    python update_manual.py gex --flip 6871 --call-wall 7000 --put-wall 7000 --price 6933
    python update_manual.py crowding --theme "AI Infra" --pct 90 --detail "MU, LRCX, NOW"
    python update_manual.py event --date "2026-05-01" --name "Powell Term Ends" --severity HIGH
    python update_manual.py show                    # Print current overrides
    python update_manual.py show auto               # Print automated data from raw files
"""

import json
import argparse
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DATA_FILE = DATA_DIR / "manual_overrides.json"


def load():
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            return json.load(f)
    return {}


def save(data):
    data["last_updated"] = datetime.now().isoformat()
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved to {DATA_FILE}")


def cmd_hf(args):
    """Update HF gross leverage (from prime brokerage reports)."""
    data = load()
    l1 = data.get("l1_leverage", {})
    l1["updated"] = datetime.now().strftime("%Y-%m-%d")

    if args.gs is not None:
        l1["hf_gross_gs"] = args.gs
    if args.jpm is not None:
        l1["hf_gross_jpm"] = args.jpm
    if args.ms_pctl is not None:
        l1["hf_gross_ms_percentile"] = args.ms_pctl
    if args.multistrat is not None:
        l1["multistrat_gross"] = args.multistrat
    if args.etf is not None:
        l1["leveraged_etf_bn"] = args.etf
    if args.notes:
        l1["notes"] = args.notes

    data["l1_leverage"] = l1
    save(data)
    print(f"Updated HF leverage: GS {l1.get('hf_gross_gs', '?')}%, "
          f"JPM {l1.get('hf_gross_jpm', '?')}%")


def cmd_gex(args):
    """Update GEX data from Barchart screenshots."""
    data = load()
    gex = data.get("gex_manual", {})
    gex["updated"] = datetime.now().strftime("%Y-%m-%d")
    gex["source"] = args.source or gex.get("source", "Barchart")

    if args.flip is not None:
        gex["flip_point"] = args.flip
    if args.call_wall is not None:
        gex["call_wall"] = args.call_wall
    if args.put_wall is not None:
        gex["put_wall"] = args.put_wall
    if args.price is not None:
        gex["last_price"] = args.price
    if args.ivol is not None:
        gex["ivol"] = args.ivol
    if args.hvol is not None:
        gex["hvol"] = args.hvol
    if args.iv_rank is not None:
        gex["iv_rank"] = args.iv_rank
    if args.iv_pctl is not None:
        gex["iv_percentile"] = args.iv_pctl

    data["gex_manual"] = gex
    save(data)
    print(f"Updated GEX: Flip={gex.get('flip_point')}, "
          f"Call Wall={gex.get('call_wall')}, Put Wall={gex.get('put_wall')}")


def cmd_crowding(args):
    """Update L4 crowding themes (narrative from 13F analysis)."""
    data = load()
    l4 = data.get("l4_crowding", {})
    l4["updated"] = datetime.now().strftime("%Y-%m-%d")
    themes = l4.get("themes", [])

    if args.clear:
        themes = []
        print("Cleared all crowding themes")
    elif args.theme:
        new_theme = {
            "name": args.theme,
            "pct": args.pct or 50,
            "detail": args.detail or "",
        }
        # Replace if same name exists
        themes = [t for t in themes if t["name"] != args.theme]
        themes.append(new_theme)
        print(f"Added/updated theme: {args.theme} ({args.pct}%)")

    if args.headline:
        l4["headline"] = args.headline
    if args.notes:
        l4["notes"] = args.notes

    l4["themes"] = themes
    data["l4_crowding"] = l4
    save(data)


def cmd_event(args):
    """Add/remove one-off calendar events (manual additions only)."""
    data = load()
    cal = data.setdefault("l6_calendar_manual_additions", {})
    events = cal.setdefault("events", [])

    if args.remove:
        before = len(events)
        events = [e for e in events if e["date"] != args.date]
        cal["events"] = events
        save(data)
        print(f"Removed {before - len(events)} event(s) on {args.date}")
        return

    new_event = {
        "date": args.date,
        "name": args.name,
        "severity": args.severity.upper(),
    }

    # Replace if same date+name exists
    events = [e for e in events if not (e["date"] == args.date and e["name"] == args.name)]
    events.append(new_event)
    events.sort(key=lambda e: e["date"])
    cal["events"] = events
    save(data)
    print(f"Added manual event: {args.date} — {args.name} ({args.severity})")


def cmd_show(args):
    """Show current overrides or automated data."""
    section = args.section

    if section == "auto":
        # Show automated data from raw collector files
        print("=== Automated Data (from collectors) ===\n")
        for raw_file in ["raw_finra.json", "raw_edgar.json", "raw_calendar.json"]:
            path = DATA_DIR / raw_file
            if path.exists():
                with open(path) as f:
                    raw = json.load(f)
                print(f"--- {raw_file} ---")
                print(json.dumps(raw, indent=2, default=str))
                print()
            else:
                print(f"--- {raw_file} --- NOT FOUND")
        return

    data = load()

    if section:
        key_map = {
            "hf": "l1_leverage",
            "l1": "l1_leverage",
            "crowding": "l4_crowding",
            "l4": "l4_crowding",
            "gex": "gex_manual",
            "calendar": "l6_calendar_manual_additions",
            "events": "l6_calendar_manual_additions",
            "l6": "l6_calendar_manual_additions",
        }
        key = key_map.get(section, section)
        if key in data:
            print(json.dumps(data[key], indent=2))
        else:
            print(f"Section '{section}' not found. Available: {list(data.keys())}")
    else:
        print(json.dumps(data, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="Update manual overrides for the leverage monitor.\n"
                    "Margin debt, momentum, and calendar events are now automated.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command")

    # hf (replaces "margin" — only HF fields remain manual)
    p_hf = sub.add_parser("hf", help="Update HF gross leverage (quarterly)")
    p_hf.add_argument("--gs", type=float, help="GS HF gross leverage %%")
    p_hf.add_argument("--jpm", type=float, help="JPM HF gross leverage %%")
    p_hf.add_argument("--ms-pctl", type=float, help="MS HF gross percentile")
    p_hf.add_argument("--multistrat", type=float, help="Multistrat gross %%")
    p_hf.add_argument("--etf", type=float, help="Leveraged ETF AUM in billions")
    p_hf.add_argument("--notes", type=str, help="Additional notes")

    # gex
    p_gex = sub.add_parser("gex", help="Update GEX data from Barchart/SpotGamma")
    p_gex.add_argument("--flip", type=float, help="GEX flip point")
    p_gex.add_argument("--call-wall", type=float, help="Call wall strike")
    p_gex.add_argument("--put-wall", type=float, help="Put wall strike")
    p_gex.add_argument("--price", type=float, help="Last SPX price")
    p_gex.add_argument("--ivol", type=float, help="Implied vol %%")
    p_gex.add_argument("--hvol", type=float, help="Historical vol %%")
    p_gex.add_argument("--iv-rank", type=float, help="IV Rank %%")
    p_gex.add_argument("--iv-pctl", type=float, help="IV Percentile %%")
    p_gex.add_argument("--source", type=str, help="Data source (default: Barchart)")

    # crowding (narrative themes from 13F analysis)
    p_crowd = sub.add_parser("crowding", help="Update L4 crowding themes")
    p_crowd.add_argument("--theme", type=str, help="Theme name")
    p_crowd.add_argument("--pct", type=int, help="Concentration percentile (0-100)")
    p_crowd.add_argument("--detail", type=str, help="Theme detail text")
    p_crowd.add_argument("--headline", type=str, help="Crowding headline")
    p_crowd.add_argument("--notes", type=str, help="Additional notes")
    p_crowd.add_argument("--clear", action="store_true", help="Clear all themes")

    # event (one-off manual calendar additions)
    p_event = sub.add_parser("event", help="Add/remove one-off calendar events")
    p_event.add_argument("--date", required=True, help="Event date (YYYY-MM-DD)")
    p_event.add_argument("--name", help="Event name")
    p_event.add_argument("--severity", default="MED", help="HIGH or MED")
    p_event.add_argument("--remove", action="store_true", help="Remove events on this date")

    # show
    p_show = sub.add_parser("show", help="Show current overrides or automated data")
    p_show.add_argument("section", nargs="?",
                        help="Section to show (hf, gex, crowding, calendar, auto)")

    args = parser.parse_args()

    if args.command == "hf":
        cmd_hf(args)
    elif args.command == "gex":
        cmd_gex(args)
    elif args.command == "crowding":
        cmd_crowding(args)
    elif args.command == "event":
        cmd_event(args)
    elif args.command == "show":
        cmd_show(args)
    else:
        parser.print_help()
        print("\nAutomated (no manual input needed):")
        print("  - FINRA margin debt (from collectors/finra_collector.py)")
        print("  - Momentum / Debt-GDP / SPX YoY (from collectors/finra_collector.py)")
        print("  - Calendar events (from collectors/calendar_collector.py)")
        print("  - Market concentration (from collectors/edgar_collector.py)")
        print("\nManual (still requires input):")
        print("  - HF gross leverage (hf command — quarterly)")
        print("  - GEX from Barchart (gex command — daily)")
        print("  - Crowding narrative themes (crowding command — quarterly)")
        print("  - One-off calendar events (event command — as needed)")


if __name__ == "__main__":
    main()
