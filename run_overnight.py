#!/usr/bin/env python3
"""
Leverage Monitor & Options Activity Pipeline — Main Orchestrator

Runs: collectors → processor → scoring engine → renderer → deploy

Usage:
    python run_overnight.py              # Full pipeline
    python run_overnight.py --collect    # Collectors only
    python run_overnight.py --process    # Processor only
    python run_overnight.py --score      # Scoring engine only
    python run_overnight.py --render     # Renderer only
    python run_overnight.py --test       # Dry run with status check

Schedule via cron (Mac mini):
    0 6 * * 1-5   cd ~/options-monitor && python run_overnight.py >> logs/run.log 2>&1
    0 13 * * 1-5  cd ~/options-monitor && python run_overnight.py >> logs/run.log 2>&1
"""

import sys
import time
import json
import base64
import argparse
import subprocess
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from collectors.cboe_collector import collect_all as collect_cboe
from collectors.squeeze_collector import collect_all as collect_squeeze
from collectors.yahoo_collector import collect_all as collect_yahoo
from collectors.fred_collector import collect_all as collect_fred
from collectors.finra_collector import collect_all as collect_finra
from collectors.edgar_collector import collect_all as collect_edgar
from collectors.calendar_collector import collect_all as collect_calendar
from processor.options_processor import process
from scoring.scoring_engine import run as run_scoring
from renderer.renderer import render as run_renderer

LOG_DIR = Path(__file__).parent / "logs"
DATA_DIR = Path(__file__).parent / "data"
OUTPUT_DIR = Path(__file__).parent / "output"

REPO_OWNER = "c7t8jmsg5m-a11y"
REPO_NAME = "22vtradingdesk"


def push_to_github():
    """Push latest data to GitHub (options-monitor branch)."""
    repo_dir = Path(__file__).parent

    try:
        subprocess.run(
            ["git", "add", "data/latest.json", "data/alerts.json",
             "data/leverage_monitor.json", "data/manual_overrides.json"],
            cwd=str(repo_dir), capture_output=True
        )

        result = subprocess.run(
            ["git", "commit", "-m",
             f"Auto update {datetime.now().strftime('%Y-%m-%d %H:%M')}"],
            cwd=str(repo_dir), capture_output=True, text=True
        )

        if "nothing to commit" in result.stdout:
            print("  [Git] No changes to push")
            return

        subprocess.run(["git", "push", "origin", "options-monitor"],
                       cwd=str(repo_dir), capture_output=True)
        print("  [Git] Pushed data to options-monitor")

    except Exception as e:
        print(f"  [Git] Push failed: {e}")


def push_html_to_main():
    """
    Push rendered leverage monitor HTML to main branch via GitHub API.
    This avoids branch switching on the Mac mini.
    """
    html_file = OUTPUT_DIR / "leverage" / "index.html"
    if not html_file.exists():
        print("  [Deploy] No rendered HTML found — skipping main branch push")
        return

    try:
        # Read the rendered HTML
        with open(html_file) as f:
            html_content = f.read()

        # Encode as base64 for the GitHub API
        content_b64 = base64.b64encode(html_content.encode()).decode()

        # Get current file SHA (needed for updates)
        sha_result = subprocess.run(
            ["gh", "api",
             f"repos/{REPO_OWNER}/{REPO_NAME}/contents/leverage/index.html",
             "--jq", ".sha"],
            capture_output=True, text=True
        )

        sha = sha_result.stdout.strip() if sha_result.returncode == 0 else None

        # Build the API call
        cmd = [
            "gh", "api",
            f"repos/{REPO_OWNER}/{REPO_NAME}/contents/leverage/index.html",
            "-X", "PUT",
            "-f", f"message=Auto-update leverage monitor {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "-f", f"content={content_b64}",
            "-f", "branch=main",
        ]

        if sha:
            cmd.extend(["-f", f"sha={sha}"])

        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            print("  [Deploy] Pushed leverage/index.html to main branch")
        else:
            print(f"  [Deploy] GitHub API error: {result.stderr[:200]}")

    except Exception as e:
        print(f"  [Deploy] Failed to push HTML to main: {e}")


def run_collectors():
    """Run all data collectors with error isolation."""
    results = {}
    collectors = [
        ("CBOE", collect_cboe),
        ("SqueezeMetrics", collect_squeeze),
        ("Yahoo Options", collect_yahoo),
        ("FRED", collect_fred),
        ("FINRA Margin", collect_finra),
        ("Crowding", collect_edgar),
        ("Calendar", collect_calendar),
    ]

    for name, collector_fn in collectors:
        try:
            start = time.time()
            data = collector_fn()
            elapsed = time.time() - start
            results[name] = {"status": "OK", "time": f"{elapsed:.1f}s"}
            print(f"  {name} completed in {elapsed:.1f}s")
        except Exception as e:
            results[name] = {"status": "FAILED", "error": str(e)}
            print(f"  {name} failed: {e}")

    return results


def run_processor():
    """Run the data processor."""
    try:
        start = time.time()
        output = process()
        elapsed = time.time() - start
        print(f"  Processor completed in {elapsed:.1f}s")
        return {"status": "OK", "time": f"{elapsed:.1f}s",
                "alerts": len(output.get("alerts", []))}
    except Exception as e:
        print(f"  Processor failed: {e}")
        return {"status": "FAILED", "error": str(e)}


def run_scoring_engine():
    """Run the scoring engine."""
    try:
        start = time.time()
        output = run_scoring()
        elapsed = time.time() - start
        if output:
            print(f"  Scoring engine completed in {elapsed:.1f}s "
                  f"(score: {output['composite_score']}/10)")
            return {"status": "OK", "time": f"{elapsed:.1f}s",
                    "score": output["composite_score"],
                    "regime": output["regime_label"]}
        else:
            return {"status": "SKIPPED", "reason": "No latest.json"}
    except Exception as e:
        print(f"  Scoring engine failed: {e}")
        return {"status": "FAILED", "error": str(e)}


def run_html_renderer():
    """Run the HTML renderer."""
    try:
        start = time.time()
        output_path = run_renderer()
        elapsed = time.time() - start
        if output_path:
            print(f"  Renderer completed in {elapsed:.1f}s")
            return {"status": "OK", "time": f"{elapsed:.1f}s",
                    "output": output_path}
        else:
            return {"status": "SKIPPED", "reason": "No leverage_monitor.json"}
    except ImportError:
        print("  Renderer skipped — jinja2 not installed (pip install jinja2)")
        return {"status": "SKIPPED", "reason": "jinja2 not installed"}
    except Exception as e:
        print(f"  Renderer failed: {e}")
        return {"status": "FAILED", "error": str(e)}


def run_full_pipeline():
    """Run the complete pipeline: collect → process → score → render → deploy."""
    print("=" * 60)
    print(f"LEVERAGE MONITOR PIPELINE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    total_start = time.time()

    # Step 1: Collect
    print("\n[1/5] COLLECTING DATA...")
    collector_results = run_collectors()

    # Step 2: Process
    print("\n[2/5] PROCESSING...")
    processor_result = run_processor()

    # Step 3: Score
    print("\n[3/5] SCORING...")
    scoring_result = run_scoring_engine()

    # Step 4: Render
    print("\n[4/5] RENDERING...")
    renderer_result = run_html_renderer()

    # Step 5: Deploy
    print("\n[5/5] DEPLOYING...")
    push_to_github()
    push_html_to_main()

    # Summary
    total_elapsed = time.time() - total_start
    print("\n" + "=" * 60)
    print(f"PIPELINE COMPLETE — {total_elapsed:.1f}s total")
    print("=" * 60)

    for name, result in collector_results.items():
        status = "OK" if result["status"] == "OK" else "FAIL"
        print(f"  [{status}] {name}: {result['status']}")

    print(f"  [{'OK' if processor_result['status'] == 'OK' else 'FAIL'}] "
          f"Processor: {processor_result['status']}")
    print(f"  [{'OK' if scoring_result['status'] == 'OK' else 'SKIP'}] "
          f"Scoring: {scoring_result.get('score', 'N/A')}/10 "
          f"— {scoring_result.get('regime', 'N/A')}")
    print(f"  [{'OK' if renderer_result['status'] == 'OK' else 'SKIP'}] "
          f"Renderer: {renderer_result['status']}")

    if processor_result.get("alerts", 0) > 0:
        print(f"\n  {processor_result['alerts']} alert(s) — check data/alerts.json")

    # Log the run
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "collectors": collector_results,
        "processor": processor_result,
        "scoring": scoring_result,
        "renderer": renderer_result,
        "total_time": f"{total_elapsed:.1f}s",
    }
    log_file = LOG_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w") as f:
        json.dump(log_entry, f, indent=2)


def test_status():
    """Check system status without running collectors."""
    print("LEVERAGE MONITOR — STATUS CHECK")
    print("-" * 40)

    # Check latest.json
    latest = DATA_DIR / "latest.json"
    if latest.exists():
        with open(latest) as f:
            data = json.load(f)
        ts = data.get("timestamp", "unknown")
        print(f"  Latest data: {ts}")

        pc = data.get("pc_ratio", {})
        gex = data.get("gex", {})
        skew = data.get("skew", {})
        vix = data.get("vix", {})
        fin = data.get("financing", {})

        print(f"  P/C Ratio: {pc.get('equity', 'N/A')} ({pc.get('signal', 'N/A')})")
        print(f"  P/C 5d MA: {pc.get('equity_5d_ma', 'N/A')}")
        print(f"  GEX: {gex.get('estimate_bn', 'N/A')}B ({gex.get('level', 'N/A')})")
        print(f"  SKEW: {skew.get('cboe_skew', 'N/A')} ({skew.get('signal', 'N/A')})")

        vix_change = vix.get('change')
        if vix_change is not None:
            print(f"  VIX: {vix.get('current', 'N/A')} ({vix_change:+.2f})")
        else:
            print(f"  VIX: {vix.get('current', 'N/A')}")

        # Financing data
        nfci = fin.get("nfci", {}).get("current")
        hy_oas = fin.get("hy_oas", {}).get("current")
        sofr = fin.get("sofr", {}).get("current")
        if any([nfci, hy_oas, sofr]):
            print(f"  NFCI: {nfci or 'N/A'}")
            print(f"  HY OAS: {hy_oas or 'N/A'}bp")
            print(f"  SOFR: {sofr or 'N/A'}%")

        alerts = data.get("alerts", [])
        if alerts:
            print(f"\n  Active Alerts ({len(alerts)}):")
            for a in alerts:
                icon = "!!" if a["severity"] == "critical" else "!"
                print(f"    [{icon}] {a['message']}")
    else:
        print("  No data found. Run: python run_overnight.py")

    # Check leverage monitor
    lm = DATA_DIR / "leverage_monitor.json"
    if lm.exists():
        with open(lm) as f:
            monitor = json.load(f)
        print(f"\n  Leverage Monitor: {monitor.get('composite_score')}/10 "
              f"— {monitor.get('regime_label')}")
    else:
        print("\n  Leverage Monitor: not yet generated")

    # Check manual overrides
    mo = DATA_DIR / "manual_overrides.json"
    if mo.exists():
        with open(mo) as f:
            overrides = json.load(f)
        print(f"  Manual Overrides: last updated {overrides.get('last_updated', 'unknown')}")
    else:
        print("  Manual Overrides: not found (create data/manual_overrides.json)")

    # Check dependencies
    print("\n  Dependencies:")
    for pkg in ["requests", "beautifulsoup4", "yfinance", "pandas",
                "pyyaml", "jinja2"]:
        try:
            if pkg == "beautifulsoup4":
                __import__("bs4")
            elif pkg == "pyyaml":
                __import__("yaml")
            else:
                __import__(pkg)
            print(f"    OK {pkg}")
        except ImportError:
            print(f"    MISSING {pkg} — pip install {pkg}")

    # Check FRED API key
    import os
    env_file = Path(__file__).parent / ".env"
    has_key = os.environ.get("FRED_API_KEY") is not None
    if not has_key and env_file.exists():
        with open(env_file) as f:
            has_key = "FRED_API_KEY=" in f.read()
    print(f"    {'OK' if has_key else 'MISSING'} FRED_API_KEY")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Leverage Monitor & Options Pipeline")
    parser.add_argument("--collect", action="store_true",
                        help="Run collectors only")
    parser.add_argument("--process", action="store_true",
                        help="Run processor only")
    parser.add_argument("--score", action="store_true",
                        help="Run scoring engine only")
    parser.add_argument("--render", action="store_true",
                        help="Run renderer only")
    parser.add_argument("--test", action="store_true",
                        help="Status check")
    args = parser.parse_args()

    if args.test:
        test_status()
    elif args.collect:
        run_collectors()
    elif args.process:
        run_processor()
    elif args.score:
        run_scoring_engine()
    elif args.render:
        run_html_renderer()
    else:
        run_full_pipeline()
