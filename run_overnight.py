#!/usr/bin/env python3
"""
Options Activity Monitor — Main Orchestrator
Runs all collectors → processor → outputs latest.json

Usage:
    python run_overnight.py              # Full pipeline
    python run_overnight.py --collect    # Collectors only
    python run_overnight.py --process    # Processor only
    python run_overnight.py --test       # Dry run with status check

Schedule via cron (Mac mini):
    0 6 * * 1-5 cd ~/options-monitor && python run_overnight.py >> logs/run.log 2>&1
"""

import sys
import time
import json
import argparse
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from collectors.cboe_collector import collect_all as collect_cboe
from collectors.squeeze_collector import collect_all as collect_squeeze
from collectors.yahoo_collector import collect_all as collect_yahoo
from processor.options_processor import process

LOG_DIR = Path(__file__).parent / "logs"
DATA_DIR = Path(__file__).parent / "data"


def push_to_github():
    """Push latest data to GitHub so Claude can pull it on demand."""
    import subprocess
    repo_dir = Path(__file__).parent
    
    try:
        subprocess.run(["git", "add", "data/latest.json", "data/alerts.json"],
                       cwd=str(repo_dir), capture_output=True)
        
        result = subprocess.run(
            ["git", "commit", "-m", f"Auto update {datetime.now().strftime('%Y-%m-%d %H:%M')}"],
            cwd=str(repo_dir), capture_output=True, text=True
        )
        
        if "nothing to commit" in result.stdout:
            print("  [Git] No changes to push")
            return
        
        subprocess.run(["git", "push", "origin", "options-monitor"],
                       cwd=str(repo_dir), capture_output=True)
        print("  ✓ Pushed to GitHub")
        
    except Exception as e:
        print(f"  ✗ Git push failed: {e}")


def run_collectors():
    """Run all data collectors with error isolation."""
    results = {}
    collectors = [
        ("CBOE", collect_cboe),
        ("SqueezeMetrics", collect_squeeze),
        ("Yahoo Options", collect_yahoo),
    ]
    
    for name, collector_fn in collectors:
        try:
            start = time.time()
            data = collector_fn()
            elapsed = time.time() - start
            results[name] = {"status": "OK", "time": f"{elapsed:.1f}s"}
            print(f"  ✓ {name} completed in {elapsed:.1f}s")
        except Exception as e:
            results[name] = {"status": "FAILED", "error": str(e)}
            print(f"  ✗ {name} failed: {e}")
    
    return results


def run_processor():
    """Run the data processor."""
    try:
        start = time.time()
        output = process()
        elapsed = time.time() - start
        print(f"  ✓ Processor completed in {elapsed:.1f}s")
        return {"status": "OK", "time": f"{elapsed:.1f}s", "alerts": len(output.get("alerts", []))}
    except Exception as e:
        print(f"  ✗ Processor failed: {e}")
        return {"status": "FAILED", "error": str(e)}


def run_full_pipeline():
    """Run the complete pipeline: collect → process → report."""
    print("=" * 60)
    print(f"OPTIONS ACTIVITY MONITOR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    total_start = time.time()
    
    # Step 1: Collect
    print("\n[1/2] COLLECTING DATA...")
    collector_results = run_collectors()
    
    # Step 2: Process
    print("\n[2/2] PROCESSING...")
    processor_result = run_processor()
    
    # Summary
    total_elapsed = time.time() - total_start
    print("\n" + "=" * 60)
    print(f"PIPELINE COMPLETE — {total_elapsed:.1f}s total")
    print("=" * 60)
    
    for name, result in collector_results.items():
        status = "✓" if result["status"] == "OK" else "✗"
        print(f"  {status} {name}: {result['status']}")
    
    status = "✓" if processor_result["status"] == "OK" else "✗"
    print(f"  {status} Processor: {processor_result['status']}")
    
    if processor_result.get("alerts", 0) > 0:
        print(f"\n  ⚠ {processor_result['alerts']} alert(s) — check data/alerts.json")
    
    # Check if latest.json was written
    latest = DATA_DIR / "latest.json"
    if latest.exists():
        print(f"\n  Dashboard data: {latest}")
    
    # Log the run
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "collectors": collector_results,
        "processor": processor_result,
        "total_time": f"{total_elapsed:.1f}s",
    }
    log_file = LOG_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, "w") as f:
        json.dump(log_entry, f, indent=2)
    
    # Auto-push to GitHub
    push_to_github()


def test_status():
    """Check system status without running collectors."""
    print("OPTIONS MONITOR — STATUS CHECK")
    print("-" * 40)
    
    # Check data freshness
    latest = DATA_DIR / "latest.json"
    if latest.exists():
        with open(latest) as f:
            data = json.load(f)
        ts = data.get("timestamp", "unknown")
        print(f"  Latest data: {ts}")
        
        # Quick summary
        pc = data.get("pc_ratio", {})
        gex = data.get("gex", {})
        skew = data.get("skew", {})
        vix = data.get("vix", {})
        
        print(f"  P/C Ratio: {pc.get('equity', 'N/A')} ({pc.get('signal', 'N/A')})")
        print(f"  GEX: {gex.get('estimate_bn', 'N/A')}B ({gex.get('level', 'N/A')})")
        print(f"  SKEW: {skew.get('cboe_skew', 'N/A')} ({skew.get('signal', 'N/A')})")
        print(f"  VIX: {vix.get('current', 'N/A')} ({vix.get('change', 'N/A'):+.2f})" if vix.get('change') else f"  VIX: {vix.get('current', 'N/A')}")
        
        alerts = data.get("alerts", [])
        if alerts:
            print(f"\n  Active Alerts ({len(alerts)}):")
            for a in alerts:
                icon = "🔴" if a["severity"] == "critical" else "🟡"
                print(f"    {icon} {a['message']}")
    else:
        print("  No data found. Run: python run_overnight.py")
    
    # Check dependencies
    print("\n  Dependencies:")
    for pkg in ["requests", "beautifulsoup4", "yfinance", "pandas", "pyyaml"]:
        try:
            __import__(pkg.replace("-", "_").replace("4", ""))
            print(f"    ✓ {pkg}")
        except ImportError:
            # Handle beautifulsoup4 special case
            if pkg == "beautifulsoup4":
                try:
                    __import__("bs4")
                    print(f"    ✓ {pkg}")
                    continue
                except ImportError:
                    pass
            print(f"    ✗ {pkg} — pip install {pkg}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Options Activity Monitor")
    parser.add_argument("--collect", action="store_true", help="Run collectors only")
    parser.add_argument("--process", action="store_true", help="Run processor only")
    parser.add_argument("--test", action="store_true", help="Status check")
    args = parser.parse_args()
    
    if args.test:
        test_status()
    elif args.collect:
        run_collectors()
    elif args.process:
        run_processor()
    else:
        run_full_pipeline()
