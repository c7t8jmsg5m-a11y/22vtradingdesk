"""
Calendar Events Collector
Generates: OPEX, Quad Witching, Quarter-End, Tax Dates, Treasury Refunding
Scrapes: FOMC meeting dates, CPI/NFP release dates
Schedule: Daily (events are deterministic + cached scrapes)
"""

import json
import calendar as cal_mod
import requests
from datetime import date, datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"

LOOK_AHEAD_DAYS = 90

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
}

# Months with Summary of Economic Projections (dot plot)
# FOMC publishes SEP at Mar, Jun, Sep, Dec meetings
SEP_MONTHS = {3, 6, 9, 12}


def third_friday(year, month):
    """Return the 3rd Friday of a given month."""
    c = cal_mod.monthcalendar(year, month)
    fridays = [week[cal_mod.FRIDAY] for week in c if week[cal_mod.FRIDAY] != 0]
    return date(year, month, fridays[2])


def prev_business_day(d):
    """If d falls on weekend, return previous Friday."""
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def next_business_day(d):
    """If d falls on weekend, return next Monday."""
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def generate_opex_dates(start, end):
    """Generate monthly OPEX (3rd Friday) and mark quad witching."""
    events = []
    d = date(start.year, start.month, 1)
    while d <= end:
        opex = third_friday(d.year, d.month)
        if start <= opex <= end:
            is_quad = d.month in (3, 6, 9, 12)
            if is_quad:
                events.append({
                    "date": opex.isoformat(),
                    "name": "Quad Witching OPEX",
                    "severity": "HIGH",
                    "source": "deterministic",
                })
            else:
                events.append({
                    "date": opex.isoformat(),
                    "name": "Monthly OPEX",
                    "severity": "MED",
                    "source": "deterministic",
                })
        # Next month
        if d.month == 12:
            d = date(d.year + 1, 1, 1)
        else:
            d = date(d.year, d.month + 1, 1)
    return events


def generate_quarter_end_dates(start, end):
    """Generate quarter-end rebalancing dates."""
    events = []
    quarter_ends = [
        date(start.year, 3, 31),
        date(start.year, 6, 30),
        date(start.year, 9, 30),
        date(start.year, 12, 31),
        date(start.year + 1, 3, 31),
        date(start.year + 1, 6, 30),
    ]
    for qe in quarter_ends:
        bd = prev_business_day(qe)
        if start <= bd <= end:
            events.append({
                "date": bd.isoformat(),
                "name": "Quarter-End Rebalancing",
                "severity": "HIGH",
                "source": "deterministic",
            })
    return events


def generate_tax_dates(start, end):
    """Generate estimated tax payment deadlines."""
    events = []
    # Quarterly estimated tax payments
    tax_dates = [
        (start.year, 1, 15),
        (start.year, 4, 15),
        (start.year, 6, 15),
        (start.year, 9, 15),
        (start.year + 1, 1, 15),
        (start.year + 1, 4, 15),
    ]
    for y, m, d in tax_dates:
        td = next_business_day(date(y, m, d))
        if start <= td <= end:
            events.append({
                "date": td.isoformat(),
                "name": "Estimated Tax Payments",
                "severity": "MED",
                "source": "deterministic",
            })
    return events


def generate_treasury_refunding(start, end):
    """Generate Treasury quarterly refunding announcement dates.
    Typically first Wednesday of Feb, May, Aug, Nov."""
    events = []
    refunding_months = [2, 5, 8, 11]
    for year in [start.year, start.year + 1]:
        for month in refunding_months:
            # First Wednesday of the month
            c = cal_mod.monthcalendar(year, month)
            wed = cal_mod.WEDNESDAY
            first_wed = None
            for week in c:
                if week[wed] != 0:
                    first_wed = date(year, month, week[wed])
                    break
            if first_wed and start <= first_wed <= end:
                events.append({
                    "date": first_wed.isoformat(),
                    "name": "Treasury Refunding Announcement",
                    "severity": "MED",
                    "source": "deterministic",
                })
    return events


def scrape_fomc_dates():
    """Scrape FOMC meeting dates from Federal Reserve website."""
    events = []
    url = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(url, timeout=15, headers=HEADERS)
        if resp.status_code != 200:
            print(f"  [Calendar] FOMC scrape failed: HTTP {resp.status_code}")
            return events

        soup = BeautifulSoup(resp.text, "html.parser")

        # FOMC calendar page has meeting panels with dates
        # Look for elements containing month/day patterns
        panels = soup.find_all("div", class_="fomc-meeting")
        if not panels:
            # Try alternative selectors
            panels = soup.find_all("div", class_="panel")

        # Parse date text from each panel
        import re
        year_pattern = re.compile(r'20\d{2}')
        current_year = None

        for panel in panels:
            text = panel.get_text(" ", strip=True)

            # Detect year headers
            year_match = year_pattern.search(text)
            if year_match:
                current_year = int(year_match.group())

            # Look for date patterns like "January 28-29" or "March 18-19*"
            # Asterisk indicates SEP/dot plot meeting
            date_patterns = re.findall(
                r'(January|February|March|April|May|June|July|August|'
                r'September|October|November|December)\s+(\d{1,2})(?:-(\d{1,2}))?(\*)?',
                text
            )
            for match in date_patterns:
                month_name, day1, day2, has_sep = match
                if not current_year:
                    current_year = date.today().year
                try:
                    month_num = list(cal_mod.month_name).index(month_name)
                    # Use the last day of multi-day meetings (decision day)
                    meeting_day = int(day2) if day2 else int(day1)
                    meeting_date = date(current_year, month_num, meeting_day)

                    has_dot_plot = bool(has_sep) or month_num in SEP_MONTHS
                    name = "FOMC Meeting + Dot Plot" if has_dot_plot else "FOMC Meeting"

                    events.append({
                        "date": meeting_date.isoformat(),
                        "name": name,
                        "severity": "HIGH" if has_dot_plot else "MED",
                        "source": "fed_scrape",
                    })
                except (ValueError, IndexError):
                    continue

        if events:
            print(f"  [Calendar] Scraped {len(events)} FOMC dates from Fed website")
        else:
            print("  [Calendar] FOMC scrape: no dates parsed — using fallback")

    except ImportError:
        print("  [Calendar] BeautifulSoup not installed — skipping FOMC scrape")
    except Exception as e:
        print(f"  [Calendar] FOMC scrape error: {e}")

    return events


def scrape_bls_releases(release_type="cpi"):
    """Scrape BLS release schedule for CPI or NFP dates."""
    events = []
    urls = {
        "cpi": "https://www.bls.gov/schedule/news_release/cpi.htm",
        "nfp": "https://www.bls.gov/schedule/news_release/empsit.htm",
    }
    names = {
        "cpi": "CPI Release",
        "nfp": "NFP (Employment Situation)",
    }

    url = urls.get(release_type)
    name = names.get(release_type, release_type.upper())
    if not url:
        return events

    try:
        from bs4 import BeautifulSoup
        import re

        resp = requests.get(url, timeout=15, headers=HEADERS)
        if resp.status_code != 200:
            print(f"  [Calendar] BLS {release_type} scrape failed: HTTP {resp.status_code}")
            return events

        soup = BeautifulSoup(resp.text, "html.parser")

        # BLS schedule pages have tables with release dates
        tables = soup.find_all("table")
        current_year = date.today().year

        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                text = row.get_text(" ", strip=True)

                # Look for date patterns like "Friday, March 12, 2026"
                date_match = re.search(
                    r'(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+'
                    r'(January|February|March|April|May|June|July|August|'
                    r'September|October|November|December)\s+(\d{1,2}),?\s*(20\d{2})?',
                    text
                )
                if date_match:
                    month_name, day_str, year_str = date_match.groups()
                    year = int(year_str) if year_str else current_year
                    try:
                        month_num = list(cal_mod.month_name).index(month_name)
                        release_date = date(year, month_num, int(day_str))
                        events.append({
                            "date": release_date.isoformat(),
                            "name": name,
                            "severity": "MED",
                            "source": "bls_scrape",
                        })
                    except (ValueError, IndexError):
                        continue

        if events:
            print(f"  [Calendar] Scraped {len(events)} {release_type.upper()} dates from BLS")
        else:
            print(f"  [Calendar] BLS {release_type} scrape: no dates parsed")

    except ImportError:
        print("  [Calendar] BeautifulSoup not installed — skipping BLS scrape")
    except Exception as e:
        print(f"  [Calendar] BLS {release_type} scrape error: {e}")

    return events


def load_manual_additions():
    """Load one-off manual calendar events from manual_overrides.json."""
    overrides_file = DATA_DIR / "manual_overrides.json"
    if not overrides_file.exists():
        return []

    try:
        with open(overrides_file) as f:
            overrides = json.load(f)

        # Check new key first, then legacy key
        manual = overrides.get("l6_calendar_manual_additions", {}).get("events", [])
        if not manual:
            # Legacy: read from l6_calendar but only events with no "source" field
            # (these are truly manual one-offs like "Powell Term Ends")
            legacy = overrides.get("l6_calendar", {}).get("events", [])
            manual = [e for e in legacy if e.get("source") is None]
        return manual
    except Exception as e:
        print(f"  [Calendar] Error loading manual additions: {e}")
        return []


def collect_all():
    """Generate all calendar events for the next 90 days."""
    print("[Calendar Collector] Starting...")

    today = date.today()
    end = today + timedelta(days=LOOK_AHEAD_DAYS)

    all_events = []

    # Deterministic events
    all_events.extend(generate_opex_dates(today, end))
    all_events.extend(generate_quarter_end_dates(today, end))
    all_events.extend(generate_tax_dates(today, end))
    all_events.extend(generate_treasury_refunding(today, end))
    print(f"  [Calendar] Generated {len(all_events)} deterministic events")

    # Scraped events
    fomc_status = "OK"
    fomc_events = scrape_fomc_dates()
    if fomc_events:
        # Filter to look-ahead window
        fomc_filtered = [
            e for e in fomc_events
            if today <= date.fromisoformat(e["date"]) <= end
        ]
        all_events.extend(fomc_filtered)
    else:
        fomc_status = "FAILED"

    bls_cpi_status = "OK"
    cpi_events = scrape_bls_releases("cpi")
    if cpi_events:
        cpi_filtered = [
            e for e in cpi_events
            if today <= date.fromisoformat(e["date"]) <= end
        ]
        all_events.extend(cpi_filtered)
    else:
        bls_cpi_status = "SKIPPED"

    bls_nfp_status = "OK"
    nfp_events = scrape_bls_releases("nfp")
    if nfp_events:
        nfp_filtered = [
            e for e in nfp_events
            if today <= date.fromisoformat(e["date"]) <= end
        ]
        all_events.extend(nfp_filtered)
    else:
        bls_nfp_status = "SKIPPED"

    # Manual one-off additions
    manual_events = load_manual_additions()
    if manual_events:
        manual_filtered = []
        for e in manual_events:
            try:
                if today <= date.fromisoformat(e["date"]) <= end:
                    e["source"] = "manual"
                    manual_filtered.append(e)
            except (ValueError, KeyError):
                continue
        all_events.extend(manual_filtered)
        if manual_filtered:
            print(f"  [Calendar] Added {len(manual_filtered)} manual events")

    # Deduplicate by date + name
    seen = set()
    deduped = []
    for event in all_events:
        key = (event["date"], event["name"])
        if key not in seen:
            deduped.append(event)
            seen.add(key)

    # Sort by date
    deduped.sort(key=lambda e: e["date"])

    results = {
        "timestamp": datetime.now().isoformat(),
        "source": "calendar_collector",
        "look_ahead_days": LOOK_AHEAD_DAYS,
        "events": deduped,
        "fomc_scrape_status": fomc_status,
        "bls_cpi_scrape_status": bls_cpi_status,
        "bls_nfp_scrape_status": bls_nfp_status,
        "total_events": len(deduped),
    }

    # Save raw
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_file = DATA_DIR / "raw_calendar.json"
    with open(raw_file, "w") as f:
        json.dump(results, f, indent=2, default=str)

    high_count = sum(1 for e in deduped if e.get("severity") == "HIGH")
    print(f"[Calendar Collector] Done. {len(deduped)} events ({high_count} HIGH). Saved to {raw_file}")
    return results


if __name__ == "__main__":
    collect_all()
