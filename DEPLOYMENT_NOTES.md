# Leverage Monitor — Deployment Notes

## Data Priority Rule (CRITICAL)

The renderer (`renderer/renderer.py`) passes two data sources to the Jinja2 template:
- `data` → `leverage_monitor.json` (updated by Claude with Bloomberg Terminal values)
- `latest` → `latest.json` (pipeline data from Mac mini, can be 1-3 days stale)

**The template now uses "resolved" variables that ALWAYS prefer `leverage_monitor.json`
layer data over `latest.json` pipeline data.** This was implemented 2026-03-04 after
stale pipeline data (VIX, GEX flip, IV/HV) leaked into client-facing output.

### How it works

At the top of `leverage_monitor.html.j2`, resolved variables are computed:
```
resolved_vix = l5a_d.vix → fallback → latest.vix.current
resolved_ivol = l5a_d.ivol (Bloomberg only, no pipeline fallback)
resolved_gex_flip = l5b_d.gex_flip → fallback → latest.gex.flip_point
resolved_spx = l5b_d.spx_last → fallback → l5a_d.spx_last
```

### What this means for updates

When updating the monitor, always populate these fields in `leverage_monitor.json`:
- `layers.l5a.data.vix` — VIX from Bloomberg
- `layers.l5a.data.ivol` — Implied vol from Barchart/Bloomberg
- `layers.l5a.data.hvol` — Historical vol
- `layers.l5a.data.iv_rank` — IV Rank
- `layers.l5a.data.iv_percentile` — IV Percentile
- `layers.l5b.data.gex_flip` — Gamma flip from Barchart screenshot
- `layers.l5b.data.spx_last` — SPX close from Bloomberg
- `layers.l5b.data.call_wall` / `put_wall` — From Barchart
- `layers.l5b.data.equity_pc` — P/C ratio
- `layers.l5b.data.vix_30d_roc_pct` — VIX 30d rate of change

If these are populated, the template will use them. If not, it falls back to pipeline data.

### Score Display

Composite score now displays as decimal (7.5, not 7). Previous score also shows decimal.
The meter bar still uses integer segments (7 lit for 7.5).

### Mode 3 Deployment

Always use Mode 3: update JSON → run renderer.py → copy output to main branch → push.
Never rebuild from scratch. Never use placeholder data.

## Last Updated
2026-03-04 — Template overhaul to fix stale data leakage
