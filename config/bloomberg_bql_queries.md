# Unified Bloomberg BQL Query — Options Monitor + Leverage Monitor
# Run daily from 22V workstation (Windows PC)
# Feeds both the Options Activity Dashboard and the Maguire Leverage Monitor

# ============================================================
# HOW TO USE
# ============================================================
# Option A: Paste individual queries into BQL<GO> on the terminal
# Option B: Run via Bloomberg Excel Add-in (=BQL() function)
# Option C: Run via Python blpapi / xbbg library on the 22V workstation
#
# Output: Save results to a shared JSON or CSV that both the
# Options Monitor and Leverage Monitor pipelines consume.
# ============================================================


# ============================================================
# SECTION 1: OPTIONS ACTIVITY MONITOR
# ============================================================

# --- 1A. Put/Call Ratio ---

# CBOE Equity Put/Call Ratio (daily)
get(px_last) for('PCUSEQTR Index') with(dates=range(-30d,0d), fill=prev)
# Returns: 30-day history of equity-only P/C ratio

# CBOE Index Put/Call Ratio
get(px_last) for('PCUSIDXT Index') with(dates=range(-30d,0d), fill=prev)

# CBOE Total Put/Call Ratio
get(px_last) for('PCUSTOTT Index') with(dates=range(-30d,0d), fill=prev)


# --- 1B. VIX & Volatility Structure ---

# VIX spot
get(px_last, chg_pct_1d) for('VIX Index')

# VIX term structure (spot vs futures for contango/backwardation)
get(px_last) for('VIX Index', 'UX1 Index', 'UX2 Index', 'UX3 Index', 'UX4 Index')
# VIX spot, front month, 2nd month, 3rd month, 4th month

# VVIX (vol of vol)
get(px_last) for('VVIX Index')

# CBOE SKEW Index
get(px_last) for('SKEW Index') with(dates=range(-180d,0d), fill=prev)

# Realized vol (20-day)
get(volatility(period=20)) for('SPX Index')

# Implied vs Realized spread
get(ivol_atm(period=30d), volatility(period=20)) for('SPX Index')


# --- 1C. SPX/SPY Options Volume & Positioning ---

# SPX total options volume (calls vs puts) — use OMON or:
get(opt_put_vol, opt_call_vol, opt_put_call_ratio) for('SPX Index')

# SPY options volume
get(opt_put_vol, opt_call_vol, opt_put_call_ratio) for('SPY US Equity')


# --- 1D. Vol Surface / 25-Delta Risk Reversal ---

# ATM implied vol (30-day)
get(ivol_atm(period=30d)) for('SPX Index')

# 25-delta put and call vols (30-day)
get(ivol_delta_put(delta=25, period=30d), ivol_delta_call(delta=25, period=30d)) for('SPX Index')

# Risk reversal = 25d call vol - 25d put vol
# (Calculate in post-processing from the above)

# Skew: 25d put vol - ATM vol
# (Calculate in post-processing)


# --- 1E. Gamma Exposure Inputs ---
# Pull full SPX options chain for DIY GEX calculation

# Near-term SPX options chain (nearest 2 expirations)
get(opt_gamma, open_int, opt_volume, option_type, strike_px, px_last, days_to_expiration)
  for(options('SPX Index'))
  with(expiration=range(0d,14d))

# GEX formula (post-process):
# For each strike:
#   call_gex = call_gamma * call_OI * 100 * spot_price
#   put_gex  = put_gamma * put_OI * 100 * spot_price * (-1)
# Total GEX = sum(call_gex) + sum(put_gex)
# Call wall = strike with max call_gex
# Put wall = strike with max abs(put_gex)
# Flip point = strike where cumulative GEX crosses zero


# --- 1F. 0DTE Volume ---
# (Best pulled intraday via OMON or OVDV; BQL for EOD snapshot)

get(opt_volume, option_type, days_to_expiration)
  for(options('SPX Index'))
  with(expiration=0d)
# Filter for days_to_expiration = 0 to isolate 0DTE


# ============================================================
# SECTION 2: MAGUIRE LEVERAGE MONITOR
# ============================================================

# --- 2A. Layer 1: Leverage Levels ---

# FINRA Margin Debt (monthly, ~3 week lag)
get(px_last) for('FINRMRGD Index') with(dates=range(-365d,0d), fill=prev)
# Returns margin debt in $M; latest available month

# S&P 500 total market cap (for normalization)
get(cur_mkt_cap) for('SPX Index')
# Alternative: get(px_last) for('WCAUWRLD Index') for world mkt cap

# US nominal GDP (for margin debt / GDP ratio)
get(px_last) for('GDP CUR$ Index')

# Margin debt derived metrics (post-process):
# margin_debt_yoy = (current - 12mo ago) / 12mo ago
# margin_debt_to_mktcap = margin_debt / spx_mktcap
# margin_debt_to_gdp = margin_debt / gdp


# --- 2B. Layer 3: Financing Conditions ---

# Goldman Sachs Financial Conditions Index
get(px_last) for('GSUSFCI Index') with(dates=range(-90d,0d), fill=prev)

# Chicago Fed NFCI
get(px_last) for('NFCIINDX Index') with(dates=range(-90d,0d), fill=prev)

# High-Yield Credit Spread (ICE BofA HY OAS)
get(px_last) for('BAMLHYSP Index') with(dates=range(-90d,0d), fill=prev)
# Alternative ticker: 'LF98OAS Index'

# SOFR (overnight financing rate)
get(px_last) for('SOFRRATE Index') with(dates=range(-30d,0d), fill=prev)

# IG Credit Spread (for comparison)
get(px_last) for('BAMLCOAS Index') with(dates=range(-90d,0d), fill=prev)

# HYG price (credit ETF health check)
get(px_last, chg_pct_1d) for('HYG US Equity')

# 2Y-10Y Treasury spread (yield curve)
get(px_last) for('USYC2Y10 Index') with(dates=range(-90d,0d), fill=prev)


# --- 2C. Layer 4: Positioning / Crowding Proxies ---

# Hedge Fund VIP proxy — GSTHHVIP basket performance
get(px_last, chg_pct_1d, chg_pct_5d) for('GSTHHVIP Index')

# Most crowded longs basket
get(px_last, chg_pct_1d) for('GSTHHFML Index')

# Most shorted basket
get(px_last, chg_pct_1d) for('GSTHSMS Index')

# Crowded vs market divergence (post-process):
# If VIP basket underperforming SPX by >2% over 5d → de-grossing signal


# --- 2D. Layer 5: Vol Regime (shared with Options Monitor) ---
# Already covered in Section 1B above — VIX, VVIX, term structure, skew
# Cross-reference with GEX from Section 1E


# --- 2E. Layer 6: Calendar / Liquidity Events ---

# Fed Funds Futures (implied rate path)
get(px_last) for('FF1 Comdty', 'FF2 Comdty', 'FF3 Comdty')
# Front 3 Fed Funds futures → implied rate expectations

# Treasury yields (for financing pressure)
get(px_last) for('USGG2YR Index', 'USGG10YR Index', 'USGG30YR Index')


# --- 2F. Cross-Asset Context (bonus, useful for both systems) ---

# Dollar index
get(px_last, chg_pct_1d) for('DXY Curncy')

# Gold (stress hedge demand)
get(px_last, chg_pct_1d) for('GC1 Comdty')

# Bitcoin (risk appetite proxy)
get(px_last, chg_pct_1d) for('XBTUSD BGN Curncy')

# SPX level + recent performance
get(px_last, chg_pct_1d, chg_pct_5d, chg_pct_1m) for('SPX Index')


# ============================================================
# SECTION 3: COMBINED EXCEL TEMPLATE
# ============================================================
#
# To run all of this from a single Bloomberg Excel sheet:
#
# Cell A1: =BQL("get(px_last) for('PCUSEQTR Index','PCUSIDXT Index','PCUSTOTT Index','VIX Index','VVIX Index','SKEW Index')")
# Cell A10: =BQL("get(px_last) for('GSUSFCI Index','NFCIINDX Index','BAMLHYSP Index','SOFRRATE Index')")
# Cell A20: =BQL("get(px_last) for('FINRMRGD Index')")
# Cell A25: =BQL("get(px_last, chg_pct_1d) for('GSTHHVIP Index','GSTHHFML Index','GSTHSMS Index')")
# Cell A30: =BQL("get(px_last) for('UX1 Index','UX2 Index','UX3 Index','UX4 Index')")
# Cell A35: =BQL("get(ivol_delta_put(delta=25,period=30d),ivol_delta_call(delta=25,period=30d),ivol_atm(period=30d)) for('SPX Index')")
#
# Then have a VBA macro or Python script that:
# 1. Refreshes all BQL cells
# 2. Reads the values
# 3. Writes to a JSON file on a shared drive or pushes to GitHub
# 4. The Options Monitor and Leverage Monitor pipelines consume that JSON
#
# Schedule: Windows Task Scheduler → run at 6:00 AM, 12:00 PM, 4:00 PM


# ============================================================
# SECTION 4: PYTHON BLPAPI ALTERNATIVE
# ============================================================
#
# If you prefer running via Python on the 22V workstation:
#
# pip install xbbg
#
# from xbbg import blp
# import json
#
# # Single pull for all tickers
# tickers = [
#     'PCUSEQTR Index', 'PCUSIDXT Index', 'PCUSTOTT Index',
#     'VIX Index', 'VVIX Index', 'SKEW Index',
#     'GSUSFCI Index', 'NFCIINDX Index', 'BAMLHYSP Index', 'SOFRRATE Index',
#     'FINRMRGD Index', 'GSTHHVIP Index', 'GSTHHFML Index', 'GSTHSMS Index',
#     'UX1 Index', 'UX2 Index', 'UX3 Index', 'UX4 Index',
#     'HYG US Equity', 'DXY Curncy', 'GC1 Comdty', 'SPX Index',
#     'USGG2YR Index', 'USGG10YR Index', 'USYC2Y10 Index',
# ]
#
# data = blp.bdp(tickers, ['px_last', 'chg_pct_1d'])
# data.to_json('bloomberg_pull.json')
#
# # Historical for charts
# hist = blp.bdh(
#     ['PCUSEQTR Index', 'VIX Index', 'SKEW Index', 'GSUSFCI Index', 'BAMLHYSP Index', 'FINRMRGD Index'],
#     'px_last',
#     start_date='2025-08-01'
# )
# hist.to_json('bloomberg_history.json')


# ============================================================
# TICKER CHEAT SHEET
# ============================================================
#
# OPTIONS MONITOR:
#   PCUSEQTR Index    — Equity Put/Call Ratio
#   PCUSIDXT Index    — Index Put/Call Ratio
#   PCUSTOTT Index    — Total Put/Call Ratio
#   VIX Index         — CBOE VIX
#   VVIX Index        — Vol of Vol
#   SKEW Index        — CBOE SKEW
#   UX1-4 Index       — VIX Futures (term structure)
#
# LEVERAGE MONITOR:
#   FINRMRGD Index    — FINRA Margin Debt
#   GSUSFCI Index     — GS Financial Conditions
#   NFCIINDX Index    — Chicago Fed NFCI
#   BAMLHYSP Index    — HY Credit Spread (OAS)
#   SOFRRATE Index    — SOFR Rate
#   GSTHHVIP Index    — GS HF VIP Basket
#   GSTHHFML Index    — GS HF Most Crowded Longs
#   GSTHSMS Index     — GS Most Shorted
#   USYC2Y10 Index    — 2s10s Yield Curve
#
# CROSS-ASSET:
#   DXY Curncy        — Dollar Index
#   GC1 Comdty        — Gold Front Month
#   HYG US Equity     — HY Credit ETF
#   SPX Index         — S&P 500
#   USGG2YR Index     — 2Y Treasury
#   USGG10YR Index    — 10Y Treasury
