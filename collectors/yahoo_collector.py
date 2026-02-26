"""
Yahoo Finance Options Chain Collector
Fetches: SPY/SPX options chains, strike-level OI, volume, 0DTE detection
Source: yfinance library
Schedule: Daily at 6:00 AM EST, optional intraday refresh
"""

import json
import os
from datetime import datetime, date
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"


def fetch_options_chain(ticker="SPY", num_expirations=4):
    """
    Fetch options chains for the nearest N expirations.
    Returns call/put volume, OI, and strike-level data.
    """
    result = {
        "ticker": ticker,
        "chains": [],
        "summary": {
            "total_call_volume": 0,
            "total_put_volume": 0,
            "total_call_oi": 0,
            "total_put_oi": 0,
            "zero_dte_call_volume": 0,
            "zero_dte_put_volume": 0,
        },
        "top_call_strikes": [],
        "top_put_strikes": [],
        "timestamp": None,
    }
    
    try:
        import yfinance as yf
        import pandas as pd
        
        tk = yf.Ticker(ticker)
        expirations = tk.options
        
        if not expirations:
            print(f"  [Yahoo] No options expirations found for {ticker}")
            return result
        
        today_str = str(date.today())
        all_calls = []
        all_puts = []
        
        for exp in expirations[:num_expirations]:
            try:
                chain = tk.option_chain(exp)
                calls = chain.calls.copy()
                puts = chain.puts.copy()
                
                calls["expiration"] = exp
                puts["expiration"] = exp
                
                is_zero_dte = (exp == today_str)
                calls["is_zero_dte"] = is_zero_dte
                puts["is_zero_dte"] = is_zero_dte
                
                all_calls.append(calls)
                all_puts.append(puts)
                
                call_vol = calls["volume"].fillna(0).sum()
                put_vol = puts["volume"].fillna(0).sum()
                call_oi = calls["openInterest"].fillna(0).sum()
                put_oi = puts["openInterest"].fillna(0).sum()
                
                result["summary"]["total_call_volume"] += int(call_vol)
                result["summary"]["total_put_volume"] += int(put_vol)
                result["summary"]["total_call_oi"] += int(call_oi)
                result["summary"]["total_put_oi"] += int(put_oi)
                
                if is_zero_dte:
                    result["summary"]["zero_dte_call_volume"] += int(call_vol)
                    result["summary"]["zero_dte_put_volume"] += int(put_vol)
                
                result["chains"].append({
                    "expiration": exp,
                    "is_zero_dte": is_zero_dte,
                    "call_volume": int(call_vol),
                    "put_volume": int(put_vol),
                    "call_oi": int(call_oi),
                    "put_oi": int(put_oi),
                    "pc_ratio": round(put_vol / call_vol, 4) if call_vol > 0 else None,
                })
                
                print(f"  [Yahoo] {ticker} {exp}: Calls={call_vol:,.0f} Puts={put_vol:,.0f} "
                      f"P/C={put_vol/call_vol:.3f}" if call_vol > 0 else f"  [Yahoo] {ticker} {exp}: no volume")
                
            except Exception as e:
                print(f"  [Yahoo] Error fetching {ticker} {exp}: {e}")
        
        # Aggregate and find top strikes
        if all_calls:
            all_calls_df = pd.concat(all_calls, ignore_index=True)
            all_puts_df = pd.concat(all_puts, ignore_index=True)
            
            # Top call strikes by volume
            call_by_strike = (all_calls_df.groupby("strike")
                             .agg({"volume": "sum", "openInterest": "sum"})
                             .fillna(0)
                             .sort_values("volume", ascending=False)
                             .head(10))
            
            result["top_call_strikes"] = [
                {"strike": float(strike), "volume": int(row["volume"]), "oi": int(row["openInterest"])}
                for strike, row in call_by_strike.iterrows()
            ]
            
            # Top put strikes by volume
            put_by_strike = (all_puts_df.groupby("strike")
                            .agg({"volume": "sum", "openInterest": "sum"})
                            .fillna(0)
                            .sort_values("volume", ascending=False)
                            .head(10))
            
            result["top_put_strikes"] = [
                {"strike": float(strike), "volume": int(row["volume"]), "oi": int(row["openInterest"])}
                for strike, row in put_by_strike.iterrows()
            ]
        
        # Calculate blended P/C
        s = result["summary"]
        if s["total_call_volume"] > 0:
            s["blended_pc_ratio"] = round(s["total_put_volume"] / s["total_call_volume"], 4)
        else:
            s["blended_pc_ratio"] = None
        
        if s["total_call_volume"] > 0:
            s["zero_dte_call_pct"] = round(s["zero_dte_call_volume"] / s["total_call_volume"] * 100, 1)
        else:
            s["zero_dte_call_pct"] = 0
            
        if s["total_put_volume"] > 0:
            s["zero_dte_put_pct"] = round(s["zero_dte_put_volume"] / s["total_put_volume"] * 100, 1)
        else:
            s["zero_dte_put_pct"] = 0
        
        print(f"  [Yahoo] {ticker} Summary: "
              f"Calls={s['total_call_volume']:,} Puts={s['total_put_volume']:,} "
              f"P/C={s.get('blended_pc_ratio', 'N/A')} "
              f"0DTE%={s.get('zero_dte_call_pct', 0):.0f}%/{s.get('zero_dte_put_pct', 0):.0f}%")
        
    except ImportError:
        print("  [Yahoo] yfinance not installed. Run: pip install yfinance")
    except Exception as e:
        print(f"  [Yahoo] Chain fetch failed: {e}")
    
    result["timestamp"] = datetime.now().isoformat()
    return result


def estimate_net_premium(ticker="SPY"):
    """
    Estimate net premium flow from options chain data.
    Approximation: volume * midpoint price for each contract.
    """
    premium = {
        "call_premium_est": 0,
        "put_premium_est": 0,
        "net_premium": 0,
        "timestamp": None,
    }
    
    try:
        import yfinance as yf
        
        tk = yf.Ticker(ticker)
        expirations = tk.options
        
        if not expirations:
            return premium
        
        for exp in expirations[:3]:  # Nearest 3 expirations
            try:
                chain = tk.option_chain(exp)
                
                # Estimate premium = volume * (bid + ask) / 2 * 100 (contract multiplier)
                calls = chain.calls
                puts = chain.puts
                
                call_mid = ((calls["bid"] + calls["ask"]) / 2).fillna(0)
                put_mid = ((puts["bid"] + puts["ask"]) / 2).fillna(0)
                
                call_prem = (calls["volume"].fillna(0) * call_mid * 100).sum()
                put_prem = (puts["volume"].fillna(0) * put_mid * 100).sum()
                
                premium["call_premium_est"] += call_prem
                premium["put_premium_est"] += put_prem
                
            except Exception:
                continue
        
        premium["net_premium"] = premium["call_premium_est"] - premium["put_premium_est"]
        
        # Convert to billions for display
        premium["call_premium_bn"] = round(premium["call_premium_est"] / 1e9, 3)
        premium["put_premium_bn"] = round(premium["put_premium_est"] / 1e9, 3)
        premium["net_premium_bn"] = round(premium["net_premium"] / 1e9, 3)
        
        print(f"  [Yahoo] Net Premium: Calls=${premium['call_premium_bn']:.2f}B, "
              f"Puts=${premium['put_premium_bn']:.2f}B, Net=${premium['net_premium_bn']:.2f}B")
        
    except Exception as e:
        print(f"  [Yahoo] Premium estimate failed: {e}")
    
    premium["timestamp"] = datetime.now().isoformat()
    return premium


def estimate_vol_surface(ticker="SPY"):
    """
    Rough 25-delta risk reversal estimate from options chain.
    Uses nearest monthly expiration, finds ~25-delta strikes,
    compares implied vols. Filters for liquid strikes only.
    """
    surface = {
        "put_25d_iv": None,
        "call_25d_iv": None,
        "risk_reversal": None,
        "atm_iv": None,
        "timestamp": None,
    }

    try:
        import yfinance as yf
        import numpy as np

        tk = yf.Ticker(ticker)
        spot = tk.info.get("regularMarketPrice") or tk.info.get("previousClose")
        expirations = tk.options

        if not expirations or not spot:
            print(f"  [Yahoo] Vol surface: no expirations or spot price for {ticker}")
            return surface

        # Use ~30 DTE expiration for cleaner vol surface
        target_exp = None
        for exp in expirations:
            exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
            dte = (exp_date - date.today()).days
            if 20 <= dte <= 45:
                target_exp = exp
                break

        if not target_exp:
            target_exp = expirations[min(2, len(expirations) - 1)]

        chain = tk.option_chain(target_exp)
        calls = chain.calls.copy()
        puts = chain.puts.copy()

        # Filter for liquid strikes: must have volume > 0 and bid > 0 and IV > 0
        liquid_calls = calls[
            (calls["volume"].fillna(0) > 0) &
            (calls["bid"].fillna(0) > 0) &
            (calls["impliedVolatility"].fillna(0) > 0.01)
        ]
        liquid_puts = puts[
            (puts["volume"].fillna(0) > 0) &
            (puts["bid"].fillna(0) > 0) &
            (puts["impliedVolatility"].fillna(0) > 0.01)
        ]

        if liquid_calls.empty or liquid_puts.empty:
            print(f"  [Yahoo] Vol surface: no liquid options found for {ticker} {target_exp}")
            return surface

        # ATM = closest liquid strike to spot
        atm_call = liquid_calls.iloc[(liquid_calls["strike"] - spot).abs().argsort()[:1]]
        atm_iv = atm_call["impliedVolatility"].values[0] if not atm_call.empty else None

        # ~25-delta put ≈ ~3-4% OTM put (narrower range for more liquid strikes)
        put_25d_strike = spot * 0.965
        # Find nearest liquid put to the target strike
        nearest_put = liquid_puts.iloc[(liquid_puts["strike"] - put_25d_strike).abs().argsort()[:1]]
        put_25d_iv = nearest_put["impliedVolatility"].values[0] if not nearest_put.empty else None
        put_strike_used = nearest_put["strike"].values[0] if not nearest_put.empty else None

        # ~25-delta call ≈ ~3-4% OTM call
        call_25d_strike = spot * 1.035
        nearest_call = liquid_calls.iloc[(liquid_calls["strike"] - call_25d_strike).abs().argsort()[:1]]
        call_25d_iv = nearest_call["impliedVolatility"].values[0] if not nearest_call.empty else None
        call_strike_used = nearest_call["strike"].values[0] if not nearest_call.empty else None

        if put_25d_iv and call_25d_iv:
            put_iv_pct = round(put_25d_iv * 100, 2)
            call_iv_pct = round(call_25d_iv * 100, 2)
            atm_iv_pct = round(atm_iv * 100, 2) if atm_iv else None
            rr = round((call_25d_iv - put_25d_iv) * 100, 2)

            # Sanity check: IVs should be reasonable (5-80% for equity options)
            if put_iv_pct < 5 or call_iv_pct < 5:
                print(f"  [Yahoo] Vol surface: IVs suspiciously low "
                      f"(put={put_iv_pct}%, call={call_iv_pct}%), falling back to ATM")
                if atm_iv_pct and atm_iv_pct >= 5:
                    surface["atm_iv"] = atm_iv_pct
                return surface

            # Sanity check: put and call should be different strikes
            if put_strike_used == call_strike_used:
                print(f"  [Yahoo] Vol surface: put and call landed on same strike ({put_strike_used})")

            surface["put_25d_iv"] = put_iv_pct
            surface["call_25d_iv"] = call_iv_pct
            surface["risk_reversal"] = rr
            surface["atm_iv"] = atm_iv_pct

            print(f"  [Yahoo] Vol Surface ({target_exp}): "
                  f"25Δ Put={put_iv_pct:.1f}% @{put_strike_used}, "
                  f"ATM={atm_iv_pct:.1f}%, "
                  f"25Δ Call={call_iv_pct:.1f}% @{call_strike_used}, "
                  f"RR={rr:.1f}")
        else:
            print(f"  [Yahoo] Vol surface: could not extract IVs for {ticker} {target_exp}")

    except Exception as e:
        print(f"  [Yahoo] Vol surface failed: {e}")

    surface["timestamp"] = datetime.now().isoformat()
    return surface


def collect_all():
    """Run all Yahoo Finance options collectors."""
    print("[Yahoo Options Collector] Starting...")
    
    results = {
        "spy_chain": fetch_options_chain("SPY", num_expirations=4),
        "net_premium": estimate_net_premium("SPY"),
        "vol_surface": estimate_vol_surface("SPY"),
        "collected_at": datetime.now().isoformat(),
        "source": "yahoo_collector",
    }
    
    # Save raw data
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_file = DATA_DIR / "raw_yahoo.json"
    with open(raw_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"[Yahoo Options Collector] Done. Saved to {raw_file}")
    return results


if __name__ == "__main__":
    collect_all()
