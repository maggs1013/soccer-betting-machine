#!/usr/bin/env python3
import os
import pandas as pd

OUT = "data/odds_upcoming.csv"

def load_csv(fn):
    return pd.read_csv(fn) if os.path.exists(fn) else pd.DataFrame()

def main():
    # Load from your various feeders; adjust filenames if different in your repo
    a = load_csv("data/odds_api_upcoming.csv")
    b = load_csv("data/manual_odds.csv")
    c = load_csv("data/odds_scraped_oddsportal.csv")

    frames = []
    for src,df in [("api",a),("manual",b),("scrape",c)]:
        if df.empty: continue
        if "fixture_id" not in df.columns:
            # try to build fixture_id from home/away/date if needed
            pass
        df["source"] = src
        frames.append(df)

    if not frames:
        out = pd.DataFrame(columns=["fixture_id","bookmaker","oddsH","oddsD","oddsA","odds_over","total_line","source","num_books"])
        out.to_csv(OUT, index=False)
        print("No odds sources found; wrote empty odds_upcoming.csv")
        return

    odds = pd.concat(frames, ignore_index=True)

    # Normalize columns to canonical names (extend as needed)
    rename_map = {
        "home_odds":"oddsH","draw_odds":"oddsD","away_odds":"oddsA",
        "over_odds":"odds_over","total":"total_line"
    }
    for k,v in rename_map.items():
        if k in odds.columns and v not in odds.columns:
            odds[v] = odds[k]

    if "bookmaker" not in odds.columns:
        odds["bookmaker"] = odds.get("source","unknown")

    # num_books per fixture
    odds["num_books"] = odds.groupby("fixture_id")["bookmaker"].transform("nunique")

    # Keep one row per fixture with blended or representative prices
    keep_cols = ["fixture_id","bookmaker","oddsH","oddsD","oddsA","odds_over","total_line","num_books","source"]
    keep_cols = [c for c in keep_cols if c in odds.columns]
    # simple representative collapse: pick best (max) decimal odds for each side
    agg = odds.groupby("fixture_id").agg({
        "oddsH":"max","oddsD":"max","oddsA":"max","odds_over":"max","total_line":"last","num_books":"max"
    }).reset_index()
    # supply a bookmaker tag for info (optional)
    agg["bookmaker"] = "best_of_sources"

    agg.to_csv(OUT, index=False)
    print("Wrote", OUT, "rows:", len(agg))

if __name__ == "__main__":
    main()