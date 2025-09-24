#!/usr/bin/env python3
"""
council_tournament_scan.py

Produce a quick, human-readable scan of UEFA fixtures for Council triage.

Reads:
  - data/UPCOMING_7D_enriched.csv
  - runs/YYYY-MM-DD/ODDS_MOVE_FEATURES.csv (optional)
  - (optional) relies on columns added by engineer_tournament_extras.py:
      engine_home_league_strength, engine_away_league_strength
      engine_home_team_spi, engine_away_team_spi
      engine_lsi_diff, engine_tsi_diff
      engine_home_euro_matches_365/730, engine_away_euro_matches_365/730
      engine_home_euro_wr_shrunk, engine_away_euro_wr_shrunk
      engine_is_neutral, engine_comp_stage

Writes:
  - runs/YYYY-MM-DD/TOURNAMENT_SCAN.md

What it shows:
  - Summary counts of UCL/UEL/UECL fixtures
  - A ranked table of UEFA fixtures with:
      date, league, home, away, stage, neutral?,
      lsi_diff, tsi_diff, home/away SPI,
      UEFA exp (365/730), shrunk WRs,
      odds move deltas (if present)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

DATA = "data"
RUN_DIR = os.path.join("runs", datetime.utcnow().strftime("%Y-%m-%d"))
OUT = os.path.join(RUN_DIR, "TOURNAMENT_SCAN.md")

UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")
ODMF = os.path.join(RUN_DIR, "ODDS_MOVE_FEATURES.csv")

UEFA_TOKENS = [
    "champions league","uefa champions","ucl",
    "europa league","uefa europa","uel",
    "conference league","uefa europa conference","uecl"
]

def is_uefa(league):
    if not isinstance(league, str): return False
    s = league.lower()
    return any(tok in s for tok in UEFA_TOKENS)

def safe_read_csv(path, cols=None):
    if not os.path.exists(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = np.nan
        return df
    except Exception:
        return pd.DataFrame(columns=cols or [])

def df_to_md(df):
    if df is None or len(df)==0:
        return "_(no rows)_"
    try:
        return df.to_markdown(index=False)
    except Exception:
        cols = list(df.columns)
        if not cols: return "_(no rows)_"
        lines = []
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("| " + " | ".join(["---"]*len(cols)) + " |")
        for _, r in df.iterrows():
            vals = ["" if pd.isna(v) else str(v) for v in r.tolist()]
            lines.append("| " + " | ".join(vals) + " |")
        return "\n".join(lines)

def main():
    os.makedirs(RUN_DIR, exist_ok=True)
    up = safe_read_csv(UP)
    if up.empty:
        open(OUT,"w").write("# TOURNAMENT SCAN\n\n_No upcoming fixtures to scan this run._\n")
        print("TOURNAMENT_SCAN.md written (no fixtures)."); return

    # normalize
    if "league" not in up.columns: up["league"] = "GLOBAL"
    up["date"] = pd.to_datetime(up.get("date", pd.NaT), errors="coerce")

    # pick UEFA fixtures
    uefa = up[up["league"].astype(str).apply(is_uefa)].copy()
    if uefa.empty:
        open(OUT,"w").write("# TOURNAMENT SCAN\n\n_No UEFA fixtures found in UPCOMING._\n")
        print("TOURNAMENT_SCAN.md written (no UEFA)."); return

    # Try to pull tournament extras (these may not exist if the extra step wasn't run yet)
    for c in [
        "engine_home_league_strength","engine_away_league_strength",
        "engine_home_team_spi","engine_away_team_spi",
        "engine_lsi_diff","engine_tsi_diff",
        "engine_home_euro_matches_365","engine_home_euro_matches_730","engine_home_euro_wr_shrunk",
        "engine_away_euro_matches_365","engine_away_euro_matches_730","engine_away_euro_wr_shrunk",
        "engine_is_neutral","engine_comp_stage"
    ]:
        if c not in uefa.columns: uefa[c] = np.nan

    # Odds move deltas (optional)
    od = safe_read_csv(ODMF, ["fixture_id","delta_implied_home","delta_implied_away","delta_implied_over"])
    if "fixture_id" not in uefa.columns:
        # create a canonical fallback ID, consistent with model_predict.py helper
        def canonical_id(row):
            date = str(row.get("date","")).split(" ")[0].replace("-","")
            h = str(row.get("home_team","")).strip().lower().replace(" ","_")
            a = str(row.get("away_team","")).strip().lower().replace(" ","_")
            return f"{date}__{h}__vs__{a}"
        uefa["fixture_id"] = uefa.apply(canonical_id, axis=1)

    if not od.empty:
        uefa = uefa.merge(od, on="fixture_id", how="left")
    else:
        uefa["delta_implied_home"] = np.nan
        uefa["delta_implied_away"] = np.nan
        uefa["delta_implied_over"] = np.nan

    # Build view table
    view_cols = [
        "date","league","home_team","away_team",
        "engine_comp_stage","engine_is_neutral",
        "engine_lsi_diff","engine_tsi_diff",
        "engine_home_team_spi","engine_away_team_spi",
        "engine_home_euro_matches_365","engine_away_euro_matches_365",
        "engine_home_euro_wr_shrunk","engine_away_euro_wr_shrunk",
        "delta_implied_home","delta_implied_away","delta_implied_over"
    ]
    for c in view_cols:
        if c not in uefa.columns: uefa[c] = np.nan

    # Rank by abs(tsi_diff) then abs(lsi_diff) as a default "interestingness"
    uefa["_rank_key"] = uefa["engine_tsi_diff"].abs().fillna(0) + 0.5*uefa["engine_lsi_diff"].abs().fillna(0)
    table = uefa.sort_values(["date","_rank_key"], ascending=[True, False])[view_cols].head(30)

    # Counts summary
    counts = (
        uefa["league"]
        .str.lower()
        .map(lambda s: "UCL" if "champions" in s or "ucl" in s
                      else "UEL" if "europa league" in s or "uel" in s
                      else "UECL" if "conference" in s or "uecl" in s
                      else "UEFA (other)"))
    summary = counts.value_counts().rename_axis("competition").reset_index(name="fixtures")

    # Write
    buf = []
    buf.append("# TOURNAMENT SCAN")
    buf.append("")
    buf.append(f"_Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC_")
    buf.append("")
    buf.append("## Summary")
    buf.append(df_to_md(summary))
    buf.append("")
    buf.append("## UEFA Fixtures (top 30 by |TSI diff| + 0.5·|LSI diff|)")
    buf.append(df_to_md(table))
    buf.append("")
    buf.append("### Column notes")
    buf.append("- **engine_lsi_diff**: home_domestic_LSI − away_domestic_LSI (league strength prior)")
    buf.append("- **engine_tsi_diff**: home_TSI − away_TSI (team SPI-based prior)")
    buf.append("- **euro_matches_365/730**: team UEFA matches in last 365/730 days; **euro_wr_shrunk** = WR shrunk to 1/3 with α=5")
    buf.append("- **delta_implied_***: AM→T−60 implied-probability changes (positive favors that side)")
    buf.append("- **engine_is_neutral**: 1 if neutral/final heuristics; **engine_comp_stage** from league text")

    with open(OUT, "w") as f:
        f.write("\n".join(buf))
    print("TOURNAMENT_SCAN.md written:", OUT)

if __name__ == "__main__":
    main()