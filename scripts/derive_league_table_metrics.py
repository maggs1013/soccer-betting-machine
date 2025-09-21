# scripts/derive_league_table_metrics.py
# Build a league xG table using your pipeline outputs:
# - HIST_matches.csv (results)
# - xg_metrics_hybrid.csv (team xG hybrid)
# - sd_538_spi.csv (optional)
#
# Output: data/LEAGUE_XG_TABLE.csv

import os
import pandas as pd
import numpy as np

DATA = "data"
OUT  = os.path.join(DATA, "LEAGUE_XG_TABLE.csv")

def safe_read(p, cols=None):
    if not os.path.exists(p):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(p)
        if cols:
            for c in cols:
                if c not in df.columns:
                    df[c] = np.nan
        return df
    except Exception:
        return pd.DataFrame(columns=cols or [])

def result_points(hg, ag):
    if pd.isna(hg) or pd.isna(ag):
        return (np.nan, np.nan)
    hg, ag = int(hg), int(ag)
    if hg > ag:  # home win
        return (3, 0)
    elif hg == ag:
        return (1, 1)
    else:
        return (0, 3)

def main():
    # Load core files
    hist = safe_read(os.path.join(DATA, "HIST_matches.csv"),
                     ["date","home_team","away_team","home_goals","away_goals"])
    hybx = safe_read(os.path.join(DATA, "xg_metrics_hybrid.csv"),
                     ["team","xg_hybrid","xga_hybrid","xgd90_hybrid"])
    spi  = safe_read(os.path.join(DATA, "sd_538_spi.csv"))  # optional

    if hist.empty:
        print("[WARN] HIST_matches.csv empty; writing header-only table.")
        pd.DataFrame(columns=["league","team","gp","w","d","l","gf","ga","pts","xgf","xga","xg_diff","clinicality","def_overperf","spi_off","spi_def"]).to_csv(OUT, index=False)
        return

    # If your HIST has a 'league' or 'competition' column, keep it; otherwise set 'league' as 'Unknown'
    if "league" not in hist.columns:
        hist["league"] = "Unknown"
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")

    # Build per-team traditional table (from results)
    # Home rows
    h = hist[["league","home_team","home_goals","away_goals"]].copy()
    h.rename(columns={"home_team":"team","home_goals":"gf","away_goals":"ga"}, inplace=True)
    # Away rows
    a = hist[["league","away_team","home_goals","away_goals"]].copy()
    a.rename(columns={"away_team":"team","home_goals":"ga","away_goals":"gf"}, inplace=True)

    stack = pd.concat([h.assign(ha="H"), a.assign(ha="A")], ignore_index=True)
    # W/D/L
    stack["w"] = np.where(stack["gf"] > stack["ga"], 1, 0)
    stack["d"] = np.where(stack["gf"] == stack["ga"], 1, 0)
    stack["l"] = np.where(stack["gf"] < stack["ga"], 1, 0)

    # Points
    # We need original home/away orientation to compute points cleanly; approximate from ha already computed
    # For stacked rows, points = 3 for w, 1 for d, 0 for l
    stack["pts"] = stack["w"]*3 + stack["d"]*1

    base_table = (stack.groupby(["league","team"], as_index=False)
                        .agg(gp=("team","count"),
                             w=("w","sum"), d=("d","sum"), l=("l","sum"),
                             gf=("gf","sum"), ga=("ga","sum"),
                             pts=("pts","sum")))

    # Join xG hybrid (per-team; we use the hybrid as "season-strength" proxy if per-team/season)
    # If hybx has multiple rows per team (e.g., different sources), collapse to mean.
    if not hybx.empty:
        hybx_agg = hybx.groupby("team", as_index=False)[["xg_hybrid","xga_hybrid"]].mean()
        base_table = base_table.merge(hybx_agg, on="team", how="left")
        base_table.rename(columns={"xg_hybrid":"xgf","xga_hybrid":"xga"}, inplace=True)
    else:
        base_table["xgf"] = np.nan
        base_table["xga"] = np.nan

    # Compute derived metrics
    base_table["xg_diff"]     = base_table["xgf"] - base_table["xga"]
    base_table["clinicality"] = base_table["gf"]  - base_table["xgf"]  # + = clinical finishing / overperformance
    base_table["def_overperf"]= base_table["xga"] - base_table["ga"]   # + = outperforming expected defensively

    # SPI optional join (avg per team)
    if not spi.empty:
        # Try to detect off/def columns (names vary); keep the most common
        spi_cols = {c.lower(): c for c in spi.columns}
        off_col = spi_cols.get("off", spi_cols.get("offense", None))
        def_col = spi_cols.get("def", spi_cols.get("defense", None))
        team_col = spi_cols.get("team", None) or "team"
        if team_col not in spi.columns:
            if "squad" in spi.columns:
                spi = spi.rename(columns={"squad":"team"})
                team_col = "team"
        if off_col and def_col and team_col in spi.columns:
            spi_agg = spi.groupby(team_col, as_index=False)[[off_col, def_col]].mean()
            spi_agg.rename(columns={off_col:"spi_off", def_col:"spi_def"}, inplace=True)
            base_table = base_table.merge(spi_agg.rename(columns={team_col:"team"}), on="team", how="left")
        else:
            base_table["spi_off"] = np.nan
            base_table["spi_def"] = np.nan
    else:
        base_table["spi_off"] = np.nan
        base_table["spi_def"] = np.nan

    # Sort for readability: by league then xg_diff desc then pts desc
    base_table.sort_values(["league","xg_diff","pts"], ascending=[True, False, False], inplace=True)

    base_table.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(base_table)}")
    # simple coverage log
    print("[INFO] xG coverage:", base_table["xgf"].notna().sum(), "/", len(base_table))

if __name__ == "__main__":
    main()