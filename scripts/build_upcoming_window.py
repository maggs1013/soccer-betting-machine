# Build an "upcoming 7 days" enriched table from soccerdata fixtures + your manual odds.
# Writes data/UPCOMING_7D_enriched.csv

import os, pandas as pd
from datetime import datetime, timedelta

DATA="data"
fixtures_path=os.path.join(DATA,"sd_fd_fixtures.csv")      # from soccerdata
manual_path  =os.path.join(DATA,"manual_odds.csv")         # your FanDuel odds
hyb_xg_path  =os.path.join(DATA,"xg_metrics_hybrid.csv")

out_path=os.path.join(DATA,"UPCOMING_7D_enriched.csv")

# Load sources
fx=pd.read_csv(fixtures_path) if os.path.exists(fixtures_path) else pd.DataFrame()
od=pd.read_csv(manual_path) if os.path.exists(manual_path) else pd.DataFrame()
xg=pd.read_csv(hyb_xg_path) if os.path.exists(hyb_xg_path) else pd.DataFrame()

# Normalize
for df in (fx,od):
    if "date" in df.columns:
        df["date"]=pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)

# Keep 7-day window
today=pd.Timestamp.now().normalize()
fx7=fx[(fx["date"]>=today) & (fx["date"]<today+pd.Timedelta(days=7))].copy()

# Merge odds (FanDuel) by date/home/away if present
need_odds={"date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"}
if not od.empty and need_odds.issubset(od.columns):
    up = fx7.merge(od[need_odds], on=["date","home_team","away_team"], how="left")
else:
    up = fx7.copy()
    for c in ["home_odds_dec","draw_odds_dec","away_odds_dec"]: up[c]=None

# Merge xG hybrid (team → home/away columns)
if not xg.empty and "team" in xg.columns:
    hx = xg.rename(columns={"team":"home_team","xg_hybrid":"home_xg","xga_hybrid":"home_xga","xgd90_hybrid":"home_xgd90"})
    ax = xg.rename(columns={"team":"away_team","xg_hybrid":"away_xg","xga_hybrid":"away_xga","xgd90_hybrid":"away_xgd90"})
    up = up.merge(hx[["home_team","home_xg","home_xga","home_xgd90"]], on="home_team", how="left")
    up = up.merge(ax[["away_team","away_xg","away_xga","away_xgd90"]], on="away_team", how="left")

# Slot placeholders for injuries/lineups/refs/travel/crowd (enrichment step will also fill)
for c in ["home_rest_days","away_rest_days","home_travel_km","away_travel_km",
          "home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
          "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]:
    if c not in up.columns: up[c]=None

up.sort_values(["date","home_team"], inplace=True)
up.to_csv(out_path, index=False)
print("[OK] wrote", out_path, len(up))
