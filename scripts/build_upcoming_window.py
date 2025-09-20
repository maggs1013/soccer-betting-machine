# scripts/build_upcoming_window.py
# Build "next 7 days" enriched table, normalized to your league allowlist.

import os, pandas as pd

DATA="data"
fixtures_path=os.path.join(DATA,"sd_fd_fixtures.csv")
manual_path  =os.path.join(DATA,"manual_odds.csv")
hyb_xg_path  =os.path.join(DATA,"xg_metrics_hybrid.csv")
allow_path   =os.path.join(DATA,"leagues_allowlist.csv")
league_map_p =os.path.join(DATA,"league_name_map.csv")

out_path=os.path.join(DATA,"UPCOMING_7D_enriched.csv")

def load_csv(p): return pd.read_csv(p) if os.path.exists(p) else pd.DataFrame()

fx = load_csv(fixtures_path); od = load_csv(manual_path); xg = load_csv(hyb_xg_path)
allow = load_csv(allow_path); lmap = load_csv(league_map_p)

# League normalizer
name_map = {}
if not lmap.empty and {"raw","canonical"}.issubset(lmap.columns):
    name_map = {str(r.raw).strip(): str(r.canonical).strip() for _,r in lmap.iterrows()}

def norm_league(s):
    if pd.isna(s): return s
    s = str(s).strip()
    return name_map.get(s, s)

# Normalize dates and leagues
for df in (fx, od):
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)
if "league" in fx.columns:
    fx["league"] = fx["league"].apply(norm_league)

# Filter to allowlist if present
if not allow.empty and "league_canonical" in allow.columns and "league" in fx.columns:
    fx = fx[fx["league"].isin(allow["league_canonical"])].copy()

# Merge FanDuel odds into fixtures
need = {"date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"}
if not od.empty and need.issubset(od.columns):
    up = fx.merge(od[list(need)], on=["date","home_team","away_team"], how="left")
else:
    up = fx.copy()
    for c in ["home_odds_dec","draw_odds_dec","away_odds_dec"]:
        up[c] = None

# Merge xG hybrid as home_*/away_*
if not xg.empty and "team" in xg.columns:
    hx = xg.rename(columns={"team":"home_team","xg_hybrid":"home_xg","xga_hybrid":"home_xga","xgd90_hybrid":"home_xgd90"})
    ax = xg.rename(columns={"team":"away_team","xg_hybrid":"away_xg","xga_hybrid":"away_xga","xgd90_hybrid":"away_xgd90"})
    up = up.merge(hx[["home_team","home_xg","home_xga","home_xgd90"]], on="home_team", how="left")
    up = up.merge(ax[["away_team","away_xg","away_xga","away_xgd90"]], on="away_team", how="left")

# Placeholders for enrichment defaults
for c in ["home_rest_days","away_rest_days","home_travel_km","away_travel_km",
          "home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
          "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]:
    if c not in up.columns: up[c] = None

up.sort_values(["date","home_team"], inplace=True)
up.to_csv(out_path, index=False)
print("[OK] wrote", out_path, len(up))