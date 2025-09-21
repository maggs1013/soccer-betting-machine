# scripts/build_upcoming_window.py
# Build "next 7 days" enriched table using odds from Odds API only; fallback fixtures to UPCOMING_fixtures.csv if soccerdata has none.

import os
import pandas as pd

DATA = "data"
fixtures_sd_path = os.path.join(DATA, "sd_fd_fixtures.csv")
fixtures_upc_path= os.path.join(DATA, "UPCOMING_fixtures.csv")
hyb_xg_path      = os.path.join(DATA, "xg_metrics_hybrid.csv")
allow_path       = os.path.join(DATA, "leagues_allowlist.csv")
league_map_p     = os.path.join(DATA, "league_name_map.csv")

out_path = os.path.join(DATA, "UPCOMING_7D_enriched.csv")

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception as e:
        print(f"[WARN] Failed reading {p}: {e}"); return pd.DataFrame()

fx_sd  = safe_read(fixtures_sd_path)
fx_upc = safe_read(fixtures_upc_path)     # this is produced by the odds step / or legacy
xg     = safe_read(hyb_xg_path)
allow  = safe_read(allow_path)
lmap   = safe_read(league_map_p)

for df in (fx_sd, fx_upc):
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)

# choose fixtures
fx = fx_sd.copy()
if fx.empty and not fx_upc.empty:
    fx = fx_upc.copy()
    if "date" in fx.columns:
        today = pd.Timestamp.now().normalize()
        fx = fx[(fx["date"]>=today) & (fx["date"]<today+pd.Timedelta(days=7))].copy()

# league normalization/filter
name_map = {}
if not lmap.empty and {"raw","canonical"}.issubset(lmap.columns):
    name_map = {str(r.raw).strip(): str(r.canonical).strip() for _, r in lmap.iterrows()}
def norm_league(s):
    if pd.isna(s): return s
    s = str(s).strip()
    return name_map.get(s, s)

if "league" in fx.columns:
    fx["league"] = fx["league"].apply(norm_league)
if not allow.empty and "league_canonical" in allow.columns and "league" in fx.columns:
    fx = fx[fx["league"].isin(allow["league_canonical"])].copy()

# ensure essential cols
for c in ["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec"]:
    if c not in fx.columns: fx[c] = None

# merge xG hybrid
if not xg.empty and "team" in xg.columns:
    hx = xg.rename(columns={"team":"home_team","xg_hybrid":"home_xg","xga_hybrid":"home_xga","xgd90_hybrid":"home_xgd90"})
    ax = xg.rename(columns={"team":"away_team","xg_hybrid":"away_xg","xga_hybrid":"away_xga","xgd90_hybrid":"away_xgd90"})
    fx = fx.merge(hx[["home_team","home_xg","home_xga","home_xgd90"]], on="home_team", how="left")
    fx = fx.merge(ax[["away_team","away_xg","away_xga","away_xgd90"]], on="away_team", how="left")

# placeholders for enrichment defaults if missing
for c in ["home_rest_days","away_rest_days","home_travel_km","away_travel_km",
          "home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
          "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]:
    if c not in fx.columns: fx[c] = None

if "date" in fx.columns:
    fx.sort_values(["date","home_team","away_team"], inplace=True, na_position="last")

fx.to_csv(out_path, index=False)
print(f"[OK] wrote {out_path} rows={len(fx)}")