# scripts/generate_matchday_templates.py
# Build injuries.csv and lineups.csv templates for the next 7 days of fixtures
# filtered to the leagues in data/leagues_allowlist.csv. This ensures you always
# see the exact games that need entries.

import os, pandas as pd

DATA = "data"
FX  = os.path.join(DATA, "sd_fd_fixtures.csv")           # from soccerdata
ALLOW = os.path.join(DATA, "leagues_allowlist.csv")
INJ = os.path.join(DATA, "injuries.csv")
LUP = os.path.join(DATA, "lineups.csv")

os.makedirs(DATA, exist_ok=True)

# Load fixtures window (already next 7 days in fetch_soccerdata.py)
fx = pd.read_csv(FX) if os.path.exists(FX) else pd.DataFrame()
allow = pd.read_csv(ALLOW) if os.path.exists(ALLOW) else pd.DataFrame(columns=["league_canonical"])

# Optional league normalization if 'league' column present in fixtures
# If you want to normalize league names first, add a league map here.
def normalize_league(s):
    return str(s).strip()

if not fx.empty and "league" in fx.columns:
    fx["league"] = fx["league"].apply(normalize_league)

# Filter to allowed leagues if possible
if not fx.empty and "league" in fx.columns and not allow.empty:
    fx = fx[fx["league"].isin(allow["league_canonical"])].copy()

# Keep only needed columns
keep_cols = [c for c in ["date","league","home_team","away_team"] if c in fx.columns]
fx = fx[keep_cols].dropna(subset=[c for c in keep_cols if c != "league"]).copy()

# Build injuries template rows (both teams per fixture)
inj_rows = []
for r in fx.itertuples(index=False):
    date = getattr(r, "date", None)
    home = getattr(r, "home_team", None)
    away = getattr(r, "away_team", None)
    if pd.isna(date) or pd.isna(home) or pd.isna(away): continue
    inj_rows.append({"date": date, "team": home, "injury_index": 0.30})
    inj_rows.append({"date": date, "team": away, "injury_index": 0.30})

# Build lineups template (flags 0)
lu_rows = []
for r in fx.itertuples(index=False):
    date = getattr(r, "date", None)
    home = getattr(r, "home_team", None)
    away = getattr(r, "away_team", None)
    if pd.isna(date) or pd.isna(home) or pd.isna(away): continue
    for t in [home, away]:
        lu_rows.append({"date": date, "team": t, "key_att_out": 0, "key_def_out": 0, "keeper_changed": 0})

# Merge with existing injuries/lineups (do not duplicate)
def merge_templates(path, cols, new_rows):
    prev = pd.read_csv(path) if os.path.exists(path) else pd.DataFrame(columns=cols)
    new = pd.DataFrame(new_rows, columns=cols)
    all_rows = pd.concat([prev, new], ignore_index=True).drop_duplicates(subset=["date","team"], keep="first")
    all_rows.to_csv(path, index=False)
    return len(all_rows), len(new_rows)

inj_count, inj_new = merge_templates(INJ, ["date","team","injury_index"], inj_rows)
lu_count, lu_new   = merge_templates(LUP, ["date","team","key_att_out","key_def_out","keeper_changed"], lu_rows)

print(f"[OK] injuries.csv rows: total={inj_count}, added={inj_new}")
print(f"[OK] lineups.csv rows:  total={lu_count}, added={lu_new}")