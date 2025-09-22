# scripts/01_enrich_fixtures.py
# One-button enrichment (no CLI args required).
# - Reads /data defaults; if missing/empty, proceeds with safe fallbacks
# - Normalizes team names using team_name_map.csv + teams_alias_overrides.csv
# - Ensures enriched upcoming file has:
#     * home/away hybrid xG (xg_metrics_hybrid.csv)
#     * StatsBomb team features (xA, GK PSxG–Goals, set-piece/open-play share)
#     * SPI (FiveThirtyEight) per team
#     * Rolling form (last5/last10) per team
# - Fills safe defaults for injuries, refs, priors if absent
# - Writes back to data/UPCOMING_7D_enriched.csv

import os
import pandas as pd
import numpy as np

DATA = "data"

# Inputs (defaults)
PATH = {
    "fixtures"     : os.path.join(DATA, "UPCOMING_7D_enriched.csv"),
    "teams"        : os.path.join(DATA, "teams_master.csv"),
    "league_xg"    : os.path.join(DATA, "xg_metrics_hybrid.csv"),
    "sb_totals"    : os.path.join(DATA, "xg_statsbomb.csv"),
    "sb_features"  : os.path.join(DATA, "team_statsbomb_features.csv"),
    "form"         : os.path.join(DATA, "team_form_features.csv"),
    "understat"    : os.path.join(DATA, "xg_understat.csv"),
    "spi"          : os.path.join(DATA, "sd_538_spi.csv"),
    "map_main"     : os.path.join(DATA, "team_name_map.csv"),
    "map_over"     : os.path.join(DATA, "teams_alias_overrides.csv"),
    "out"          : os.path.join(DATA, "UPCOMING_7D_enriched.csv"),
}

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

def load_name_map():
    nm = {}
    for fn in (PATH["map_main"], PATH["map_over"]):
        df = safe_read(fn, ["raw","canonical"])
        if not df.empty and {"raw","canonical"}.issubset(df.columns):
            for r in df.itertuples(index=False):
                raw = str(getattr(r, "raw")).strip()
                can = str(getattr(r, "canonical")).strip()
                if raw and can:
                    nm[raw] = can  # overrides win (second file loaded last)
    return nm

def apply_map(s, nm):
    if s is None: return s
    return s.astype(str).str.strip().map(lambda x: nm.get(x, x))

def ensure_cols(df, cols_defaults):
    for c, v in cols_defaults.items():
        if c not in df.columns:
            df[c] = v
    return df

def clamp(v, lo, hi):
    try: v = float(v)
    except: return (lo + hi)/2.0
    return max(lo, min(hi, v))

def merge_hybrid_xg(up, nm):
    hyb = safe_read(PATH["league_xg"], ["team","xg_hybrid","xga_hybrid","xgd90_hybrid"])
    if hyb.empty: 
        # make sure columns exist
        for c in ["home_xg","away_xg","home_xga","away_xga","home_xgd90","away_xgd90"]:
            if c not in up.columns: up[c] = np.nan
        return up
    hyb["team"] = apply_map(hyb["team"], nm)
    hx = hyb.rename(columns={
        "team":"home_team", "xg_hybrid":"home_xg", "xga_hybrid":"home_xga", "xgd90_hybrid":"home_xgd90"
    })
    ax = hyb.rename(columns={
        "team":"away_team", "xg_hybrid":"away_xg", "xga_hybrid":"away_xga", "xgd90_hybrid":"away_xgd90"
    })
    up = up.merge(hx[["home_team","home_xg","home_xga","home_xgd90"]], on="home_team", how="left")
    up = up.merge(ax[["away_team","away_xg","away_xga","away_xgd90"]], on="away_team", how="left")
    return up

def merge_statsbomb_features(up, nm):
    sbf = safe_read(PATH["sb_features"], ["team","xa_sb","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"])
    if sbf.empty:
        for c in ["home_xa_sb","away_xa_sb","home_psxg_minus_goals_sb","away_psxg_minus_goals_sb",
                  "home_setpiece_xg_sb","away_setpiece_xg_sb","home_openplay_xg_sb","away_openplay_xg_sb"]:
            if c not in up.columns: up[c] = np.nan
        return up
    sbf["team"] = apply_map(sbf["team"], nm)
    h = sbf.rename(columns={
        "team":"home_team",
        "xa_sb":"home_xa_sb",
        "psxg_minus_goals_sb":"home_psxg_minus_goals_sb",
        "setpiece_xg_sb":"home_setpiece_xg_sb",
        "openplay_xg_sb":"home_openplay_xg_sb",
    })
    a = sbf.rename(columns={
        "team":"away_team",
        "xa_sb":"away_xa_sb",
        "psxg_minus_goals_sb":"away_psxg_minus_goals_sb",
        "setpiece_xg_sb":"away_setpiece_xg_sb",
        "openplay_xg_sb":"away_openplay_xg_sb",
    })
    up = up.merge(h[["home_team","home_xa_sb","home_psxg_minus_goals_sb","home_setpiece_xg_sb","home_openplay_xg_sb"]],
                  on="home_team", how="left")
    up = up.merge(a[["away_team","away_xa_sb","away_psxg_minus_goals_sb","away_setpiece_xg_sb","away_openplay_xg_sb"]],
                  on="away_team", how="left")
    return up

def merge_spi(up, nm):
    spi = safe_read(PATH["spi"])
    if spi.empty:
        for c in ["home_spi_off","away_spi_off","home_spi_def","away_spi_def"]:
            if c not in up.columns: up[c] = np.nan
        return up
    # try to find team/off/def columns
    cols = {c.lower(): c for c in spi.columns}
    team_col = cols.get("team") or ("team" if "team" in spi.columns else None)
    if team_col is None:
        if "squad" in spi.columns:
            spi = spi.rename(columns={"squad":"team"})
            team_col = "team"
        else:
            for c in ["team_name","name"]:
                if c in spi.columns: 
                    spi = spi.rename(columns={c:"team"}); team_col = "team"; break
    off_col = cols.get("off") or cols.get("offense")
    def_col = cols.get("def") or cols.get("defense")
    if team_col is None or off_col is None or def_col is None:
        for c in ["home_spi_off","away_spi_off","home_spi_def","away_spi_def"]:
            if c not in up.columns: up[c] = np.nan
        return up

    spi["team"] = apply_map(spi[team_col].astype(str), nm)
    agg = spi.groupby("team", as_index=False)[[off_col, def_col]].mean()
    h = agg.rename(columns={"team":"home_team", off_col:"home_spi_off", def_col:"home_spi_def"})
    a = agg.rename(columns={"team":"away_team", off_col:"away_spi_off", def_col:"away_spi_def"})
    up = up.merge(h, on="home_team", how="left")
    up = up.merge(a, on="away_team", how="left")
    return up

def merge_form(up, nm):
    form = safe_read(PATH["form"], [
        "team","last5_ppg","last5_gdpg","last5_xgpg","last5_xgapg",
        "last10_ppg","last10_gdpg","last10_xgpg","last10_xgapg"
    ])
    if form.empty:
        # fill placeholders
        for base in ["home","away"]:
            for c in ["last5_ppg","last5_gdpg","last5_xgpg","last5_xgapg","last10_ppg","last10_gdpg","last10_xgpg","last10_xgapg"]:
                col = f"{base}_{c}"
                if col not in up.columns: up[col] = np.nan
        return up
    form["team"] = apply_map(form["team"], nm)
    h = form.rename(columns={c: f"home_{c}" for c in form.columns})
    a = form.rename(columns={c: f"away_{c}" for c in form.columns})
    up = up.merge(h.rename(columns={"home_team":"home_team"})[
                      ["home_team","home_last5_ppg","home_last5_gdpg","home_last5_xgpg","home_last5_xgapg",
                       "home_last10_ppg","home_last10_gdpg","home_last10_xgpg","home_last10_xgapg"]],
                  on="home_team", how="left")
    up = up.merge(a.rename(columns={"away_team":"away_team"})[
                      ["away_team","away_last5_ppg","away_last5_gdpg","away_last5_xgpg","away_last5_xgapg",
                       "away_last10_ppg","away_last10_gdpg","away_last10_xgpg","away_last10_xgapg"]],
                  on="away_team", how="left")
    return up

def main():
    os.makedirs(DATA, exist_ok=True)

    # Read fixtures base; if empty, create shell
    up = safe_read(PATH["fixtures"])
    if up.empty:
        up = pd.DataFrame(columns=["date","home_team","away_team"])
    # Normalize types
    if "date" in up.columns:
        up["date"] = pd.to_datetime(up["date"], errors="coerce").dt.tz_localize(None)

    # Load maps and normalize team names
    nm = load_name_map()
    for col in ["home_team","away_team"]:
        if col in up.columns:
            up[col] = apply_map(up[col].astype(str), nm)

    # Ensure essential columns exist
    up = ensure_cols(up, {
        "home_odds_dec": np.nan, "draw_odds_dec": np.nan, "away_odds_dec": np.nan,
        "home_injury_index": 0.30, "away_injury_index": 0.30,
        "home_gk_rating": 0.60, "away_gk_rating": 0.60,
        "home_setpiece_rating": 0.60, "away_setpiece_rating": 0.60,
        "ref_pen_rate": 0.30, "crowd_index": 0.70,
        "home_travel_km": 0.0, "away_travel_km": 200.0
    })

    # Merge hybrid xG and SB features, SPI, form
    up = merge_hybrid_xg(up, nm)
    up = merge_statsbomb_features(up, nm)
    up = merge_spi(up, nm)
    up = merge_form(up, nm)

    # Teams master (GK/set-piece/crowd) override if file has entries
    tm = safe_read(PATH["teams"], ["team","gk_rating","setpiece_rating","crowd_index"])
    if not tm.empty:
        tm["team"] = apply_map(tm["team"], nm)
        h = tm.rename(columns={"team":"home_team","gk_rating":"home_gk_rating","setpiece_rating":"home_setpiece_rating","crowd_index":"home_crowd_index"})
        a = tm.rename(columns={"team":"away_team","gk_rating":"away_gk_rating","setpiece_rating":"away_setpiece_rating","crowd_index":"away_crowd_index"})
        up = up.merge(h[["home_team","home_gk_rating","home_setpiece_rating","home_crowd_index"]], on="home_team", how="left")
        up = up.merge(a[["away_team","away_gk_rating","away_setpiece_rating","away_crowd_index"]], on="away_team", how="left")
        # prefer teams_master values where present
        for base in ["home","away"]:
            for c in ["gk_rating","setpiece_rating"]:
                up[f"{base}_{c}"] = np.where(up[f"{base}_{c}"].notna(), up[f"{base}_{c}"], up.get(f"{base}_{c}", np.nan))
        # crowd_index: prefer *_crowd_index average if both given
        if "home_crowd_index" in up.columns:
            up["crowd_index"] = np.where(up["home_crowd_index"].notna(), up["home_crowd_index"], up["crowd_index"])
            up.drop(columns=["home_crowd_index"], inplace=True, errors="ignore")
        if "away_crowd_index" in up.columns:
            # away crowd is the opponent venue; keep global crowd index as is for now
            up.drop(columns=["away_crowd_index"], inplace=True, errors="ignore")

    # Final tidy
    if "date" in up.columns:
        up = up.sort_values(["date","home_team","away_team"], na_position="last")

    up.to_csv(PATH["out"], index=False)
    print(f"[OK] 01_enrich_fixtures: wrote {PATH['out']} rows={len(up)}")

if __name__ == "__main__":
    main()