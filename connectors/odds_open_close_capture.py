#!/usr/bin/env python3
"""
odds_open_close_capture.py — persist opening/closing odds & compute line-moves

Inputs:
  data/UPCOMING_fixtures.csv   (current snapshot from odds fetcher)
  data/ODDS_SNAPSHOTS.csv      (stateful cache across runs; created if missing)

Behavior:
- For each fixture_id:
  * If opening fields not seen before, save current snapshot as opening.
  * Always set closing fields to current snapshot.
- Writes back:
  * data/ODDS_SNAPSHOTS.csv  (state)
  * data/UPCOMING_fixtures.csv with columns appended/updated:
      open_home_odds, open_draw_odds, open_away_odds,
      close_home_odds, close_draw_odds, close_away_odds,
      open_ou_total, close_ou_total,
      open_spread_home_line, close_spread_home_line,
      open_spread_away_line, close_spread_away_line,
      h2h_home_move, h2h_draw_move, h2h_away_move,
      ou_move, spread_home_line_move, spread_away_line_move
Safe: if any numeric field missing, computes what is available and leaves the rest NaN.
"""

import os, pandas as pd, numpy as np

DATA="data"
FIX=os.path.join(DATA,"UPCOMING_fixtures.csv")
STATE=os.path.join(DATA,"ODDS_SNAPSHOTS.csv")

OPEN_COLS = [
  "open_home_odds","open_draw_odds","open_away_odds",
  "open_ou_total","open_spread_home_line","open_spread_away_line"
]
CLOSE_COLS = [
  "close_home_odds","close_draw_odds","close_away_odds",
  "close_ou_total","close_spread_home_line","close_spread_away_line"
]

def safe_read(p):
    if not os.path.exists(p): return pd.DataFrame()
    try: return pd.read_csv(p)
    except Exception: return pd.DataFrame()

def ensure_fixture_id(df):
    if "fixture_id" in df.columns: return df
    if {"date","home_team","away_team"}.issubset(df.columns):
        def mk_id(r):
            d=str(r.get("date","NA")).replace("-","").replace("T","_").replace(":","")
            h=str(r.get("home_team","NA")).strip().lower().replace(" ","_")
            a=str(r.get("away_team","NA")).strip().lower().replace(" ","_")
            return f"{d}__{h}__vs__{a}"
        df=df.copy(); df["fixture_id"]=df.apply(mk_id, axis=1)
    return df

def num(s):
    return pd.to_numeric(s, errors="coerce")

def main():
    fx = ensure_fixture_id(safe_read(FIX))
    if fx.empty or "fixture_id" not in fx.columns:
        print("odds_open_close_capture: fixtures empty or fixture_id missing; nothing to do.")
        return

    st = safe_read(STATE)

    # Normalized current fields
    fx["home_odds_dec"]  = num(fx.get("home_odds_dec"))
    fx["draw_odds_dec"]  = num(fx.get("draw_odds_dec"))
    fx["away_odds_dec"]  = num(fx.get("away_odds_dec"))
    fx["ou_main_total"]  = num(fx.get("ou_main_total"))
    fx["spread_home_line"]=num(fx.get("spread_home_line"))
    fx["spread_away_line"]=num(fx.get("spread_away_line"))

    if st.empty:
        # initialize state with opening = current
        st = pd.DataFrame({
            "fixture_id": fx["fixture_id"],
            "open_home_odds": fx["home_odds_dec"],
            "open_draw_odds": fx["draw_odds_dec"],
            "open_away_odds": fx["away_odds_dec"],
            "open_ou_total": fx["ou_main_total"],
            "open_spread_home_line": fx["spread_home_line"],
            "open_spread_away_line": fx["spread_away_line"]
        })
    else:
        # Merge: keep earliest seen opening; add new fixtures
        st = st.merge(fx[["fixture_id"]], on="fixture_id", how="outer")
        for col, src in [
            ("open_home_odds","home_odds_dec"),
            ("open_draw_odds","draw_odds_dec"),
            ("open_away_odds","away_odds_dec"),
            ("open_ou_total","ou_main_total"),
            ("open_spread_home_line","spread_home_line"),
            ("open_spread_away_line","spread_away_line")
        ]:
            if col not in st.columns: st[col]=np.nan
            # Fill openings only if NaN (persist earliest)
            st[col] = st[col].combine_first(
                fx.set_index("fixture_id")[src] if src in fx.columns else pd.Series(dtype=float)
            )

    # Build closing from current snapshot
    clos = fx[["fixture_id","home_odds_dec","draw_odds_dec","away_odds_dec",
               "ou_main_total","spread_home_line","spread_away_line"]].copy()
    clos.rename(columns={
        "home_odds_dec":"close_home_odds",
        "draw_odds_dec":"close_draw_odds",
        "away_odds_dec":"close_away_odds",
        "ou_main_total":"close_ou_total",
        "spread_home_line":"close_spread_home_line",
        "spread_away_line":"close_spread_away_line"
    }, inplace=True)

    # Attach closing to fixtures for output
    for col in CLOSE_COLS:
        fx[col] = fx["fixture_id"].map(clos.set_index("fixture_id")[col] if col in clos.columns else pd.Series(dtype=float))

    # Attach openings from state to fixtures
    st_idx = st.set_index("fixture_id")
    for col in OPEN_COLS:
        fx[col] = fx[col] if col in fx.columns else np.nan
        fx[col] = fx["fixture_id"].map(st_idx[col] if col in st_idx.columns else pd.Series(dtype=float))

    # Compute moves
    fx["h2h_home_move"] = num(fx["close_home_odds"]) - num(fx["open_home_odds"])
    fx["h2h_draw_move"] = num(fx["close_draw_odds"]) - num(fx["open_draw_odds"])
    fx["h2h_away_move"] = num(fx["close_away_odds"]) - num(fx["open_away_odds"])
    fx["ou_move"]       = num(fx["close_ou_total"]) - num(fx["open_ou_total"])
    fx["spread_home_line_move"] = num(fx["close_spread_home_line"]) - num(fx["open_spread_home_line"])
    fx["spread_away_line_move"] = num(fx["close_spread_away_line"]) - num(fx["open_spread_away_line"])

    # Persist state & fixtures
    st.to_csv(STATE, index=False)
    fx.to_csv(FIX, index=False)
    print(f"odds_open_close_capture: updated openings/closings for {len(fx)} fixtures; wrote {STATE} & {FIX}")

if __name__ == "__main__":
    main()