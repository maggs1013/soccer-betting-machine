import os, pandas as pd
IN_H="data/raw_football_data.csv"; IN_U="data/raw_theodds_fixtures.csv"
OUT_H="data/HIST_matches.csv"; OUT_U="data/UPCOMING_fixtures.csv"
def reorder_hist(df):
    cols=["date","home_team","away_team","home_goals","away_goals","home_odds_dec","draw_odds_dec","away_odds_dec",
          "home_rest_days","away_rest_days","home_travel_km","away_travel_km","home_injury_index","away_injury_index",
          "home_gk_rating","away_gk_rating","home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]
    return df[cols]
def reorder_up(df):
    cols=["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec",
          "home_rest_days","away_rest_days","home_travel_km","away_travel_km",
          "home_injury_index","away_injury_index","home_gk_rating","away_gk_rating",
          "home_setpiece_rating","away_setpiece_rating","ref_pen_rate","crowd_index"]
    return df[cols]
h=pd.read_csv(IN_H) if os.path.exists(IN_H) else pd.DataFrame(columns=reorder_hist(pd.DataFrame(columns=[])).columns)
u=pd.read_csv(IN_U) if os.path.exists(IN_U) else pd.DataFrame(columns=reorder_up(pd.DataFrame(columns=[])).columns)
h.to_csv(OUT_H,index=False); u.to_csv(OUT_U,index=False)
print("[OK] wrote", OUT_H, len(h), "|", OUT_U, len(u))
