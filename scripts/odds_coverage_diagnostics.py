# scripts/odds_coverage_diagnostics.py
# Checks upcoming 7-day fixtures for missing odds and writes a quick CSV report.

import os, pandas as pd

DATA = "data"
UP7  = os.path.join(DATA,"UPCOMING_7D_enriched.csv")
OUT  = os.path.join(DATA,"ODDS_COVERAGE_REPORT.csv")

def main():
    if not os.path.exists(UP7):
        print("[MISS] UPCOMING_7D_enriched.csv")
        pd.DataFrame(columns=["date","home_team","away_team","has_odds"]).to_csv(OUT, index=False)
        return

    df = pd.read_csv(UP7)
    if df.empty:
        print("[EMPTY] UPCOMING_7D_enriched.csv")
        pd.DataFrame(columns=["date","home_team","away_team","has_odds"]).to_csv(OUT, index=False)
        return

    need = {"home_odds_dec","draw_odds_dec","away_odds_dec"}
    has = need.issubset(df.columns)
    if not has:
        print("[WARN] Odds columns missing in upcoming 7D file.")
        df[["date","home_team","away_team"]].assign(has_odds=False).to_csv(OUT, index=False)
        return

    df["has_odds"] = df["home_odds_dec"].notna() & df["draw_odds_dec"].notna() & df["away_odds_dec"].notna()
    df[["date","home_team","away_team","has_odds"]].to_csv(OUT, index=False)
    pct = 100.0 * df["has_odds"].mean() if len(df) else 0.0
    print(f"[OK] wrote {OUT}   odds_coverage={pct:.1f}%")

if __name__ == "__main__":
    main()