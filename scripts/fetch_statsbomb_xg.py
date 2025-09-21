# scripts/fetch_statsbomb_xg.py
# Safe fetcher: aggregate team xG/xGA from StatsBomb Open Data.

import os
import pandas as pd
from statsbombpy import sb

DATA = "data"
OUT  = os.path.join(DATA, "xg_statsbomb.csv")

os.makedirs(DATA, exist_ok=True)

# Keep it simple: pull all available competitions from open data
def main():
    try:
        comps = sb.competitions()
    except Exception as e:
        print("[WARN] StatsBomb comps fetch failed:", e)
        pd.DataFrame(columns=["team","competition","season_name","xg_sb","xga_sb"]).to_csv(OUT, index=False)
        return

    rows = []

    for _, c in comps.iterrows():
        comp_id = int(c["competition_id"])
        season_id = int(c["season_id"])
        comp_name = str(c["competition_name"])
        season_name = str(c["season_name"])

        try:
            matches = sb.matches(competition_id=comp_id, season_id=season_id)
        except Exception as e:
            print(f"[WARN] matches failed for {comp_name} {season_name}:", e)
            continue

        match_ids = matches["match_id"].tolist()
        team_xg = {}

        for mid in match_ids:
            try:
                ev = sb.events(match_id=mid, fmt="df")
            except Exception as e:
                print(f"[WARN] events failed match {mid}:", e)
                continue

            # Defensive guard: make sure xG column exists
            if "shot_statsbomb_xg" not in ev.columns or "team" not in ev.columns:
                continue

            shots = ev[ev["shot_statsbomb_xg"].notna()].copy()
            if shots.empty:
                continue

            # Aggregate for each team in that match
            for team in shots["team"].dropna().unique():
                xg_for = shots.loc[shots["team"] == team, "shot_statsbomb_xg"].sum()
                xg_against = shots.loc[shots["team"] != team, "shot_statsbomb_xg"].sum()
                key = (team, comp_name, season_name)
                if key not in team_xg:
                    team_xg[key] = {"xg": 0.0, "xga": 0.0}
                team_xg[key]["xg"] += float(xg_for)
                team_xg[key]["xga"] += float(xg_against)

        for (team, comp, season), vals in team_xg.items():
            rows.append({
                "team": str(team).strip(),
                "competition": comp,
                "season_name": season,
                "xg_sb": vals["xg"],
                "xga_sb": vals["xga"],
            })

    out = pd.DataFrame(rows)
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()