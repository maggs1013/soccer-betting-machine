# scripts/fetch_statsbomb_xg.py
# Pulls shot-level xG from StatsBomb Open Data and aggregates to team totals.
# Writes: data/xg_statsbomb.csv  with columns: team, competition, season_name, xg_sb, xga_sb

import os
import pandas as pd
from statsbombpy import sb

DATA = "data"
OUT  = os.path.join(DATA, "xg_statsbomb.csv")

os.makedirs(DATA, exist_ok=True)

# Which open-data competitions to include.
# StatsBomb Open Data doesn't cover every league/season; these are common, adjust as you like.
INCLUDE_COMPS = {
    # competition_id: list of season_ids OR [] for all seasons in that comp
    49: [],   # FA Women's Super League
    37: [],   # Women's World Cup
    43: [],   # UEFA Women's Euro
    72: [],   # NWSL Challenge Cup
    42: [],   # Men's World Cup 2018
    106: [],  # World Cup 2022 (if available in open data set)
    # Add others from sb.competitions()
}

def main():
    try:
        comps = sb.competitions()
    except Exception as e:
        print("[WARN] StatsBomb comps fetch failed:", e)
        pd.DataFrame(columns=["team","competition","season_name","xg_sb","xga_sb"]).to_csv(OUT, index=False)
        return

    rows = []

    # Walk all requested comps/seasons
    for _, c in comps.iterrows():
        comp_id = int(c["competition_id"])
        season_id = int(c["season_id"])
        if comp_id not in INCLUDE_COMPS:
            continue
        # filter by season subset if specified
        allowed_seasons = INCLUDE_COMPS[comp_id]
        if allowed_seasons and season_id not in allowed_seasons:
            continue

        comp_name = str(c["competition_name"])
        season_name = str(c["season_name"])

        try:
            matches = sb.matches(competition_id=comp_id, season_id=season_id)
        except Exception as e:
            print(f"[WARN] matches failed for {comp_name} {season_name}:", e)
            continue

        match_ids = matches["match_id"].tolist()
        # Aggregate xG for and against at team level
        team_xg = {}

        for mid in match_ids:
            try:
                ev = sb.events(match_id=mid, fmt="df")
            except Exception as e:
                print(f"[WARN] events failed match {mid}:", e)
                continue

            shots = ev[ev["type"] == "Shot"].copy()
            if shots.empty:
                continue

            # StatsBomb xG lives in 'shot_statsbomb_xg'
            if "shot_statsbomb_xg" not in shots.columns:
                continue

            # For and Against by team in that match
            for team in shots["team"].dropna().unique():
                xg_for = shots.loc[shots["team"] == team, "shot_statsbomb_xg"].sum()
                # opponent xg = shots where team != this team (in same match)
                xg_against = shots.loc[shots["team"] != team, "shot_statsbomb_xg"].sum()
                key = (team, comp_name, season_name)
                if key not in team_xg:
                    team_xg[key] = {"xg": 0.0, "xga": 0.0}
                team_xg[key]["xg"] += float(xg_for)
                team_xg[key]["xga"] += float(xg_against)

        for (team, comp_name2, season_name2), vals in team_xg.items():
            rows.append({
                "team": str(team).strip(),
                "competition": comp_name2,
                "season_name": season_name2,
                "xg_sb": vals["xg"],
                "xga_sb": vals["xga"],
            })

    out = pd.DataFrame(rows)
    out.to_csv(OUT, index=False)
    print(f"[OK] wrote {OUT} rows={len(out)}")

if __name__ == "__main__":
    main()