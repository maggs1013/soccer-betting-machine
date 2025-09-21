# scripts/fetch_statsbomb_xg.py
# Safer StatsBomb fetcher: team-level xG/xGA, xA, PSxG-G, set-piece vs open-play xG.

import os
import pandas as pd
from statsbombpy import sb

DATA = "data"
OUT_XG = os.path.join(DATA, "xg_statsbomb.csv")
OUT_FEATS = os.path.join(DATA, "team_statsbomb_features.csv")

os.makedirs(DATA, exist_ok=True)

def to_df(obj):
    """Ensure events return as DataFrame."""
    if isinstance(obj, pd.DataFrame):
        return obj
    try:
        return pd.DataFrame(obj)
    except Exception:
        return pd.DataFrame()

def safe_events(match_id):
    try:
        ev = sb.events(match_id=match_id)
        return to_df(ev)
    except Exception as e:
        print(f"[WARN] events failed for match {match_id}: {e}")
        return pd.DataFrame()

def main():
    try:
        comps = sb.competitions()
    except Exception as e:
        print("[WARN] StatsBomb comps fetch failed:", e)
        pd.DataFrame(columns=["team","competition","season_name","xg_sb","xga_sb"]).to_csv(OUT_XG, index=False)
        pd.DataFrame(columns=["team","competition","season_name","xa_sb","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"]).to_csv(OUT_FEATS, index=False)
        return

    rows_xg, rows_feat = [], []

    for _, c in comps.iterrows():
        comp_id = int(c["competition_id"])
        season_id = int(c["season_id"])
        comp_name, season_name = str(c["competition_name"]), str(c["season_name"])

        try:
            matches = sb.matches(competition_id=comp_id, season_id=season_id)
        except Exception as e:
            print(f"[WARN] matches failed for {comp_name} {season_name}: {e}")
            continue

        team_xg, team_feats = {}, {}
        for mid in matches["match_id"].tolist():
            ev = safe_events(mid)
            if ev.empty: 
                continue

            cols = set(ev.columns)
            shots = ev[ev["shot_statsbomb_xg"].notna()] if "shot_statsbomb_xg" in cols else pd.DataFrame()

            # xG for/against
            if not shots.empty and "team" in shots.columns:
                for tm in shots["team"].dropna().unique():
                    s_team = shots.loc[shots["team"] == tm]
                    s_opp  = shots.loc[shots["team"] != tm]

                    key = (tm, comp_name, season_name)
                    team_xg.setdefault(key, {"xg":0.0,"xga":0.0})
                    team_xg[key]["xg"]  += float(s_team["shot_statsbomb_xg"].sum())
                    team_xg[key]["xga"] += float(s_opp["shot_statsbomb_xg"].sum())

                    team_feats.setdefault(key, {"xa":0.0,"psxg":0.0,"goals_conceded":0.0,"sp_xg":0.0,"op_xg":0.0})
                    if "play_pattern" in s_team.columns:
                        sp_mask = s_team["play_pattern"].isin(["From Corner","From Free Kick","From Throw In"])
                        team_feats[key]["sp_xg"] += float(s_team.loc[sp_mask,"shot_statsbomb_xg"].sum())
                        team_feats[key]["op_xg"] += float(s_team.loc[~sp_mask,"shot_statsbomb_xg"].sum())

                    # GK PSxG-G
                    if "shot_post_shot_xg" in s_opp.columns and "shot_outcome" in s_opp.columns:
                        psxg = float(s_opp["shot_post_shot_xg"].fillna(0).sum())
                        goals = float((s_opp["shot_outcome"] == "Goal").sum())
                        team_feats[key]["psxg"] += psxg
                        team_feats[key]["goals_conceded"] += goals

            # xA from passes
            if "pass_assisted_shot_id" in ev.columns and "team" in ev.columns:
                passes = ev[ev["pass_assisted_shot_id"].notna()].copy()
                if not passes.empty and "id" in ev.columns and "shot_statsbomb_xg" in ev.columns:
                    shots_small = ev[["id","shot_statsbomb_xg"]].rename(columns={"id":"assisted_shot_id"})
                    passes = passes.merge(shots_small, left_on="pass_assisted_shot_id", right_on="assisted_shot_id", how="left")
                    for tm in passes["team"].dropna().unique():
                        xA = float(passes.loc[passes["team"]==tm,"shot_statsbomb_xg"].fillna(0).sum())
                        key = (tm, comp_name, season_name)
                        team_feats.setdefault(key, {"xa":0.0,"psxg":0.0,"goals_conceded":0.0,"sp_xg":0.0,"op_xg":0.0})
                        team_feats[key]["xa"] += xA

        # write rows
        for (tm, comp, season), vals in team_xg.items():
            rows_xg.append({"team":tm,"competition":comp,"season_name":season,"xg_sb":vals["xg"],"xga_sb":vals["xga"]})
        for (tm, comp, season), vals in team_feats.items():
            rows_feat.append({
                "team":tm,"competition":comp,"season_name":season,
                "xa_sb":vals["xa"],
                "psxg_minus_goals_sb":vals["psxg"]-vals["goals_conceded"],
                "setpiece_xg_sb":vals["sp_xg"],
                "openplay_xg_sb":vals["op_xg"]
            })

    pd.DataFrame(rows_xg).to_csv(OUT_XG,index=False)
    pd.DataFrame(rows_feat).to_csv(OUT_FEATS,index=False)
    print(f"[OK] wrote {OUT_XG} ({len(rows_xg)}) and {OUT_FEATS} ({len(rows_feat)})")

if __name__ == "__main__":
    main()