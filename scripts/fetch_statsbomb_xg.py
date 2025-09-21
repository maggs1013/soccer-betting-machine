# scripts/fetch_statsbomb_xg.py
# Robust StatsBomb Open Data fetch:
# - team xG (for) and xGA (against)
# - team xA (sum xG of shots assisted by that team's passes)
# - GK shot-stopping (PSxG - Goals conceded)
# - set-piece vs open-play xG split
#
# Writes:
#   data/xg_statsbomb.csv                (team xG/xGA totals)
#   data/team_statsbomb_features.csv     (xA, psxg_minus_goals, setpiece_xg, openplay_xg)

import os
import pandas as pd
from statsbombpy import sb

DATA = "data"
OUT_XG   = os.path.join(DATA, "xg_statsbomb.csv")
OUT_FEAT = os.path.join(DATA, "team_statsbomb_features.csv")

os.makedirs(DATA, exist_ok=True)

# ---------- helpers ----------
def to_df(obj):
    """Force return as DataFrame; otherwise empty DF."""
    if isinstance(obj, pd.DataFrame):
        return obj
    try:
        return pd.DataFrame(obj)
    except Exception:
        return pd.DataFrame()

def pick_col(df, candidates):
    """Return first existing column from candidates, else None."""
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def safe_events(match_id):
    try:
        ev = sb.events(match_id=match_id)
        ev = to_df(ev)
        # flatten any possible multiindex columns to strings
        if isinstance(ev.columns, pd.MultiIndex):
            ev.columns = [".".join([str(x) for x in tup if x not in (None, "")]) for tup in ev.columns]
        return ev
    except Exception as e:
        print(f"[WARN] events failed for match {match_id}: {e}")
        return pd.DataFrame()

# ---------- main ----------
def main():
    try:
        comps = sb.competitions()
    except Exception as e:
        print("[WARN] StatsBomb comps fetch failed:", e)
        pd.DataFrame(columns=["team","competition","season_name","xg_sb","xga_sb"]).to_csv(OUT_XG, index=False)
        pd.DataFrame(columns=["team","competition","season_name","xa_sb","psxg_minus_goals_sb","setpiece_xg_sb","openplay_xg_sb"]).to_csv(OUT_FEAT, index=False)
        return

    rows_xg, rows_feat = [], []

    for _, c in comps.iterrows():
        comp_id   = int(c["competition_id"])
        season_id = int(c["season_id"])
        comp_name = str(c["competition_name"])
        season_nm = str(c["season_name"])

        try:
            matches = sb.matches(competition_id=comp_id, season_id=season_id)
        except Exception as e:
            print(f"[WARN] matches failed for {comp_name} {season_nm}: {e}")
            continue

        team_xg   = {}  # (team, comp, season) -> {"xg":float, "xga":float}
        team_feat = {}  # -> {"xa":float, "psxg":float, "goals":float, "sp_xg":float, "op_xg":float}

        for mid in matches.get("match_id", []):
            ev = safe_events(mid)
            if ev.empty:
                continue

            # Resolve column names (support multiple variants)
            col_team   = pick_col(ev, ["team", "possession_team", "possession_team.name"])
            col_type   = pick_col(ev, ["type", "type_name", "type.name"])
            col_xg     = pick_col(ev, ["shot_statsbomb_xg", "shot.statsbomb_xg"])
            col_psxg   = pick_col(ev, ["shot_post_shot_xg", "shot.post_shot_xg"])
            col_out    = pick_col(ev, ["shot_outcome", "shot.outcome_name", "shot.outcome.name", "shot_outcome_name"])
            col_ppat   = pick_col(ev, ["play_pattern", "play_pattern_name", "play_pattern.name"])
            col_id     = pick_col(ev, ["id"])
            col_assist = pick_col(ev, ["pass_assisted_shot_id", "pass.assisted_shot_id"])

            # If we lack team or any way to find shots, skip
            if not col_team:
                continue

            # Identify SHOTS: prefer "type == Shot" if type column exists; else rows with non-null xG
            if col_type and "Shot" in ev[col_type].astype(str).unique():
                shots = ev[ev[col_type].astype(str) == "Shot"].copy()
            elif col_xg and ev[col_xg].notna().any():
                shots = ev[ev[col_xg].notna()].copy()
            else:
                shots = pd.DataFrame()

            # xG for/against, set-pieces vs open play
            if not shots.empty:
                # ensure xG column present
                if not col_xg:
                    # nothing to sum without xG
                    pass
                else:
                    for tm in shots[col_team].dropna().astype(str).unique():
                        s_tm  = shots.loc[shots[col_team].astype(str) == tm]
                        s_opp = shots.loc[shots[col_team].astype(str) != tm]

                        key = (tm, comp_name, season_nm)
                        team_xg.setdefault(key, {"xg":0.0, "xga":0.0})
                        team_xg[key]["xg"]  += float(s_tm[col_xg].fillna(0).sum())
                        team_xg[key]["xga"] += float(s_opp[col_xg].fillna(0).sum())

                        team_feat.setdefault(key, {"xa":0.0,"psxg":0.0,"goals":0.0,"sp_xg":0.0,"op_xg":0.0})
                        if col_ppat:
                            sp_mask = s_tm[col_ppat].astype(str).isin(["From Corner","From Free Kick","From Throw In"])
                            team_feat[key]["sp_xg"] += float(s_tm.loc[sp_mask,  col_xg].fillna(0).sum())
                            team_feat[key]["op_xg"] += float(s_tm.loc[~sp_mask, col_xg].fillna(0).sum())

                        # GK PSxG vs Goals conceded (against)
                        if col_psxg and col_out:
                            psxg_against = float(s_opp[col_psxg].fillna(0).sum())
                            goals_conceded = float((s_opp[col_out].astype(str) == "Goal").sum())
                            team_feat[key]["psxg"]  += psxg_against
                            team_feat[key]["goals"] += goals_conceded

            # xA: passes that assisted a shot (join assisted_shot_id with shots id)
            if col_assist and col_id:
                passes = ev[ev[col_assist].notna()].copy()
                if not passes.empty and col_xg:
                    # build lookup of shot id -> xG
                    shot_lu = ev[[col_id, col_xg]].dropna(subset=[col_xg]).rename(columns={col_id: "assisted_shot_id"})
                    passes  = passes.merge(shot_lu, left_on=col_assist, right_on="assisted_shot_id", how="left")
                    for tm in passes[col_team].dropna().astype(str).unique():
                        xA = float(passes.loc[passes[col_team].astype(str) == tm, col_xg].fillna(0).sum())
                        key = (tm, comp_name, season_nm)
                        team_feat.setdefault(key, {"xa":0.0,"psxg":0.0,"goals":0.0,"sp_xg":0.0,"op_xg":0.0})
                        team_feat[key]["xa"] += xA

        # Write rows for this competition-season
        for (tm, comp, season), vals in team_xg.items():
            rows_xg.append({"team": tm, "competition": comp, "season_name": season,
                            "xg_sb": vals["xg"], "xga_sb": vals["xga"]})
        for (tm, comp, season), vals in team_feat.items():
            rows_feat.append({"team": tm, "competition": comp, "season_name": season,
                              "xa_sb": vals["xa"],
                              "psxg_minus_goals_sb": vals["psxg"] - vals["goals"],
                              "setpiece_xg_sb": vals["sp_xg"],
                              "openplay_xg_sb": vals["op_xg"]})

    pd.DataFrame(rows_xg).to_csv(OUT_XG, index=False)
    pd.DataFrame(rows_feat).to_csv(OUT_FEAT, index=False)
    print(f"[OK] wrote {OUT_XG} ({len(rows_xg)}) and {OUT_FEAT} ({len(rows_feat)})")

if __name__ == "__main__":
    main()