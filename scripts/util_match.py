import pandas as pd
from difflib import SequenceMatcher
from .util_textnorm import normalize_team_name, alias_canonical

def soft_join(left: pd.DataFrame, right: pd.DataFrame, left_key: str, right_key: str, threshold: float=0.92):
    # normalize copies
    l = left.copy()
    r = right.copy()
    l["_norm"] = l[left_key].astype(str).map(alias_canonical)
    r["_norm"] = r[right_key].astype(str).map(alias_canonical)

    # direct join first
    merged = l.merge(r.drop_duplicates("_norm"), on="_norm", how="left", suffixes=("","_r"))

    # soft-match fill for still-missing
    missing = merged[merged.filter(like="_r").columns].isna().all(axis=1)
    if missing.any():
        rlist = r["_norm"].dropna().unique().tolist()
        best = []
        for nm in merged.loc[missing, "_norm"]:
            if not nm:
                best.append(None)
                continue
            scores = [(SequenceMatcher(None, nm, cand).ratio(), cand) for cand in rlist]
            score, cand = max(scores) if scores else (0, None)
            best.append(cand if score >= threshold else None)
        merged.loc[missing, "_norm"] = best
        merged = merged.drop(columns=r.filter(like="_norm").columns.tolist()).merge(
            r.drop_duplicates("_norm"), on="_norm", how="left", suffixes=("","_r")
        )

    # drop helper
    return merged.drop(columns=["_norm"])