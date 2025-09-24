#!/usr/bin/env python3
"""
add_competition_flags.py
Tag upcoming fixtures with competition flags:
  - engine_is_uefa: 1 if UCL/UEL/UECL, else 0
  - engine_comp_bucket: {'UCL','UEL','UECL','Domestic','Other'}

Merges into data/UPCOMING_7D_enriched.csv.
"""

import os
import numpy as np
import pandas as pd

DATA = "data"
UP   = os.path.join(DATA, "UPCOMING_7D_enriched.csv")

UCL_TOKENS  = ["champions league","uefa champions","ucl"]
UEL_TOKENS  = ["europa league","uefa europa","uel"]
UECL_TOKENS = ["conference league","uefa europa conference","uecl"]
BIG5_TOKENS = ["premier league","english premier","la liga","bundesliga","serie a","ligue 1"]

def has_tok(s, toks):
    if not isinstance(s, str): return False
    s2 = s.lower()
    return any(tok in s2 for tok in toks)

def comp_bucket(league):
    if has_tok(league, UCL_TOKENS):  return "UCL"
    if has_tok(league, UEL_TOKENS):  return "UEL"
    if has_tok(league, UECL_TOKENS): return "UECL"
    if has_tok(league, BIG5_TOKENS): return "Domestic"
    return "Other"

def main():
    if not os.path.exists(UP):
        print(f"[WARN] {UP} missing; nothing to tag."); return
    up = pd.read_csv(UP)
    if "league" not in up.columns: up["league"] = "GLOBAL"
    up["engine_comp_bucket"] = up["league"].astype(str).map(comp_bucket)
    up["engine_is_uefa"] = up["engine_comp_bucket"].isin(["UCL","UEL","UECL"]).astype(int)
    up.to_csv(UP, index=False)
    print(f"[OK] competition flags merged → {UP}")

if __name__ == "__main__":
    main()