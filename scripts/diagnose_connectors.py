#!/usr/bin/env python3
"""
diagnose_connectors.py — Must-Have per-league proof (status + count)

- Reads CORE_LEAGUE_IDS (if set) else samples first N from discovery
- For each league: try window call, then fallback next=N
- Prints a table with lid, season, status, count, short preview

Env:
  API_FOOTBALL_KEY, FDORG_TOKEN
  CORE_LEAGUE_IDS (CSV) optional
  LEAGUE_CHECK_SAMPLE (default 9)
  AF_FIXTURES_LOOKAHEAD_DAYS (default 120)
  AF_FALLBACK_NEXT (default 300)
"""

import os, sys, csv, json, requests
from datetime import datetime, date, timedelta

REP="reports"; os.makedirs(REP, exist_ok=True)
OUT=os.path.join(REP,"CONNECTOR_DIAG.md")
AF="https://v3.football.api-sports.io"
FD="https://api.football-data.org/v4"

def today(): return date.today()
def iso(d): return d.strftime("%Y-%m-%d")
def preview(s, n=120): return (s or "").replace("\n"," ")[:n]

def read_discovery(path="data/discovered_leagues.csv"):
    rows=[]
    if os.path.exists(path):
        try:
            with open(path, newline="", encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    lid=(r.get("league_id") or "").strip()
                    sea=(r.get("season") or "").strip()
                    try: sea=int(sea) if sea else None
                    except: sea=None
                    if lid: rows.append({"lid":lid,"season":sea})
        except: pass
    return rows

def write_md(lines):
    with open(OUT,"w",encoding="utf-8") as f: f.write("\n".join(lines)+"\n")
    print(f"diagnose_connectors: wrote {OUT}")

def main():
    lines=["# CONNECTOR DIAG", f"_Generated: {datetime.utcnow().isoformat()}Z_", ""]
    key=os.environ.get("API_FOOTBALL_KEY","").strip()
    token=os.environ.get("FDORG_TOKEN","").strip()
    core=os.environ.get("CORE_LEAGUE_IDS","").strip()
    sample=int(os.environ.get("LEAGUE_CHECK_SAMPLE","9"))
    look=int(os.environ.get("AF_FIXTURES_LOOKAHEAD_DAYS","120"))
    nxt=int(os.environ.get("AF_FALLBACK_NEXT","300"))

    lines+=["## Secret presence","","- API_FOOTBALL_KEY: "+("SET" if key else "NOT SET"),
             "- FDORG_TOKEN: "+("SET" if token else "NOT SET"), ""]

    # build list
    cand=[]
    if core:
        cand=[{"lid":s.strip(), "season":None} for s in core.split(",") if s.strip()]
    else:
        cand=read_discovery()[:sample]

    # attach seasons from discovery when available
    disco=read_discovery()
    season_map={r["lid"]:r["season"] for r in disco}
    for c in cand:
        if c["season"] is None: c["season"]=season_map.get(c["lid"])

    # AF per-league table
    lines+=["## API-Football (per league)","","| lid | season | status | count | preview |","|---:|---:|---:|---:|---|"]
    if not key:
        lines+=["| - | - | - | 0 | (API_FOOTBALL_KEY not set) |"]
    else:
        hdr={"x-apisports-key":key}
        t=today(); end=t+timedelta(days=look)
        for c in cand[:sample]:
            lid=c["lid"]; season=c["season"] or t.year
            try:
                r=requests.get(f"{AF}/fixtures", headers=hdr,
                               params={"league": int(lid), "season": int(season),
                                       "from": iso(t), "to": iso(end)}, timeout=45)
                status=r.status_code; cnt=0
                if status==200:
                    try: cnt=len((r.json() or {}).get("response",[]))
                    except: cnt=0
                # fallback
                if cnt==0:
                    r2=requests.get(f"{AF}/fixtures", headers=hdr,
                                    params={"league": int(lid), "season": int(season),
                                            "next": nxt}, timeout=45)
                    status=r2.status_code
                    if status==200:
                        try: cnt=len((r2.json() or {}).get("response",[]))
                        except: cnt=0
                    pv=preview(r2.text)
                else:
                    pv=preview(r.text)
                lines.append(f"| {lid} | {season} | {status} | {cnt} | `{pv}` |")
            except Exception as e:
                lines.append(f"| {lid} | {season} | ERR | 0 | `{preview(str(e))}` |")
    lines.append("")

    # FD checks
    lines+=["## Football-Data.org","","- /competitions and PL/matches status (token helps quotas)"]
    hdr={"X-Auth-Token":token} if token else {}
    try:
        r=requests.get(f"{FD}/competitions", headers=hdr, timeout=40)
        lines+=[f"- /competitions status: **{r.status_code}**", f"  - preview: `{preview(r.text)}`"]
    except Exception as e:
        lines+=[f"- /competitions error: {e}"]
    try:
        r=requests.get(f"{FD}/competitions/PL/matches", headers=hdr, timeout=40)
        lines+=[f"- /competitions/PL/matches status: **{r.status_code}**", f"  - preview: `{preview(r.text)}`"]
    except Exception as e:
        lines+=[f"- /competitions/PL/matches error: {e}"]

    write_md(lines); return 0

if __name__=="__main__":
    sys.exit(main())