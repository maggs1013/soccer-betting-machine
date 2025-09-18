import os, time, json, requests

API_KEY_FBR = os.environ.get("FBR_API_KEY", "").strip()
ODDS_KEY    = os.environ.get("THE_ODDS_API_KEY", "").strip()

report = {"fbr": {}, "odds": {}}

def probe(url, params=None, headers=None):
    t0 = time.time()
    try:
        r = requests.get(url, params=params or {}, headers=headers or {}, timeout=15)
        ms = int((time.time()-t0)*1000)
        ok = r.status_code == 200
        data = (r.json() if ok else {"status": r.status_code, "text": r.text[:200]})
        return {"ok": ok, "ms": ms, "sample": data}
    except Exception as e:
        ms = int((time.time()-t0)*1000); return {"ok": False, "ms": ms, "error": str(e)}

# FBR
if API_KEY_FBR:
    hdr = {"X-API-Key": API_KEY_FBR}
    report["fbr"]["leagues"] = probe("https://fbrapi.com/leagues", headers=hdr)
    report["fbr"]["league_seasons_epl"] = probe("https://fbrapi.com/league-seasons", params={"league_id":9}, headers=hdr)
    report["fbr"]["standings_epl"] = probe("https://fbrapi.com/league-standings", params={"league_id":9}, headers=hdr)
else:
    report["fbr"]["note"] = "No FBR_API_KEY set"

# Odds API
if ODDS_KEY:
    report["odds"]["sports"] = probe("https://api.the-odds-api.com/v4/sports/", params={"apiKey":ODDS_KEY})
else:
    report["odds"]["note"] = "No THE_ODDS_API_KEY set"

os.makedirs("data", exist_ok=True)
with open("data/api_probe_report.json","w") as f:
    json.dump(report, f, indent=2)
print("[OK] wrote data/api_probe_report.json")
