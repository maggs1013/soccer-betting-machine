import re
import unicodedata

def normalize_team_name(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = s.lower().strip()
    s = re.sub(r"fc|afc|cf|sc|ss|calcio|club|the|\\.", " ", s)
    s = re.sub(r"\\s+"," ", s).strip()
    return s

# common manual alias corrections, extend as needed
ALIASES = {
    "leicester": "leicester city",
    "birmingham": "birmingham city",
    "west brom": "west bromwich albion",
    "wba": "west bromwich albion",
    "preston": "preston north end",
    "qpr": "queens park rangers",
    "ipswich": "ipswich town",
    "plymouth": "plymouth argyle",
    "norwich": "norwich city",
    "stoke": "stoke city",
    "hull": "hull city",
    "swansea": "swansea city",
    "bristol": "bristol city",
}

def alias_canonical(s: str) -> str:
    n = normalize_team_name(s)
    return ALIASES.get(n, n)