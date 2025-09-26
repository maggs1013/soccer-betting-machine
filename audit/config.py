EXPECTED_SCHEMAS = {
    "UPCOMING_fixtures": ["date","home_team","away_team","home_odds_dec","draw_odds_dec","away_odds_dec","league"],
    "SPI": ["team","spi_off","spi_def","spi","league","date"],
    "FBREF_TEAM_STATS": ["team","league","season","shots","shots_against","xg","xga","possession","date"],
    "UNDERSTAT_XG": ["match_id","date","home_team","away_team","xg_home","xg_away","league"],
    "STATSBOMB_XG": ["match_id","date","home_team","away_team","xg_home","xg_away","league"],
    "FD_HIST": ["date","league","home_team","away_team","fthg","ftag","ftr","odd_h","odd_d","odd_a"]
}

# What we currently use (your pipeline core). Extend to compare with provider capabilities.
FIELDS_IN_USE = {
    "odds": ["home_odds_dec","draw_odds_dec","away_odds_dec"],
    "spi": ["spi_off","spi_def"],
    "fbref": ["shots","shots_against","xg","xga","possession"],
    "xg": ["xg_home","xg_away"]
}

# What the providers can expose (a subset; the probes will enrich this at runtime)
PROVIDER_CAPS_BASE = {
    "odds": ["h2h_odds","ou_lines","btts","open_close","bookmaker_count"],
    "spi":  ["team","league","spi","spi_off","spi_def","rank","conf_int_low","conf_int_high","date"],
    "fbref":["shots","shots_against","xg","xga","possession","pass_pct","pressures","set_pieces","cards","gk_psxg","gk_psxg_prevented"],
    "understat":["xg_shot_level","xg_assists","deep_completions","key_passes","set_piece_xg"],
    "statsbomb":["shot_events","shot_freeze_frames","keeper_actions","pressures","pass_networks"],
    "footballdata":["cards","corners","ht_scores","referee","odds_many_books"]
}