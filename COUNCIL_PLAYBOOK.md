# Council Playbook — Using Artifacts (Guidance, Not Binding)

> This playbook is a **guide**, not a rulebook. Use judgment. When in doubt, skepticism wins.

## Start here every morning
1. Open `runs/<DATE>/_INDEX.json` (triage: ECE, coverage, veto count, edges).
2. Skim `AUTO_BRIEFING.md` (auto; not binding).
3. Check `FLAGS.csv` (per-fixture blockers: caps, consistency, feasibility, veto presence).

## If ECE/coverage are red
- ECE > 0.05 in any league → observe-only that league.
- Coverage poor → observe-only that league/day.

## Build the Prelim Slate
- Start from `ACTIONABILITY_REPORT.csv` (stake > 0).
- Filter by `feasible == 1` in `EXECUTION_FEASIBILITY.csv`.
- Remove fixtures with consistency flags in `CONSISTENCY_CHECKS.csv`.
- Remove fixtures covered by veto slices in `ANTI_MODEL_VETOES.csv` (review `VETO_HISTORY.csv` for context).

## Skeptic pass (Stage 6)
- Cross-check with `LINE_MOVE_LOG.csv`. Market moving against us? Re-evaluate/skip.
- Consult `MODEL_ACCURACY_BY_ODDS.csv` and `MODEL_ACCURACY_BY_LEAGUE_TIER.csv`. Are we weak in this slice? Skip or haircut.

## Synthesis (Stages 7–8)
- A & B resolve conflicts. Deadlock = skip.
- Enforce caps: per-bet ≤ 2%, daily ≤ 7%, cluster caps by league/kickoff.
- Log final slate and rationale.

## Post-mortem (Weekly)
- `MODEL_ACCURACY_SUMMARY.csv` & `ROI_BY_SLICE.csv`: Where are we actually winning?
- `FEATURE_DRIFT.csv`: Watch for shifts; adjust features/weights.

> News lives in Council sessions (not code). If news changes priors, log it in `council_news_log.md` with votes.