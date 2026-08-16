# Checkpoint: consolidated 3a_PrepDMBenchmarks.R (core built + verified)

Timestamp: 2026-08-15 18:29 EDT

Implements the staged first milestone of `0815d_plan_simplify-raw-dm-benchmarks.md`:
build one Chapter-3 producer, verify the three contracts against the on-disk
oracles, then stop before the downstream consumer rewrites / cache deletions.

## What was built

`3a_PrepDMBenchmarks.R` (new, not yet wired into 3_Precompute.R). Recomputes
matching, pairwise correlations, and risk adjustment directly from
`<ver> LongShort.RData` + ticker + czret/czsum + FF factors. Reads none of the
legacy intermediates (MatchPub.RData, PairwiseCorrelationsActualAndMatches.RDS,
MatchPubRiskAdjusted.RData). Phased, releasing large objects between phases:

- Phase A (was 3a_ResearchVsDMPrep): dm{comp,tic}_sumstats, raw_dm_benchmarks,
  plotdat0, ret_for_plot0/1/_MaxPredictors.
- Phase B (was 2b): candidateReturns + allRhos, in memory.
- Phase C (was 3d): matched_uncorr_benchmark.
- Phase D (was 2d + 3e): risk_adjusted_dm_benchmarks.

Output dir override: `PREP_DM_OUT_DIR` (default ../Data/Processed). Validation
ran to a temp dir; production caches untouched.

## Verification (temp-dir outputs vs on-disk oracles)

Per-phase pre-checks (all exact): candidateReturns 23,275 pairs / 15.94M rows
max|dret|=0; correlations max|drho|=0; matched-uncorr SHA-256 identical;
risk-adj abnormal returns/betas max|diff|=0 across 15.94M shared rows.

Full end-to-end run comparison:
- EXACT equivalent: dmcomp_sumstats, dmtic_sumstats, ret_for_plot0/1/_MaxPred,
  plotdat0$comp_event_time/$comp_matched, raw_dm_benchmarks (all 4 series),
  matched_uncorr_benchmark (fingerprint + panel + pairs), risk$published_stats.
- risk_adjusted_dm_benchmarks DM side DIFFERS by design (see below).

Runtime ~15 min, peak RAM ~12 GB (4 cores).

## Material finding: on-disk risk_adjusted_dm_benchmarks.RDS is STALE

Commit d820119 tightened matched_uncorr_r_reltol 0.30->0.10 and regenerated
MatchPub + matched-uncorr + raw (Aug 15) but never rebuilt MatchPubRiskAdjusted
(2d, still Aug 14). So the current risk-adjusted contract sits on 71,670
candidate pairs (30% tol); a consistent single run uses the current 23,275
(10% tol), a strict subset. New vs old: capm eligible pairs 18,509 vs 57,173;
ff4 15,162 vs 47,603; capm predictors 123 vs 136. The risk-adjustment MATH is
identical on shared pairs; only the candidate universe narrows. Carrying this
correction downstream will change S4b / SA07 / SA09 tables.

## Not done yet (deferred phase, needs go-ahead)

Consumer migration (S5a, SA10, S4b, SA07, SA09), 3_Precompute/2_DataMining
rewiring, deletion of 2b/2d/3a/3d/3e + 3 legacy caches, 2c->2b / 3f->3d renames,
test + exhibit_map + runtime-doc updates. Open decision: accept the
risk-adjusted correction (narrower, consistent) before rewriting S4b/SA07/SA09.
