# Plan: make 3d_MatchedUncorrData.R the single source for the matched-uncorr figure family

*2026-08-14 16:01 EDT — branch `clean/matched-uncorr-pipe`*

Snapshot of a design discussion. Goal: collapse the duplicated matched-uncorr
logic so the concept is defined once upstream, and SA10 (Fig B.3 / B.4) becomes
a thin downstream consumer like S2e is for Fig 2c.

## Current wiring (what feeds what)

All three canonical exhibits descend from one precompute cache,
`../Data/Processed/matched_uncorr_benchmark.RDS`, built by
`3d_MatchedUncorrData.R`:

```
inputs:  MatchPub.RData (readRDS :40, keeps $candidateReturns)
         PairwiseCorrelationsActualAndMatches.RDS (:60)
         czsum_allpredictors.RDS, czret_keeponly.RDS, ret_for_plot0.RDS
   |
   v
3d_MatchedUncorrData.R  ->  matched_uncorr_benchmark.RDS
  |-- 3d_Fig2Data.R (reads it at :145) -> fig2_panel_agg.RDS -> S2e_Fig2Plots.R -> Fig2c
  |-- S3a_MPStyleDecayModels.R -> mp_style_decay_models.RDS -> S3b_MPStyleDecayTables.R -> Tab 3 / Tab 4 / Tab B.1

MatchPub.RData + PairwiseCorrelationsActualAndMatches.RDS   (re-read, screens hardcoded)
  |-- SA10_...R -> Fig B.3 (corr<=10) / Fig B.4 (d_mean_0.1)   <-- the drift path
```

The benchmark's `panel` already carries the three aligned series the figures
want, per `(pubname, eventDate)`:

- `published_ret_scaled`         = Published
- `matched_ret_scaled`           = Matched on t & mean return (history-qualified, **no** corr screen)
- `matched_uncorr_ret_scaled`    = Matched **and** corr <= 0.10

The t / mean-return tolerances are applied **upstream in 2b** when MatchPub's
candidate set is built; 3d only adds the history floor and the correlation
screen. So `keep_matched_uncorr = passes_history & passes_correlation`.

## The parallel path that should be folded in

`Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R` (Fig B.3
`*_Correlation10`, Fig B.4 `*_d_mean_0.1`) re-derives the *same concept* from the
raw caches, independently:

- reads `MatchPub.RData` + `PairwiseCorrelationsActualAndMatches.RDS` directly
  (a second deserialize of the large MatchPub cache that 3d already reads);
- applies its screens **inline with hardcoded literals** — `exclCorrelations =
  c(10)`, `r_tol_cor <- 0.1` — instead of `globalSettings$matched_uncorr_*`.

SA10's `matchRet` == `matched_ret_scaled`; its `matchRetAlt` (corr<=10) ==
`matched_uncorr_ret_scaled`. So SA10's headline "All" figure is the same
economic object as Fig 2c panel (c), computed a second way. **This is the
silent-drift bug:** retune the threshold in `config.R` and B.3/B.4 don't move.

## The split principle: slow / fast, not compute / render

The dividing line is **not** "all compute upstream, only drawing downstream."
It's **slow upstream, fast downstream**:

- The one expensive thing in this subtree is deserializing the large
  `MatchPub.RData` and constructing the per-signal, per-pair event-level matched
  returns. That must happen once, in 3d.
- Everything SA10 does after that — filter `rho <= cc`, apply `diff_rbar <=
  r_tol`, renormalize, aggregate to `(pubname, eventDate)`, roll the 60-month
  window, compute clustered SEs, facet by theory, draw — is cheap arithmetic on
  an already-built panel and legitimately stays downstream.

Consequences of adopting slow/fast:
- Retires "bake one aggregated column per robustness variant." Aggregation is
  the fast part; no reason to precompute each cut. One event-level long table
  serves every threshold as a fast filter.
- Retires lifting the rolling-window + clustered-SE step into precompute
  ("Grade B"). That step is fast and per-signal; keep it in the render helper.

## Runtime reality check (docs/runtimes_and_ram.md)

Tempers the *speed* motivation, and leaves two measurement gaps:

- The true cost center is the `make_DM_event_returns()` / `adj_R2_with_PPCA()`
  fork helpers (23-27 GB band; `3e` ~51 min, `3a` ~12 min). **Neither SA10 nor
  3d_MatchedUncorrData calls those.**
- All of Sections S2-SA combined is **~7 min**; SA10 is a slice of that. So the
  time saved by the refactor is small.
- Gaps: `3d_MatchedUncorrData.R` is **not in the runtimes table**, and SA10
  isn't timed individually. Before/after `time Rscript` on both would confirm
  the MatchPub-read dedup actually pays.

So sell this refactor on **correctness + legibility** (one screen, one source,
no drift), with dedup-of-the-MatchPub-read and lower SA10 memory footprint as
secondary benefits — not as a speed win.

## Plan

**Shape: one script (`3d_MatchedUncorrData.R`), two output artifacts.** Do NOT
overload the benchmark cache — it is fingerprint-guarded and byte-stability
sensitive (S3a/S3b fixest FE calcs depend on its exact row content/order; the
code comments call this out). Adding the event-level table + extra columns to
that same object would entangle the regression-stability contract and fatten a
file the tables load.

```
3d_MatchedUncorrData.R   (slow: one MatchPub deserialize)
  |-- matched_uncorr_benchmark.RDS      (LEAN, fingerprinted; UNCHANGED)
  |     -> S3a->S3b (Tab 3/4/B.1),  3d_Fig2Data->S2e (Fig 2c)
  |-- matched_uncorr_eventlevel.RDS     (NEW, heavy/long)
        -> SA10 (fast: filter -> aggregate -> roll -> SE -> facet -> draw)
```

### Steps

1. **Add a category column.** The benchmark panel joins `czsum`, whose
   `transmute` (3d :~33) drops `theory`. B.3/B.4 facet by theory. Join
   `DataInput/SignalsTheoryChecked.csv` so `theory` is available downstream.

2. **Emit the event-level long cache.** 3d currently `rm()`s `candidate_returns`
   after aggregating. Instead retain and write the filtered event-level table:
   `(pubname, matched_name, eventDate, ret [signed], rho,
   mean_return_rel_distance, theory)`. This is the single heavy artifact; every
   robustness cut becomes a fast filter on it.

3. **Rewrite SA10 as a fast consumer.** It reads `matched_uncorr_eventlevel.RDS`
   only — never `MatchPub.RData` or `PairwiseCorrelations` again. Screens are
   applied downstream from the cache columns:
   - B.3 corr sweep: `filter(rho <= cc/100)` (currently only `cc = 10`;
     commented `c(10,20,30,40,50)` now trivially supported).
   - B.4 `d_mean_0.1`: `filter(rho <= 10/100 & mean_return_rel_distance <= 0.1)`
     — B.4 is a **distinct, tighter** keep set, not a slice of the canonical
     column, which is exactly why the event-level table (not a baked column) is
     required.
   - Thresholds come from `globalSettings` where they correspond to canonical
     constants; genuinely appendix-only robustness values can stay as SA10
     locals but should be named, not bare literals.

4. **Confirm B.3-"All" == Fig 2c.** With both now sourced from the same event
   panel, verify the "All" B.3 line and Fig 2c panel-(c) line match (allowing
   for any intended normalization difference). If they diverge, that divergence
   is now a single, inspectable choice rather than two implementations.

5. **Decide the non-paper prelude plots.** SA10 also renders `All_DM` / `*_DM`
   (correlation-*un*excluded) figures that are **not** in the exhibit map. The
   refactor is the moment to confirm they're still wanted or drop them.

### Validation

- Emit an explicit validation report at each stage, and require it to print
  **the number of surviving signals** at every screen. Concretely: after the
  history floor, after the corr <= 10 screen, and after the B.4
  `d_mean_0.1` tighter screen, log the count of distinct `pubname` (and, where
  it clarifies, distinct matched pairs) that survive. These counts are the
  primary refactor invariant — they must match between the old inline SA10 path
  and the new event-level-cache path at `cc = 10`, `r_tol_cor = 0.1`. A change
  in surviving-signal count is the fast signal that a screen was reordered,
  a threshold silently shifted, or a join dropped rows.
- **The surviving-signal counts are also a paper exhibit, not just a console
  log.** Write the validation table to disk as a LaTeX table destined for the
  appendix: one row per screen (history floor, corr <= 10, `d_mean_0.1`), with
  columns for the threshold applied, surviving distinct signals, and surviving
  matched pairs. Emit it to `../Results/` alongside the other appendix tables
  (matching the existing `\input{}`-style fragment convention used by the other
  generated tables) so it can be `\input` into the appendix. The console log and
  the table are the same numbers rendered two ways; produce both from one
  computation so they cannot drift.
- `time Rscript 3d_MatchedUncorrData.R` and `time Rscript
  Appendices/SA10_...R` before and after, to quantify the MatchPub-read dedup.
- Benchmark cache fingerprint (`pair_fingerprint_sha256`) must be **unchanged** —
  the benchmark object and S3a/S3b/Fig2c paths are not to move.
- Diff the produced `Fig_PublicationsVsDataMining_*` PDFs against the current
  `../Results/` versions; they should be identical at `cc = 10`,
  `r_tol_cor = 0.1` (this is a refactor, not a spec change).

## Open questions

- Size of `matched_uncorr_eventlevel.RDS` (event-level per surviving pair). Large
  but < MatchPub; acceptable as its own cache.
- Whether the rolling/SE step is truly cheap at B.3/B.4 scale (~200 signals x
  theory facets x thresholds). Expected seconds; if SA10 still feels slow after
  the split, that's the next hoist candidate — but only if measured.
- Whether any other appendix figure (SA08/SA09 share the plot helper) should
  read the same event-level cache; out of scope for this pass.
