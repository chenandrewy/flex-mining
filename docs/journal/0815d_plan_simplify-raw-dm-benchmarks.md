# Plan: consolidate data-mined benchmark preparation after Figure 2 cleanup

Timestamp: 2026-08-15 17:25 EDT

## Decision

Use `0815b_plan_prep-dm-benchmarks.md` as the architectural base, updated for
the Figure 2 rationalization that has already been completed.

Consolidate the overlapping raw and factor-adjusted data-mined benchmark
calculations into one Chapter 3 producer:

```text
3a_PrepDMBenchmarks.R
```

After equivalence checks and consumer migration, retire:

- `2b_MatchDataMinedToPub.R`;
- `2d_RiskAdjustDataMinedSignals.R`;
- `3a_ResearchVsDMPrep.R`;
- `3d_MatchedUncorrData.R`; and
- `3e_FactorAdjustedDMPrep.R`.

Chapter 2 should end after constructing the accounting and ticker mined-return
universes. Matching mined strategies to published predictors, calculating
pair diagnostics, selecting raw benchmarks, and factor-adjusting selected
pairs are all Chapter 3 benchmark preparation.

This is smaller than the original `0815b` plan in one important respect:
Figure 2 ownership is already fixed and should not be redesigned again.
`S2e_Fig2Plots.R` remains the owner of panel-specific samples, common-sample
composition, labels, rolling means, clustered standard errors, and rendering.
The consolidated Chapter 3 producer writes the three calculation contracts
that `S2e` already consumes.

## Current state after the completed Figure 2 refactor

The current calculation producers are:

```text
3a_ResearchVsDMPrep.R
  -> dmcomp_sumstats.RDS
  -> dmtic_sumstats.RDS
  -> raw_dm_benchmarks.RDS
  -> ret_for_plot*.RDS

3d_MatchedUncorrData.R
  <- MatchPub.RData
  <- PairwiseCorrelationsActualAndMatches.RDS
  -> matched_uncorr_benchmark.RDS

3e_FactorAdjustedDMPrep.R
  <- MatchPubRiskAdjusted.RData
  -> risk_adjusted_dm_benchmarks.RDS

S2e_Fig2Plots.R
  <- raw_dm_benchmarks.RDS
  <- matched_uncorr_benchmark.RDS
  <- risk_adjusted_dm_benchmarks.RDS
  -> Figure 2 and CI variants
```

`3d_Fig2Data.R` has already been deleted. Its former benchmark calculations
now have calculation-oriented owners, and Figure 2 assembly is downstream.
The consolidation should preserve those output contracts and that ownership
boundary.

## Desired pipeline

```text
Chapter 2: construct mined-return universes
  2a_CompustatToLongshort.R
  2c_TickerToLongshort.R
                  |
                  v
Chapter 3: prepare DM benchmarks
  3a_PrepDMBenchmarks.R
    A. compact invariant pair statistics
    B. named raw pair selections
    C. selected pair-month returns
    D. factor adjustments and alpha screens
    E. reusable benchmark contracts
                  |
       +----------+----------+-------------------+
       v                     v                   v
  raw_dm_             matched_uncorr_     risk_adjusted_dm_
  benchmarks.RDS      benchmark.RDS       benchmarks.RDS
       |                     |                   |
       +---------------------+-------------------+
                             v
                    downstream exhibits
```

`3a_PrepDMBenchmarks.R` is one calculation owner, not one indivisible
operation. Keep clear internal phases and release large objects between them.
Explicit phase switches or small cache-validation helpers are sufficient; a
general dependency framework is not required.

## Phase A: compact invariant pair catalog

Refactor `sumstats_for_DM_Strats()` so its published summaries, published
returns, mined-return source, and settings are explicit arguments rather than
chapter-global objects.

For both accounting and ticker universes, retain separate typed key columns:

```text
(pubname, mining_universe, sweight, dmname)
```

Do not use pasted identifiers as stored primary keys or joins. Assert key
uniqueness and retain `dmname` separately so Section 5 can join the mined
formula documentation.

Required pair statistics include:

- published sample dates, mean return, and t-statistic;
- mined in-sample mean, t-statistic, and sign;
- absolute and relative mean/t-stat distances;
- in-sample and final-year month counts;
- minimum stocks in both portfolio legs;
- raw pairwise correlation; and
- signed pairwise correlation, aligned with the signed mined return.

The screening correlation must be:

```r
cor_signed = cor_raw * sign(rbar_dm)
```

Define relative-distance behavior once, including zero denominators:

```text
finite distance, nonzero reference -> distance / abs(reference)
zero distance, zero reference      -> 0
nonzero distance, zero reference   -> Inf
missing input                       -> NA
```

Attach schema/calculation versions, the data version, included-signal
universe, settings, and input fingerprints. Preserve `dmcomp_sumstats.RDS` and
`dmtic_sumstats.RDS` as compatibility outputs initially; the compact catalog,
not a broad monthly panel, is the tolerance-agnostic durable layer.

The size finding from `0815b` remains decisive: the accounting catalog has
about 4.8 million pair-stat rows, while materializing monthly returns for all
of them would expand the data enormously. Do not create a tolerance-agnostic
pair-month cache.

## Phase B: named pair selections

Represent selections through one tested selector and explicit named
specifications rather than scattered filters. At minimum preserve:

- accounting strategies with `abs(t) > 2` and current quality screens;
- top-5% accounting strategies;
- top-5% ticker strategies;
- configured mean/t-stat-matched accounting strategies;
- history-qualified matched strategies;
- matched-and-uncorrelated strategies; and
- the base pair universes required by the live factor-adjusted analyses.

Each selection records all active thresholds, normalization rules, source
universe, and a stable fingerprint of sorted composite pair keys. Preserve
exact threshold inclusivity, missing-value behavior, weighting rules, and
current scientific definitions.

The fixed `abs(t) > 2` boundary is a named specification, not an invariant
restriction on the pair catalog.

## Phase C: selected pair-month returns

Materialize monthly returns only after Phase B selects the pairs required by a
live raw or factor-adjusted output. Join selected composite keys to the mined
long-short universe once where specifications overlap.

Retain:

- the full composite pair key;
- calendar month and event month relative to the published sample end;
- original/post-sample classification;
- signed raw return;
- pair-specific in-sample mean and normalization denominator; and
- named-specification membership.

Raw benchmarks sign each mined return to align it with its published
predictor, normalize each pair by its own signed in-sample mean, and then
average across available pairs by predictor/event month. Preserve both scaled
and unscaled variants and current match counts where live consumers require
them.

Do not write another broad durable cache equivalent to `MatchPub.RData`.
Focused durable panels and on-demand materialization from pair keys are
preferable.

## Phase D: factor-adjusted returns

Move the factor calculations currently split across `2d`,
`3e_FactorAdjustedDMPrep.R`, `S4b`, `SA07`, and `SA09` under the consolidated
Chapter 3 preparation path. This is a calculation consolidation, not a change
in scientific specification.

Preserve all factor variants required by live exhibits, including:

- raw returns;
- CAPM with separate original- and post-sample coefficient regimes;
- FF4/Carhart with separate original- and post-sample coefficient regimes;
- full-sample CAPM; and
- full-sample FF3.

The current “time-varying” models use two sample-specific coefficient regimes;
they are not rolling-beta models. Preserve the current factor definitions,
minimum-observation rules, missing-factor handling, coefficient extraction,
raw-t intersections, alpha-t screens, and pair-level normalization.

Use shared model-fitting helpers for the published and data-mined sides. Store
only coefficients, pair statistics, abnormal returns, eligibility flags, and
aggregates that a live consumer needs. Do not recreate the monolithic 2.2 GB
`MatchPubRiskAdjusted.RData` cache.

Grouped table estimands and clustered inference over theory, discipline,
model-use, or journal groups remain in `S4b`; Chapter 3 supplies their prepared
published/DM raw and abnormal-return panels.

## Phase E: preserve calculation contracts

The consolidated producer should continue to write:

```text
raw_dm_benchmarks.RDS
matched_uncorr_benchmark.RDS
risk_adjusted_dm_benchmarks.RDS
```

Preserve `matched_uncorr_benchmark.RDS` as the focused, fingerprinted contract
shared by Figure 2(c) and Section 3. Preserve the existing three-contract
interface to `S2e_Fig2Plots.R`.

Continue writing `ret_for_plot0.RDS`, `ret_for_plot1.RDS`,
`ret_for_plot_MaxPredictors.RDS`, `plotdat0.RDS`, `dmcomp_sumstats.RDS`, and
`dmtic_sumstats.RDS` only while live consumers still require them. Treat them
as compatibility outputs rather than alternate calculation owners.

Do not move Figure 2 panel assembly or rolling display statistics back into
Chapter 3. `S2e` should continue to build those in memory from the three
benchmark contracts.

## Consumer migration and legacy-cache removal

Migrate every live consumer away from the legacy caches:

- `S5a` selects documented example pairs from the compact catalog and joins
  only the required raw returns;
- `SA10` selects its all-matched, correlation-excluded, and tighter-matching
  variants from the catalog and materializes their raw returns;
- `S4b`, `SA07`, and `SA09` consume prepared factor-adjusted contracts rather
  than `MatchPubRiskAdjusted.RData`; and
- driver preflights require the new Chapter 3 contracts, not the legacy files.

After equivalence passes and no live reference remains, delete:

```text
<dataVersion> MatchPub.RData
PairwiseCorrelationsActualAndMatches.RDS
<dataVersion> MatchPubRiskAdjusted.RData
```

Also remove their calculations from the source tree by retiring `2b` and `2d`.
Do not leave compatibility writers for files with no consumers; that would
preserve the stale-intermediate-data problem this refactor is meant to solve.

## Chapter and script end state

Use contiguous prefixes that reflect final execution order. Chapter 2 should
contain only the two mined-universe producers:

```text
2a_CompustatToLongshort.R
2b_TickerToLongshort.R       # renamed from 2c_TickerToLongshort.R
```

Chapter 3 should be ordered:

```text
3a_PrepDMBenchmarks.R
3b_DataMiningSummary.R
3c_DMCorrelationsPCA.R
3d_DMSpanPCA.R               # renamed from 3f_DMSpanPCA.R
```

`3_Precompute.R` should invoke these four scripts in that order.
`3a_PrepDMBenchmarks.R` replaces the current `3a`, `3d`, and `3e` benchmark
producers. The summary, correlation/PCA, and spanning calculations remain
separate because they do not duplicate benchmark preparation.

The final source tree should not contain
`2b_MatchDataMinedToPub.R`, `2d_RiskAdjustDataMinedSignals.R`,
`3a_ResearchVsDMPrep.R`, `3d_MatchedUncorrData.R`, or
`3e_FactorAdjustedDMPrep.R`. Delete the superseded scripts only after the
replacement has reproduced their live output contracts; then reuse the
vacated `2b` and `3d` prefixes for the ticker and DM-span scripts. Update all
driver calls, script headers, tests, and documentation in the same rename
change so no stale path remains.

## Implementation sequence

1. Record characterization oracles for the compact pair summaries, raw
   benchmark variants, matched benchmark pairs/panels, factor-adjusted panels,
   Section 5 tables, SA10 variants, S4b tables, and SA07/SA09 figures.
2. Make the pair-statistics calculation explicit-argument and define the
   composite key, signed-correlation semantics, distance behavior, metadata,
   and selector tests.
3. Reproduce all named raw pair selections and fingerprints from the compact
   catalog.
4. Materialize only the union of selected raw and factor-required pair-month
   returns and reproduce the raw and matched benchmark contracts.
5. Move `2d` and `3e` factor calculations into shared Chapter 3 helpers and
   reproduce the factor-adjusted contracts, including the previously fixed
   FF3/FF4 coefficient-unpacking behavior.
6. Build `3a_PrepDMBenchmarks.R` as the phase orchestrator, with temporary
   output controls so validation does not overwrite current artifacts.
7. Migrate consumers one at a time: `S5a`, `SA10`, `S4b`, `SA07`, and `SA09`.
   Preserve `S2e`'s current three-contract input and display ownership.
8. Delete the five superseded scripts and three legacy caches only after all
   calculation and exhibit equivalence checks pass.
9. Rename `2c_TickerToLongshort.R` to `2b_TickerToLongshort.R` and
   `3f_DMSpanPCA.R` to `3d_DMSpanPCA.R`. Update `2_DataMining.R`,
   `3_Precompute.R`, exhibit-driver preflights, script headers, focused tests,
   `docs/exhibit_map.md`, and runtime/RAM documentation atomically.

## Verification contract

- Pair-stat equivalence for means, t-statistics, coverage, stock counts, and
  raw/signed correlations.
- Explicit EW/VW composite-key uniqueness and duplicate rejection.
- Selector tests for zero denominators, missing values, exact boundaries,
  absolute/relative tolerances, ranks, history, stock counts, and correlation.
- Exact named-spec pair counts, predictor counts, and stable fingerprints.
- Raw pair-event and aggregate-return equivalence for accounting `t > 2`,
  accounting top 5%, ticker top 5%, matched, and matched-uncorrelated panels.
- Published and DM CAPM, FF3, and FF4 coefficient/abnormal-return equivalence
  under every retained coefficient regime.
- Exact alpha-screen eligibility and normalization-denominator equivalence.
- `raw_dm_benchmarks.RDS`, `matched_uncorr_benchmark.RDS`, and
  `risk_adjusted_dm_benchmarks.RDS` schema and value equivalence.
- Figure 2 long/aggregate data and all eight PDFs remain equivalent through
  the existing `S2e` path.
- Section 3 observation counts and matched pair fingerprint remain unchanged.
- Section 5 inspection workbook/TeX, SA10 plots, S4b tables, and SA07/SA09
  figures retain their current inputs and displayed results.
- No script, preflight, or test references `MatchPub.RData`,
  `PairwiseCorrelationsActualAndMatches.RDS`, or
  `MatchPubRiskAdjusted.RData` in the end state.
- `2_DataMining.R` invokes exactly `2a` then `2b`; `3_Precompute.R` invokes
  exactly `3a` through `3d` in order; no live source or documentation refers
  to the superseded `2c_TickerToLongshort.R` or `3f_DMSpanPCA.R` paths.
- Record runtime, peak RAM, and durable output size by phase.

## Completion condition

This task is complete when Chapter 2 only constructs mined-return universes,
one Chapter 3 producer owns raw and factor-adjusted DM benchmark preparation,
all live consumers use the new calculation contracts, Figure 2 retains its
current downstream assembly boundary, and the old matching, risk-adjustment,
and three superseded Chapter 3 producer scripts plus all three legacy
broad/intermediate caches have been removed. The surviving Chapter 2 and
Chapter 3 scripts have contiguous execution-order prefixes with no numbering
gaps.
