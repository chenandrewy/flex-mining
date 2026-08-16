# Plan: consolidate all data-mined benchmark preparation in Chapter 3

Timestamp: 2026-08-15 15:29 EDT

## Decision

Chapter 2 should end when the mined long-short return universes have been
constructed. Matching mined strategies to published predictors, computing
pair diagnostics, constructing raw and factor-adjusted benchmark returns, and
assembling Figure 1/Figure 2 inputs are all Chapter 3 empirical preparation.

Replace the current division across `2b_MatchDataMinedToPub.R`,
`2d_RiskAdjustDataMinedSignals.R`, `3a_ResearchVsDMPrep.R`,
`3d_MatchedUncorrData.R`, and the benchmark calculations inside
`3d_Fig2Data.R` with one Chapter 3 owner:

```text
3a_PrepDMBenchmarks.R
```

This should be one logical producer, not one indivisible calculation. It must
have explicit internal cache layers so a change to a matching threshold,
factor-adjustment specification, or Figure 2 composition does not force the
invariant mined-strategy summary statistics to be recomputed.

Exhibit scripts remain consumers. They should not estimate factor loadings,
reconstruct pair-level t-statistics, reapply matching rules, or independently
normalize mined strategies.

## Why the larger consolidation is warranted

The current scripts repeatedly compute different representations of the same
benchmark objects:

- `2b` manually computes mined-strategy in-sample means, t-statistics, stock
  counts, coverage, event-time returns, and a separate pairwise-correlation
  file.
- `3a` calls `sumstats_for_DM_Strats()` to recompute almost the same raw pair
  statistics, then builds the Figure 1 and alternative-mining panels.
- `3d_MatchedUncorrData.R` reloads pair-level monthly returns and the separate
  correlation file, reconstructs pair statistics, applies the canonical
  screens, normalizes pairs, and writes the matched-uncorrelated benchmark.
- `2d` factor-adjusts a large pair-month cache before downstream selection and
  writes a 2.2 GB risk-adjusted object in the current vintage.
- `3d_Fig2Data.R` independently computes published CAPM and FF4 adjustments,
  recomputes data-mined alpha t-statistics from `2d`, normalizes and aggregates
  them, and combines those results with three other DM benchmark definitions.
- `S4b`, `SA07`, and `SA09` repeat substantial parts of the published-alpha,
  data-mined-alpha-statistic, screening, normalization, and aggregation logic.

The duplication is both a runtime problem and a specification-drift problem.
The desired architecture computes every invariant statistic once and makes
each benchmark specification an explicit selection from a shared catalog.

## Important size finding

The tolerance-agnostic object must be a compact pair-statistics table, not a
tolerance-agnostic monthly return panel. In the current `dmcomp_sumstats.RDS`:

- all compact published/DM pair-stat rows: 4,813,736;
- rows with `abs(tstat) > 2`: 1,275,974;
- rows also passing the stock/final-year quality screens: 1,083,375;
- rows passing the current 10% return/t-stat matching and quality screens:
  18,063.

The existing old `MatchPub` cache has 23,275 pair keys and 15,940,975 monthly
rows. Expanding all 1.28 million `abs(t)>2` pairs before downstream selection
would increase the monthly panel by roughly fifty-fold or more. The currently
edited tolerance-agnostic `candidateReturns` design therefore must not become
the new cache contract.

## Desired pipeline

```text
Chapter 2: mine return universes
  2a_CompustatToLongshort.R
  2c_TickerToLongshort.R
            |
            v
Chapter 3: prepare all DM benchmarks
  3a_PrepDMBenchmarks.R
    Phase A: invariant pair statistics
    Phase B: named benchmark pair selections
    Phase C: selected pair-month returns
    Phase D: published and DM factor adjustments
    Phase E: normalized benchmark panels and pair diagnostics
    Phase F: Figure 1 and Figure 2 analysis inputs
            |
            +--> S2a/S2e render figures
            +--> S3a estimates learning regressions
            +--> S4b estimates grouped means/inference and renders tables
            +--> SA07/SA09/SA10 render appendix exhibits
            +--> S5a renders selected-predictor inspection tables
```

`2b_MatchDataMinedToPub.R` and `2d_RiskAdjustDataMinedSignals.R` should leave
the Chapter 2 driver. Their reusable calculations move into helpers called by
`3a_PrepDMBenchmarks.R`; their monolithic cache contracts should be retired
after equivalence checks and consumer migration.

`3d_Fig2Data.R` should also be retired as a benchmark-computation script.
Figure 2 rendering stays in `S2e_Fig2Plots.R`, but the long per-signal panel,
rolling aggregates, and clustered standard errors it consumes should be
outputs of `3a_PrepDMBenchmarks.R`.

## Phase A: invariant pair-statistics catalog

Use one generalized, explicit-argument version of `sumstats_for_DM_Strats()`
for both the Compustat and ticker mining universes. Avoid its current reliance
on chapter-global `czsum` and `czret` objects.

The Compustat catalog should have one row per:

```text
(published predictor, mining universe, weighting method, mined strategy)
```

Preserve those identifying fields as typed, separate columns rather than
encoding them in a pasted `candSignalname` such as `ew::225`. The catalog's
logical key is:

```text
(actSignal, mining_universe, sweight, dmname)
```

Use explicit multi-column joins. A pasted representation may be generated
transiently for logging or stable fingerprints, but it is not a stored primary
key and should not be used for joins. Fingerprints should be constructed from
sorted explicit key columns.

The current data happen to satisfy `actSignal -> sweight`, so the legacy
`(actSignal, candSignalname)` key does not produce observed EW/VW collisions.
The new schema should not rely on that implicit dependency: assert key
uniqueness and the relevant functional dependencies directly. Keep `dmname`
available separately so `S5a` can join mined-signal documentation on the
actual mined formula identifier.

Required invariant columns include:

- published predictor and mined-strategy identifiers;
- mining universe (`accounting` or `ticker`) and `sweight`;
- published sample start/end and published in-sample mean/t-statistic;
- mined in-sample mean, t-statistic, and signed versions;
- absolute and relative mean/t-stat distances;
- number of in-sample months, final-year months, and minimum stocks in each
  portfolio leg;
- raw and signed pairwise correlation with the published predictor;
- metadata identifying the data version, included-signal universe, input
  fingerprints, and calculation/schema version.

Correlation calculation belongs in this invariant catalog. Correlation
*thresholding* belongs in Phase B. Because downstream candidate returns are
signed to align the mined strategy with its published predictor, the catalog's
screening correlation must be the signed value:

```r
rho_signed = rho_raw * sign(rbar_dm)
```

Validate missing-observation semantics before deleting
`PairwiseCorrelationsActualAndMatches.RDS`.

Suggested durable output:

```text
../Data/Processed/dm_benchmark_pair_stats.RDS
```

The existing `dmcomp_sumstats.RDS` and `dmtic_sumstats.RDS` names may be
written temporarily as compatibility outputs, but the end state should have
one documented pair-statistics contract rather than overlapping caches.

## Phase B: named benchmark specifications

Selections should be data, not scattered filters. Define a registry of named
specifications, each recording its source universe and complete screening
rule. At minimum the current paper needs:

1. `raw_t2_accounting`: accounting strategies with `abs(t)>2` and standing
   quality/coverage screens; this feeds Figure 1 and raw comparisons.
2. `top5_accounting`: top 5% of accounting-strategy in-sample t-statistics.
3. `top5_ticker`: top 5% of ticker-strategy in-sample t-statistics.
4. `sumstat_matched`: configured mean/t-stat matching plus quality and history
   requirements, before correlation exclusion.
5. `matched_uncorr`: `sumstat_matched` plus the signed-correlation ceiling;
   this remains the canonical Figure 2(c)/Tables 3--4 benchmark.
6. Risk-adjusted variants based on the intended base matched universe, with
   pair-level in-sample alpha t-stat screens applied only after Phase D has
   produced those statistics.

Every selected-pair table should carry a specification name and a stable pair
fingerprint. Downstream consumers should assert the expected fingerprint or
specification metadata rather than rerun the selection rule.

Define relative-distance behavior once in the Phase B selector, including its
zero-denominator cases:

```text
distance/reference
  finite distance, nonzero |reference| -> distance / |reference|
  zero distance, zero reference        -> 0
  nonzero distance, zero reference     -> Inf
  missing distance or reference        -> NA
```

This prevents `0/0` from becoming an accidental failed match while correctly
rejecting a nonzero deviation from a zero published statistic under a finite
relative tolerance.

Each named specification's metadata should record all active and inactive
filters explicitly: absolute and relative mean/t-stat tolerances, raw t-stat
or rank screens, stock/final-year/history requirements, correlation ceiling,
mining universe, weighting rule, normalization, and factor-adjustment
definition. Use explicit `Inf`, `NULL`, or an enabled flag for an inactive
filter; avoid expressions such as `.1 * Inf`.

The fixed `abs(t)>2` boundary should not be imposed on the compact Phase A
catalog unless it is explicitly accepted as a permanent scientific boundary.
It is safe to impose it when materializing the `raw_t2_accounting` monthly
panel.

## Phase C: materialize only needed pair-month returns

Join selected pair keys back to the mined long-short universe and create
pair-level event-time returns only after Phase B selection. Preserve:

- `actSignal`, composite `candSignalname`, mining universe, and `sweight`;
- calendar date and event date relative to the published sample end;
- published sample start/end and in-sample/post-sample indicator;
- raw signed return;
- the pair's raw in-sample mean and normalization denominator.

Do not create one broad monthly panel for every pair in the Phase A catalog.
If several named specifications overlap, materialize the union of their pair
keys once and store membership flags/specification keys separately.

Suggested output:

```text
../Data/Processed/dm_benchmark_pair_returns.RDS
```

The selected pair-month panel's logical observation key is:

```text
(actSignal, mining_universe, sweight, dmname, calendarDate)
```

Retain `eventDate` and assert that it maps one-to-one to `calendarDate` within
`actSignal`, but use calendar date as the safer physical observation key.

The physical format can be reconsidered if RDS size remains excessive, but
format migration is separate from the scientific refactor.

## Phase D: factor-adjust published and data-mined returns once

Integrate the risk-adjustment calculations currently split across `2d`,
`3d_Fig2Data.R`, `S4b`, `SA07`, and `SA09`.

The required adjustment families are:

- raw returns;
- CAPM with sample-specific coefficients (in-sample coefficients during the
  original sample and post-sample coefficients afterward; currently called
  `*_tv`);
- FF4/Carhart with sample-specific coefficients, labeled FF3+Momentum in
  Figure 2;
- CAPM with full-sample coefficients;
- FF3 with full-sample coefficients;
- retain other currently required coefficient/return columns only if a live
  downstream exhibit consumes them.

Compute the published and DM sides through shared model-fitting helpers with
the same minimum-observation, period-definition, missing-factor, and
coefficient-extraction rules. The current phrase “time-varying” means two
sample-specific regimes (original-sample and post-sample coefficients), not a
rolling beta; preserve that definition and document it in metadata.

For each published predictor and each selected DM pair, Phase D should store:

- model coefficients by estimation regime;
- abnormal-return event series;
- in-sample abnormal mean, standard deviation, observation count, and
  t-statistic;
- normalization denominator and normalized abnormal-return series;
- explicit validity flags for insufficient observations or near-zero
  denominators.

This removes the need for consumers to reconstruct `rbar_t`, alpha means,
alpha t-statistics, coefficient regimes, and normalized return columns.

## Phase E: canonical benchmark panels

Build reusable predictor/event-month panels by applying named pair selections,
pair-level raw or alpha t-stat screens, individual-pair normalization, and
cross-pair aggregation once.

At minimum expose:

- published raw normalized returns;
- raw `abs(t)>2` accounting benchmark;
- top-5% accounting and ticker benchmarks;
- sum-stat-matched benchmark;
- matched-and-excluding-correlated benchmark;
- published and DM CAPM sample-specific normalized alphas;
- published and DM FF4 sample-specific normalized alphas;
- published and DM CAPM full-sample normalized alphas;
- published and DM FF3 full-sample normalized alphas;
- number of available matched pairs at each predictor/event month;
- the published and DM signal/pair eligibility flags used by each panel.

The canonical `matched_uncorr_benchmark.RDS` may remain as a focused,
fingerprinted compatibility artifact for Figure 2(c) and Section 3, but it
should be written from these shared Phase A--E objects rather than maintained
by an independent producer.

The grouped tables' final post-sample means, paired differences, and clustered
standard errors can remain in `S4b`: those are exhibit estimands over theory,
model, discipline, or journal groups, not reusable DM benchmark statistics.
Their inputs, screens, and normalized published/DM return columns should come
from Phase E.

## Phase F: Figure 1 and Figure 2 analysis inputs

Fold all of the current `3d_Fig2Data.R` data construction into
`3a_PrepDMBenchmarks.R`, including panel (a)'s risk adjustments. Figure 2
should be assembled from Phase E panels:

- panel (a): published/DM CAPM and FF4 sample-specific normalized alphas,
  with the documented raw-t and alpha-t eligibility intersections;
- panel (b): raw benchmark restricted to annual-accounting published signals
  and pre-2003 publications;
- panel (c): published, sum-stat-matched, and matched-uncorrelated raw returns
  from the canonical pair universe;
- panel (d): published, top-5% accounting, and top-5% ticker mining returns.

Write:

```text
ret_for_plot0.RDS                 # compatibility during migration
ret_for_plot1.RDS                 # compatibility during migration
matched_uncorr_benchmark.RDS      # focused canonical contract
fig2_panel_long.RDS
fig2_panel_agg.RDS
```

`S2a_ResearchVsDMPlots.R` and `S2e_Fig2Plots.R` remain renderers. Figure 2's
rolling five-year means and calendar-month/predictor clustered standard errors
are downstream statistics of the prepared per-signal panel and belong to this
producer, not to the renderer.

## Rerun and invalidation behavior

One script must not imply all-or-nothing recomputation. Each phase should read
the prior durable layer when its contract and inputs are still valid.

At a minimum distinguish these invalidation classes:

- mined-return or published-sample changes invalidate Phase A onward;
- pair-statistic/correlation code changes invalidate Phase A onward;
- matching/quality/correlation settings invalidate Phase B onward;
- factor data or factor-model code changes invalidate Phase D onward, but not
  raw pair statistics;
- panel eligibility or Figure 2 composition changes invalidate Phase E/F
  only;
- plot styling changes invalidate no Chapter 3 data.

Implement this initially with metadata and explicit phase switches or a small
cache-validation helper. A future dependency framework is optional; it is not
required to establish the contracts now.

Within a full run, release the large mined-return and pair-month objects between
phases where possible. The current chapter drivers use separate R processes to
return memory to the OS; the consolidated script must use deliberate `rm()` and
`gc()` or phase subprocesses so ownership consolidation does not increase peak
RAM.

## Consumer end state

- `S2a`: render Figure 1 from prepared raw panels.
- `S2e`: render Figure 2 and its CI variants from `fig2_panel_agg.RDS`.
- `S3a`: estimate regressions from the canonical matched-uncorrelated panel.
- `S4b`: consume prepared raw/risk-adjusted predictor-month panels; estimate
  grouped paper estimands and inference only.
- `SA07`: render full-sample risk-adjusted figures from prepared panels.
- `SA09`: restrict prepared sample-specific alpha panels to annual-accounting
  predictors and render.
- `SA10`: apply its appendix-specific selection to the shared pair catalog or
  consume a named prepared variant; do not reload a separate correlation file.
- `S5a`: inspect selected pair keys and join documentation; do not depend on a
  broad monthly MatchPub cache.

No consumer should read `MatchPub.RData`,
`PairwiseCorrelationsActualAndMatches.RDS`, or
`MatchPubRiskAdjusted.RData` in the end state.

## Implementation sequence

1. Make `sumstats_for_DM_Strats()` explicit-argument and define the Phase A
   schema, composite key, signed-correlation semantics, and metadata.
2. Write Phase B selector functions and reproduce current pair counts and
   fingerprints for raw, configured-matched, and matched-uncorrelated specs.
3. Materialize only the union of pairs needed by live downstream raw and
   risk-adjusted analyses.
4. Consolidate factor-model fitting for published and DM returns and verify it
   against the fixed FF3 coefficient-unpacking regression tests/fixtures.
5. Produce canonical Phase E panels and move Figure 2(a)--(d) assembly into
   `3a_PrepDMBenchmarks.R`.
6. Migrate consumers one at a time, keeping legacy outputs as temporary
   equivalence oracles.
7. Remove `2b`, `2d`, `3a_ResearchVsDMPrep.R`,
   `3d_MatchedUncorrData.R`, and `3d_Fig2Data.R` only after all live consumers
   have moved and exhibit-equivalence tests pass.
8. Remove obsolete large caches and compatibility outputs in a separate,
   explicitly reviewed cleanup change.

## Verification contract

- Pair-stat equivalence for means, t-statistics, coverage, stock counts, and
  correlations, with explicit EW/VW-key tests.
- Selector edge cases covering absolute versus relative tolerances,
  zero-on-zero comparisons, nonzero distances from zero references, missing
  and non-finite statistics, exact threshold boundaries, duplicate inputs,
  and uniqueness of returned multi-column pair keys.
- Named-spec pair counts, predictor counts, and stable fingerprints.
- Event-return equivalence for sampled published/DM pairs before aggregation.
- Published and DM coefficient/abnormal-return equivalence for CAPM, FF3, and
  FF4 under both coefficient regimes.
- Exact pair-level alpha-screen equivalence and normalization-denominator
  equivalence.
- Figure 1 and Figure 2 long/aggregate data equivalence before PDF comparison.
- Existing matched-uncorrelated cache and Section 3 observation-count
  invariants.
- S4b and appendix fixture tests for grouped risk-adjusted tables.
- Runtime, peak RAM, and output-size measurements for every phase.

The current on-disk `CZ-style-v8b MatchPub.RData` still has the legacy schema
(`candidateReturns` and `user` only) and is useful as an equivalence oracle. Do
not overwrite it merely to exercise the in-progress tolerance-agnostic `2b`
implementation; that implementation expands the wrong layer of the pipeline.
