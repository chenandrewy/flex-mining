# Plan: rationalize Figure 2 calculations before consolidating DM benchmark prep

Timestamp: 2026-08-15 16:13 EDT

## Decision

Before implementing the larger `3a_PrepDMBenchmarks.R` architecture described
in `0815b_plan_prep-dm-benchmarks.md`, remove the calculation-ownership problem
embodied by `3d_Fig2Data.R`.

Do not merely rename that file or move its code into Figure 2 helpers. Delete it
by assigning each calculation to the Chapter 3 producer that conceptually owns
the resulting benchmark:

- raw accounting/ticker benchmark variants belong with the existing raw DM
  benchmark preparation in `3a_ResearchVsDMPrep.R`;
- sum-stat-matched and matched-uncorrelated returns belong with
  `3d_MatchedUncorrData.R`;
- published and DM factor-adjusted benchmark calculations belong in a new
  calculation-oriented Chapter 3 risk-adjustment producer;
- Figure 2 panel selection, common-sample composition, labels, rolling means,
  clustered standard errors, and rendering belong in the Section 2 Figure 2
  producer.

This is the next, deliberately narrower refactor and should be committed as a
behavior-preserving checkpoint before the broader consolidation of `2b`, `2d`,
`3a`, and `3d` internals.

The tracked source worktree was clean when this plan was written. The only
uncommitted file was the new `0815b` planning journal entry. The discarded
tolerance-agnostic MatchPub edits are not a base for this work.

## Problem with the current file

`3d_Fig2Data.R` is named for a presentation artifact but performs several
scientifically distinct calculations:

1. Panel (a) estimates published CAPM and FF4/Carhart coefficients separately
   in the original and post-sample periods, constructs abnormal returns,
   computes alpha means/t-statistics, filters published signals, reloads the
   large risk-adjusted DM pair cache, recomputes DM alpha t-statistics, screens
   DM pairs, normalizes pair returns, and aggregates the benchmark.
2. Panel (b) defines annual-accounting and pre-2003 published-signal samples
   and restricts an existing raw accounting benchmark.
3. Panel (c) reads the canonical matched-uncorrelated cache and attaches Figure
   2 labels.
4. Panel (d) selects top-5% accounting and ticker strategies and recomputes
   their event-time benchmark returns.
5. The final block converts all four panels to rolling five-year means and
   calendar-month/predictor clustered standard errors.

These blocks do not share one calculation concept. Their only commonality is
that Figure 2 displays them. That makes Figure 2 the accidental owner of raw
benchmark construction and risk-adjustment logic, permits duplication with
`3a`, `2d`, `S4b`, `SA07`, and `SA09`, and causes presentation changes to be
treated as Chapter 3 data-preparation changes.

## Desired calculation ownership

| Current `3d_Fig2Data.R` work | Calculation owner after this refactor |
| --- | --- |
| Raw published and accounting `abs(t)>2` returns | `3a_ResearchVsDMPrep.R` |
| Top-5% accounting benchmark | `3a_ResearchVsDMPrep.R` |
| Top-5% ticker benchmark | `3a_ResearchVsDMPrep.R` |
| Sum-stat-matched benchmark | `3d_MatchedUncorrData.R` |
| Matched-and-uncorrelated benchmark | `3d_MatchedUncorrData.R` |
| Published CAPM and FF4 sample-specific adjustments | New Chapter 3 risk-adjustment producer |
| DM CAPM and FF4 sample-specific alpha statistics, screens, normalization, and aggregation | New Chapter 3 risk-adjustment producer |
| Annual-accounting/pre-2003 panel restrictions | Section 2 Figure 2 assembly |
| Panel labels, ordering, and common-sample joins | Section 2 Figure 2 assembly |
| Rolling five-year means and clustered SEs | Section 2 Figure 2 assembly |
| PDF rendering and CI variants | Section 2 Figure 2 assembly/rendering |

The existing script names need not all be finalized in this commit, but each
file must have a calculation-based responsibility. Avoid creating another
script whose only concept is “data needed by Figure 2.”

## Raw benchmark producer

Extend `3a_ResearchVsDMPrep.R`, which already computes DM in-sample summary
statistics and raw Figure 1 benchmark panels, to own all raw mining variants
needed by Figure 2:

- published normalized returns;
- accounting mining with `abs(t)>2` and the standing quality rules;
- top-5% accounting mining;
- top-5% ticker mining.

Panel (d)'s two calls to `SelectDMStrats()`/`make_DM_event_returns()` should
move here. The top-5% accounting calculation should reuse the existing work
that currently feeds `ret_for_plot1`, not run a second time for Figure 2.

Write a calculation-oriented artifact, tentatively:

```text
../Data/Processed/raw_dm_benchmarks.RDS
```

with a documented schema or named list such as:

```r
list(
  published = ...,
  accounting_t2 = ...,
  accounting_top5 = ...,
  ticker_top5 = ...,
  metadata = ...
)
```

Each series should retain `pubname`, `eventDate`, `calendarDate`, normalized
return, and match availability/count fields needed to impose common samples.
Record the screen, mining universe, normalization, and source-cache metadata.

Continue writing `ret_for_plot0.RDS` and `ret_for_plot1.RDS` during this
pre-consolidation refactor because Figure 1, appendices, and other consumers
still use them. They are compatibility artifacts, not the preferred new
Figure 2 contract.

Panel (b) should not require another calculated benchmark. It selects the
annual-accounting and pre-2003 published-signal subsets from the prepared raw
published/accounting series. The classifications can initially be constructed
by the Section 2 assembler; the later consolidated prep can expose reusable
signal metadata if other consumers need it.

## Matched benchmark producer

`3d_MatchedUncorrData.R` already calculates and writes the three panel (c)
series on the appropriate pair/sample contract:

- published normalized returns;
- sum-stat-matched normalized DM returns;
- matched-and-uncorrelated normalized DM returns.

Keep `matched_uncorr_benchmark.RDS` as the focused, fingerprinted contract
shared by Figure 2(c) and the Section 3 regressions. Do not reconstruct any of
its pair selection, history, correlation, normalization, or aggregation logic
in the Figure 2 assembler.

This pre-consolidation commit does not need to merge `3d_MatchedUncorrData.R`
with `3a`. A later calculation-oriented rename such as
`3d_MatchedDMBenchmarks.R` is reasonable, but renaming is useful only after its
ownership contract is clear and tested.

## Risk-adjusted benchmark producer

Create a new Chapter 3 calculation producer, tentatively:

```text
3e_RiskAdjustedDMBenchmarks.R
```

If sequential numbering is desired, rename the current `3e_DMSpanPCA.R` to
`3f_DMSpanPCA.R`; otherwise choose a later unused prefix. The important point
is the calculation name, not the exact number.

For this first refactor, the new producer may consume the legacy
`<dataVersion> MatchPubRiskAdjusted.RData` written by
`2d_RiskAdjustDataMinedSignals.R`. It should absorb all additional panel (a)
calculations currently performed inside `3d_Fig2Data.R`:

### Published side

- raw in-sample mean/t-statistic used for the baseline eligibility screen;
- CAPM coefficients estimated separately in the original and post-sample
  periods;
- FF4/Carhart coefficients estimated separately in those periods;
- abnormal-return event series using the appropriate coefficient regime;
- original-sample alpha mean and alpha t-statistic;
- normalized abnormal-return series;
- eligibility flags for the raw-t and model-alpha-t intersections.

### Data-mined side

- configured base pair universe inherited from the current risk-adjusted pair
  cache contract;
- original-sample CAPM and FF4 alpha mean/count/standard deviation/t-statistic
  for each published/DM pair;
- pair eligibility under the model-specific alpha-t screen;
- individual-pair normalization using its original-sample alpha mean;
- aggregation across eligible pairs by published predictor/event month;
- number of available and eligible pairs.

The current term “time-varying” means two coefficient regimes—original sample
and post-sample—not rolling betas. Preserve exactly the existing factor,
minimum-observation, period, missing-value, raw-t intersection, alpha-t screen,
and normalization definitions in this commit.

Write a calculation-oriented artifact, tentatively:

```text
../Data/Processed/risk_adjusted_dm_benchmarks.RDS
```

containing CAPM and FF4 published/DM predictor-event panels, eligibility flags,
pair counts, and metadata. Do not store Figure 2 labels, panel letters, colors,
linetypes, or rolling plotting statistics.

This creates an intentional transitional dependency:

```text
2d legacy pair-level factor cache
  -> Chapter 3 risk-adjusted benchmark calculation
  -> Figure 2 and later other consumers
```

During the larger `PrepDMBenchmarks` refactor, move the factor estimation now
owned by `2d` into the same Chapter 3 risk-adjustment calculation path and
retire the legacy cache. The first commit should not combine that expensive
cache migration with Figure 2 ownership cleanup.

## Figure 2 assembly and rendering

After the calculation moves, the Section 2 Figure 2 producer should read:

```text
raw_dm_benchmarks.RDS
matched_uncorr_benchmark.RDS
risk_adjusted_dm_benchmarks.RDS
```

It should then perform only Figure 2 composition:

- panel (a): select CAPM and FF4 published/DM prepared series;
- panel (b): restrict the raw published/accounting benchmark to
  annual-accounting and pre-2003 published-signal groups;
- panel (c): select the three prepared matched-benchmark series;
- panel (d): select published, accounting-top-5%, and ticker-top-5% series;
- impose the existing common sample within each comparison;
- attach panel IDs and legend labels;
- convert the selected series to the standard long schema;
- calculate the displayed rolling five-year means and clustered standard
  errors;
- render the main and CI PDFs.

The rolling means and clustered standard errors are Figure 2 display
statistics, not reusable DM benchmark calculations. They belong with Figure 2
assembly. A presentation change should not invalidate Chapter 3 benchmark
caches.

It is acceptable to retain `fig2_panel_long.RDS` and `fig2_panel_agg.RDS`
temporarily as equivalence/debug artifacts. Because no live consumer other
than `S2e_Fig2Plots.R` uses them, the desired end state is to construct them in
memory during the Section 2 Figure 2 run rather than require them as Chapter 3
inputs.

After equivalence is established:

- remove `3d_Fig2Data.R`;
- remove its invocation from `3_Precompute.R`;
- remove `fig2_panel_agg.RDS` from the Section 2 preflight list if it becomes
  transient;
- update `helpers/fig2_helpers.R`, `docs/exhibit_map.md`, runtime notes, and
  tests to describe the new calculation owners.

## Commit boundary and non-goals

The commit should change ownership without changing the scientific
specification or displayed exhibits.

In scope:

- move raw top-5% calculations to the raw benchmark producer;
- introduce the Chapter 3 risk-adjusted benchmark producer;
- make the matched cache the only panel (c) calculation source;
- make Section 2 assemble/render Figure 2 from the three calculation outputs;
- delete `3d_Fig2Data.R` after output equivalence;
- add calculation-contract and exhibit-equivalence tests;
- update pipeline preflights and documentation.

Out of scope:

- rewriting `2b` or changing the MatchPub cache boundary;
- moving `2d`'s factor regressions in the same commit;
- changing matching, correlation, history, raw-t, or alpha-t thresholds;
- changing factor models or coefficient regimes;
- changing pair normalization or common-sample rules;
- consolidating all raw/matched/risk producers into
  `3a_PrepDMBenchmarks.R`;
- deleting legacy caches used by other consumers;
- changing Figure 2 labels, styling, axes, or confidence-band definitions.

## Implementation sequence

1. Add characterization checks for the existing `fig2_panel_long.RDS` and
   `fig2_panel_agg.RDS`: schemas, panel/label sets, row counts, event-date
   coverage, return/rolling/SE values, and common-sample predictor counts.
2. Add temporary-output controls so regenerated Figure 2 data and PDFs do not
   overwrite the current artifacts used as equivalence oracles.
3. Extend `3a_ResearchVsDMPrep.R` to calculate and write the raw accounting/t2,
   accounting/top-5%, and ticker/top-5% benchmark contract without duplicating
   existing accounting work.
4. Confirm and test that `matched_uncorr_benchmark.RDS` contains everything
   needed for panel (c), with no Figure-specific reconstruction.
5. Move panel (a)'s published adjustment, DM alpha-statistic, eligibility,
   normalization, and aggregation calculations into the new Chapter 3
   risk-adjusted benchmark producer.
6. Change the Section 2 Figure 2 producer to assemble the four panels from the
   raw, matched, and risk-adjusted contracts; calculate rolling display
   statistics and render PDFs.
7. Compare the new long and aggregated Figure 2 data with the legacy caches.
8. Render all main/CI PDFs to a temporary directory and compare them visually
   and structurally.
9. Delete `3d_Fig2Data.R`, remove obsolete pipeline dependencies, and update
   tests/documentation only after equivalence passes.
10. Commit this ownership refactor as the clean baseline for the larger
    `PrepDMBenchmarks` work.

## Verification contract

### Raw benchmarks

- Accounting `abs(t)>2` and top-5% pair selections match the current `3a` and
  Figure 2 calculations.
- Ticker top-5% selection and event-time returns match the legacy panel (d).
- Per-series predictor/event-month observations and normalized returns match.

### Matched benchmarks

- Figure 2(c) and Section 3 continue to use the same pair fingerprint,
  predictor universe, and canonical cache.
- The Figure 2 assembler does not read MatchPub or pairwise-correlation caches.

### Risk-adjusted benchmarks

- Published CAPM/FF4 coefficients, abnormal returns, alpha means/t-statistics,
  normalization denominators, and eligibility sets match the legacy panel (a).
- DM pair alpha statistics, qualifying keys, normalized event returns,
  aggregates, and match counts match the legacy panel (a).
- Preserve the prior FF3/FF4 coefficient-unpacking bugfix and test against
  existing fixtures where applicable.

### Figure 2

- `fig2_panel_long` matches after deterministic sorting by panel, label,
  predictor, and event date.
- `fig2_panel_agg` matches after sorting by panel, label, and event date;
  compare floating-point values with an explicit tight tolerance if byte-level
  identity is not stable after harmless ordering changes.
- All eight PDFs retain the same series, axes, labels, line styles, CI ribbons,
  and dimensions.
- Main and CI artifacts are generated only by the Section 2 Figure 2 producer.

### Operational checks

- Record runtime and peak RAM for raw, matched, risk-adjusted, assembly, and
  rendering stages.
- Confirm that Figure 2 styling or panel-label edits rerun no Chapter 3
  benchmark calculation.
- Confirm that no current non-Figure consumer loses a required compatibility
  cache in this commit.

## Relationship to the larger plan

This refactor establishes clear calculation contracts before consolidation:

```text
raw DM benchmarks
matched DM benchmarks
risk-adjusted DM benchmarks
        -> Figure 2 composition
```

The later `3a_PrepDMBenchmarks.R` work can then consolidate duplicated
pair-statistic construction, selection, event-return materialization, and
factor estimation underneath these contracts without simultaneously untangling
Figure 2 presentation code. If the later architecture changes physical cache
boundaries, the scientific ownership and consumer interfaces established here
should remain recognizable.
