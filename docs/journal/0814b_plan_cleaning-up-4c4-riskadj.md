# Cleaning up `4c4_RiskAdj`

Snapshot written 2026-08-14 11:09 EDT. This is a staged cleanup plan, not an
implementation log.

## Decision

Clean up `4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R` in two deliberately
separate passes:

1. Fix the pipeline and ownership of its outputs **without changing any
   statistical logic or reported values**.
2. Only after the reorganized pipeline reproduces the old outputs, fix the
   known coding and statistical problems.

This order matters. `4c4` is 1,618 lines and mixes published-return
adjustments, data-mined benchmark construction, figures, main-paper tables,
an appendix table, diagnostic exports, and formatting. Refactoring and
correcting all of that at once would make a changed table cell difficult to
attribute.

The intended short name for the main by-group script is
`S4b_RVsDM_ByGroup.R`.

## What `4c4` currently owns

The script reads cleaned published-predictor data, matched data-mined returns,
sample-specific risk-adjusted data-mined returns, factor returns, and the
manual paper classifications. It writes to
`../Results/RiskAdjusted/TstatFilter/`.

The manuscript-relevant six-column outputs are currently:

- `Table_RiskAdjusted_TimeVarying_ff4_t2.{csv,tex}`: source numbers for
  Table 6, by theoretical foundation and equilibrium-model type.
- `Table_RiskAdjusted_TimeVarying_DisciplineJournal_ff4_t2.{csv,tex}`:
  source numbers for Table 7, by discipline and journal rank.
- `Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel_ff4_t2.{csv,tex}`:
  source numbers for Table IA.10.

The same script also emits:

- raw-only versions of the three tables;
- raw, CAPM, and FF4 event-time figures;
- a combined time-varying-alpha summary CSV;
- four raw diagnostic CSVs; and
- unintended duplicate temporary PDFs under `../Results`.

It does not write into the separate writing repository. Tables 6, 7, and
IA.10 are still copied into the manuscript by hand.

## Related workflows that must not be conflated

Several other scripts share inputs, helpers, or methods with `4c4`, but do not
currently produce Tables 6 and 7:

- `3d_Fig2Data.R` and `S2e_Fig2Plots.R` own the in-chain Figure 2 factor
  adjustment panels. This may make the figures emitted by `4c4` redundant,
  but phase one should prove that before deleting them.
- `Appendices/SA09_AccountingOnlyAlphaPlots.R` owns the accounting-only
  risk-adjusted figures.
- `Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R` is a separate,
  similarly structured full-sample workflow. It produces Tables IA.6, IA.7,
  and IA.11 and Figure IA.1. It likely shares some cleanup needs, but it should
  not be silently changed as part of the first `4c4` migration.
- `2d_RiskAdjustDataMinedSignals.R` produces the risk-adjusted data-mining
  cache consumed by all of these workflows.

## Phase one: pipeline cleanup with frozen logic

The defining constraint is that phase one may change filenames, script
boundaries, drivers, preflight checks, and formatting ownership, but not the
sample, normalization, grouping, estimates, standard errors, rounding, or
displayed values.

### 1. Capture a baseline

Run the existing `4c4` from the current processed-data vintage and retain a
manifest of every file it writes. Save the three manuscript-source CSVs and
TeX files as comparison fixtures outside the live output paths. Record runtime
and peak memory if practical.

The baseline should include the actual current filenames. Existing comments,
the exhibit map, the manuscript, and the current export code refer to
different generations of the names.

### 2. Classify outputs before moving code

Assign every output to exactly one of these classes:

- main-paper by-group tables: Tables 6 and 7;
- appendix by-group table: Table IA.10;
- live paper figure owned here;
- figure already owned by the Figure 2 or appendix pipeline;
- diagnostic CSV or plot; or
- accidental temporary file.

Do not preserve a figure merely because `4c4` writes it, but do not delete it
until the exhibit map and the writing repository confirm that another script
owns the live equivalent.

The expected phase-one allowlist is only six files: the three manuscript-source
TeX tabulars and matching CSVs retained for numerical review. Subject to the
baseline check, remove the other 17 outputs and the code that produces them:

- six raw-only table files;
- three figures already superseded by the live Figure 2/appendix pipelines;
- five diagnostic CSVs; and
- three accidental temporary PDFs.

The purpose of the baseline manifest is to make these deletions deliberate and
reviewable, not to preserve every historical side effect of `4c4`.

### 3. Create the focused Section 4 script

Create `S4b_RVsDM_ByGroup.R` for the by-group analysis and the main-paper
Table 6 and Table 7 tabulars. It should have the required run/input/output
header and fail early when a cache is missing.

For phase one, move the existing calculation path mechanically. Avoid
rewriting estimators, combining duplicated blocks, or replacing helper
functions even when the existing implementation is awkward.

The main outputs should be complete `tabular` fragments, from
`\begin{tabular}` through `\end{tabular}`. Captions, labels, explanatory notes,
and table floats remain outside this public analysis repository.

`S4b_RVsDM_ByGroup.R` should not reproduce the raw-only tables, diagnostic
CSVs, plots, or temporary files. Its output contract is Table 6 and Table 7
TeX plus one audit CSV for each table.

### 4. Remove the unused figures

The current paper does not use any of the three PDFs written by `4c4`; the live
factor-adjustment figures are owned by `3d`/`S2e` and the appendix scripts.
Confirm this against the baseline manifest and exhibit map, then delete the
three figure-building paths rather than moving them to another script.

Plot construction should not remain in `S4b_RVsDM_ByGroup.R` merely because
the old monolithic script happened to calculate tables and plots together.

### 5. Separate appendix ownership

Table IA.10 should have an appendix renderer or otherwise be explicitly owned
by the appendix stage. It must consume the same frozen calculation used in the
phase-one baseline; do not create a second implementation of the Any Model
estimates.

Its output contract is one TeX tabular and one matching audit CSV. It should
not preserve the raw-only Any Model table or general diagnostic exports.

The existing full-sample appendix workflow in `SA07` remains separate. This
step concerns the sample-specific Any Model versus No Model table currently
embedded in `4c4`, not the full-sample robustness tables.

### 6. Wire the drivers

Add the focused Section 4 work to the appropriate driver and extend preflight
validation to its real cache inputs. Add any appendix renderer to
`SA_Appendices.R`. The default pipeline should rebuild the source artifacts
without invoking chapter 1 or chapter 2 implicitly.

This is acceptable even though adding the entire current `4c4` to `MAIN.R` is
not: the driver should call the reduced scripts with clear output ownership,
not the monolith.

### 7. Prove no behavioral change

Against the same inputs:

- compare every unrounded CSV cell;
- compare every displayed estimate and standard error in the TeX;
- verify row order, headings, and rounding;
- verify that the six allowlisted outputs remain and the other 17 old outputs
  have no live paper consumer;
- verify that the raw-only tables, figures, and diagnostic CSVs are no longer
  generated;
- confirm that the new scripts do not write unintended temporary PDFs; and
- parse all R files and run the Section 4 and appendix preflights.

Any numerical difference fails phase one, even if the new number appears more
defensible. Record such a difference for phase two instead of fixing it during
the split.

## Phase-two backlog: known logic and coding problems

These findings are intentionally deferred until the phase-one equivalence
check passes:

- The captions say standard errors are clustered by calendar month and
  predictor, but `compute_outperformance()` uses `sd(x) / sqrt(n)`.
- The outperformance standard error is formed as
  `sqrt(pub_oos_se^2 + dm_oos_se^2)`, rather than estimated from the paired
  published-minus-data-mined return with the stated clustering.
- The discipline mapping classifies only the top-three accounting journals as
  Accounting. In the current included sample, nine signals from other
  accounting journals are consequently classified as Finance.
- The final six-column Any Model export averages the Stylized and
  Dynamic/Quantitative subgroup estimates and approximates their standard
  errors, even though an underlying pooled Any Model calculation already
  exists earlier in the script.
- Raw, CAPM, and FF4 samples and filters should be checked directly against the
  paper specification and asserted in code.
- The script checks for `MatchedRiskAdjSummary.RData` but does not read it.
- Several calculations and exports are duplicated, output names are stale or
  inconsistent, and the claimed detailed-export count does not match the live
  writes.
- The plot helper's temporary filename and cleanup filename disagree, leaving
  duplicate PDFs behind.

The full-sample `SA07` fork should receive its own later correctness audit for
the same helper and aggregation patterns. Fixing a shared helper during phase
two must not silently change appendix exhibits without an explicit comparison.

## Deferred cross-repository work

The eventual design should give the analysis repo ownership of generated
`tabular` fragments and the writing repo ownership of floats, captions, labels,
and notes, with an explicit installation/sync step between them. That is not
part of either phase above. Until then, the hand-copy step remains, but its
source files and producer should be unambiguous and in the default analysis
pipeline.

## Phase-one completion condition

The pipeline cleanup is complete when the monolithic `4c4` is gone; exactly the
three manuscript-source TeX tabulars and three matching audit CSVs remain from
its 23-file output set; every retained output has one named owner and is
reachable from a driver; and the new pipeline reproduces the old retained
numerical outputs exactly from the same caches. Only then should the phase-two
correctness changes begin.
