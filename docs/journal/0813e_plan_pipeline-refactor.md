# Plan: four-chapter pipeline refactor

Snapshot written 2026-08-13 21:49 EDT. This records the agreed direction after
reviewing `docs/runtimes.md`; it is a plan, not an implementation log.

## Goal

Make the execution order reflect computational cost and cache boundaries. A
user changing a plot or table should be able to regenerate every exhibit
without repeating mining, correlations, PCA, event-return construction, or
other expensive analysis.

The top-level pipeline will have four chapters:

| Chapter | Job | Main outputs | Expected cost |
|---|---|---|---|
| `1_Download_and_Clean.R` | Acquire the external data and clean it into foundational inputs | `../Data/Raw` and the cleaned inputs under `../Data/Processed` | Network-dependent; run for a new data vintage |
| `2_DataMining.R` | Construct, match, and risk-adjust the mined strategies (`2a`-`2d`) | Core mined-strategy and matched-return datasets under `../Data/Processed` | Hours |
| `3_Precompute.R` | Perform slow or memory-intensive reusable analysis | Exhibit-ready caches under `../Data/Processed` | Minutes to potentially hours |
| `4_Exhibits.R` | Read caches and render the paper outputs | PDFs, TeX, and final exports under `../Results` | Fast |

Use the spelling `Precompute`, not `PreCompute`.

## Chapter contracts

1. Chapters 1-3 may write data under `../Data/Processed`.
2. Chapter 4 treats `../Data/Processed` as read-only and writes deliverables
   under `../Results`.
3. Intermediate RDS and CSV files are data, not results. Move intermediates
   currently stored in `../Results` into `../Data/Processed`.
4. Chapter 4 must never trigger chapters 2 or 3 implicitly. If a required
   cache is missing, fail early with the cache name and the chapter that builds
   it. In particular, exhibit scripts must not source `2d` as a fallback.
5. Each cache has exactly one producer. Each precompute script documents its
   inputs and outputs, and each exhibit script documents the caches it reads.
6. Chapter 1 continues to own both download and cleaning. Network operations
   remain explicitly guarded so that cleaning existing raw files does not
   accidentally replace the data vintage.

## Initial script allocation

### Chapter 1: download and clean

Keep acquisition and cleaning in the same chapter. This includes the current
Google Drive and WRDS work, the CZ/CRSP/Compustat cleaning now interleaved in
`1_Download_and_Clean.R`, and `1a_ValidDenoms.R`.

The important safety property is unchanged: refreshing `../Data/Raw` is an
explicit action because WRDS does not provide an as-of rebuild of the current
vintage.

### Chapter 2: data mining

Keep the current `2_DataMining.R` chain together:

- `2a_CompustatToLongshort.R`
- `2b_MatchDataMinedToPub.R`
- `2c_TickerToLongshort.R`
- `2d_RiskAdjustDataMinedSignals.R`

This is already a coherent build stage. `2a` dominates the observed runtime
at about two hours; the other three scripts complete the mined-data products
that downstream analysis expects.

### Chapter 3: precompute

Move reusable computation here, including work that is short in wall time but
has a dangerous memory spike.

Known members:

- Current `4c1_ResearchVsDMprep.R`. It is already a cache builder and writes
  `dmcomp_sumstats.RDS`, `dmtic_sumstats.RDS`, `plotdat0.RDS`, and the
  `ret_for_plot*.RDS` files. Its `make_DM_event_returns()` memory problem must
  be fixed before treating the new pipeline as reliable.
- Current `4c20a_Fig2Data.R`, which already builds and saves the Figure 2 long
  and aggregate panels separately from plotting.
- The calculation half of current `4a_DataMiningSummary.R`. Save its summary
  data as a processed-data cache; leave only LaTeX rendering in chapter 4.
- Both expensive calculations in current
  `4b_DMCorrelationsPCASummary.R`: pairwise correlations and PCA. Save both
  result sets under `../Data/Processed`; leave table formatting in chapter 4.
- The correlation, PCA, spanning, and event-return calculations in current
  `4e_DM_Span_PCA.R`. Save plot-ready series and table data; leave plotting and
  formatting in chapter 4.

Candidates that require timing or inspection before final allocation:

- `4c3_ResearchVsAcctVsTicker.R`
- `4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R`
- `4c5_FullSampleRiskAdjustedResearchVsDMPlots.R`
- `4c6_MPStyleDecayTables.R`
- `8a_EZThemes.R` and `8b_EZThemesRobustness.R`

These scripts are unmeasured in `docs/runtimes.md`, and several perform
substantial analysis despite also emitting an exhibit. Split any meaningful
calculation from its renderer rather than classifying the whole mixed script
as an exhibit script.

The operative test is: if changing fonts, labels, table formatting, or plot
aesthetics repeats a meaningful statistical calculation, the chapter-3/4
split is incomplete.

### Chapter 4: exhibits

Move the current fast chapter 3 (`3a`-`3g`) here. Also include:

- The rendering half of the current `4a`.
- The correlation-quantile and PCA table renderers from the current `4b`.
- Current `4c2_ResearchVsDMPlots.R`.
- Current `4c20b_Fig2Plots.R`.
- Plot and table renderers split from the other retained chapter-4 scripts.
- The retained theme-table renderers from current chapter 8.
- Final CSV exports, either at the end of this driver or through the existing
  clearly separate `99_ExportDataToCsv.R` utility.

Only paths needed for the paper's 53-exhibit contract, plus explicitly retained
data exports, belong in the default driver. Audit apparent dead ends and
superseded variants instead of carrying them forward automatically.

## Proposed cache products

Names can be refined during implementation, but chapter 3 should leave a
legible set resembling:

```text
../Data/Processed/
  dmcomp_sumstats.RDS
  dmtic_sumstats.RDS
  plotdat0.RDS
  ret_for_plot0.RDS
  ret_for_plot1.RDS
  ret_for_plot_MaxPredictors.RDS
  dm_oos_summary.RDS
  dm_pairwise_correlations.RDS
  dm_pca_summary.RDS
  fig2_panel_long.RDS
  fig2_panel_agg.RDS
  dm_span_analysis.RDS
```

The exact set should be driven by consumers, not by preserving temporary
objects from the old scripts.

## Migration sequence

1. **Measure the unmeasured tail.** Add per-script elapsed-time and peak-memory
   evidence for the scripts after `4c1`, using the existing chapter-2 cache so
   this does not repeat `2a` unnecessarily.
2. **Write the dependency manifest.** For every retained script, record inputs,
   cache outputs, and final exhibits. Resolve the unclear ownership of files
   such as `../Data/9a_DMThemes*.RData` before moving code.
3. **Split mixed scripts without renaming them.** Start with `4a`, `4b`, and
   `4e`. Make the computational and rendering halves explicit while minimizing
   behavioral changes.
4. **Fix unsafe precomputation.** Repair the `4c1` parallel memory duplication
   and audit the similar `adj_R2_with_PPCA()` path before relying on chapter 3.
5. **Normalize cache locations.** Move intermediate data products out of
   `../Results`, update their consumers, and add fail-fast input checks.
6. **Create the new drivers and renumber scripts.** Add `3_Precompute.R` and
   `4_Exhibits.R`, move the old fast chapter-3 scripts under chapter 4, and
   resolve collisions such as the two current `4c6` names.
7. **Update `MAIN.R`.** Give the four stages independent switches. Preserve a
   conspicuous guard around external downloads and document the normal
   iteration path: build chapters 1-3 when inputs or analysis change, then run
   chapter 4 freely.
8. **Validate against the artifact contract.** Starting from the same processed
   data, compare the reorganized pipeline with the existing outputs and verify
   that it rebuilds all 53 paper exhibits, apart from any explicitly documented
   hand-maintained artifact.
9. **Run in layers.** Test chapter 4 from existing caches first, then chapter 3
   plus chapter 4, and reserve a clean chapters-1-through-4 run for final
   validation because chapter 2 costs hours.

Do the behavioral splits before the broad renaming so failures can be
attributed to computation changes rather than to a large mechanical move.

## Suggested `MAIN.R` controls

```r
run_download_and_clean <- FALSE
run_data_mining        <- FALSE
run_precompute         <- TRUE
run_exhibits           <- TRUE
```

If download and cleaning need separate operational controls for safety, keep
them as internal options within chapter 1 rather than creating another
top-level chapter.

## Acceptance criteria

- Chapter 4 runs successfully from a complete processed-data cache without
  invoking expensive upstream work.
- A formatting-only exhibit change does not modify `../Data/Processed`.
- Chapters 2 and 3 can be skipped independently when their outputs are current.
- Missing or stale caches produce actionable errors rather than implicit
  rebuilds.
- The default chain accounts for all 53 paper exhibits and its explicit data
  export deliverables.
- No intermediate analysis data remains ambiguously owned under `../Results`.
- Runtime documentation describes the new chapter boundaries and identifies
  both long-running and high-memory stages.
