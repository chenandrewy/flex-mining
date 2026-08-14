# Local-code review before merging

Snapshot: 2026-08-14 09:50 EDT.

Scope: the code on the cleanup branch considered as a self-contained codebase.
This review deliberately ignores divergence from upstream `main` and merge
conflicts. No fixes were made as part of the review.

The overnight pipeline completed with status 0, and all tracked R files parsed.
That is useful evidence that the orchestration runs, but it does not catch the
logic errors below.

## Dead code and incomplete cleanup

### Cleanup already underway

At the time of this snapshot, deletion of these files is staged:

- `4c3_ResearchVsAcctVsTicker.R`: dead standalone workflow. Its useful Figure
  2 panel-(d) calculation already lives in `3d_Fig2Data.R`.
- `install_pcamethods.R`: uncalled package installer. Installation instructions
  have been moved into `docs/environment.md`.

The stale `4c3` references in `3d_Fig2Data.R` and `docs/runtimes_and_ram.md` are
also being removed in the working tree.

### Permanently disabled legacy renderers remain in active scripts

The pipeline refactor separated expensive calculation from cheap exhibit
rendering. For example, `3b_DataMiningSummary.R` now writes reusable CSV caches,
and `S2b_DataMiningSummaryTables.R` reads those caches and writes the TeX table.
Before that split, `3b` did both jobs in one process.

The old rendering code was not removed during the split. Instead, four scripts
set a local constant, `render_legacy <- FALSE`, and wrap the old output code in
`if (render_legacy)`. Because the flag is hard-coded inside each script, is not
part of `config.R`, and is never changed by a driver, those branches are
unreachable in every supported run:

- `3b_DataMiningSummary.R`
- `3c_DMCorrelationsPCA.R`
- `3e_DMSpanPCA.R`
- `S3a_MPStyleDecayModels.R`

What they duplicate:

- `3b` still contains the old construction of `dm-sortsFull.tex`; `S2b` is now
  the live renderer for that table.
- `3c` still contains the old `quantilesCorDM.tex` and `DM_pca.tex` writers;
  `S2c` is now the live renderer.
- `3e` still contains the old calls that save the two spanning PDFs;
  `Appendices/SA11_DMSpanPCAPlots.R` is now the live renderer.
- `S3a` still contains old `fixest::etable()` calls; it now saves fitted models
  and `S3b_MPStyleDecayTables.R` is the live table renderer.

Thus the false branches do not provide a fallback path or a user-selectable
equivalence check. They are frozen copies of output code that now has a second,
live implementation elsewhere. Keeping both makes future output-format changes
easy to apply to the wrong copy.

`3e_DMSpanPCA.R` also constructs legacy ggplot and xtable objects outside those
false branches, even though it does not save the legacy plots. That is active
but discarded work inside an expensive precomputation stage.

### Unreferenced shared helpers

Static reference checks found several top-level helper functions with no caller
in the live R code, including:

- `calculate_alpha_shrinkage`
- `f.describe_numeric`
- `make_signal_list_yz`
- `nchoose2ports`
- `process_event_time_returns`
- `read_fst_yearm`
- `aggregate_dm_no_norm` and `aggregate_dm_no_norm_fs`

There may be more. These should be verified and deleted or moved out of the
always-sourced helper set. Every script that sources `0_Environment.R` currently
loads all of them.

## Smaller cleanup findings

- `3e_DMSpanPCA.R` sources `0_Environment.R` three times in one process;
  `S4a_DataCounts.R` sources it twice. Besides being redundant, each source
  reloads configuration and resets the RNG seed.
- The unused `f.describe_numeric()` labels the 2nd percentile as `q5`.
- The generated risk-versus-mispricing table says `Banz 2981` rather than
  `Banz 1981`.
- The archived `CodeArchive/text-analysis/6_TextAnalysis.R` wrapper has source
  paths that work only when run from its own directory, contrary to the active
  repository convention of running from the repository root. The workflow is
  archived and also lacks its optional NLP dependencies, so this is low
  priority.

## For after merge

The issues below are known analysis or reproducibility gaps, but they are not
part of the cleanup needed before this branch merges. Address them in a later
pass rather than expanding the current refactor.

### Fix `f.custom.t()`

`helpers/stats.R` currently has:

```r
if(length(x[!is.na(x)]) > 1 & sd(x[!is.na(x)] > 1e-8))
```

The comparison is inside `sd()`. It therefore takes the standard deviation of
a logical vector rather than checking whether the standard deviation of `x` is
larger than the tolerance. A varying, all-positive vector such as `c(1, 2, 3)`
returns `NaN`; the function tends to run only when observations occur on both
sides of zero.

This function feeds rolling t-statistics in:

- `3b_DataMiningSummary.R`
- `3c_DMCorrelationsPCA.R`
- `Appendices/SA05_DecayVsModelcountPlot.R`

Fixing it changes cached statistics and at least one appendix calculation, so
the affected caches and exhibits should be regenerated and checked as part of
the later analysis pass.

### Complete the Section 4 preflight validation

`S4_Heterogeneity.R` checks only:

- `czsum_allpredictors.RDS`
- `czret_keeponly.RDS`

But `S4a_DataCounts.R` later reads the versioned `LongShort.RData` and
`DataIntermediate/freq_obs_1963.csv`. A standalone Section 4 run can pass the
driver's preflight and fail much later. Its declared inputs and required-file
check should cover what the child actually reads.

### Reconcile the Section 3 event-period definitions

In `S3a_MPStyleDecayModels.R`, the main published/averaged-DM regressions use
overlapping indicators:

```r
postSample = calendarDate >= sampend
postPub    = calendarDate >= pubdate
```

Here `postPub` is an additional post-publication effect on top of the
post-sample effect.

The individual-DM regressions later use mutually exclusive indicators:

```r
postSample = calendarDate >= sampend & calendarDate < pubdate
postPub    = calendarDate >= pubdate
```

The nearby comment says these definitions match the main regression, but they
do not. The individual-DM `postPub` coefficient is a post-publication level,
whereas the main `postPub` coefficient is an additional change. Later work
should choose the intended parameterization and use it consistently before
comparing the models or publishing the tables.

### Bring `4c4` and its hand-copied tables into the pipeline

`4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R` is 1,618 lines and is not reachable
from `MAIN.R` or any section driver. It is not dead: its outputs supply numbers
that are manually pasted into:

- Table 6 (`tab:hetero-bytheory`)
- Table 7 (`tab:hetero-byjournal`)
- Table IA.10 (`tab:hetero-model`)

Later work should extract the calculations actually needed for those three
tables, give them an explicit stage/renderer, and write paper-ready TeX instead
of relying on manual copying. The goal is not simply to add the entire current
`4c4` script to `MAIN.R`; its scope should be reduced first.

### Automate the other hand-modified exhibits

The exhibit map records five additional tables that are not produced directly
by the pipeline:

- Tables 3 and 4 (`HandTable_MPStyleRegsMain.tex` and
  `HandTable_MPStyleRegsTimeFE.tex`)
- the draft tolerance-matched versions of Tables 3 and 4
  (`HandTable_MPStyleRegsMain_Tol30.tex` and
  `HandTable_MPStyleRegsTimeFE_Tol30.tex`)
- Table IA.9, whose values currently come from an Excel sheet

The four Section 3 tables are hand-transcribed from `S3b` output. Later work
should make `S3b` emit their final six-column manuscript layouts directly. The
Table IA.9 spreadsheet calculation needs a separate decision: either encode it
in a reproducible script or document it explicitly as an external input with a
stable installation step.

Together with the three `4c4` tables, these are the eight hand-formatted,
hand-transcribed, or externally maintained paper tables listed in
`docs/exhibit_map.md`.

## Suggested order

Before merge:

1. Complete the `4c3`/installer cleanup already underway.
2. Remove disabled renderers, discarded plot construction, redundant
   environment reloads, and confirmed-unused helpers.

After merge, address all correctness findings, then handle `4c4` and all eight
hand-maintained exhibits as a separate reproducibility project.
