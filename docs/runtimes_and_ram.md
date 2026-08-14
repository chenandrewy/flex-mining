# Script runtimes and memory

How long each script in the `MAIN.R` chain takes and how much RAM it needs, so
you can tell a slow stage from a hung one and size parallel work safely.

The measurements below come from a run started 2026-08-13 17:48 EDT on 24
cores / 62 GB RAM / **no swap**, with `globalSettings$num_cores = 10` and
`dataVersion = 'CZ-style-v8b'`. Timings came from output-file mtimes and the
run log rather than instrumentation, so treat them as ±1 minute.

That run used the pipeline structure that preceded the current Chapters 1-9.
The names below map the measured work onto the current scripts. The run was
OOM-killed during the work now performed by `3a_ResearchVsDMPrep.R`, so later
work remains unmeasured. The OOM mechanism has since been mitigated; see
[Memory](#memory).

## Current pipeline

`MAIN.R` starts every chapter as a separate `Rscript` process. Chapters 2-8
also start every child script as a separate process. Consequently, memory is
returned to the operating system at both chapter and child-script boundaries.

| Chapter | Job | Runtime evidence |
|---|---|---|
| `1_Download_and_Clean.R` | Download and clean a new data vintage | Not measured; skipped in the benchmark run. |
| `2_DataMining.R` | Construct, match, and risk-adjust mined strategies | **2h 13m measured**. |
| `3_Precompute.R` | Build reusable summaries, event panels, PCA results, and regression caches | Partially measured; details below. |
| `4_ResearchVsDataMining.R` | Introduction and Section 2 exhibits | Not measured after the refactor. Reads upstream data without modifying processed caches. |
| `5_Learning.R` | Section 3 learning tables | Not measured after the refactor. Renders cached regression models. |
| `6_Heterogeneity.R` | Section 4 exhibits | Not measured after the refactor. |
| `7_BestPredictors.R` | Section 4b inspect tables | Not measured after the refactor. |
| `8_Appendices.R` | Appendix-only exhibits and diagnostics | Not measured after the refactor. |
| `9_ExportDataToCsv.R` | Shared-data CSV exports | Not measured. |

## Runtimes

| Current script | Runtime | Notes |
|---|---:|---|
| `0_Environment.R` | < 1 min | Definitions only; sourced separately by many scripts. |
| `1_Download_and_Clean.R` | not measured | Network-dependent and deliberately skipped in the benchmark run. |
| `1a_ValidDenoms.R` | ~2 min | Reads `CompustatAnnual.RData`; writes two `DataIntermediate` CSVs. |
| **`2_DataMining.R`** | **2h 13m** | 17:50 → 20:03. Dominates the measured runtime. |
| ↳ `2a_CompustatToLongshort.R` | **2h 02m** | 17:50 → 19:52. See the phase breakdown below. |
| ↳ `2b_MatchDataMinedToPub.R` | ~3 min | Writes `MatchPub.RData` (380 MB in the measured vintage). |
| ↳ `2c_TickerToLongshort.R` | ~2 min | Writes `ticker_Harvey2017JF.RDS`. |
| ↳ `2d_RiskAdjustDataMinedSignals.R` | ~6 min | Writes `MatchPubRiskAdjusted.RData` (2.35 GB in the measured vintage). |
| **`3_Precompute.R`** | partial | The benchmark run died during work now assigned to `3a`. |
| ↳ `3a_ResearchVsDMPrep.R` | ~4 min on an earlier run | Previously `4c1_ResearchVsDMprep.R`; the benchmark run was OOM-killed here. Runtime under the current process and multicore design is not yet measured. |
| ↳ `3b_DataMiningSummary.R` | ~20 min | Previously the calculation half of `4a_DataMiningSummary.R`. |
| ↳ `3c_DMCorrelationsPCA.R` | ~11 min | Previously `4b_DMCorrelationsPCASummary.R`; the old “around an hour” comment was stale. |
| ↳ `3d_Fig2Data.R` | not measured | Builds the Figure 2 cache. |
| ↳ `3e_DMSpanPCA.R` | not measured | Runs PCA/spanning and several event-return calculations. |
| ↳ `3f_MPStyleDecayModels.R` | not measured | Estimates the MP-style regressions cached for Chapter 5. |
| **Chapters 4-8** | not measured | Rendering chapters; each child gets a fresh R process. |
| **`9_ExportDataToCsv.R`** | not measured | Reads chapter-2 caches and writes `../Data/Export`. |

### Inside `2a_CompustatToLongshort.R`

| Phase | Runtime |
|---|---:|
| `foreach` over 29,315 signals (10 workers) | ~89 min, or ~3.1 min per 1,000 signals |
| `.combine = rbind` plus compressed `saveRDS` | ~33 min |

After the progress log in `../Data/make_many_ls.log` reaches the final signal,
there can still be roughly half an hour of silent, single-threaded combine and
save work. That was normal in the measured run, not a hang. Confirm with `ps`:
the master should use roughly one full core during this phase.

## Memory

### Historical OOM

The benchmark run was OOM-killed in the predecessor of
`3a_ResearchVsDMPrep.R`: exit code 137 (SIGKILL), `oom_kill 1` in
`/sys/fs/cgroup/memory.events`, and a 61.6 GiB peak on a 62 GB machine with no
swap. The old pipeline kept one R process alive across all stages, so it
carried a 19.9 GB allocator high-water mark out of `2a`. It then used a
10-worker PSOCK cluster that serialized a multi-GB mined-return panel to every
worker. Full historical diagnosis:
`docs/journal/0813d,ram-issues.md`.

### Current mitigations

The current code addresses all three contributors to that failure:

1. `MAIN.R` launches each chapter with `Rscript`, and Chapters 2-8 launch each
   child the same way. A later stage no longer inherits `2a`'s memory floor.
2. On Unix, `make_DM_event_returns()` and `adj_R2_with_PPCA()` use the multicore
   backend, allowing workers to share the read-only mined-return panel through
   copy-on-write. On non-Unix systems they retain a PSOCK fallback capped at
   two workers.
3. Both helpers deserialize the large mined-strategy RDS once, extract the
   needed members, and remove the enclosing object. The old code deserialized
   the file twice.

These changes remove the basis for the old “~5 GB per worker plus a 20 GB
floor” sizing table. That table should not be used for the current Linux
pipeline. Forked workers can still allocate private memory while joining and
aggregating data, so the fix reduces the dominant duplication but does not
make memory use free.

### Scripts to monitor

The following current scripts call the large-panel helpers:

| Script | Large-panel work |
|---|---|
| `3a_ResearchVsDMPrep.R` | Three `make_DM_event_returns()` calls. |
| `3d_Fig2Data.R` | Two `make_DM_event_returns()` calls. |
| `3e_DMSpanPCA.R` | One `adj_R2_with_PPCA()` call and six `make_DM_event_returns()` calls. |

`4c3_ResearchVsAcctVsTicker.R` also calls `make_DM_event_returns()`, but it is
not in the default `MAIN.R` chain.

The current defaults use `globalSettings$num_cores`. On this Linux sandbox,
do not lower that setting solely because of the historical PSOCK estimate.
Instead, measure peak memory under the current fork-based implementation. If a
specific helper still approaches the machine limit, pass it a smaller
`ncores` value locally so the two-hour `2a` stage is not slowed unnecessarily.

For a clean elapsed-time measurement, run one script in a fresh process, for
example:

```bash
time Rscript 3a_ResearchVsDMPrep.R
```

Peak RSS requires separate process monitoring in the current container because
GNU `/usr/bin/time` is not installed. When recording new memory measurements,
do not combine them with the historical long-lived-process figures above.

## Rules of thumb

- `2a` is the only firmly measured multi-hour script and dominates the known
  runtime.
- Silence during `2a`'s combine/save phase or a parallel event-return section
  is not by itself evidence of a hang; check the process before stopping it.
- `../Data/make_many_ls.log` carries `2a`'s own progress and ETA.
- Chapters 4-8 can be run independently for paper iteration, provided their
  upstream caches exist.
- With no swap, memory exhaustion has no slow thrashing phase: monitor peak
  RSS during the still-unmeasured Chapter 3 scripts.
