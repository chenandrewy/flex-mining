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

Chapter 3 is the memory-critical part of the pipeline. An earlier design that
kept a single R process alive across all stages peaked near the machine limit
(about 62 GB) and was terminated by the out-of-memory killer during the work
now in `3a_ResearchVsDMPrep.R`. The current design keeps memory bounded in three
ways:

1. `MAIN.R` launches each chapter as a separate `Rscript` process, and
   Chapters 2-8 launch each child script the same way. Memory returns to the
   operating system at every chapter and child boundary, so a later stage never
   inherits an earlier stage's allocator high-water mark.
2. On Unix, `make_DM_event_returns()` and `adj_R2_with_PPCA()` use the multicore
   (fork) backend, so workers share the read-only mined-return panel through
   copy-on-write instead of each holding a copy. On non-Unix systems they fall
   back to a PSOCK cluster capped at two workers.
3. Both helpers deserialize the large mined-strategy RDS once, extract the
   members they need, and drop the enclosing object.

Forked workers can still allocate private memory while joining and aggregating
data, so these measures reduce the dominant duplication but do not make memory
use free. See [Sizing parallelism](#sizing-parallelism) for how to choose
`num_cores` against available RAM.

### Scripts to monitor

These scripts call the large-panel helpers and are the ones to watch for peak
memory:

| Script | Large-panel work |
|---|---|
| `3a_ResearchVsDMPrep.R` | Three `make_DM_event_returns()` calls. |
| `3d_Fig2Data.R` | Two `make_DM_event_returns()` calls. |
| `3e_DMSpanPCA.R` | One `adj_R2_with_PPCA()` call and six `make_DM_event_returns()` calls. |

`4c3_ResearchVsAcctVsTicker.R` also calls `make_DM_event_returns()` but is not
in the default `MAIN.R` chain.

### Sizing parallelism

Parallel work uses `globalSettings$num_cores`. A conservative planning heuristic
is to budget roughly 5 GB of RAM per worker and keep the total within available
memory: for example, 4 workers on a machine with about 20 GB free. The default
`num_cores = 4` follows this rule.

Treat this as an upper-bound rule of thumb rather than a measured requirement.
Under the Unix fork backend the large read-only panel is shared across workers
instead of copied, so actual per-worker use is often lower; the budget mainly
covers the private memory a worker allocates while joining and aggregating.

Rather than lowering `num_cores` further pre-emptively, measure peak memory
under the current fork-based implementation and reduce parallelism only where a
specific helper approaches the machine limit, by passing a smaller `ncores` to
that helper. This keeps the multi-hour `2a` stage at full core count.

To measure, run one script in a fresh process:

```bash
time Rscript 3a_ResearchVsDMPrep.R
```

`time` reports elapsed time; peak resident memory needs a separate monitor
(for example `/usr/bin/time -v` where available, or sampling the process while
it runs). Record new memory figures against the current process-per-chapter
design rather than the earlier single-process numbers.

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
