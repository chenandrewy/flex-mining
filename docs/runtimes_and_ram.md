# Script runtimes and memory

How long each script in the `MAIN.R` chain takes and how much RAM it needs, so
you can tell a slow stage from a hung one, and size `ncores` before a run dies
three hours in.

Measured on a run started 2026-08-13 17:48 EDT with `run_downloads = FALSE`,
on 24 cores / 62 GB RAM / **no swap**, `globalSettings$num_cores = 10`,
`dataVersion = 'CZ-style-v8b'`. Timings come from output-file mtimes and the
`source()` lines in the run log, not from instrumentation, so treat them as
±1 minute. The run was OOM-killed in `4c1`, so anything after it is unmeasured.

## Runtimes

| Script | Runtime | Notes |
|---|---|---|
| `0_Environment.R` | < 1 min | Definitions only. Sourced repeatedly by other scripts. |
| `1_Download_and_Clean.R` | not measured | Skipped at `run_downloads = FALSE`. WRDS + Google Drive. |
| `1a_ValidDenoms.R` | ~2 min | Reads `CompustatAnnual.RData`; writes the two `DataIntermediate` CSVs. |
| **`2_DataMining.R`** | **2h 13m** | 17:50 → 20:03. Dominates everything. |
| ↳ `2a_CompustatToLongshort.R` | **2h 02m** | 17:50 → 19:52. See breakdown below. |
| ↳ `2b_MatchDataMinedToPub.R` | ~3 min | → `MatchPub.RData` (380 MB). |
| ↳ `2c_TickerToLongshort.R` | ~2 min | → `ticker_Harvey2017JF.RDS`. |
| ↳ `2d_RiskAdjustDataMinedSignals.R` | ~6 min | → `MatchPubRiskAdjusted.RData` (2.35 GB). |
| **`3_RiskVsMispricing.R`** | **~2 min** | All of `3a`–`3g`. Cheap because stage 2 did the work. |
| **`4_ResearchVsDataMining.R`** | **partial** | Crashed in `4c1`. |
| ↳ `4a_DataMiningSummary.R` | ~20 min | Emits CSVs only. Slow but not a cache producer. |
| ↳ `4b_DMCorrelationsPCASummary.R` | ~11 min | Its in-code comment says "around an hour or so" — stale. Produces `DM_pca.tex`, a used exhibit. |
| ↳ `4c1_ResearchVsDMprep.R` | ~4 min | **OOM-killed** this run; the 4 min figure is from an earlier run the same day. Not slow, but spikes hard. |
| ↳ `4c2` … `4e` | unmeasured | Never reached. |
| `6_TextAnalysis.R` | unmeasured | Never reached. |
| `8_DMThemes.R` | unmeasured | Never reached. |
| `99_ExportDataToCsv.R` | unmeasured | Never reached. |

### Inside `2a_CompustatToLongshort.R`

| Phase | Runtime |
|---|---|
| `foreach` over 29,315 signals (10 workers) | ~89 min, ~3.1 min per 1000 signals |
| `.combine = rbind` + compressed `saveRDS` | ~33 min |

Worth knowing: after the progress log in `../Data/make_many_ls.log` stops at
the final signal, there is still half an hour of silent, single-threaded work
before the script returns. That is normal, not a hang. Confirm with `ps` — the
master should be at ~100% of one core.

## Memory

The 2026-08-13 run was **OOM-killed** in `4c1_ResearchVsDMprep.R`: exit code
137 (SIGKILL), `oom_kill 1` in `/sys/fs/cgroup/memory.events`, `memory.peak`
61.6 GiB against 62 GB with no swap. The victim was the master R process, not
a worker. Full write-up: `docs/journal/0813d,ram-issues.md`.

### A high floor plus a spike, not a leak

Master RSS was flat at **19.9 GB** across four stage transitions (+23 MB over
an hour), so nothing was accumulating. Two separate effects:

- **The floor.** `2a` grows the R heap to service its peak. `rm(list = ls())`
  at the top of `2b` and `2d` frees the objects, but R does not reliably
  return pages to the OS, so RSS stays at the high-water mark for the life of
  the process. Because `MAIN.R` `source()`s everything into one long-lived R
  session, every later script inherits `2a`'s ~20 GB floor.
- **The spike.** `make_DM_event_returns()` (`0_Environment.R:1264`) starts a
  PSOCK cluster. PSOCK workers are separate processes, not forks, so there is
  no copy-on-write sharing: `foreach` serializes a full copy of the return
  panel to each worker. Ten workers on a 42 GB headroom is roughly 4 GB each,
  and it lost.

### Sizing `ncores`

**Choose `ncores` from available RAM, not from core count.** The machine has
24 cores, but cores are not the binding constraint — copies of the return
panel are.

Budget **~5 GB per worker** for anything that calls
`make_DM_event_returns()` or `adj_R2_with_PPCA()`:

| Workers | RAM needed | Fits in 62 GB with a 20 GB floor? |
|---|---|---|
| 4 | **~20 GB** | yes, ~22 GB slack — **recommended** |
| 6 | ~30 GB | tight |
| 10 | ~50 GB | no — this is what died |

So: **4 workers, implying about 20 GB of RAM.** On a machine with more free
memory, scale up proportionally; on a smaller one, down. The per-worker figure
is derived from the crash, not measured — one `object.size(dm_rets)` would
replace the estimate with a fact.

Cost of the reduction is small: `4c1` is ~4 min at 10 workers, expect ~8–10 at
4. Irrelevant next to a crashed run.

**Do not lower `globalSettings$num_cores` globally.** That is the knob `2a`
uses, and `2a` is memory-light by design — it writes `../Data/tmpAllDat.fst`
so workers read slices from disk instead of receiving copies
(`2a_CompustatToLongshort.R:238`). Dropping the global setting would add real
time to the two-hour script for no benefit. Set the lower value on the
`ncores` default of the affected helpers instead.

### Which scripts carry the risk

Grepping stage B for `readRDS(...LongShort)`, `makePSOCKcluster`,
`make_DM_event_returns`, and `adj_R2_with_PPCA`:

| Hits | Script |
|---|---|
| 7 | `4e_DM_Span_PCA.R` |
| 3 | `4c20a_Fig2Data.R` |
| 2 | `4c3_ResearchVsAcctVsTicker.R` |
| 1 | `4c2_ResearchVsDMPlots.R` |
| 1 | `4c1_ResearchVsDMprep.R` (the one that crashed) |

None of the others touch the pattern. Changing the default in the shared
helper covers all five at once.

### Durable fixes

1. Adopt the `fst` pattern from `2a:238` in `make_DM_event_returns()` so
   workers read slices instead of receiving copies. Makes cost independent of
   core count.
2. Run each chapter as its own `Rscript` process rather than `source()`ing
   into one session. Memory resets at every boundary, so `4c1` would start
   from ~1 GB instead of inheriting `2a`'s 20 GB — with ~60 GB of headroom,
   even 10 workers would fit.
3. Collapse the double `readRDS(DMname)` in `make_DM_event_returns()`, which
   deserializes the entire `stratdat` list twice to pull two elements.

## Rules of thumb

- `2a` is ~77% of a full run. Everything else together is well under an hour.
- Nothing prints during `2a`'s combine/save phase or `4c1`'s parallel section.
  Silence is not failure; check `ps` before concluding anything.
- `../Data/make_many_ls.log` carries `2a`'s own progress and ETA.
- With no swap, there is no thrashing phase to warn you. The run works, then
  it is gone.
