# Script runtimes and memory

How long each script in the `MAIN.R` chain takes and how much RAM it needs, so
you can tell a slow stage from a hung one and size parallel work safely.

The measured baseline below comes from a full end-to-end run of Chapters 2-9 on a
24-core / 62 GB machine with **effectively no swap**, using
`globalSettings$num_cores = 4` and `dataVersion = 'CZ-style-v8b'`. Total wall
time is **~6h 14m**. Runtimes come from output-file mtimes and a stage monitor;
treat sub-minute figures as approximate. Chapter 1 re-pulls external data and is
run only when refreshing the vintage, so it is not part of these figures. Later
pipeline refactors moved a few minutes of work between stages; no newer full-run
total has been measured.

## Current pipeline

`MAIN.R` starts every stage as a separate `Rscript` process. Chapters 2-3 and
Sections S2-SA also start every child script as a separate process.
Consequently, memory is returned to the operating system at both stage and
child-script boundaries.

## Runtimes

Measured at `num_cores = 4`.

| Current script                      |             Runtime | Notes                                                                                                                                                                                                                                                                                          |
| ----------------------------------- | ------------------: | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `0_Environment.R`                   |             < 1 min | Definitions only; sourced separately by many scripts.                                                                                                                                                                                                                                          |
| `1_Download_and_Clean.R`            |        not measured | Network-dependent; run only to refresh the data vintage.                                                                                                                                                                                                                                       |
| `1a_ValidDenoms.R`                  |              ~2 min | Reads `CompustatAnnual.RData`; writes two `DataIntermediate` CSVs.                                                                                                                                                                                                                             |
| **`2_DataMining.R`**                | needs remeasurement | Now owns only construction of the two mined universes; the prior total included matching and risk adjustment.                                                                                                                                                                                 |
| ↳ `2a_CompustatToLongshort.R`       |         **~4h 05m** | See the phase breakdown below.                                                                                                                                                                                                                                                                 |
| ↳ `2c_TickerToLongshort.R`          |            ~1-2 min | Writes `ticker_Harvey2017JF.RDS`.                                                                                                                                                                                                                                                              |
| **`3_Precompute.R`**                | needs remeasurement | Contains raw benchmark prep, DM summaries, and both broad-universe factor-adjustment regimes.                                                                                                                                                                                                  |
| ↳ `3a_PrepDMBenchmarks.R`           |          ~9 min 20 s | Four-worker measurement; owns raw accounting/ticker variants, the matched event-time panel, and compact matched-uncorrelated pairs.                                                                                                                                                             |
| ↳ `3b_DataMiningSummary.R`          |             ~24 min |                                                                                                                                                                                                                                                                                                |
| ↳ `3c_FactorAdjustedDMPrep.R`       |          ~3 min 50 s | Fits sample-specific CAPM/FF4 and full-sample CAPM/FF3 across 127 windows. Both passes share one mined-return matrix and write 3.8 MB and 3.9 MB contracts.                                                                                                                                     |
| Appendix SA11 PCA preparation       | ~66 min historical | The former Chapter 3 correlation/PCA and PCA-span stages now run only with `SA_Appendices.R`.                                                                                                                                                                                                  |
| **Sections S2-S4**                  | **~7 min baseline** | Main-text exhibit stages; each child is a fresh R process reading upstream caches. Section 3 estimates the main MP-style decay models in `S3a_MPStyleDecayModels.R` (writes `mp_style_decay_models.RDS`, ~31 MB), then `S3b_MPStyleDecayTables.R` renders them. Appendix runtimes are listed separately because SA11 dominates them.                               |
| **`9_ExportDataToCsv.R`**           |              ~1 min | Reads chapter-2 caches; writes `../Data/Export`.                                                                                                                                                                                                                                               |

### Inside `2a_CompustatToLongshort.R`

Single parallel `foreach` over 29,315 signals, then a single-threaded combine
(`.combine = rbind`) and compressed `saveRDS`.

| Phase                                              |                                                     Runtime (`num_cores = 4`) |
| -------------------------------------------------- | ----------------------------------------------------------------------------: |
| `foreach` dispatch over 29,315 signals (4 workers) | ~214 min. Per-1,000 pace starts ~10 min and improves to ~7.3 min as it warms. |
| Straggler workers + combine + compressed `saveRDS` |                                                                       ~31 min |

`2a` goes quiet twice without hanging. The progress counter in
`../Data/make_many_ls.log` tracks dispatch, not completion, so once it reaches the
last signal a few workers keep all cores near 100% for another 25-30 min finishing
heavy signals assigned earlier; the log stops advancing but `ps` shows the workers
busy. When they finish, the master does a single-threaded `rbind` and compressed
save on roughly one core, rewriting `LongShort.RData` (~313 MB in this vintage).

At `num_cores = 10`, `2a` runs in roughly half the time; see
[Sizing parallelism](#sizing-parallelism).

## Memory

Chapter 3 is the memory-critical part of the pipeline. An earlier single-process
design kept one R process alive across all stages, peaked near the machine limit
(~62 GB), and could be terminated by the out-of-memory killer during the work now
in `3a_PrepDMBenchmarks.R`. The current design bounds memory by running each
stage -- and, within Chapters 2-3 and Sections S2-SA, each child script -- as
its own `Rscript` process, so memory returns to the operating system at every
boundary and no stage
inherits an earlier one's allocator high-water mark. Within Chapter 3,
`make_DM_event_returns()` and `adj_R2_with_PPCA()` deserialize the multi-GB
mined-strategy RDS once and drop the enclosing object, and on Unix they fork
instead of spawning PSOCK workers, so the read-only panel is shared copy-on-write
rather than copied per worker; the non-Unix fallback is a PSOCK cluster capped at
two workers.

At `num_cores = 4`, Chapter 3 completes with no OOM. Aggregate R resident memory
(master plus 4 forked workers) historically peaks around **23-27 GB** during
the work now isolated in Appendix SA11, well
under a 62 GB machine, with the fork workers running at ~100% CPU and ~2.3-5 GB
private each -- consistent with the read-only panel being shared rather than
copied. Chapter 2 holds around 22-23 GB during `2a` and drops to ~12 GB at each
child boundary.

### Parallel scripts, by memory

Every script that runs work in parallel, heaviest first, and the stretch to watch
for peak memory. The top group calls the large-panel fork helpers
(`make_DM_event_returns()` / `adj_R2_with_PPCA()` in `helpers/matching.R`), which
deserialize the multi-GB mined-strategy RDS once and, on Unix, share it across
workers copy-on-write -- the ~23-27 GB peak band. The bottom group runs its own
`makePSOCKcluster`/`makeCluster` over a multi-GB panel, where each worker holds a
separate copy.

| Script                      | Parallel work                                      | Footprint                               | In `MAIN.R`? |
| --------------------------- | -------------------------------------------------- | --------------------------------------- | ------------ |
| `Appendices/SA11_DMSpanPCAPrep.R` | 6× `make_DM_event_returns` + 1× `adj_R2_with_PPCA` | fork, shared panel (heaviest)       | appendices   |
| `3a_PrepDMBenchmarks.R`     | 3× `make_DM_event_returns`                         | fork, shared panel                      | yes          |
| `2a_CompustatToLongshort.R` | `foreach` over 29,315 signals                      | own PSOCK cluster, Compustat/CRSP panel | yes          |
| `Appendices/SA11_DMCorrelationsPCAPrep.R` | `parLapply` over correlation pairs          | own `makeCluster`                   | appendices   |

On non-Unix systems the fork helpers fall back to a PSOCK cluster capped at two
workers, each copying the panel, so the top-group scripts are far more
memory-hungry off Unix. `Appendices/SA03_StructuralBreak.R` uses `foreach` syntax but runs
sequentially (`%do%`), so it is not parallel.

### Sizing parallelism

Parallel work uses `globalSettings$num_cores`, currently 4. A conservative
planning heuristic is to budget roughly 5 GB of RAM per worker and keep the total
within available memory: for example, 4 workers on a machine with about 20 GB
free. The default `num_cores = 4` follows this rule, and the observed
23-27 GB Chapter-3 peak leaves ample headroom on a 62 GB machine.

Treat the 5 GB/worker figure as an upper-bound rule of thumb rather than a
measured requirement. Under the Unix fork backend the large read-only panel is
shared across workers instead of copied, so actual per-worker use is often lower;
the budget mainly covers the private memory a worker allocates while joining and
aggregating.

`num_cores` is the main runtime lever. `2a` dominates the total and scales with
worker count: its `foreach` runs at ~3.1 min per 1,000 signals at
`num_cores = 10` versus ~7.3 min at `num_cores = 4`, roughly halving `2a` from
~4h to ~2h. On a machine with more free RAM, raising `num_cores` is the
straightforward way to shorten the run; the fork-shared Chapter-3 peak leaves
room to do so. If a specific helper approaches the machine limit, pass a smaller
`ncores` to that helper rather than lowering the global setting, so the
multi-hour `2a` stage keeps full core count.

To measure a single script in a fresh process:

```bash
time Rscript Appendices/SA11_DMSpanPCAPrep.R
```

`time` reports elapsed time; peak resident memory needs a separate monitor
(for example `/usr/bin/time -v` where available, or sampling the process while
it runs).

## Rules of thumb

- `2a` dominates the runtime (~4h at `num_cores = 4`); appendix PCA spanning
  remains the longest robustness stage (~51 min).
- Silence during `2a`'s straggler tail or combine/save phase, or during a
  Chapter-3 parallel event-return section, is not by itself evidence of a hang;
  check the process with `ps` before stopping it. Workers near 100% CPU or a
  master using ~1 core mean it is working.
- `../Data/make_many_ls.log` carries `2a`'s dispatch progress and ETA, but the
  ETA reaches zero ~30 min before `2a` actually finishes (straggler + save).
- Main-text Sections S2-S4 can be run independently for paper iteration,
  provided their upstream caches exist; together they take only a few minutes.
  A full `SA_Appendices.R` run additionally includes the much slower SA11 PCA
  robustness preparation.
- With effectively no swap, memory exhaustion has no slow thrashing phase.
  Chapter 3 stays at 23-27 GB at `num_cores = 4`, but it is still the stretch to
  watch if `num_cores` is raised or the vintage grows.
