# RAM: MAIN.R got OOM-killed in 4c1

Snapshot written 2026-08-13 21:31 EDT. Branch `cleanup/main.r`.

## What happened

The run started 17:48 died at ~20:41 with exit code 137 during
`4c1_ResearchVsDMprep.R`, inside `make_DM_event_returns()`.

## What ran out of RAM

The **master R process** — PID 21804, the one running `MAIN.R` itself. Not a
worker, not the container.

Evidence:

- Exit code **137 = 128 + 9**, so it took SIGKILL directly. Had a worker been
  the victim, the master would have died with an R-level error
  ("error reading from connection") and exit code 1.
- `/sys/fs/cgroup/memory.events` → `oom_kill 1`. Exactly one process killed.
- The kernel picks the largest RSS, and the master was the fattest target:
  ~20 GB steady for hours, versus a few GB per worker.
- `memory.max` is `max`, so there is no container limit — the ceiling is the
  machine: **62 GB, no swap**. `memory.peak` recorded **61.6 GiB**.

No swap means no soft landing. It went from working to killed with no
thrashing phase to notice.

## Why

`make_DM_event_returns()` (`0_Environment.R:1264`) does:

```r
dm_rets <- readRDS(DMname)$ret        # 313 MB compressed on disk
dm_info <- readRDS(DMname)$port_list  # ...reads the whole file a SECOND time
...
cl <- makePSOCKcluster(ncores)        # ncores = 10
foreach(pubi = 1:npub, .combine = rbind, ...) %dopar% {
    eventpan <- dm_rets %>% inner_join(matchcur, ...)   # dm_rets used inside
}
```

Three things compound:

1. **`dm_rets` is copied to every worker.** `makePSOCKcluster` starts separate
   R processes, not forks, so there is no copy-on-write sharing. `foreach`
   sees `dm_rets` in the loop body and serializes a full copy to each worker.
   `num_cores = round(.4 * detectCores())` (`0_Environment.R:90`) and the box
   has 24 cores, so **10 workers**, each with its own complete copy, plus the
   master's.
2. **That panel is multi-GB.** ~29,315 signals x ~599 months on average x
   portids, six columns, with `dmname` as a character column. Estimated, not
   measured — measuring means loading it. Ten copies of "a few GB" plus a
   20 GB master is how you reach 62 GB.
3. **`readRDS(DMname)` runs twice**, deserializing the entire `stratdat` list
   each time to pull one element. The master transiently holds two full
   copies.

The spike is sudden: `4c1` is a ~4 minute script (per an earlier run the same
day), not a long grind. It sat fine until the cluster spawned.

## The repo already contains the fix

`2a_CompustatToLongshort.R:238`, immediately before its own parallel loop:

```r
# Save to fst such that parallelization works without having to load big file onto each worker
fst::write_fst(alldat, '../Data/tmpAllDat.fst')
```

Same problem, already diagnosed and solved — workers read the slices they need
off disk instead of each receiving a copy. `make_DM_event_returns()` never got
the treatment, which is why `2a` survived 29,315 iterations and `4c1` died on a
couple hundred.

## Suggested fix

In increasing order of effort:

1. **Lower `ncores` for this call.** Passing `ncores = 3` roughly thirds the
   peak. One-line change, would get a run past this point tonight. Does not
   fix the underlying pattern.
2. **Collapse the double `readRDS`.** Read once into a local, take `$ret` and
   `$port_list` from it, drop the rest. Pure waste removal, no behavior
   change.
3. **Adopt the `fst` pattern from `2a`.** Write `dm_rets` to
   `../Data/tmpDMRets.fst` once, have each worker `read_fst` only the
   `sweight`/`dmname` rows it needs. This is the real fix and makes the
   function's cost independent of core count.

Worth doing 2 and 3 together; 1 alone is a workaround.

## Why this matters for the refactor

`4c1` is in the `MAIN.R` chain **and** feeds the paper — it produces
`plotdat0.RDS`, which the `Fig_PublicationsVsDataMining_*` exhibits depend on.
So it is in the keep set and needs fixing regardless of what gets deleted.

Also note `4b_DMCorrelationsPCASummary.R`, which ran just before, completed
fine in ~11 min and produces `DM_pca.tex` — a used exhibit. It had been a
deletion candidate; it is not.

## Open

- Actual in-memory size of `dm_rets` is unmeasured. Worth an `object.size()`
  before choosing between fix 1 and fix 3.
- The same `readRDS(DMname)$ret` + `makePSOCKcluster` shape appears in
  `adj_R2_with_PPCA()` (`0_Environment.R:1356`). Not yet reached by any run
  tonight, so untested, but it looks like the same bug.
