# Plan: config split + function extraction into helpers/

Snapshot written 2026-08-13 EDT. Executes the `0813f,plan,config-and-helpers.md`
follow-up ("defer function extraction to a fresh agent").

## Scope

Two behavior-preserving commits, in order:

1. **Config split.** Pull run choices out of `0_Environment.R` into `config.R`.
2. **Function extraction.** Move the ~38 functions out of `0_Environment.R`
   into thematic `helpers/` files, sourced centrally from `0_Environment.R`.

`compnames` and `colors` stay in `0_Environment.R` (reference data, not knobs).
No function bodies are rewritten — text is moved verbatim.

## Commit 1: config.R

- `config.R` = pure data, **no packages, no functions**, so it can be sourced
  standalone and cheaply.
  - `globalSettings` (dataVersion, signal/portfolio/matching choices, num_cores,
    debug flags) moves here.
  - `set.seed(1337)` moves here (affects cached sampling).
  - Stage switches `run_*` currently in `MAIN.R` move here as a `runStages` list.
- `0_Environment.R` sources `config.R` after `library()`/detach/options/dirs.
- `MAIN.R` sources `config.R` and reads `runStages$...` instead of local `run_*`.
- The two `sys.source("0_Environment.R", envir = settings_env)` drivers
  (`3_Precompute.R`, `7_BestPredictors.R`) switch to
  `sys.source("config.R", envir = settings_env)` — they only read
  `globalSettings$dataVersion`, so this skips loading every package.

## Commit 2: helpers/ extraction

### Grouping (6 new files)

- `helpers/mining.R`: make_signal_list, make_signal_list_yz, dataset_to_signal,
  signal_to_ports, make_many_ls, nchoose2ports, tic_kth_letter_port
- `helpers/matching.R`: restrictInclSignals, sumstats_for_DM_Strats,
  SelectDMStrats, make_DM_event_returns, process_event_time_returns,
  adj_R2_with_PPCA, inspect_one_pub, import_docs
- `helpers/plotting.R`: ReturnPlotsNoDM, ReturnPlotsNoDMAlpha, ReturnPlotsWithDM,
  ReturnPlotsWithDM4series, ReturnPlotsWithDM_std_errors_indicators
  (plus the commented-out ReturnPlotsWithDM_std_errors block)
- `helpers/stats.R`: calculate_alpha_shrinkage, f.custom.t, f.sharp,
  f.describe_numeric, fntile, f.desc.returns, f.ls.past.returns, compute_pca
- `helpers/implied_category.R`: mkt_implied_category, ff3_implied_category,
  ff4_implied_category, ff5_implied_category, add_catID
- `helpers/utils.R`: .ls.objects, lsos, read_fst_yearm, round_numbers_in_strings

Arguable placements (revisit if desired): `restrictInclSignals` and
`import_docs` are in `matching.R`.

### Cautions from the audit

- **Do NOT centralize the existing `_tv`/`_fs` helpers.**
  `risk_adjusted_helpers_tv.R` and `_fs.R` define the same names on purpose
  (extract_beta, extract_ff*_coeffs, create_summary_tables, ...); scripts source
  one or the other. Central sourcing would clobber. Left untouched.
- **Free-variable globals resolve at call time** (CCM, czsum/czret,
  signal_list/signali/port_list, crsp, yz_dt, allret/stratdat/compdoc, dmcomp,
  colors, globalSettings-in-default-args). Preserved only if helpers land in the
  same env as `0_Environment.R`.
- **Named `implied_category.R`** (not `risk_adjusted_helpers*`) to avoid
  confusion with the existing `_tv`/`_fs` files.
- Apparently unused externally (move, do not delete): nchoose2ports,
  make_signal_list_yz, calculate_alpha_shrinkage, read_fst_yearm,
  f.describe_numeric, fntile, ff4_implied_category.

### Sourcing mechanism

In `0_Environment.R`, after packages + config.R:

```r
for (f in c("utils","mining","matching","stats","plotting","implied_category"))
  source(file.path("helpers", paste0(f, ".R")), local = TRUE)
```

`local = TRUE` evaluates each file in `0_Environment.R`'s own frame:
globalenv under `source("0_Environment.R")`, settings_env under the
`sys.source` drivers. Invocation-agnostic; removes the source-order landmine.

## Verification harness (run per commit)

1. Baseline before edits: source `0_Environment.R`, snapshot `sort(ls())` and,
   for every function, `list(formals, deparse(body))`.
2. After edits: same snapshot; assert identical `ls()` object set and
   `identical()` on each function's formals+body → proves move, not rewrite.
3. Confirm both `settings_env` drivers resolve the same `version_prefix`.
4. Syntax smoke: `parse()` every file in `helpers/`.
