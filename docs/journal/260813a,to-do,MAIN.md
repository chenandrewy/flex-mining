# To-do: get MAIN.R running end to end

Started 2026-08-13 17:46 EDT. Branch `cleanup/main.r`.

## Goal

Run `MAIN.R` as-is (`run_downloads <- FALSE`, so no WRDS pull), let it fail,
patch whatever breaks, re-run. The full run takes a very long time, so the
loop is: run in background, tail the log, fix, resume from the failing script
rather than from the top when that is safe.

## How the run is invoked

```
cd /workspace/Live,Risk-vs/flex-mining
Rscript MAIN.R > docs/journal/logs/main_<n>.log 2>&1
```

(logs kept out of git if they get big; scratchpad copy at
`/tmp/claude-1000/-workspace-Live-Risk-vs-flex-mining/.../scratchpad`)

## Script list in MAIN.R

1. `0_Environment.R`
2. `1_Download_and_Clean.R` — SKIPPED (`run_downloads <- FALSE`)
3. `2_DataMining.R`
4. `3_RiskVsMispricing.R`
5. `4_ResearchVsDataMining.R`
6. `6_TextAnalysis.R`
7. `8_DMThemes.R`
8. `99_ExportDataToCsv.R`

## Status

| # | Script | Status | Notes |
|---|--------|--------|-------|
| 0 | 0_Environment.R | not started | |
| 2 | 2_DataMining.R | not started | |
| 3 | 3_RiskVsMispricing.R | not started | |
| 4 | 4_ResearchVsDataMining.R | not started | |
| 6 | 6_TextAnalysis.R | not started | |
| 8 | 8_DMThemes.R | not started | |
| 99 | 99_ExportDataToCsv.R | not started | |

## To do

- [ ] Run 1: launch `Rscript MAIN.R` in the background, capture full log.
- [ ] Triage first failure; record script, line, error verbatim below.
- [ ] Patch, re-run from the failing script if intermediate state allows.
- [ ] Repeat until the whole list completes.
- [ ] Final clean end-to-end run for confirmation.
- [ ] Commit the patches (separate commits per fix where sensible).

## Failures and fixes

(none yet)

## Open questions / risks

- Missing input data: `../Data/Raw` must already hold the current vintage.
  If a script needs a file the skipped `1_Download_and_Clean.R` would have
  produced, that is a real blocker — do not re-enable downloads without
  asking; the WRDS pull is irreversible.
- Dropbox online-only files can fail to read (EDEADLK / SIGBUS). If a read
  dies that way, ask the user to `dbhydrate -f <path>` rather than
  regenerating the data.
- Missing R packages: install from the pinned P3M snapshot only after asking.
- `4c18a_DownloadFirmSignals.py` is Python and may be sourced by a `4c*`
  chain — check whether the `4_` script reaches it.
