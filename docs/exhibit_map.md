# Exhibit map: script → output → exhibit

Which script builds each paper exhibit, and whether `MAIN.R` rebuilds it.

- **Contract:** the 53 files in `exhibits/`, every one referenced from
  `../risk-vs-writing/latex-risk-vs/sections/*.tex`.
- **Producer:** the script whose write target (`ggsave`, `writeLines`,
  `saveRDS`, `kbl`, …) matches the exhibit basename.
- **In chain:** reachable from `MAIN.R` via `source()` at the default
  `run_downloads = FALSE`. `1_Download_and_Clean.R` runs only when `TRUE`.

## MAIN.R chain (run_downloads = FALSE)

```
MAIN.R
  0_Environment.R                      (support; sourced by ~everything)
  1a_ValidDenoms.R
  2_DataMining.R           -> 2a 2b 2c 2d
  3_RiskVsMispricing.R     -> 3a 3b 3c 3d 3e 3f 3g
  4_ResearchVsDataMining.R -> 4a 4b 4c1 4c2 4c3
                              4c4(FF4) 4c5(non-FF4) 4c6_AccountingOnlyPlots 4c7 4d 4d2 4e
  6_TextAnalysis.R         -> 6a 6b
  8_DMThemes.R             -> 8a 8b
  99_ExportDataToCsv.R
```

Variant selection matters: `3a` (not `…FF4`), `4c4` FF4 (not `…TV`),
`4c5` non-FF4, `4c6_AccountingOnlyPlots` (not the MPStyle tables script).

## Exhibit → producer

Paper numbers are from the compiled `../risk-vs-writing` (`IA.*` = internet
appendix; `B.*` = appendix B). A row lists all numbers when its files map to
several exhibits.

| Exhibit(s)                                                     | Paper #                | Producer                                      | In chain? |
|----------------------------------------------------------------|------------------------|-----------------------------------------------|-----------|
| ApproachVsJournalsPart1/2/3.tex, SignalsByTheoryAndJournal.tex | Tab. 5, IA.8           | 3c_DataCounts.R                               | yes       |
| samp_split_summary.tex, break_vs_sampend.pdf                   | Tab. IA.12; Fig. IA.5  | 3d_StructuralBreak.R                          | yes       |
| Fig_DecayVsWords_Names2.pdf                                    | Fig. IA.2              | 3e_DecayVsWordcountPlot.R                     | yes       |
| Fig_DecayVsJournal_Means.pdf                                   | Fig. IA.4              | 3g_DecayVsJournal.R                           | yes       |
| Fig_Risk_via_CAPM/FF3/FF5.pdf                                  | Fig. IA.3              | 3a_RiskVsMispricingPlots.R                    | yes       |
| dm-sortsFull.tex, dm-sortsPost2003.tex                         | Tab. 1, IA.1           | 4a_DataMiningSummary.R                        | yes       |
| DM_pca.tex, quantilesCorDM.tex                                 | Tab. 1, IA.2           | 4b_DMCorrelationsPCASummary.R                 | yes       |
| Fig_DM_t_min_2_se_indicators_calendar.pdf                      | Fig. 1                 | 4c2_ResearchVsDMPlots.R                       | yes       |
| Fig_DM_t_top5Pct_AccountingOnly_CalendarSE.pdf                 | Fig. B.2               | 4c2 + 4c6_AccountingOnlyPlots.R               | yes       |
| Fig_DM_t_min_2_AccountingOnly_CalendarSE.pdf                   | Fig. B.2               | 4c6_AccountingOnlyPlots.R                     | yes       |
| Fig_DM_CAPM/FF4_tv_AccountingOnly_CalendarSE.pdf               | Fig. B.2               | 4c7_AccountingOnlyAlphaPlots.R                | yes       |
| Fig_FullSampleRiskAdj_capm/ff3_alpha_fs_t2.pdf                 | Fig. IA.1              | 4c5_FullSampleRiskAdjustedResearchVsDMPlots.R | yes       |
| Table_RiskAdjusted_FullSample_*_Appendix.tex (3)               | Tab. IA.6, IA.7, IA.11 | 4c5 (non-FF4)                                 | yes       |
| Fig_PublicationsVsDataMining_* (8)                             | Fig. B.3, B.4          | 4d_ResearchVsDMRobustnessCorrelationsEtc.R    | yes       |
| inspect-BMdec/Mom12m/Size.tex                                  | Tab. 8-10              | 4d2_InspectTables.R                           | yes       |
| Fig_DM_unspan_match_t_g_PCA/cor.pdf                            | Fig. B.5               | 4e_DM_Span_PCA.R                              | yes       |
| theme_ez_decay.tex                                             | Tab. 2                 | 8a_EZThemes.R                                 | yes       |
| theme_ez_decayinSampEnd1990/2000/2010.tex                      | Tab. IA.3-5            | 8b_EZThemesRobustness.R                       | yes       |
| Table_MPStyleRegsIndividualDM.tex                              | Tab. B.1               | 5b_MPStyleDecayTables.R                       | **no**    |
| Fig2a/b/c/d + _CI (8)                                          | Fig. 2, B.1            | 4c20b_Fig2Plots.R                             | **no**    |
| HandTable_MPStyleRegs{Main,TimeFE}.tex                         | Tab. 3, 4              | hand-transcribed from 5b output               | n/a       |

## Gaps

Used exhibits **not** rebuilt by `MAIN.R`:

1. `5b_MPStyleDecayTables.R` → `Table_MPStyleRegsIndividualDM.tex` (and the
   `Table_MPStyleRegs{Main,Unscaled}` tables the HandTables are transcribed from).
2. `4c20b_Fig2Plots.R` → the 8 `Fig2*` exhibits.
3. HandTables — no script writes them; transcribed by hand from #1.

Folding #1 and #2 into the chain makes `MAIN.R` rebuild the whole paper except
the two hand-transcribed tables.

## Inline exhibits (no `exhibits/` file, so no row above)

Four paper tables are typeset directly in the section `.tex` with hardcoded
numbers — no script and no `exhibits/` file back them, so no script rebuilds
them:

| Paper #   | Label                | Section                   |
|-----------|----------------------|---------------------------|
| Tab. 6    | tab:hetero-bytheory  | 04_hetero.tex             |
| Tab. 7    | tab:hetero-byjournal | 04_hetero.tex             |
| Tab. IA.9 | tab:mp-theory-no-reg | 92_internet_appendix.tex  |
| Tab. IA.10| tab:hetero-model     | 92_internet_appendix.tex  |

With these four plus the table above, every float in the compiled paper is
accounted for. (Fig. B.5a/B.5b are the two panels of Fig. B.5, already listed.)

## Notes

- `exhibits/` also holds an empty `Extra/` subdir; the count is 53, not 54.
- In-chain scripts with no used exhibit are upstream data/prep (`1a`, `2a`–`2d`,
  `4c1`, `4c3`) or emit outputs outside the 53 (`3b`, `3f`, `4c4`, `6a`, `6b`,
  `99`) that may feed slides or the referee response.
