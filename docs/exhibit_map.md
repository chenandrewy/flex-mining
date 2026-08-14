# Exhibit map: script → output → exhibit

Which script builds each paper exhibit, and whether `MAIN.R` rebuilds it.

- **Contract:** the paper source repository currently has 58 files in its
  `exhibits/` directory. Its `sections/*.tex` files reference 56 of them: 52
  generated outputs and four hand-formatted MP-style tables. Two older
  hand-formatted tolerance variants are present but unused.
- **Producer:** the script whose write target (`ggsave`, `writeLines`,
  `saveRDS`, `kbl`, …) matches the exhibit basename.
- **In chain:** run by `MAIN.R` at the default `runStages` (config.R): chapters
  3–9 on, chapters 1 (`download_and_clean`) and 2 (`data_mining`) off.

## MAIN.R chain (default runStages)

`MAIN.R` runs each chapter as a separate `Rscript`; the exhibit-producing leaf
scripts each chapter runs are listed below. Chapter 3 builds reusable caches and
emits no exhibit.

```
MAIN.R  (reads runStages from config.R)
  1_Download_and_Clean.R, 1a_ValidDenoms.R          [chapter 1; off by default]
  2_DataMining.R            -> 2a 2b 2c 2d           [chapter 2; off by default]
  3_Precompute.R            -> 3a 3b 3c 3d 3e        (prep caches; no exhibits)
  4_ResearchVsDataMining.R  -> 4c2 4b1 4b2 4e1 4c9
  5_Learning.R              -> 5a 5b
  6_Heterogeneity.R         -> 4a3
  7_BestPredictors.R        -> 4d2
  8_Appendices.R            -> 4a1 4a2 4a4 4a5 4a6 4a7 4c5 4c6 4c7 4d1 4d3 4e2
  9_ExportDataToCsv.R
```

`4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R` is **not** run by any chapter (out of
chain); it is only used to hand-copy Tables 6, 7, and IA.10.

## Exhibit → producer

Paper numbers are from the compiled paper in the separate writing repository.
No particular location of that repository relative to this one is assumed.
Main-body exhibits carry their section as a `§N` prefix; appendix numbers
already encode their section (`IA.*` = internet appendix, `B.*` = appendix B)
so take no prefix. Each row points to a single exhibit; Tab 1 and Fig B.2 recur
because several producers feed one exhibit.

The eight `n/a`/`n/a*` rows are **hand-formatted or hand-transcribed**. Four are
`HandTable` files included by the paper; four have numbers pasted directly into
the paper `.tex`, so their first column gives a LaTeX label rather than a file.
`In chain? = n/a` means `MAIN.R` does not rebuild the paper table; `n/a*`
additionally flags that the producing script (`4c4`) is out of chain, so its
source numbers are not rebuilt either. The `4c4` rows copy from `_ff4_t2`
outputs under `../Results/RiskAdjusted/TstatFilter/`; `HandTable` rows copy from
in-chain `5b`; Tab IA.9 comes from a spreadsheet.

### Main text

| Exhibit                                                      | Paper #                       | Producer                                            | In chain? |
|--------------------------------------------------------------|-------------------------------|-----------------------------------------------------|-----------|
| Fig_DM_t_min_2_se_indicators_calendar.pdf                    | Sec. 1 Fig 1                  | 4c2_ResearchVsDMPlots.R                             | yes       |
| dm-sortsFull.tex                                             | Sec. 2 Tab 1                  | 4b1_DataMiningSummaryTables.R                       | yes       |
| DM_pca.tex                                                   | Sec. 2 Tab 1                  | 4b2_DMCorrelationsPCATables.R                       | yes       |
| theme_ez_decay.tex                                           | Sec. 2 Tab 2                  | 4e1_EZThemes.R                                      | yes       |
| Fig2a/b/c/d (4)                                              | Sec. 2 Fig 2                  | 4c9_Fig2Plots.R                                     | yes       |
| Fig2c_MatchedExclCorr_Tol10.pdf                              | Sec. 2 Fig 2 draft comparison | 4c9_Fig2Plots.R                                     | yes       |
| HandTable_MPStyleRegsMain.tex                                | Sec. 3 Tab 3                  | hand-transcribed from 5b output                     | n/a       |
| HandTable_MPStyleRegsMain_Tol30.tex                          | Sec. 3 draft "Tab 3b"         | hand-transcribed from 5b output                     | n/a       |
| HandTable_MPStyleRegsTimeFE.tex                              | Sec. 3 Tab 4                  | hand-transcribed from 5b output                     | n/a       |
| HandTable_MPStyleRegsTimeFE_Tol30.tex                        | Sec. 3 draft "Tab 4b"         | hand-transcribed from 5b output                     | n/a       |
| ApproachVsJournalsPart1/2/3.tex                              | Sec. 4 Tab 5                  | 4a3_DataCounts.R                                    | yes       |
| tab:hetero-bytheory (inline; hand-copied)                    | Sec. 4 Tab 6                  | 4c4 -> Table_RiskAdjusted_TheoryModel_ff4_t2        | n/a*      |
| tab:hetero-byjournal (inline; hand-copied)                   | Sec. 4 Tab 7                  | 4c4 -> Table_RiskAdjusted_DisciplineJournal_ff4_t2  | n/a*      |
| inspect-BMdec.tex                                            | Sec. 5 Tab 8                  | 4d2_InspectTables.R                                 | yes       |
| inspect-Mom12m.tex                                           | Sec. 5 Tab 9                  | 4d2_InspectTables.R                                 | yes       |
| inspect-Size.tex                                             | Sec. 5 Tab 10                 | 4d2_InspectTables.R                                 | yes       |

### Appendices

| Exhibit                                                      | Paper #                       | Producer                                            | In chain? |
|--------------------------------------------------------------|-------------------------------|-----------------------------------------------------|-----------|
| Table_MPStyleRegsIndividualDM.tex                            | Tab B.1                       | 5b_MPStyleDecayTables.R                             | yes       |
| Fig2a/b/c/d_CI (4)                                           | Fig B.1                       | 4c9_Fig2Plots.R                                     | yes       |
| Fig_DM_t_top5Pct_AccountingOnly_CalendarSE.pdf               | Fig B.2                       | 4c2 + 4c6_AccountingOnlyPlots.R                     | yes       |
| Fig_DM_t_min_2_AccountingOnly_CalendarSE.pdf                 | Fig B.2                       | 4c6_AccountingOnlyPlots.R                           | yes       |
| Fig_DM_CAPM/FF4_tv_AccountingOnly_CalendarSE.pdf             | Fig B.2                       | 4c7_AccountingOnlyAlphaPlots.R                      | yes       |
| Fig_PublicationsVsDataMining_*_Correlation10 (4)             | Fig B.3                       | 4d1_ResearchVsDMRobustnessCorrelationsEtc.R         | yes       |
| Fig_PublicationsVsDataMining_*_d_mean_0.1 (4)                | Fig B.4                       | 4d1_ResearchVsDMRobustnessCorrelationsEtc.R         | yes       |
| Fig_DM_unspan_match_t_g_PCA/cor.pdf                          | Fig B.5                       | 4d3_DMSpanPCAPlots.R                                | yes       |
| dm-sortsPost2003.tex                                         | Tab IA.1                      | 4b1_DataMiningSummaryTables.R                       | yes       |
| quantilesCorDM.tex                                           | Tab IA.2                      | 4b2_DMCorrelationsPCATables.R                       | yes       |
| theme_ez_decayinSampEnd1990.tex                              | Tab IA.3                      | 4e2_EZThemesRobustness.R                            | yes       |
| theme_ez_decayinSampEnd2000.tex                              | Tab IA.4                      | 4e2_EZThemesRobustness.R                            | yes       |
| theme_ez_decayinSampEnd2010.tex                              | Tab IA.5                      | 4e2_EZThemesRobustness.R                            | yes       |
| Table_RiskAdjusted_FullSample_Appendix.tex                   | Tab IA.6                      | 4c5_FullSampleRiskAdjustedResearchVsDMPlots.R       | yes       |
| Table_RiskAdjusted_FullSample_DisciplineJournal_Appendix.tex | Tab IA.7                      | 4c5_FullSampleRiskAdjustedResearchVsDMPlots.R       | yes       |
| SignalsByTheoryAndJournal.tex                                | Tab IA.8                      | 4a3_DataCounts.R                                    | yes       |
| tab:mp-theory-no-reg (inline; hand-copied)                   | Tab IA.9                      | Excel2LaTeX (sheet "MP theory")                     | n/a       |
| tab:hetero-model (inline; hand-copied)                       | Tab IA.10                     | 4c4 -> Table_RiskAdjusted_AnyModelVsNoModel_ff4_t2  | n/a*      |
| Table_RiskAdjusted_FullSample_AnyModelVsNoModel_Appendix.tex | Tab IA.11                     | 4c5_FullSampleRiskAdjustedResearchVsDMPlots.R       | yes       |
| samp_split_summary.tex                                       | Tab IA.12                     | 4a4_StructuralBreak.R                               | yes       |
| Fig_FullSampleRiskAdj_capm/ff3_alpha_fs_t2.pdf               | Fig IA.1                      | 4c5_FullSampleRiskAdjustedResearchVsDMPlots.R       | yes       |
| Fig_DecayVsWords_Names2.pdf                                  | Fig IA.2                      | 4a5_DecayVsWordcountPlot.R                          | yes       |
| Fig_Risk_via_CAPM/FF3/FF5.pdf                                | Fig IA.3                      | 4a1_RiskVsMispricingPlots.R                         | yes       |
| Fig_DecayVsJournal_Means.pdf                                 | Fig IA.4                      | 4a7_DecayVsJournal.R                                | yes       |
| break_vs_sampend.pdf                                         | Fig IA.5                      | 4a4_StructuralBreak.R                               | yes       |

## Gaps

At its default stages, `MAIN.R` rebuilds the source outputs for all 52 generated
files referenced by the paper. It does **not** install those outputs into a
writing-repository `exhibits/` directory; that directory is outside this repo
and its location is deliberately unspecified.

The eight paper tables it does **not** produce are:

1. Tables 3 and 4 and their draft tolerance-matched comparisons (four
   `HandTable` files) — copied by hand from in-chain `5b` output.
2. Tables 6, 7, IA.10 — copied by hand from out-of-chain `4c4`'s `_ff4_t2`
   output (`4c4` is not run by any chapter).
3. Table IA.9 — from an Excel sheet; no R script.

## Notes

- `4c4` = `4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R`. With the hand-formatted
  and hand-transcribed rows included, every float in the compiled paper is
  accounted for. (Fig. B.5a/B.5b are the two panels of Fig. B.5, already
  listed.)
- Of the 58 files currently in the paper's `exhibits/` directory, the two not
  referenced by `sections/*.tex` are `HandTable_MPStyleRegsMain_Tol10.tex` and
  `HandTable_MPStyleRegsTimeFE_Tol10.tex`.
- In-chain scripts that produce no exhibit are upstream prep: chapter 2
  (`2a`–`2d`), chapter 3 (`3a`–`3e`, reusable caches), and `4a2`/`4a6` (decay
  tables/plots outside the paper contract, e.g. for slides or the referee
  response).
