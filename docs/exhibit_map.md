# Exhibit map: script → output → exhibit

Which script builds each paper exhibit, and whether `MAIN.R` rebuilds it.

- **Contract:** the paper source repository has 53 files in its `exhibits/`
  directory, all referenced by `sections/*.tex`. The sections reference 56
  exhibit basenames in total: the other three are time-varying risk-adjusted
  tables read from `../Results`. The two official
  `Table_MPStyleRegs{NoTimeFE,TimeFE}.tex` files and the individual-DM appendix
  table are R-generated.
- **Producer:** the script whose write target (`ggsave`, `writeLines`,
  `saveRDS`, `kbl`, …) matches the exhibit basename.
- **In chain:** run by `MAIN.R` at the default `runStages` (config.R): stages
  3, S2–SA, and 9 on; stages 1 (`download_and_clean`) and 2 (`data_mining`) off.

## MAIN.R chain (default runStages)

`MAIN.R` runs each chapter as a separate `Rscript`; the exhibit-producing leaf
scripts each chapter runs are listed below. Chapter 3 builds reusable caches and
emits no exhibit.

```
MAIN.R  (reads runStages from config.R)
  1_Download_and_Clean.R, 1a_ValidDenoms.R          [chapter 1; off by default]
  2_DataMining.R            -> 2a 2b 2c 2d           [chapter 2; off by default]
  3_Precompute.R            -> 3a 3b 3c 3d_MatchedUncorr 3d_Fig2 3e
                                                        (prep caches; no exhibits)
  S2_ResearchVsDataMining.R -> S2a S2b S2c S2d S2e
  S3_Learning.R             -> S3a S3b
  S4_Heterogeneity.R        -> S4a
  S5_BestPredictors.R       -> S5a
  SA_Appendices.R           -> Appendices/SA01 through Appendices/SA12
  9_ExportDataToCsv.R
```

`4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R` is **not** run by any stage (out of
chain); it is only used to hand-copy Tables 6, 7, and IA.10.

`3d_MatchedUncorrData.R` builds the canonical matched-uncorr pair set and
signal-month panel used by both `3d_Fig2Data.R` and
`S3a_MPStyleDecayModels.R`. `S2e_Fig2Plots.R` renders the four Figure 2 panels
and appendix confidence-interval variants into `../Results/`.

## Exhibit → producer

Paper numbers are from the compiled paper in the separate writing repository.
No particular location of that repository relative to this one is assumed.
Main-body exhibits carry their section as a `§N` prefix; appendix numbers
already encode their section (`IA.*` = internet appendix, `B.*` = appendix B)
so take no prefix. Each row points to a single exhibit; Tab 1 and Fig B.2 recur
because several producers feed one exhibit.

The `n/a` row has numbers pasted directly into the paper `.tex`, so its first
column gives a LaTeX label rather than a file. `In chain? = n/a` means there is
no R producer. The three `4c4` rows are generated under
`../Results/RiskAdjusted/TstatFilter/` but are out of the `MAIN.R` chain; Tab
IA.9 comes from a spreadsheet.

### Main text

| Exhibit                                                      | Paper #                       | Producer                                            | In chain? |
|--------------------------------------------------------------|-------------------------------|-----------------------------------------------------|-----------|
| Fig_DM_t_min_2_se_indicators_calendar.pdf                    | Sec. 1 Fig 1                  | S2a_ResearchVsDMPlots.R                             | yes       |
| dm-sortsFull.tex                                             | Sec. 2 Tab 1                  | S2b_DataMiningSummaryTables.R                       | yes       |
| DM_pca.tex                                                   | Sec. 2 Tab 1                  | S2c_DMCorrelationsPCATables.R                       | yes       |
| theme_ez_decay.tex                                           | Sec. 2 Tab 2                  | S2d_EZThemes.R                                      | yes       |
| Fig2a/b/c/d (4)                                              | Sec. 2 Fig 2                  | S2e_Fig2Plots.R                                     | yes       |
| Table_MPStyleRegsNoTimeFE.tex                                | Sec. 3 Tab 3                  | S3b_MPStyleDecayTables.R                            | yes       |
| Table_MPStyleRegsTimeFE.tex                                  | Sec. 3 Tab 4                  | S3b_MPStyleDecayTables.R                            | yes       |
| ApproachVsJournalsPart1/2/3.tex                              | Sec. 4 Tab 5                  | S4a_DataCounts.R                                    | yes       |
| Table_RiskAdjusted_TimeVarying_ff4_t2.tex                   | Sec. 4 Tab 6                  | 4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R            | no        |
| Table_RiskAdjusted_TimeVarying_DisciplineJournal_ff4_t2.tex | Sec. 4 Tab 7                  | 4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R            | no        |
| inspect-BMdec.tex                                            | Sec. 5 Tab 8                  | S5a_InspectTables.R                                 | yes       |
| inspect-Mom12m.tex                                           | Sec. 5 Tab 9                  | S5a_InspectTables.R                                 | yes       |
| inspect-Size.tex                                             | Sec. 5 Tab 10                 | S5a_InspectTables.R                                 | yes       |

### Appendices

| Exhibit                                                      | Paper #                       | Producer                                            | In chain? |
|--------------------------------------------------------------|-------------------------------|-----------------------------------------------------|-----------|
| Table_MPStyleRegsIndividualDM.tex                            | Tab B.1                       | S3b_MPStyleDecayTables.R                            | yes       |
| Fig2a/b/c/d_CI (4)                                           | Fig B.1                       | S2e_Fig2Plots.R                                     | yes       |
| Fig_DM_t_top5Pct_AccountingOnly_CalendarSE.pdf               | Fig B.2                       | S2a + Appendices/SA08_AccountingOnlyPlots.R         | yes       |
| Fig_DM_t_min_2_AccountingOnly_CalendarSE.pdf                 | Fig B.2                       | Appendices/SA08_AccountingOnlyPlots.R               | yes       |
| Fig_DM_CAPM/FF4_tv_AccountingOnly_CalendarSE.pdf             | Fig B.2                       | Appendices/SA09_AccountingOnlyAlphaPlots.R          | yes       |
| Fig_PublicationsVsDataMining_*_Correlation10 (4)             | Fig B.3                       | Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R | yes       |
| Fig_PublicationsVsDataMining_*_d_mean_0.1 (4)                | Fig B.4                       | Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R | yes       |
| Fig_DM_unspan_match_t_g_PCA/cor.pdf                          | Fig B.5                       | Appendices/SA11_DMSpanPCAPlots.R                    | yes       |
| dm-sortsPost2003.tex                                         | Tab IA.1                      | S2b_DataMiningSummaryTables.R                       | yes       |
| quantilesCorDM.tex                                           | Tab IA.2                      | S2c_DMCorrelationsPCATables.R                       | yes       |
| theme_ez_decayinSampEnd1990.tex                              | Tab IA.3                      | Appendices/SA12_EZThemesRobustness.R                | yes       |
| theme_ez_decayinSampEnd2000.tex                              | Tab IA.4                      | Appendices/SA12_EZThemesRobustness.R                | yes       |
| theme_ez_decayinSampEnd2010.tex                              | Tab IA.5                      | Appendices/SA12_EZThemesRobustness.R                | yes       |
| Table_RiskAdjusted_FullSample_Appendix.tex                   | Tab IA.6                      | Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R | yes       |
| Table_RiskAdjusted_FullSample_DisciplineJournal_Appendix.tex | Tab IA.7                      | Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R | yes       |
| SignalsByTheoryAndJournal.tex                                | Tab IA.8                      | S4a_DataCounts.R                                    | yes       |
| tab:mp-theory-no-reg (inline; hand-copied)                   | Tab IA.9                      | Excel2LaTeX (sheet "MP theory")                     | n/a       |
| Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel_ff4_t2.tex | Tab IA.10                     | 4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R            | no        |
| Table_RiskAdjusted_FullSample_AnyModelVsNoModel_Appendix.tex | Tab IA.11                     | Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R | yes       |
| samp_split_summary.tex                                       | Tab IA.12                     | Appendices/SA03_StructuralBreak.R                   | yes       |
| Fig_FullSampleRiskAdj_capm/ff3_alpha_fs_t2.pdf               | Fig IA.1                      | Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R | yes       |
| Fig_DecayVsWords_Names2.pdf                                  | Fig IA.2                      | Appendices/SA04_DecayVsWordcountPlot.R              | yes       |
| Fig_Risk_via_CAPM/FF3/FF5.pdf                                | Fig IA.3                      | Appendices/SA01_RiskVsMispricingPlots.R             | yes       |
| Fig_DecayVsJournal_Means.pdf                                 | Fig IA.4                      | Appendices/SA06_DecayVsJournal.R                    | yes       |
| break_vs_sampend.pdf                                         | Fig IA.5                      | Appendices/SA03_StructuralBreak.R                   | yes       |

## Gaps

At its default stages, `MAIN.R` rebuilds the source outputs for all regular
generated exhibits, including the two official matched-uncorr MP tables. It does **not**
install those outputs into a writing-repository `exhibits/` directory; that
directory is outside this repo and its location is deliberately unspecified.

The four paper tables it does **not** produce are:

1. Tables 6, 7, IA.10 — generated by out-of-chain `4c4` and read from
   `../Results` (`4c4` is not run by any stage).
2. Table IA.9 — from an Excel sheet; no R script.

## Notes

- `4c4` = `4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R`. Every float in the
  compiled paper is accounted for. (Fig. B.5a/B.5b are the two panels of
  Fig. B.5, already listed.)
- In-chain scripts that produce no exhibit are upstream prep: chapter 2
  (`2a`–`2d`), chapter 3 (`3a`–`3e`, reusable caches), and
  `Appendices/SA02`/`Appendices/SA05` (decay
  tables/plots outside the paper contract, e.g. for slides or the referee
  response).
