# Exhibit map: script → output → exhibit

Which script builds each paper exhibit, and whether `MAIN.R` rebuilds it.

- **Contract:** the paper source repository currently has 58 files in its
  `exhibits/` directory. Its `sections/*.tex` files reference 56 of them: 52
  generated outputs and four hand-formatted MP-style tables. Two older
  hand-formatted tolerance variants are present but unused.
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
  3_Precompute.R            -> 3a 3b 3c 3d 3e        (prep caches; no exhibits)
  S2_ResearchVsDataMining.R -> S2a S2b S2c S2d S2e
  S3_Learning.R             -> S3a S3b
  S4_Heterogeneity.R        -> S4a
  S5_BestPredictors.R       -> S5a
  SA_Appendices.R           -> Appendices/SA01 through Appendices/SA12
  9_ExportDataToCsv.R
```

`4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R` is **not** run by any stage (out of
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
in-chain `S3b`; Tab IA.9 comes from a spreadsheet.

Abbreviations used below to keep the columns narrow:

- `§N` = Section N (Paper # column, main text).
- `draft cmp` = draft comparison.
- `(inline)` = number pasted inline in the paper `.tex`, hand-copied (see the
  `n/a`/`n/a*` note above).
- `Tab_RA_FS_…` in the Exhibit column abbreviates the file basename
  `Table_RiskAdjusted_FullSample_…`.

### Main text

| Exhibit                                   | Paper #            | Producer                                           | In chain? |
| ----------------------------------------- | ------------------ | -------------------------------------------------- | --------- |
| Fig_DM_t_min_2_se_indicators_calendar.pdf | §1 Fig 1           | S2a_ResearchVsDMPlots.R                            | yes       |
| dm-sortsFull.tex                          | §2 Tab 1           | S2b_DataMiningSummaryTables.R                      | yes       |
| DM_pca.tex                                | §2 Tab 1           | S2c_DMCorrelationsPCATables.R                      | yes       |
| theme_ez_decay.tex                        | §2 Tab 2           | S2d_EZThemes.R                                     | yes       |
| Fig2a/b/c/d (4)                           | §2 Fig 2           | S2e_Fig2Plots.R                                    | yes       |
| Fig2c_MatchedExclCorr_Tol10.pdf           | §2 Fig 2 draft cmp | S2e_Fig2Plots.R                                    | yes       |
| HandTable_MPStyleRegsMain.tex             | §3 Tab 3           | hand-transcribed from S3b output                   | n/a       |
| HandTable_MPStyleRegsMain_Tol30.tex       | §3 draft Tab 3b    | hand-transcribed from S3b output                   | n/a       |
| HandTable_MPStyleRegsTimeFE.tex           | §3 Tab 4           | hand-transcribed from S3b output                   | n/a       |
| HandTable_MPStyleRegsTimeFE_Tol30.tex     | §3 draft Tab 4b    | hand-transcribed from S3b output                   | n/a       |
| ApproachVsJournalsPart1/2/3.tex           | §4 Tab 5           | S4a_DataCounts.R                                   | yes       |
| tab:hetero-bytheory (inline)              | §4 Tab 6           | 4c4 -> Table_RiskAdjusted_TheoryModel_ff4_t2       | n/a*      |
| tab:hetero-byjournal (inline)             | §4 Tab 7           | 4c4 -> Table_RiskAdjusted_DisciplineJournal_ff4_t2 | n/a*      |
| inspect-BMdec.tex                         | §5 Tab 8           | S5a_InspectTables.R                                | yes       |
| inspect-Mom12m.tex                        | §5 Tab 9           | S5a_InspectTables.R                                | yes       |
| inspect-Size.tex                          | §5 Tab 10          | S5a_InspectTables.R                                | yes       |

### Appendices

| Exhibit                                          | Paper #   | Producer                                                  | In chain? |
| ------------------------------------------------ | --------- | --------------------------------------------------------- | --------- |
| Table_MPStyleRegsIndividualDM.tex                | Tab B.1   | S3b_MPStyleDecayTables.R                                  | yes       |
| Fig2a/b/c/d_CI (4)                               | Fig B.1   | S2e_Fig2Plots.R                                           | yes       |
| Fig_DM_t_top5Pct_AccountingOnly_CalendarSE.pdf   | Fig B.2   | S2a + Appendices/SA08_AccountingOnlyPlots.R               | yes       |
| Fig_DM_t_min_2_AccountingOnly_CalendarSE.pdf     | Fig B.2   | Appendices/SA08_AccountingOnlyPlots.R                     | yes       |
| Fig_DM_CAPM/FF4_tv_AccountingOnly_CalendarSE.pdf | Fig B.2   | Appendices/SA09_AccountingOnlyAlphaPlots.R                | yes       |
| Fig_PublicationsVsDataMining_*_Correlation10 (4) | Fig B.3   | Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R   | yes       |
| Fig_PublicationsVsDataMining_*_d_mean_0.1 (4)    | Fig B.4   | Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R   | yes       |
| Fig_DM_unspan_match_t_g_PCA/cor.pdf              | Fig B.5   | Appendices/SA11_DMSpanPCAPlots.R                          | yes       |
| dm-sortsPost2003.tex                             | Tab IA.1  | S2b_DataMiningSummaryTables.R                             | yes       |
| quantilesCorDM.tex                               | Tab IA.2  | S2c_DMCorrelationsPCATables.R                             | yes       |
| theme_ez_decayinSampEnd1990.tex                  | Tab IA.3  | Appendices/SA12_EZThemesRobustness.R                      | yes       |
| theme_ez_decayinSampEnd2000.tex                  | Tab IA.4  | Appendices/SA12_EZThemesRobustness.R                      | yes       |
| theme_ez_decayinSampEnd2010.tex                  | Tab IA.5  | Appendices/SA12_EZThemesRobustness.R                      | yes       |
| Tab_RA_FS_Appendix.tex                           | Tab IA.6  | Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R | yes       |
| Tab_RA_FS_DisciplineJournal_Appendix.tex         | Tab IA.7  | Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R | yes       |
| SignalsByTheoryAndJournal.tex                    | Tab IA.8  | S4a_DataCounts.R                                          | yes       |
| tab:mp-theory-no-reg (inline)                    | Tab IA.9  | Excel2LaTeX (sheet "MP theory")                           | n/a       |
| tab:hetero-model (inline)                        | Tab IA.10 | 4c4 -> Table_RiskAdjusted_AnyModelVsNoModel_ff4_t2        | n/a*      |
| Tab_RA_FS_AnyModelVsNoModel_Appendix.tex         | Tab IA.11 | Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R | yes       |
| samp_split_summary.tex                           | Tab IA.12 | Appendices/SA03_StructuralBreak.R                         | yes       |
| Fig_FullSampleRiskAdj_capm/ff3_alpha_fs_t2.pdf   | Fig IA.1  | Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R | yes       |
| Fig_DecayVsWords_Names2.pdf                      | Fig IA.2  | Appendices/SA04_DecayVsWordcountPlot.R                    | yes       |
| Fig_Risk_via_CAPM/FF3/FF5.pdf                    | Fig IA.3  | Appendices/SA01_RiskVsMispricingPlots.R                   | yes       |
| Fig_DecayVsJournal_Means.pdf                     | Fig IA.4  | Appendices/SA06_DecayVsJournal.R                          | yes       |
| break_vs_sampend.pdf                             | Fig IA.5  | Appendices/SA03_StructuralBreak.R                         | yes       |

## Gaps

At its default stages, `MAIN.R` rebuilds the source outputs for all 52 generated
files referenced by the paper. It does **not** install those outputs into a
writing-repository `exhibits/` directory; that directory is outside this repo
and its location is deliberately unspecified.

The eight paper tables it does **not** produce are:

1. Tables 3 and 4 and their draft tolerance-matched comparisons (four
   `HandTable` files) — copied by hand from in-chain `S3b` output.
2. Tables 6, 7, IA.10 — copied by hand from out-of-chain `4c4`'s `_ff4_t2`
   output (`4c4` is not run by any stage).
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
  (`2a`–`2d`), chapter 3 (`3a`–`3e`, reusable caches), and
  `Appendices/SA02`/`Appendices/SA05` (decay
  tables/plots outside the paper contract, e.g. for slides or the referee
  response).
