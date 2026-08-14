# Exhibit map: script → output → exhibit

Which script builds each paper exhibit, whether `MAIN.R` rebuilds its
calculation and paper artifact, and whether the manuscript consumes it.

- **Contract:** the paper source repository currently has 61 files in its
  `exhibits/` directory. Its `sections/*.tex` files reference 59 of them: 55
  generated outputs and four hand-formatted MP-style tables. Two older
  hand-formatted tolerance variants are present but unused.
- **Producer:** the script whose write target (`ggsave`, `writeLines`,
  `saveRDS`, `kbl`, …) matches the exhibit basename.
- **Calculation:** the analysis calculation is run by `MAIN.R` at the default
  `runStages` (config.R): stages 3, S2–SA, and 9 on; stages 1
  (`download_and_clean`) and 2 (`data_mining`) off.
- **Paper artifact:** the producer writes the file that the manuscript can
  consume. For the six S4b-owned group tables this is distinct from the nested
  audit output.
- **Wired:** the manuscript references the generated artifact rather than
  containing copied numbers.

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
  S4_Heterogeneity.R        -> S4a S4b
  S5_BestPredictors.R       -> S5a
  SA_Appendices.R           -> Appendices/SA01 through Appendices/SA12
  9_ExportDataToCsv.R
```

`S4b_RVsDM_ByGroup.R` owns the source artifacts for Tables 6, 7, IA.6, IA.7,
IA.10, and IA.11 and is in the default Section 4 pipeline. Keeping all six
group tables together gives the sample-specific and full-sample cuts the same
screening, paired estimands, and clustered-inference contract.

## Exhibit → producer

Paper numbers are from the compiled paper in the separate writing repository.
No particular location of that repository relative to this one is assumed.
Main-body exhibits carry their section as a `§N` prefix; appendix numbers
already encode their section (`IA.*` = internet appendix, `B.*` = appendix B)
so take no prefix. Each row points to a single exhibit; Tab 1 and Fig B.2 recur
because several producers feed one exhibit.

Five rows are **hand-formatted or hand-transcribed**. Four are `HandTable`
files included by the paper; Table IA.9 has numbers pasted directly into the
paper `.tex`. The `HandTable` rows copy from in-chain `S3b`; Table IA.9 comes
from a spreadsheet. All six S4b-owned tables now have all three states: their
calculations run in chain, S4b generates paper-specific fragments, and the
manuscript inputs those fragments.

Abbreviations used below to keep the columns narrow:

- `§N` = Section N (Paper # column, main text).
- `draft cmp` = draft comparison.
- `(inline)` = number pasted inline in the paper `.tex`, hand-copied.
- `Tab_RA_FS_…` in the Exhibit column abbreviates the file basename
  `Table_RiskAdjusted_FullSample_…`.

### Main text

| Exhibit                                                     | Paper #            | Producer                         | Calculation? | Paper artifact? | Wired? |
| ----------------------------------------------------------- | ------------------ | -------------------------------- | ------------ | --------------- | ------ |
| Fig_DM_t_min_2_se_indicators_calendar.pdf                   | §1 Fig 1           | S2a_ResearchVsDMPlots.R          | yes          | yes             | yes    |
| dm-sortsFull.tex                                            | §2 Tab 1           | S2b_DataMiningSummaryTables.R    | yes          | yes             | yes    |
| DM_pca.tex                                                  | §2 Tab 1           | S2c_DMCorrelationsPCATables.R    | yes          | yes             | yes    |
| theme_ez_decay.tex                                          | §2 Tab 2           | S2d_EZThemes.R                   | yes          | yes             | yes    |
| Fig2a/b/c/d (4)                                             | §2 Fig 2           | S2e_Fig2Plots.R                  | yes          | yes             | yes    |
| Fig2c_MatchedExclCorr_Tol10.pdf                             | §2 Fig 2 draft cmp | S2e_Fig2Plots.R                  | yes          | yes             | yes    |
| HandTable_MPStyleRegsMain.tex                               | §3 Tab 3           | hand-transcribed from S3b output | yes          | no              | yes    |
| HandTable_MPStyleRegsMain_Tol30.tex                         | §3 draft Tab 3b    | hand-transcribed from S3b output | yes          | no              | yes    |
| HandTable_MPStyleRegsTimeFE.tex                             | §3 Tab 4           | hand-transcribed from S3b output | yes          | no              | yes    |
| HandTable_MPStyleRegsTimeFE_Tol30.tex                       | §3 draft Tab 4b    | hand-transcribed from S3b output | yes          | no              | yes    |
| ApproachVsJournalsPart1/2/3.tex                             | §4 Tab 5           | S4a_DataCounts.R                 | yes          | yes             | yes    |
| Table_RiskAdjusted_TimeVarying_ff4_t2.tex                   | §4 Tab 6           | S4b_RVsDM_ByGroup.R              | yes          | yes             | yes    |
| Table_RiskAdjusted_TimeVarying_DisciplineJournal_ff4_t2.tex | §4 Tab 7           | S4b_RVsDM_ByGroup.R              | yes          | yes             | yes    |
| inspect-BMdec.tex                                           | §5 Tab 8           | S5a_InspectTables.R              | yes          | yes             | yes    |
| inspect-Mom12m.tex                                          | §5 Tab 9           | S5a_InspectTables.R              | yes          | yes             | yes    |
| inspect-Size.tex                                            | §5 Tab 10          | S5a_InspectTables.R              | yes          | yes             | yes    |

### Appendices

| Exhibit                                                     | Paper #   | Producer                                                  | Calculation? | Paper artifact? | Wired? |
| ----------------------------------------------------------- | --------- | --------------------------------------------------------- | ------------ | --------------- | ------ |
| Table_MPStyleRegsIndividualDM.tex                           | Tab B.1   | S3b_MPStyleDecayTables.R                                  | yes          | yes             | yes    |
| Fig2a/b/c/d_CI (4)                                          | Fig B.1   | S2e_Fig2Plots.R                                           | yes          | yes             | yes    |
| Fig_DM_t_top5Pct_AccountingOnly_CalendarSE.pdf              | Fig B.2   | S2a + Appendices/SA08_AccountingOnlyPlots.R               | yes          | yes             | yes    |
| Fig_DM_t_min_2_AccountingOnly_CalendarSE.pdf                | Fig B.2   | Appendices/SA08_AccountingOnlyPlots.R                     | yes          | yes             | yes    |
| Fig_DM_CAPM/FF4_tv_AccountingOnly_CalendarSE.pdf            | Fig B.2   | Appendices/SA09_AccountingOnlyAlphaPlots.R                | yes          | yes             | yes    |
| Fig_PublicationsVsDataMining_*_Correlation10 (4)            | Fig B.3   | Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R   | yes          | yes             | yes    |
| Fig_PublicationsVsDataMining_*_d_mean_0.1 (4)               | Fig B.4   | Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R   | yes          | yes             | yes    |
| Fig_DM_unspan_match_t_g_PCA/cor.pdf                         | Fig B.5   | Appendices/SA11_DMSpanPCAPlots.R                          | yes          | yes             | yes    |
| dm-sortsPost2003.tex                                        | Tab IA.1  | S2b_DataMiningSummaryTables.R                             | yes          | yes             | yes    |
| quantilesCorDM.tex                                          | Tab IA.2  | S2c_DMCorrelationsPCATables.R                             | yes          | yes             | yes    |
| theme_ez_decayinSampEnd1990.tex                             | Tab IA.3  | Appendices/SA12_EZThemesRobustness.R                      | yes          | yes             | yes    |
| theme_ez_decayinSampEnd2000.tex                             | Tab IA.4  | Appendices/SA12_EZThemesRobustness.R                      | yes          | yes             | yes    |
| theme_ez_decayinSampEnd2010.tex                             | Tab IA.5  | Appendices/SA12_EZThemesRobustness.R                      | yes          | yes             | yes    |
| Tab_RA_FS_Appendix.tex                                      | Tab IA.6  | S4b_RVsDM_ByGroup.R                                       | yes          | yes             | yes    |
| Tab_RA_FS_DisciplineJournal_Appendix.tex                    | Tab IA.7  | S4b_RVsDM_ByGroup.R                                       | yes          | yes             | yes    |
| SignalsByTheoryAndJournal.tex                               | Tab IA.8  | S4a_DataCounts.R                                          | yes          | yes             | yes    |
| tab:mp-theory-no-reg (inline)                               | Tab IA.9  | Excel2LaTeX (sheet "MP theory")                           | no           | no              | inline |
| Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel_ff4_t2.tex | Tab IA.10 | S4b_RVsDM_ByGroup.R                                       | yes          | yes             | yes    |
| Tab_RA_FS_AnyModelVsNoModel_Appendix.tex                    | Tab IA.11 | S4b_RVsDM_ByGroup.R                                       | yes          | yes             | yes    |
| samp_split_summary.tex                                      | Tab IA.12 | Appendices/SA03_StructuralBreak.R                         | yes          | yes             | yes    |
| Fig_FullSampleRiskAdj_capm/ff3_alpha_fs_t2.pdf              | Fig IA.1  | Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R | yes          | yes             | yes    |
| Fig_DecayVsWords_Names2.pdf                                 | Fig IA.2  | Appendices/SA04_DecayVsWordcountPlot.R                    | yes          | yes             | yes    |
| Fig_Risk_via_CAPM/FF3/FF5.pdf                               | Fig IA.3  | Appendices/SA01_RiskVsMispricingPlots.R                   | yes          | yes             | yes    |
| Fig_DecayVsJournal_Means.pdf                                | Fig IA.4  | Appendices/SA06_DecayVsJournal.R                          | yes          | yes             | yes    |
| break_vs_sampend.pdf                                        | Fig IA.5  | Appendices/SA03_StructuralBreak.R                         | yes          | yes             | yes    |

## Gaps

At its default stages, `MAIN.R` rebuilds the source outputs for all 55 generated
files referenced by the paper. It does **not** install those outputs into a
writing-repository `exhibits/` directory; that directory is outside this repo
and its location is deliberately unspecified.

For live writing, the six S4b fragments are read directly from top-level
`../Results/`. For an arXiv package, copy those same six basenames into the
writing repository's `exhibits/` directory before changing `\exhibitspath` to
`exhibits`; the checked-in copies there verify that alternate path and preserve
the package snapshot.

The five paper tables it does **not** directly produce are:

1. Tables 3 and 4 and their draft tolerance-matched comparisons (four
   `HandTable` files) — copied by hand from in-chain `S3b` output.
2. Table IA.9 — from an Excel sheet; no R script.

## Notes

- The former `4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R` monolith was split in
  phase one. With the hand-formatted and hand-transcribed rows included, every
  float in the compiled paper is accounted for. (Fig. B.5a/B.5b are the two
  panels of Fig. B.5, already listed.)
- Of the 58 files currently in the paper's `exhibits/` directory, the two not
  referenced by `sections/*.tex` are `HandTable_MPStyleRegsMain_Tol10.tex` and
  `HandTable_MPStyleRegsTimeFE_Tol10.tex`.
- In-chain scripts that produce no exhibit are upstream prep: chapter 2
  (`2a`–`2d`), chapter 3 (`3a`–`3e`, reusable caches), and
  `Appendices/SA02`/`Appendices/SA05` (decay
  tables/plots outside the paper contract, e.g. for slides or the referee
  response).
