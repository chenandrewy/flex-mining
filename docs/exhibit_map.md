# Exhibit map: script → output → exhibit

Which script builds each paper exhibit, whether `MAIN.R` rebuilds its
calculation and paper artifact, and whether the manuscript consumes it.

- **Contract:** analysis scripts write paper artifacts to top-level
  `../Results/`. For live writing, the paper source repository sets
  `\exhibitspath` to `../../Results`; its `exhibits/` directory mirrors the
  referenced basenames for arXiv packaging. The current snapshot holds 56 files,
  all referenced by `sections/*.tex` and all R-generated — the Section 3
  MP-style tables
  (`Table_MPStyleRegs{NoTimeFE,TimeFE,IndividualDM}.tex`) and the six S4b
  risk-adjusted tables included. No hand-formatted `HandTable` MP-style variants
  remain; the one hand-transcribed number (Table IA.9) is pasted inline in the
  paper `.tex`, not a file.
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
  2_DataMining.R            -> 2a 2c                  [chapter 2; off by default]
  3_Precompute.R            -> 3a 2d_RiskAdjust 3b 3c 3e_FactorAdjusted 3f
                                                        (prep caches; no exhibits)
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

Figure 2 consumes calculation-owned Chapter 3 contracts.
`3a_PrepDMBenchmarks.R` writes `raw_dm_benchmarks.RDS`, which contains the raw
mining variants and matched-uncorrelated event-time panel. Pair identities are
kept in memory during preparation and recomputed in memory for Table B.1. Next,
`2d_RiskAdjustDataMinedSignals.R` creates the versioned risk-adjusted pair
cache, and `3e_FactorAdjustedDMPrep.R` converts that cache into the CAPM/FF4
published and data-mined panels in `risk_adjusted_dm_benchmarks.RDS`. Raw and
risk-adjusted preparation remain separate calculation paths.
`S2e_Fig2Plots.R` reads the raw and risk-adjusted benchmark files, imposes
Figure-specific samples, computes rolling display statistics, and renders the
four panels plus confidence-interval variants into `../Results/`.

### `S2e_Fig2Plots.R` outputs

The normal run writes eight paper artifacts. The four files without `_CI` make
up main-text Figure 2; the four `_CI` variants make up Appendix Figure B.1.

| Panel | Main-text output                  | Appendix output                       | Comparison |
| ----- | --------------------------------- | ------------------------------------- | ---------- |
| a     | `Fig2a_FactorAdj.pdf`             | `Fig2a_FactorAdj_CI.pdf`              | CAPM and FF3+Mom factor-adjusted returns |
| b     | `Fig2b_PubSampleLimits.pdf`       | `Fig2b_PubSampleLimits_CI.pdf`        | Annual-accounting and pre-2003 publication samples |
| c     | `Fig2c_MatchedExclCorr.pdf`       | `Fig2c_MatchedExclCorr_CI.pdf`        | Matched data mining, with and without correlated matches |
| d     | `Fig2d_AltMining.pdf`             | `Fig2d_AltMining_CI.pdf`              | Top-5% accounting and ticker-symbol mining |

For diagnostics, setting `FIG2_OUTPUT_DIR` redirects the eight PDFs away from
`../Results/`. Setting `FIG2_DATA_OUTPUT_DIR` additionally writes
`fig2_panel_long.RDS` and `fig2_panel_agg.RDS` to the requested directory; these
opt-in data files are validation artifacts, not manuscript exhibits.

## Code Result → producer

Paper numbers are from the compiled paper in the separate writing repository.
The map itself keys on exhibit basenames and does not depend on the writing
repository's relative location.
Each exhibit carries its manuscript section as a prefix: `§N` in the main
text, `§B` in Appendix B, and `§IA.N` in the internet appendix. Each row
points to a single exhibit; Tab 1 and Fig B.2 recur because several producers
feed one exhibit.

Only one row is **hand-transcribed**: Table IA.9 has its numbers pasted inline
in the paper `.tex`; every other exhibit is generated by the script in its
Producer column. All six S4b-owned tables have all three states — their
calculations run in chain, S4b generates paper-specific fragments, and the
manuscript inputs those fragments.

Abbreviations used below to keep the columns narrow:

- `§N`, `§B`, and `§IA.N` = manuscript section IDs (Exhibit column).
- `(inline)` = number pasted inline in the paper `.tex`, hand-copied.
- `*` in the Code Result column abbreviates part of a file basename.
- `Tab_RA_FS_…` in the Code Result column abbreviates the file basename
  `Table_RiskAdjusted_FullSample_…`.

### Main text

| Exhibit     | Description                           | Code Result                               | Producer                      | Wired? |
| ----------- | ------------------------------------- | ----------------------------------------- | ----------------------------- | ------ |
| §1 Fig 1    | Decay plot research vs DM             | Fig_DM_t_min_2_se_indicators_calendar.pdf | S2a_ResearchVsDMPlots.R       | yes    |
| §2 Tab 1a   | Sum stats DM                          | dm-sortsFull.tex                          | S2b_DataMiningSummaryTables.R | yes    |
| §2 Tab 1b   | DM PCA explained variance             | DM_pca.tex                                | S2c_DMCorrelationsPCATables.R | yes    |
| §2 Tab 2    | Decay by economic theme               | theme_ez_decay.tex                        | S2d_EZThemes.R                | yes    |
| §2 Fig 2a   | Factor-adjusted decay                  | Fig2a_FactorAdj.pdf                       | S2e_Fig2Plots.R               | yes    |
| §2 Fig 2b   | Decay in restricted publication samples | Fig2b_PubSampleLimits.pdf               | S2e_Fig2Plots.R               | yes    |
| §2 Fig 2c   | Matched decay excluding correlated DM  | Fig2c_MatchedExclCorr.pdf                 | S2e_Fig2Plots.R               | yes    |
| §2 Fig 2d   | Decay under alternative mining methods | Fig2d_AltMining.pdf                       | S2e_Fig2Plots.R               | yes    |
| §3 Tab 3    | Decay regressions without time FE     | Table_MPStyleRegsNoTimeFE.tex             | S3b_MPStyleDecayTables.R      | yes    |
| §3 Tab 4    | Decay regressions with time FE        | Table_MPStyleRegsTimeFE.tex               | S3b_MPStyleDecayTables.R      | yes    |
| §4 Tab 5    | Predictor counts by theory/journal    | ApproachVsJournalsPart1/2/3.tex           | S4a_DataCounts.R              | yes    |
| §4 Tab 6    | Risk adjustment by theory/model       | Table_RiskAdjusted_TimeVarying_ff4_t2.tex | S4b_RVsDM_ByGroup.R           | yes    |
| §4 Tab 7    | Risk adjustment by discipline/journal | Table_*_DisciplineJournal_ff4_t2.tex      | S4b_RVsDM_ByGroup.R           | yes    |
| §5 Tab 8    | Matched DM for book-to-market         | inspect-BMdec.tex                         | S5a_InspectTables.R           | yes    |
| §5 Tab 9    | Matched DM for momentum               | inspect-Mom12m.tex                        | S5a_InspectTables.R           | yes    |
| §5 Tab 10   | Matched DM for size                   | inspect-Size.tex                          | S5a_InspectTables.R           | yes    |

### Appendices

| Exhibit           | Description                                       | Code Result                                      | Producer                                                  | Wired? |
| ----------------- | ------------------------------------------------- | ------------------------------------------------ | --------------------------------------------------------- | ------ |
| §B Tab B.1        | Decay regressions by DM predictor                 | Table_MPStyleRegsIndividualDM.tex                | Appendices/SA13_MPStyleRegsIndividualDM.R                 | yes    |
| §B Fig B.1a       | Factor-adjusted decay with CIs                       | Fig2a_FactorAdj_CI.pdf                            | S2e_Fig2Plots.R                                           | yes    |
| §B Fig B.1b       | Restricted publication samples with CIs             | Fig2b_PubSampleLimits_CI.pdf                      | S2e_Fig2Plots.R                                           | yes    |
| §B Fig B.1c       | Matched decay excluding correlated DM, with CIs      | Fig2c_MatchedExclCorr_CI.pdf                      | S2e_Fig2Plots.R                                           | yes    |
| §B Fig B.1d       | Alternative mining methods with CIs                  | Fig2d_AltMining_CI.pdf                            | S2e_Fig2Plots.R                                           | yes    |
| §B Fig B.2a       | Accounting DM significant decay                   | Fig_DM_t_min_2_AccountingOnly_CalendarSE.pdf     | Appendices/SA08_AccountingOnlyPlots.R                     | yes    |
| §B Fig B.2b       | Accounting DM top 5% decay                        | Fig_DM_t_top5Pct_AccountingOnly_CalendarSE.pdf   | S2a + Appendices/SA08_AccountingOnlyPlots.R               | yes    |
| §B Fig B.2c–d     | Accounting DM risk-adjusted decay                 | Fig_DM_CAPM/FF4_tv_AccountingOnly_CalendarSE.pdf | Appendices/SA09_AccountingOnlyAlphaPlots.R                | yes    |
| §B Fig B.3a–d     | Decay excluding correlated DM                     | Fig_PublicationsVsDataMining_*_Correlation10 (4) | Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R   | yes    |
| §B Fig B.4a–d     | Decay with tighter DM matches                     | Fig_PublicationsVsDataMining_*_d_mean_0.1 (4)    | Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R   | yes    |
| §B Fig B.5a–b     | Unspanned DM PCA and correlations                 | Fig_DM_unspan_match_t_g_PCA/cor.pdf              | Appendices/SA11_DMSpanPCAPlots.R                          | yes    |
| §IA.1 Tab IA.1    | Post-2003 sum stats DM                            | dm-sortsPost2003.tex                             | S2b_DataMiningSummaryTables.R                             | yes    |
| §IA.1 Tab IA.2a   | DM return correlations                            | quantilesCorDM.tex                               | S2c_DMCorrelationsPCATables.R                             | yes    |
| §IA.2 Tab IA.3    | Decay by theme through 1990                       | theme_ez_decayinSampEnd1990.tex                  | Appendices/SA12_EZThemesRobustness.R                      | yes    |
| §IA.2 Tab IA.4    | Decay by theme through 2000                       | theme_ez_decayinSampEnd2000.tex                  | Appendices/SA12_EZThemesRobustness.R                      | yes    |
| §IA.2 Tab IA.5    | Decay by theme through 2010                       | theme_ez_decayinSampEnd2010.tex                  | Appendices/SA12_EZThemesRobustness.R                      | yes    |
| §IA.3 Tab IA.6    | Full-sample risk adjustment by theory             | Tab_RA_FS_Appendix.tex                           | S4b_RVsDM_ByGroup.R                                       | yes    |
| §IA.3 Tab IA.7    | Full-sample risk adjustment by discipline/journal | Tab_RA_FS_DisciplineJournal_Appendix.tex         | S4b_RVsDM_ByGroup.R                                       | yes    |
| §IA.5 Tab IA.8    | Predictor counts by theory/journal                | SignalsByTheoryAndJournal.tex                    | S4a_DataCounts.R                                          | yes    |
| §IA.5 Tab IA.9    | Predictor counts by theory                        | tab:mp-theory-no-reg (inline)                    | Excel2LaTeX (sheet "MP theory")                           | inline |
| §IA.5 Tab IA.10   | Risk adjustment by model use                      | Table_*_AnyModelVsNoModel_ff4_t2.tex             | S4b_RVsDM_ByGroup.R                                       | yes    |
| §IA.5 Tab IA.11   | Full-sample risk adjustment: model use            | Tab_RA_FS_AnyModelVsNoModel_Appendix.tex         | S4b_RVsDM_ByGroup.R                                       | yes    |
| §IA.6 Tab IA.12   | Returns by sample split                           | samp_split_summary.tex                           | Appendices/SA03_StructuralBreak.R                         | yes    |
| §IA.3 Fig IA.1a–b | Full-sample risk-adjusted decay                   | Fig_FullSampleRiskAdj_capm/ff3_alpha_fs_t2.pdf   | Appendices/SA07_FullSampleRiskAdjustedResearchVsDMPlots.R | yes    |
| §IA.4 Fig IA.2    | Decay vs paper word count                         | Fig_DecayVsWords_Names2.pdf                      | Appendices/SA04_DecayVsWordcountPlot.R                    | yes    |
| §IA.4 Fig IA.3a–c | Risk adjustment diagnostics                       | Fig_Risk_via_CAPM/FF3/FF5.pdf                    | Appendices/SA01_RiskVsMispricingPlots.R                   | yes    |
| §IA.5 Fig IA.4    | Decay by journal                                  | Fig_DecayVsJournal_Means.pdf                     | Appendices/SA06_DecayVsJournal.R                          | yes    |
| §IA.6 Fig IA.5    | Break dates vs sample ends                        | break_vs_sampend.pdf                             | Appendices/SA03_StructuralBreak.R                         | yes    |

## Gaps

At its default stages, `MAIN.R` rebuilds the source outputs for every generated
exhibit referenced by the paper. It does **not** install those outputs into a
writing-repository `exhibits/` directory; that directory is outside this repo
and its location is deliberately unspecified.

For live writing, the manuscript reads exhibit basenames directly from top-level
`../Results/` (the writing repository sets `\exhibitspath` to
`../../Results`). For an arXiv package, copy those
basenames into the writing repository's `exhibits/` directory and switch
`\exhibitspath` to `exhibits`; the checked-in copies there verify that alternate
path and preserve the package snapshot.

The only paper number `MAIN.R` does not produce with an R script is:

1. Table IA.9 — transcribed from an Excel sheet; no R script.

## Notes

- `4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R` was split and its risk-adjusted
  logic folded into `S4b_RVsDM_ByGroup.R`; every float in the compiled paper is
  accounted for. (Fig. B.5a/B.5b are the two panels of Fig. B.5, already
  listed.)
- In-chain scripts that produce no exhibit are upstream prep: chapter 2
  (`2a`–`2d`), chapter 3 (`3a`–`3e`, reusable caches), and
  `Appendices/SA02`/`Appendices/SA05` (decay
  tables/plots outside the paper contract, e.g. for slides or the referee
  response).
