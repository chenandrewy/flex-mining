# Map: script -> output -> exhibit

Snapshot written 2026-08-13 20:51 EDT. Branch `cleanup/main.r`.
Step 1 of the refactor plan in `260813b,plan,refactor.md`. Read-only; nothing
deleted or renamed. This is the legibility artifact that must exist before any
cut.

## Method

- Contract = every exhibit `\input`/`\includegraphics`ed from
  `../risk-vs-writing/latex-risk-vs/sections/*.tex`.
- Producer = the `.R`/`.py` script whose write target (`ggsave`, `writeLines`,
  `saveRDS`, `kbl`, ...) matches the exhibit basename. `paste0()`-built names
  were resolved by grepping distinctive fragments, not the whole stem.
- "In chain" = reachable from `MAIN.R` by `source()` at the default
  `run_downloads = FALSE`. Chain traced below.

## Correction to the plan's headline number

The contract is **53 exhibits, all 53 referenced** — not 54/54.
`exhibits/` holds 53 files plus an empty `Extra/` subdirectory; the earlier
`ls | wc -l` counted `Extra/` as a file. Every referenced exhibit resolves to a
file, and every file is referenced. So "unused" stays mechanically decidable;
the denominator is just 53.

## The MAIN.R chain (run_downloads = FALSE)

```
MAIN.R
  0_Environment.R                      (sourced by ~everything; support)
  1a_ValidDenoms.R
  2_DataMining.R      -> 2a 2b 2c 2d
  3_RiskVsMispricing.R-> 3a 3b 3c 3d 3e 3f 3g
  4_ResearchVsDataMining.R
        -> 4a 4b 4c1 4c2 4c3
           4c4_...TVFF4   4c5_...Plots(non-FF4)
           4c6_AccountingOnlyPlots   4c7   4d   4d2   4e
  6_TextAnalysis.R    -> 6a 6b
  8_DMThemes.R        -> 8a 8b
  99_ExportDataToCsv.R
```
`1_Download_and_Clean.R` is sourced only when `run_downloads = TRUE`.
Note which variant is in the chain: `3a` (not `3a...FF4`), `4c5...Plots` (not
`...FF4`), `4c6_AccountingOnlyPlots` (not `4c6_MPStyleDecayTables`), and for
`4c4` the FF4 variant *is* the one sourced (not `4c4...TV`).

## Exhibit -> producer (all 53)

| Exhibit | Producer | In chain? |
|---|---|---|
| ApproachVsJournalsPart1/2/3.tex | 3c_DataCounts.R | yes |
| SignalsByTheoryAndJournal.tex | 3c_DataCounts.R | yes |
| samp_split_summary.tex | 3d_StructuralBreak.R | yes |
| break_vs_sampend.pdf | 3d_StructuralBreak.R | yes |
| Fig_DecayVsWords_Names2.pdf | 3e_DecayVsWordcountPlot.R | yes |
| Fig_DecayVsJournal_Means.pdf | 3g_DecayVsJournal.R | yes |
| Fig_Risk_via_CAPM/FF3/FF5.pdf | 3a_RiskVsMispricingPlots.R | yes |
| dm-sortsFull.tex, dm-sortsPost2003.tex | 4a_DataMiningSummary.R | yes |
| DM_pca.tex, quantilesCorDM.tex | 4b_DMCorrelationsPCASummary.R | yes |
| Fig_DM_t_min_2_se_indicators_calendar.pdf | 4c2_ResearchVsDMPlots.R | yes |
| Fig_DM_t_top5Pct_AccountingOnly_CalendarSE.pdf | 4c2 and 4c6_AccountingOnlyPlots | yes |
| Fig_DM_t_min_2_AccountingOnly_CalendarSE.pdf | 4c6_AccountingOnlyPlots.R | yes |
| Fig_DM_CAPM/FF4_tv_AccountingOnly_CalendarSE.pdf | 4c7_AccountingOnlyAlphaPlots.R | yes |
| Fig_FullSampleRiskAdj_capm/ff3_alpha_fs_t2.pdf | 4c5_FullSampleRiskAdjustedResearchVsDMPlots.R | yes |
| Table_RiskAdjusted_FullSample_Appendix.tex | 4c5_FullSampleRiskAdjustedResearchVsDMPlots.R | yes |
| Table_RiskAdjusted_FullSample_DisciplineJournal_Appendix.tex | 4c5 (non-FF4) | yes |
| Table_RiskAdjusted_FullSample_AnyModelVsNoModel_Appendix.tex | 4c5 (non-FF4) | yes |
| Fig_PublicationsVsDataMining_* (8 files) | 4d_ResearchVsDMRobustnessCorrelationsEtc.R | yes |
| inspect-BMdec/Mom12m/Size.tex | 4d2_InspectTables.R | yes |
| Fig_DM_unspan_match_t_g_PCA/cor.pdf | 4e_DM_Span_PCA.R | yes |
| theme_ez_decay.tex | 8a_EZThemes.R | yes |
| theme_ez_decayinSampEnd1990/2000/2010.tex | 8b_EZThemesRobustness.R | yes |
| **Table_MPStyleRegsIndividualDM.tex** | **4c6_MPStyleDecayTables.R** | **NO** |
| **Fig2a/b/c/d + _CI (8 files)** | **4c20b_Fig2Plots.R** | **NO** |
| HandTable_MPStyleRegsMain.tex | none (hand-transcribed) | n/a |
| HandTable_MPStyleRegsTimeFE.tex | none (hand-transcribed) | n/a |

Corrections to the plan's guesses: `Fig_PublicationsVsDataMining_*` comes from
`4d`, not `4c2`. `Fig_FullSampleRiskAdj_*` and `Table_RiskAdjusted_FullSample_*`
come from the non-FF4 `4c5`.

## The real bugs: used exhibits NOT rebuilt by MAIN.R

Only three exhibit-producing paths are outside the chain, and two are scripts:

1. **`4c6_MPStyleDecayTables.R`** — writes `Table_MPStyleRegsIndividualDM.tex`
   (used directly) and the `Table_MPStyleRegs{Main,Unscaled}` regression tables
   that the two `HandTable_MPStyleRegs*` exhibits are hand-transcribed from.
   Out of chain, and colliding in prefix with the in-chain
   `4c6_AccountingOnlyPlots.R`.
2. **`4c20b_Fig2Plots.R`** — writes all 8 `Fig2*` exhibits. Out of chain. Its
   siblings `4c20a` (data), `4c20c`, `4c20d` are alternatives that also touch
   Fig2 names but are not the producers of the used files.
3. **HandTables** — no script writes them; they are transcribed by hand from
   #1's output. The drift noted in the plan (-36.7 vs -36.6) lives here.

Fold #1 and #2 into the chain and MAIN.R rebuilds the whole paper except the two
hand-transcribed tables.

## In-chain but produces no used exhibit

Necessary data/prep upstream (keep — feed downstream `.RDS` under
`../Data/Processed`): `1a`, `2a`, `2b`, `2c`, `2d`, `4c1` (prep), `4c3`
(writes `dmcomp_sumstats`/`dmtic_sumstats` consumed by `4c4`).

Dead-ends to audit (in chain, emit only outputs absent from the 53):
- `3b_RegDecayTable.R` -> `../Results/RegressionMultiColumns*.csv`
- `3f_DecayVsModelcountPlot.R` -> `Fig_DecayVsModel_*` (not an exhibit)
- `4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R` -> `Fig_RiskAdj_*`,
  `Table_RiskAdjusted_{TheoryModel,TimeVarying,...}` — none in the 53, yet this
  is the `4c4` variant MAIN sources. Surprising; resolve before trusting it.
- `6a`, `6b` -> `summary_text.csv`, `text_examples.csv`, word lists
- `2b` -> `../Results/PairwiseCorrelationsActualAndMatches.RDS`
- `99_ExportDataToCsv.R` -> `EW.csv`/`VW.csv`/`SignalList.csv` (data export
  deliverable, not a paper exhibit — keep on its own merit)

Each writes into `../Data`/`../Results` but nothing hand-copied to `exhibits/`
depends on it. They are either genuinely dead or feed slides/jeff-response
(out of scope, so they will read as unused — the noted risk).

## Out-of-chain scripts producing NO used exhibit (cut candidates)

Never sourced by MAIN.R and no output among the 53. Subject to the slides/
scope caveat, these are the deletion set:

- Variants superseded in the chain: `3a_RiskVsMispricingPlotsFF4.R`,
  `4c4_RiskAdjustedResearchVsDMPlotsTV.R`,
  `4c5_FullSampleRiskAdjustedResearchVsDMPlotsFF4.R`
- PV / valuation-beta block: `4c12`, `4c13`, `4c14`, `4c15`, `4c16`, `4c17`,
  `4c18a`(py), `4c18b`, `4c18c`, `4c18d`, `4c18e`, `4c18f`
- Other 4c: `4c8_CalendarTimePlots.R`, `4c9_SharpeRatioAnalysis.R`,
  `4c10c_RAPS_Exact.R`, `4c10d_EB_Bootstrap.R`, `4c11_BetaEvolution.R`,
  `4c21_MPStyleDecayTablesAcctOnly.R`
- Fig2 alternatives (not the used producer): `4c20a`, `4c20c`, `4c20d`
- Loose: `Word2vec.R`, `DEMO-DataMiningVsFamaFrench1992.R`,
  `install_pcamethods.R`

`4b`, which the plan flagged as spending an hour and possibly deletable, is in
fact the producer of `DM_pca.tex` and `quantilesCorDM.tex` — **keep**. Those two
exhibits had no known producer in the plan; answered.
