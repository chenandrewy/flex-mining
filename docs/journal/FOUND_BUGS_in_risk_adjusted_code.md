# FOUND_BUGS_in_risk_adjusted_code

Timestamp: 2026-08-15 23:10 EDT

This entry records an audit of the data-mined comparison sets used by
`Fig2a_FactorAdj.pdf` and
`Table_RiskAdjusted_TimeVarying_ff4_t2.tex`. It is a snapshot of the code and
Git history as inspected on the date above, not maintained documentation.

## Finding

The raw and factor-adjusted comparisons do not start from the same universe of
data-mined strategies.

- The raw benchmark uses the broad accounting-mining screen: orient the mined
  return to be positive, require raw original-sample `|t| > 2`, at least ten
  stocks in each leg, a complete final sample year, and at least 60 months of
  history. The raw mean-return and t-stat matching tolerances are effectively
  disabled (`Inf`).
- The CAPM and FF3-plus-momentum benchmarks first require the oriented mined
  strategy's raw original-sample t-stat to be within 10% of the published
  predictor's t-stat and its raw mean return to be within 30% of the published
  predictor's mean. Only this smaller pair set is factor-adjusted and then
  screened on factor-alpha `t > 2`.

Consequently, the factor-adjusted benchmark is not the factor-adjusted version
of the raw `|t| > 2` benchmark.

## Current counts

Counts recomputed from `dmcomp_sumstats.RDS` and
`risk_adjusted_dm_benchmarks.RDS`:

| Universe or stage | Pairs | Published predictors | Typical pairs per predictor |
|---|---:|---:|---:|
| Broad raw `|t| > 2` screen | 1,068,052 | 173 | mean 6,174; median 6,822 |
| Raw-stat-matched CAPM input | 68,419 | 162 | mean 422; median 351 |
| Usable original-sample CAPM abnormal returns | 67,909 | 162 | -- |
| CAPM-alpha `t > 2` | 57,173 | 160 | -- |
| CAPM pairs attached to the 136 predictors displayed in Figure 2a | 50,040 | 136 | mean 368; median 319 |

For each selected published/mined pair, `2d_RiskAdjustDataMinedSignals.R`
offers separate original- and post-sample CAPM fits. Thus Figure 2a entails at
most `68,419 * 2 = 136,838` pair/regime CAPM fits, rather than applying CAPM to
the roughly 1.07 million broad raw pairs. Applying the same two-regime CAPM
construction to the broad universe would entail roughly 2.14 million fits
before exploiting any repeated sample windows.

## Table 6 column mapping

`Table_RiskAdjusted_TimeVarying_ff4_t2.tex` quietly mixes the two universes:

| Table columns | Published measure | Data-mined comparator |
|---|---|---|
| Long-Short Return: Post-Sample | raw normalized return | none in this entry |
| Long-Short Return: Versus Data Mining | raw normalized return | broad raw `|t| > 2` `matchRet` |
| CAPM Alpha: Post-Sample | normalized published CAPM alpha | rows joined to the restricted benchmark |
| CAPM Alpha: Versus Data Mining | normalized published CAPM alpha | 10%/30% input universe, then CAPM-alpha `t > 2` |
| FF3 + Mom Alpha columns | normalized published FF4 alpha | 10%/30% input universe, then FF4-alpha `t > 2` |

The raw table data come from `ret_for_plot0`, whereas the CAPM and FF4 table
data come from `MatchPubRiskAdjusted.RData`. The 10%/30% restriction therefore
does not apply to either Long-Short Return column. It does affect both
factor-adjusted comparisons and, through the join to an available mined
benchmark, can affect which published predictor-months enter the
factor-adjusted `Post-Sample` columns.

## Calculation-repository history

- `c87d2e4` (2023-03-02), *Copying over Andrews file to generate matches much
  faster*, introduced the matcher with `t_reltol = 0.1` and
  `r_reltol = 0.3`. It predated the factor-adjusted analysis.
- `95ced0c` (2025-08-05), *Add risk-adjusted analysis comparing published vs
  data-mined signals*, introduced the CAPM analysis. From its first version,
  `2d_RiskAdjustDataMinedSignals.R` read the restricted `MatchPub.RData` rather
  than the broad raw `|t| > 2` universe.
- `472d7c4` (2025-08-05) added the separate original- and post-sample beta
  regimes used now, without changing the restricted input universe.
- `75ee52f` (2025-08-09) introduced the time-varying summary table and read the
  same restricted risk-adjusted pair cache for CAPM and FF3.
- `6ef4fec` (2025-10-18) added FF4 and the producer of the
  `_TimeVarying_ff4_t2` table. The exact FF4 table used the restricted universe
  from its inception.
- `6972834` (2026-08-08) created the consolidated current Figure 2a by copying
  the established risk-adjusted pipeline.
- `3b2d64c` (2026-08-15) retired the large raw `MatchPub.RData` cache but
  explicitly preserved the risk-adjusted 10%/30% universe as a legacy exhibit
  compatibility specification.

The current Figure 2a filename is recent, but its CAPM data-mined construction
has used the smaller universe since the first CAPM implementation on
2025-08-05.

## Writing-repository history

History was checked separately in `../risk-vs-writing`.

- `815c901` (2025-07-09) sketched a combined Raw/CAPM/FF3 table, but its values
  were explicitly provisional placeholders.
- `9e2ccc1` (2025-08-26), *Add risk-adjusted analysis results to paper*, first
  inserted computed risk-adjusted results into the manuscript as
  `Table_RiskAdjusted_t2.tex`. Its Raw/CAPM/FF3 columns already mixed the broad
  raw benchmark and restricted factor-adjusted benchmarks. The overall Raw
  entries were `56, 5` and the CAPM entries were `60, 8`, as they remain now.
- `0f02f46` (2025-10-24) added the manuscript's FF4 table. It carried the raw
  and CAPM values forward and added the restricted-universe FF4 comparison.
- `1bb1f53` (2026-08-14) wired the current generated
  `Table_RiskAdjusted_TimeVarying_ff4_t2.tex` into the reorganized manuscript;
  this was not the origin of the mixed construction.

Thus the implemented mismatch entered the calculation pipeline in August
2025 and has appeared in manuscript results since 2025-08-26.

## Disclosure issue

The manuscript describes matching the mined benchmarks to the published
predictors' sample periods, but it does not disclose that the CAPM and FF4
columns additionally match raw t-statistics and mean returns while the raw
columns do not. The current heterogeneity section even describes directly
controlling for t-stats and mean returns as an alternative robustness method,
although the main CAPM and FF4 comparisons already impose such a screen.

This matters for interpretation: differences across the Raw, CAPM, and FF4
columns combine changes in performance measurement with changes in the mined
candidate universe (and potentially the available published predictor-month
sample). They are not clean within-universe factor-adjustment comparisons.

## Possible follow-up

A future decision should explicitly choose one estimand and document it:

1. factor-adjust the broad raw `|t| > 2` universe, making the columns directly
   comparable but requiring a much larger calculation; or
2. apply the 10%/30% raw-stat matching restriction to the raw columns as well,
   making all table columns use the smaller matched universe; or
3. retain the mixed construction but label and justify the different
   universes in the table notes and manuscript.

No calculation or manuscript change was made as part of this audit.
