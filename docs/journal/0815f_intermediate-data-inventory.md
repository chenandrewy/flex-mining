# Intermediate data-file inventory (`../Data/Processed/`)

Timestamp: 2026-08-15 19:22 EDT

Snapshot taken while planning a slow, step-by-step cleanup of the DM-benchmark
pipeline. Producer/consumer columns were verified by grepping live source (not
memory); `CodeArchive/` excluded. Current data version is `CZ-style-v8b`.
"new `3a_PrepDM`" = the consolidated `3a_PrepDMBenchmarks.R` added this session
(committed `686772a`), not yet wired into `3_Precompute.R`.

## Chapter 1 -- foundational cleaned inputs (everything depends on these)

| File | Size | Produced by | Read by |
|---|---|---|---|
| `czsum_allpredictors.RDS` | <1 MB | ch1 (`1_Download_and_Clean`) | ~everywhere |
| `czret_keeponly.RDS` | 1 MB | ch1 | ~everywhere |
| `FullTextAnalysis.csv`, `TextAnalysis.csv` | 12 / <1 MB | text analysis (ch1/external) | S4a, classifiers |

## Chapter 2 -- mined universes & matching caches

| File | Size | Produced by | Read by | Status |
|---|---|---|---|---|
| `CZ-style-v8b LongShort.RData` | 300 MB | `2a` | 2b, 2d, new `3a_PrepDM`, S2d/S3a/S4/S5a/SA03/SA07/SA09, helpers | load-bearing |
| `ticker_Harvey2017JF.RDS` | 38 MB | `2c` | 3a raw path, 9 | load-bearing |
| `CZ-style-v8b MatchPub.RData` | 118 MB | `2b` | 2d, 3d, S5a, SA10 | legacy intermediate (recomputed by new 3a) |
| `PairwiseCorrelationsActualAndMatches.RDS` | <1 MB | `2b` | 3d, SA10 | legacy intermediate (recomputed by new 3a) |
| `CZ-style-v8b MatchPubRiskAdjusted.RData` | **2246 MB** | `2d` | 3e, S4b, SA07, SA09 | **STALE** (pre-10%-tightening, 71,670 pairs) |
| `CZ-style-v8b MatchedRiskAdjSummary.RData` | 6 MB | `2d` | SA07 | stale byproduct |
| `CZ-style-v8b MatchPubCoefficients.RData` | 12 MB | `2d` | nobody | **ORPHAN** |

## Chapter 3 -- benchmark & analysis caches

| File | Size | Produced by | Read by | Status |
|---|---|---|---|---|
| `dmcomp_sumstats.RDS` | 133 MB | `3a` / new `3a_PrepDM` | S2a | live |
| `dmtic_sumstats.RDS` | 14 MB | `3a` / new `3a_PrepDM` | S2a | live |
| `raw_dm_benchmarks.RDS` | 4 MB | `3a` / new `3a_PrepDM` | S2e | live |
| `matched_uncorr_benchmark.RDS` | 5 MB | `3d` / new `3a_PrepDM` | S2e, S3a | live |
| `risk_adjusted_dm_benchmarks.RDS` | 6 MB | `3e` / new `3a_PrepDM` | S2e | currently derived from stale cache |
| `ret_for_plot0.RDS` | 4 MB | `3a` | S2a, S4b, 3d, SA07 | live |
| `ret_for_plot1.RDS` | 3 MB | `3a` | S2a, SA08 | live |
| `ret_for_plot_MaxPredictors.RDS` | 4 MB | `3a` | S2a | live |
| `plotdat0.RDS` | 63 MB | `3a` | no live data reader | likely orphan byproduct |
| `PairwiseCorrelationsDM_ew.RDS` | 233 MB | ch2/`2a` | 3c | live (correlation/PCA) |
| `PairwiseCorrelationsDM_vw.RDS` | 17 MB | ch2/`2a` | 3c | live |
| `Summary_StatisticsDM_{ew,vw}.csv` | <1 MB | ch2/`2a` | 3b | live |
| `dm_pca_table.RDS`, `dm_correlation_quantiles.RDS` | <1 MB | `3c` | S2c | live |
| `dm_pca_span_classification.RDS` | 222 MB | `3f` | (3f internal) | live |
| `dm_span_analysis.RDS` | 6 MB | `3f` | SA11 | live |
| `sumsignal_oos_30y_*.csv` (6 files) | ~1 MB ea | ch3 | export/robustness | live |

## Section / Appendix caches

| File | Size | Produced by | Read by | Status |
|---|---|---|---|---|
| `mp_style_decay_models.RDS` | 933 MB | `S3a` | S3b, test | live (Tables 3/4) |

## Old-version leftovers (previous data vintage `CZ-style-v7`)

| File | Size | Status |
|---|---|---|
| `CZ-style-v7 MatchPub.RData` | 657 MB | **ORPHAN** -- superseded by v8b, no reader |
| `CZ-style-v7 LongShort.RData` | 294 MB | **ORPHAN** -- superseded by v8b, no reader |

## Cleanup read

Total ~5.3 GB. Clear-cut candidates with no live reader:
`MatchPubCoefficients.RData` (12 MB), the two `CZ-style-v7` files (951 MB), and
probably `plotdat0.RDS` (63 MB, only a `3a` byproduct -- confirm before
touching). The stale-but-still-read set (`MatchPubRiskAdjusted` 2.2 GB,
`MatchedRiskAdjSummary`) can only be removed once S4b/SA07/SA09 are migrated off
them.
