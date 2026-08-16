# Intermediate data-file inventory (`../Data/Processed/`)

Last verified: 2026-08-15 21:58 EDT

This is the maintained inventory of the current `CZ-style-v8b` processed-data
directory. Producer and consumer claims were checked against live source;
archived source is outside its scope. Sizes are rounded decimal sizes. The
directory currently contains 29 files totaling 4.12 GB (3.84 GiB).

## Chapter 1: foundational cleaned inputs

```text
| File                                      | Size  | Produced by                | Read by / status             |
|-------------------------------------------|-------|----------------------------|------------------------------|
| czsum_allpredictors.RDS                   | <1 MB | 1_Download_and_Clean.R     | Most chapters; load-bearing  |
| czret_keeponly.RDS                        | 1 MB  | 1_Download_and_Clean.R     | Most chapters; load-bearing  |
| FullTextAnalysis.csv                      | 13 MB | External / historical      | No live source reference     |
| TextAnalysis.csv                          | <1 MB | External / historical      | No live source reference     |
```

The two text-analysis CSVs remain on disk, but the current source tree reads
`DataIntermediate/TextClassification.csv` instead. Their provenance should be
confirmed before deletion.

## Chapter 2: mined universes and matching caches

```text
| File                                      | Size     | Produced by                       | Read by / status                         |
|-------------------------------------------|----------|-----------------------------------|------------------------------------------|
| CZ-style-v8b LongShort.RData              | 313 MB   | 2a_CompustatToLongshort.R         | Many chapters; load-bearing              |
| ticker_Harvey2017JF.RDS                   | 40 MB    | 2c_TickerToLongshort.R            | 3a and 9; load-bearing                   |
| CZ-style-v8b MatchPubRiskAdjusted.RData   | 2,337 MB | 2d_RiskAdjustDataMinedSignals.R | 3e, S4b, SA07, SA09; current, load-bearing |
| CZ-style-v8b MatchedRiskAdjSummary.RData  | 6 MB     | 2d_RiskAdjustDataMinedSignals.R | SA07 only; current, load-bearing           |
```

The raw `MatchPub.RData` cache has been retired. Its 22,157 kept candidate
pairs are selected exactly from `dmcomp_sumstats.RDS`; consumers materialize
only the required pair-month returns from the versioned `LongShort.RData`.
The compact selection has the same sorted pair fingerprint as the former
cache. The other 1,118 legacy pairs belonged to predictors with `Keep == FALSE`
and had no live exhibit consumer. Published/mined correlations likewise come
from the signed `cor` column in `dmcomp_sumstats.RDS`.

The risk-adjusted pair cache and summary were rebuilt without `MatchPub.RData`.
Their explicitly named compatibility specification retains the established 10%
t-statistic and 30% mean-return tolerances: 68,419 kept base pairs, with 57,173
CAPM-eligible and 47,603 FF4-eligible pairs. This is intentionally distinct from
the canonical matched-uncorr benchmark's 10%/10% screen and preserves the
frozen S4b exhibit contracts.

## Chapter 3: benchmark and analysis caches

```text
| File                                      | Size   | Produced by                | Read by / status                    |
|-------------------------------------------|--------|----------------------------|-------------------------------------|
| dmcomp_sumstats.RDS                       | 139 MB | 3a_PrepDMBenchmarks.R      | S2a; current                        |
| dmtic_sumstats.RDS                        | 14 MB  | 3a_PrepDMBenchmarks.R      | S2a; current                        |
| raw_dm_benchmarks.RDS                     | 8 MB   | 3a_PrepDMBenchmarks.R      | S2e, S3a; current                   |
| matched_uncorr_pairs.RDS                  | <1 MB  | 3a_PrepDMBenchmarks.R      | S3a; current                        |
| risk_adjusted_dm_benchmarks.RDS           | 6 MB   | 3e_FactorAdjustedDMPrep.R  | S2e; current                        |
| ret_for_plot0.RDS                         | 3 MB   | 3a_PrepDMBenchmarks.R      | S2a, S4b, SA07; current             |
| ret_for_plot1.RDS                         | 2 MB   | 3a_PrepDMBenchmarks.R      | S2a, SA08; current                  |
| PairwiseCorrelationsDM_ew.RDS             | 241 MB | 3c_DMCorrelationsPCA.R     | 3c; live phase cache                |
| PairwiseCorrelationsDM_vw.RDS             | 18 MB  | 3c_DMCorrelationsPCA.R     | 3c; live phase cache                |
| Summary_StatisticsDM_ew.csv               | <1 MB  | 3b_DataMiningSummary.R     | S2b; current                        |
| Summary_StatisticsDM_vw.csv               | <1 MB  | 3b_DataMiningSummary.R     | S2b; current                        |
| dm_pca_table.RDS                          | <1 MB  | 3c_DMCorrelationsPCA.R     | S2c; current                        |
| dm_correlation_quantiles.RDS              | <1 MB  | 3c_DMCorrelationsPCA.R     | S2c; current                        |
| dm_span_analysis.RDS                      | 5 MB   | 3f_DMSpanPCA.R             | SA11; current                       |
| sumsignal_oos_30y_*.csv (6 files)         | <1 MB  | 3b_DataMiningSummary.R     | S2b / export; current               |
```

`3a_PrepDMBenchmarks.R` owns the raw benchmark family, including the matched
and matched-uncorrelated event-time panel stored in `raw_dm_benchmarks.RDS`.
It writes the compact retained-pair table separately for S3a's individual-DM
regressions. `3e_FactorAdjustedDMPrep.R` writes the risk-adjusted benchmark
cache.

The compact matched-uncorrelated table contains 12,666 retained pairs across
137 predictors. The separately specified risk-adjusted benchmark contains
57,173 CAPM-eligible and 47,603 FF4-eligible pairs.

## Section and appendix caches

```text
| File                                      | Size   | Produced by               | Read by / status              |
|-------------------------------------------|--------|---------------------------|-------------------------------|
| mp_style_decay_models.RDS                 | 977 MB | S3a_MPStyleDecayModels.R  | S3b; current (Tables 3 and 4) |
```

## Removed or retired artifacts

```text
| File                                      | Previous size | Current status                                        |
|-------------------------------------------|---------------|-------------------------------------------------------|
| CZ-style-v7 MatchPub.RData                | 657 MB        | Removed; superseded by v8b and had no live reader     |
| CZ-style-v7 LongShort.RData               | 294 MB        | Removed; superseded by v8b and had no live reader     |
| CZ-style-v8b MatchPubCoefficients.RData   | 12 MB         | Removed; unused producer write also removed            |
| ret_for_plot_MaxPredictors.RDS            | 4 MB          | Removed; retired top-N variant; no producer or reader |
| plotdat0.RDS                              | 66 MB         | Removed; producer write also removed                  |
| dm_pca_span_classification.RDS            | 230 MB        | Removed; producer write also removed                  |
| PairwiseCorrelationsActualAndMatches.RDS  | <1 MB         | Removed; redundant with signed dmcomp pair statistics |
| CZ-style-v8b MatchPub.RData               | 123 MB        | Removed; exactly replaced by dmcomp keys + on-demand LongShort joins |
| 2b_MatchDataMinedToPub.R                  | source file   | Removed with the broad raw pair-month cache            |
| 3a_ResearchVsDMPrep.R                     | source file   | Removed; replaced by 3a_PrepDMBenchmarks.R            |
| matched_uncorr_benchmark.RDS              | 5 MB          | Removed; panel moved to raw benchmarks and pairs split out |
```

## Cleanup status

The completed cleanup removed about 1.38 GB of old-version, broad matching,
coefficient, retired top-N, and unused Chapter 3 artifacts. The producer writes for
`plotdat0.RDS` and `dm_pca_span_classification.RDS` were removed, so chapter-3
runs will not recreate them. Their file-specific wrappers, dead preprocessing,
and diagnostic-only calculations were also removed. The remaining benchmark
and PCA calculations feed live Chapter 3 outputs.

The two text-analysis CSVs add another 13 MB with no live reference, but their
external provenance makes them a confirm-before-removal category. The remaining
risk-adjusted set totals about 2.35 GB and remains load-bearing until `3e`,
`S4b`, `SA07`, and `SA09` are migrated to focused prepared contracts.
