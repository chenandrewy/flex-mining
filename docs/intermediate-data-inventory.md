# Intermediate data-file inventory (`../Data/Processed/`)

Last verified: 2026-08-16 00:56 EDT

This is the maintained inventory of the current `CZ-style-v8b` processed-data
directory. Producer and consumer claims were checked against live source;
archived source is outside its scope. Sizes are rounded decimal sizes. The
directory currently contains 27 files totaling 0.84 GB (0.78 GiB).

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
```

The raw `MatchPub.RData` cache has been retired. Its 22,157 kept candidate
pairs are selected exactly from `dmcomp_sumstats.RDS`; consumers materialize
only the required pair-month returns from the versioned `LongShort.RData`.
The compact selection has the same sorted pair fingerprint as the former
cache. The other 1,118 legacy pairs belonged to predictors with `Keep == FALSE`
and had no live exhibit consumer. Published/mined correlations likewise come
from the signed `cor` column in `dmcomp_sumstats.RDS`.

The former multi-GB risk-adjusted pair cache and its summary have no live
producer or consumer. Factor adjustment now streams window-level aggregates
from the same broad accounting `|t| > 2` selection as the raw benchmark.

## Chapter 3: benchmark and analysis caches

```text
| File                                      | Size   | Produced by                | Read by / status                    |
|-------------------------------------------|--------|----------------------------|-------------------------------------|
| dmcomp_sumstats.RDS                       | 139 MB | 3a_PrepDMBenchmarks.R      | S2a; current                        |
| dmtic_sumstats.RDS                        | 14 MB  | 3a_PrepDMBenchmarks.R      | S2a; current                        |
| raw_dm_benchmarks.RDS                     | 8 MB   | 3a_PrepDMBenchmarks.R      | S2e, S3a; current                   |
| risk_adjusted_dm_benchmarks.RDS           | 4 MB   | 3c_FactorAdjustedDMPrep.R  | S2e, S4b, SA09; current             |
| ret_for_plot0.RDS                         | 3 MB   | 3a_PrepDMBenchmarks.R      | S2a; current                        |
| ret_for_plot1.RDS                         | 2 MB   | 3a_PrepDMBenchmarks.R      | S2a, SA08; current                  |
| PairwiseCorrelationsDM_ew.RDS             | 241 MB | Appendix SA11 PCA prep     | appendix-only cache                 |
| PairwiseCorrelationsDM_vw.RDS             | 18 MB  | Appendix SA11 PCA prep     | appendix-only cache                 |
| Summary_StatisticsDM_ew.csv               | <1 MB  | 3b_DataMiningSummary.R     | S2b; current                        |
| Summary_StatisticsDM_vw.csv               | <1 MB  | 3b_DataMiningSummary.R     | S2b; current                        |
| dm_pca_table.RDS                          | <1 MB  | Appendix SA11 PCA prep     | appendix-only                       |
| dm_correlation_quantiles.RDS              | <1 MB  | Appendix SA11 PCA prep     | appendix-only                       |
| dm_span_analysis.RDS                      | 5 MB   | Appendix SA11 span prep    | appendix-only                       |
| appendix_full_sample_dm_benchmarks.RDS    | 4 MB   | 3c_FactorAdjustedDMPrep.R  | SA07 figures/tables                 |
| sumsignal_oos_30y_*.csv (6 files)         | <1 MB  | 3b_DataMiningSummary.R     | S2b / export; current               |
```

`3a_PrepDMBenchmarks.R` owns the raw benchmark family, including the matched
and matched-uncorrelated event-time panel stored in `raw_dm_benchmarks.RDS`.
The retained pair table is transient: Appendix Table B.1 recomputes it in
memory using the same helper and validates its fingerprint against the raw
benchmark metadata. `3c_FactorAdjustedDMPrep.R` writes both the sample-specific
and full-sample broad-universe risk-adjusted benchmark caches from one shared
mined-return matrix.

The in-memory matched-uncorrelated universe remains a separate robustness
selection. The risk-adjusted benchmark starts from the 1,068,052-pair broad
raw universe; model-specific eligible counts are recorded on regeneration.

## Section and appendix caches

```text
| File                                      | Size   | Produced by               | Read by / status              |
|-------------------------------------------|--------|---------------------------|-------------------------------|
| mp_style_decay_models.RDS                 | 31 MB  | S3a_MPStyleDecayModels.R  | S3b; current (Tables 3 and 4) |
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
| matched_uncorr_benchmark.RDS              | 5 MB          | Removed; panel moved to raw benchmarks; pairs rebuilt in memory |
| matched_uncorr_pairs.RDS                  | <1 MB         | Removed; Table B.1 recomputes pairs in memory              |
| CZ-style-v8b MatchPubRiskAdjusted.RData   | 2.34 GB       | Removed; replaced by streamed broad-universe aggregates    |
| CZ-style-v8b MatchedRiskAdjSummary.RData  | 6 MB          | Removed; replaced by the compact risk-adjusted contract    |
```

## Cleanup status

The completed cleanup removed about 4.25 GB of old-version, broad matching,
coefficient, retired top-N, and unused Chapter 3 artifacts and model payloads.
Moving Appendix Table B.1 to its own script reduced `mp_style_decay_models.RDS`
by about 525 MB and eliminated the separate compact-pair cache. The producer
writes for
`plotdat0.RDS` and `dm_pca_span_classification.RDS` were removed, so chapter-3
runs will not recreate them. Their file-specific wrappers, dead preprocessing,
and diagnostic-only calculations were also removed. The remaining benchmark
calculations feed live Chapter 3 outputs; PCA is isolated to Appendix SA11.

The two text-analysis CSVs add another 13 MB with no live reference, but their
external provenance makes them a confirm-before-removal category. The former
2.35 GB risk-adjusted pair cache and summary have been replaced by two focused
contracts totaling under 8 MB.
