# Intermediate data-file inventory (`../Data/Processed/`)

Last verified: 2026-08-15 21:12 EDT

This is the maintained inventory of the current `CZ-style-v8b` processed-data
directory. Producer and consumer claims were checked against live source;
archived source is outside its scope. Sizes are rounded decimal sizes. The
directory currently contains 30 files totaling 4.26 GB (3.97 GiB).

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
| CZ-style-v8b MatchPub.RData               | 123 MB   | 2b_MatchDataMinedToPub.R          | 2d, 3d, S5a, S5, SA10; load-bearing      |
| CZ-style-v8b MatchPubRiskAdjusted.RData   | 2,353 MB | 2d_RiskAdjustDataMinedSignals.R   | 3e, S4b, SA07, SA09; stale, load-bearing |
| CZ-style-v8b MatchedRiskAdjSummary.RData  | 6 MB     | 2d_RiskAdjustDataMinedSignals.R   | SA07 only; stale, load-bearing           |
```

`MatchPub.RData` was rebuilt on August 15 under the tightened matching rule and
contains 23,275 candidate pairs. Published/mined correlations now come from the
signed `cor` column in `dmcomp_sumstats.RDS`, so no separate correlation cache
is produced or consumed.

The 2.35 GB risk-adjusted pair cache and its summary were last rebuilt on
August 14. They retain the pre-tightening 71,670-pair universe and are therefore
stale. They cannot yet be removed because the listed live consumers still read
them.

## Chapter 3: benchmark and analysis caches

```text
| File                                      | Size   | Produced by                | Read by / status                    |
|-------------------------------------------|--------|----------------------------|-------------------------------------|
| dmcomp_sumstats.RDS                       | 139 MB | 3a_PrepDMBenchmarks.R      | S2a; current                        |
| dmtic_sumstats.RDS                        | 14 MB  | 3a_PrepDMBenchmarks.R      | S2a; current                        |
| raw_dm_benchmarks.RDS                     | 4 MB   | 3a_PrepDMBenchmarks.R      | S2e; current                        |
| matched_uncorr_benchmark.RDS              | 5 MB   | 3d_MatchedUncorrData.R     | S2e, S3a; current                   |
| risk_adjusted_dm_benchmarks.RDS           | 6 MB   | 3e_FactorAdjustedDMPrep.R  | S2e; stale lineage                  |
| ret_for_plot0.RDS                         | 3 MB   | 3a_PrepDMBenchmarks.R      | S2a, S4b, 3d, SA07; current         |
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

`3a_PrepDMBenchmarks.R` is now wired into `3_Precompute.R` and owns only the
raw benchmark family. Matching and factor-adjusted outputs remain separate:
`3d_MatchedUncorrData.R` writes the matched-uncorrelated cache, and
`3e_FactorAdjustedDMPrep.R` writes the risk-adjusted benchmark cache.

The current matched-uncorrelated cache contains 12,666 retained pairs across
137 predictors. The risk-adjusted benchmark was regenerated on August 15, but
from the stale risk-adjusted pair cache: its CAPM and FF4 panels still reflect
57,173 and 47,603 eligible pairs, respectively. It is therefore current as an
artifact but stale in lineage.

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
| CZ-style-v8b MatchPubCoefficients.RData   | 12 MB         | Removed, but 2d will recreate it if rerun             |
| ret_for_plot_MaxPredictors.RDS            | 4 MB          | Removed; retired top-N variant; no producer or reader |
| plotdat0.RDS                              | 66 MB         | Removed; producer write also removed                  |
| dm_pca_span_classification.RDS            | 230 MB        | Removed; producer write also removed                  |
| PairwiseCorrelationsActualAndMatches.RDS  | <1 MB         | Removed; redundant with signed dmcomp pair statistics |
| 3a_ResearchVsDMPrep.R                     | source file   | Removed; replaced by 3a_PrepDMBenchmarks.R            |
```

## Cleanup status

The completed cleanup removed about 1.26 GB of old-version, coefficient,
retired top-N, and unused Chapter 3 artifacts. The producer writes for
`plotdat0.RDS` and `dm_pca_span_classification.RDS` were removed, so chapter-3
runs will not recreate them. Their file-specific wrappers, dead preprocessing,
and diagnostic-only calculations were also removed. The remaining benchmark
and PCA calculations feed live Chapter 3 outputs.

The two text-analysis CSVs add another 13 MB with no live reference, but their
external provenance makes them a confirm-before-removal category. The stale
risk-adjusted set is much larger: `MatchPubRiskAdjusted.RData`,
`MatchedRiskAdjSummary.RData`, and the derived
`risk_adjusted_dm_benchmarks.RDS` total about 2.37 GB. That set remains
load-bearing until `3e`, `S4b`, `SA07`, and `SA09` are migrated or the upstream
risk-adjusted cache is rebuilt under the current 23,275-pair universe.
