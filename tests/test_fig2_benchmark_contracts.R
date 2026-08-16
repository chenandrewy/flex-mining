# Focused checks for Figure 2 benchmark ownership and cache schemas.
#
# How to run: from flex-mining/, run
#   Rscript tests/test_fig2_benchmark_contracts.R
# Inputs: Chapter 3 producers, the Section 2 assembler, and optional generated
#         benchmark caches under ../Data/Processed
# Outputs: none; exits nonzero on failure.

raw_producer <- readLines("3a_PrepDMBenchmarks.R")
risk_producer <- readLines("3c_FactorAdjustedDMPrep.R")
span_producer <- readLines("Appendices/SA11_DMSpanPCAPrep.R")
figure_producer <- readLines("S2e_Fig2Plots.R")
precompute <- readLines("3_Precompute.R")
section_driver <- readLines("S2_ResearchVsDataMining.R")
section_renderer <- readLines("S2a_ResearchVsDMPlots.R")

stopifnot(
  any(grepl("raw_dm_benchmarks.RDS", raw_producer, fixed = TRUE)),
  any(grepl("risk_adjusted_dm_benchmarks.RDS", risk_producer, fixed = TRUE)),
  any(grepl("appendix_full_sample_dm_benchmarks.RDS", risk_producer, fixed = TRUE)),
  any(grepl("matched = matched_panel", raw_producer, fixed = TRUE)),
  !any(grepl("matched_uncorr_pairs.RDS", raw_producer, fixed = TRUE)),
  any(grepl('run_script("3a_PrepDMBenchmarks.R")', precompute, fixed = TRUE)),
  !any(grepl("3d_MatchedUncorrData.R", precompute, fixed = TRUE)),
  any(grepl('run_script("3c_FactorAdjustedDMPrep.R")', precompute, fixed = TRUE)),
  !file.exists("2d_RiskAdjustDataMinedSignals.R"),
  !file.exists("3e_FactorAdjustedDMPrep.R"),
  !file.exists("Appendices/SA07_FullSampleFactorAdjustedDMPrep.R"),
  !file.exists("3a_ResearchVsDMPrep.R"),
  any(grepl("select_accounting_t2_pairs", raw_producer, fixed = TRUE)),
  any(grepl("select_accounting_t2_pairs", risk_producer, fixed = TRUE)),
  !any(grepl("risk_adjusted_[tr]_reltol", risk_producer)),
  all(vapply(
    c("raw_dm_benchmarks.RDS", "risk_adjusted_dm_benchmarks.RDS"),
    function(cache) any(grepl(cache, figure_producer, fixed = TRUE)),
    logical(1)
  )),
  !any(grepl("MatchPub|PairwiseCorrelations|make_DM_event_returns|SelectDMStrats",
             figure_producer)),
  !any(grepl("3d_Fig2Data.R", precompute, fixed = TRUE)),
  !file.exists("3d_Fig2Data.R"),
  !any(grepl("ret_for_plot_MaxPredictors", raw_producer, fixed = TRUE)),
  !any(grepl("ret_for_plot_MaxPredictors", section_driver, fixed = TRUE)),
  !any(grepl("ret_for_plot_MaxPredictors", section_renderer, fixed = TRUE)),
  !any(grepl("plotdat0", raw_producer, fixed = TRUE)),
  !any(grepl("dm_pca_span_classification.RDS", span_producer, fixed = TRUE)),
  !any(grepl("MaxDMpredsPerPublished", section_renderer, fixed = TRUE))
)

raw_path <- "../Data/Processed/raw_dm_benchmarks.RDS"
if (file.exists(raw_path)) {
  raw <- readRDS(raw_path)
  stopifnot(
    all(c("published", "accounting_t2", "accounting_top5", "ticker_top5",
          "matched", "metadata") %in% names(raw)),
    all(vapply(
      raw[c("published", "accounting_t2", "accounting_top5", "ticker_top5")],
      function(x) all(c("pubname", "eventDate", "calendarDate", "return") %in%
                        names(x)), logical(1)
    )),
    identical(raw$metadata$schema_version, 1L),
    all(c("published_ret_scaled", "matched_ret_scaled",
          "matched_uncorr_ret_scaled") %in% names(raw$matched))
  )
}

risk_path <- "../Data/Processed/risk_adjusted_dm_benchmarks.RDS"
if (file.exists(risk_path)) {
  risk <- readRDS(risk_path)
  stopifnot(
    all(c("capm", "ff4", "published_stats", "window_diagnostics", "metadata") %in%
          names(risk)),
    all(vapply(risk[c("capm", "ff4")], function(model) {
      all(c("pubname", "eventDate", "calendarDate", "published_return",
            "dm_return", "n_eligible_pairs", "n_pairs_available") %in%
            names(model$panel))
    }, logical(1))),
    all(c("eligible_raw_t2", "eligible_capm_t2", "eligible_ff4_t2") %in%
          names(risk$published_stats)),
    all(vapply(risk[c("capm", "ff4")], function(model) {
      all(c("pubname", "eventDate", "calendarDate", "published_return") %in%
            names(model$published_panel))
    }, logical(1))),
    identical(risk$metadata$schema_version, 2L),
    identical(risk$metadata$base_universe, "accounting_t2"),
    identical(
      risk$metadata$base_pair_fingerprint_sha256,
      raw$metadata$accounting_t2$pair_fingerprint_sha256
    )
  )
}

full_sample_path <- "../Data/Processed/appendix_full_sample_dm_benchmarks.RDS"
if (file.exists(full_sample_path) && file.exists(raw_path)) {
  full_sample <- readRDS(full_sample_path)
  stopifnot(
    all(c("capm", "ff3", "published_stats", "window_diagnostics", "metadata") %in%
          names(full_sample)),
    identical(full_sample$metadata$schema_version, 1L),
    identical(full_sample$metadata$base_universe, "accounting_t2"),
    identical(
      full_sample$metadata$base_pair_fingerprint_sha256,
      raw$metadata$accounting_t2$pair_fingerprint_sha256
    )
  )
}
