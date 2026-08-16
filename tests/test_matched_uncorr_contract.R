# Focused checks for the canonical matched-uncorr producer/consumer contract.
#
# How to run: from flex-mining/, run
#   Rscript tests/test_matched_uncorr_contract.R
# Inputs:  Chapter 3 and Section 3 R scripts; optionally the generated caches
# Outputs: none; exits nonzero on failure.

producer <- readLines("3d_MatchedUncorrData.R")
figure_consumer <- readLines("S2e_Fig2Plots.R")
table_consumer <- readLines("S3a_MPStyleDecayModels.R")
appendix_consumer <- readLines("Appendices/SA10_ResearchVsDMRobustnessCorrelationsEtc.R")
matching_helpers <- readLines("helpers/matching.R")
precompute <- readLines("3_Precompute.R")
appendix_driver <- readLines("SA_Appendices.R")

cache_name <- "matched_uncorr_benchmark.RDS"
stopifnot(
  sum(grepl(cache_name, producer, fixed = TRUE)) >= 1L,
  sum(grepl(cache_name, figure_consumer, fixed = TRUE)) == 1L,
  sum(grepl(cache_name, table_consumer, fixed = TRUE)) >= 1L,
  !any(grepl("MatchPub.RData|PairwiseCorrelationsActualAndMatches|plotdat0\\$comp_matched",
             figure_consumer)),
  !any(grepl("MatchPub.RData|PairwiseCorrelationsActualAndMatches|plotdat0\\$comp_matched",
             table_consumer)),
  !any(grepl("PairwiseCorrelationsActualAndMatches", c(
    producer, appendix_consumer, matching_helpers, precompute, appendix_driver
  ), fixed = TRUE)),
  !any(grepl("MatchPub.RData", c(
    producer, appendix_consumer, precompute, appendix_driver
  ), fixed = TRUE)),
  any(grepl("select_matched_dm_pairs", matching_helpers, fixed = TRUE)),
  any(grepl("materialize_matched_dm_returns", matching_helpers, fixed = TRUE)),
  any(grepl("rho = cor * sign(rbar)", producer, fixed = TRUE)),
  any(grepl("rho = cor * sign(rbar)", appendix_consumer, fixed = TRUE))
)

cache_path <- file.path("../Data/Processed", cache_name)
if (file.exists(cache_path)) {
  benchmark <- readRDS(cache_path)
  stopifnot(
    identical(benchmark$metadata$short_name, "matched-uncorr"),
    benchmark$metadata$pair_count == sum(benchmark$pairs$keep_matched_uncorr),
    benchmark$metadata$predictor_count == length(benchmark$surviving_predictors),
    benchmark$metadata$predictor_count == length(unique(benchmark$panel$pubname)),
    nzchar(benchmark$metadata$pair_fingerprint_sha256)
  )
}

model_path <- "../Data/Processed/mp_style_decay_models.RDS"
if (file.exists(model_path)) {
  models <- readRDS(model_path)
  if (!is.null(models$metadata$pair_fingerprint_sha256) && file.exists(cache_path)) {
    stopifnot(
      identical(models$metadata$pair_fingerprint_sha256,
                benchmark$metadata$pair_fingerprint_sha256),
      models$metadata$predictor_count == benchmark$metadata$predictor_count,
      length(unique(vapply(c(models$main_scaled, models$main_unscaled),
                           stats::nobs, numeric(1)))) == 1L
    )
  }
}
