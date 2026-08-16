# Focused checks for the matched event-panel and in-memory pair contract.
#
# How to run: from flex-mining/, run
#   Rscript tests/test_matched_uncorr_contract.R
# Inputs: Chapter 3 and Section 2/3 scripts; optionally generated caches
# Outputs: none; exits nonzero on failure.

producer <- readLines("3a_PrepDMBenchmarks.R")
figure_consumer <- readLines("S2e_Fig2Plots.R")
table_consumer <- readLines("S3a_MPStyleDecayModels.R")
table_renderer <- readLines("S3b_MPStyleDecayTables.R")
appendix_consumer <- readLines("Appendices/SA13_MPStyleRegsIndividualDM.R")
appendix_driver <- readLines("SA_Appendices.R")
matching_helpers <- readLines("helpers/matching.R")
precompute <- readLines("3_Precompute.R")

old_cache <- "matched_uncorr_benchmark.RDS"
pair_cache <- "matched_uncorr_pairs.RDS"
stopifnot(
  !file.exists(file.path("../Data/Processed", pair_cache)),
  !file.exists("3d_MatchedUncorrData.R"),
  !any(grepl("3d_MatchedUncorrData.R", precompute, fixed = TRUE)),
  !any(grepl(old_cache, c(producer, figure_consumer, table_consumer),
             fixed = TRUE)),
  any(grepl("matched = matched_panel", producer, fixed = TRUE)),
  !any(grepl(pair_cache, c(producer, table_consumer, appendix_consumer),
             fixed = TRUE)),
  any(grepl("raw_benchmarks$matched", figure_consumer, fixed = TRUE)),
  any(grepl("benchmark$matched", table_consumer, fixed = TRUE)),
  any(grepl("build_matched_uncorr_pair_data", producer, fixed = TRUE)),
  any(grepl("build_matched_uncorr_pair_data", appendix_consumer, fixed = TRUE)),
  any(grepl("rho = cor * sign(rbar)", matching_helpers, fixed = TRUE)),
  any(grepl("matched_pair_fingerprint", appendix_consumer, fixed = TRUE)),
  any(grepl("Table_MPStyleRegsIndividualDM.tex", appendix_consumer,
             fixed = TRUE)),
  !any(grepl("individual_dm", table_renderer, fixed = TRUE)),
  any(grepl('run_script("Appendices/SA13_MPStyleRegsIndividualDM.R")',
             appendix_driver, fixed = TRUE))
)

raw_path <- "../Data/Processed/raw_dm_benchmarks.RDS"
if (file.exists(raw_path)) {
  raw <- readRDS(raw_path)
  metadata <- raw$metadata$matched
  stopifnot(
    all(c("matched", "metadata") %in% names(raw)),
    all(c(
      "published_ret_scaled", "matched_ret_scaled",
      "matched_uncorr_ret_scaled"
    ) %in% names(raw$matched)),
    metadata$predictor_count == length(unique(raw$matched$pubname)),
    metadata$pair_count > 0L,
    nzchar(metadata$pair_fingerprint_sha256)
  )
}

model_path <- "../Data/Processed/mp_style_decay_models.RDS"
if (file.exists(model_path) && file.exists(raw_path)) {
  models <- readRDS(model_path)
  stopifnot(
    identical(models$metadata$pair_fingerprint_sha256,
              raw$metadata$matched$pair_fingerprint_sha256),
    !"individual_dm" %in% names(models),
    length(unique(vapply(c(models$main_scaled, models$main_unscaled),
                         stats::nobs, numeric(1)))) == 1L
  )
}
