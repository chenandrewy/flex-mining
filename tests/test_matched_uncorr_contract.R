# Focused checks for the matched event-panel and compact-pair contracts.
#
# How to run: from flex-mining/, run
#   Rscript tests/test_matched_uncorr_contract.R
# Inputs: Chapter 3 and Section 2/3 scripts; optionally generated caches
# Outputs: none; exits nonzero on failure.

producer <- readLines("3a_PrepDMBenchmarks.R")
figure_consumer <- readLines("S2e_Fig2Plots.R")
table_consumer <- readLines("S3a_MPStyleDecayModels.R")
precompute <- readLines("3_Precompute.R")

old_cache <- "matched_uncorr_benchmark.RDS"
pair_cache <- "matched_uncorr_pairs.RDS"
stopifnot(
  !file.exists("3d_MatchedUncorrData.R"),
  !any(grepl("3d_MatchedUncorrData.R", precompute, fixed = TRUE)),
  !any(grepl(old_cache, c(producer, figure_consumer, table_consumer),
             fixed = TRUE)),
  any(grepl("matched = matched_panel", producer, fixed = TRUE)),
  any(grepl(pair_cache, producer, fixed = TRUE)),
  any(grepl(pair_cache, table_consumer, fixed = TRUE)),
  any(grepl("raw_benchmarks$matched", figure_consumer, fixed = TRUE)),
  any(grepl("benchmark$matched", table_consumer, fixed = TRUE)),
  any(grepl("rho = cor * sign(rbar)", producer, fixed = TRUE)),
  any(grepl('on = c("matched_name", "sweight")', table_consumer, fixed = TRUE))
)

raw_path <- "../Data/Processed/raw_dm_benchmarks.RDS"
pair_path <- file.path("../Data/Processed", pair_cache)
if (file.exists(raw_path) && file.exists(pair_path)) {
  raw <- readRDS(raw_path)
  pairs <- readRDS(pair_path)
  metadata <- raw$metadata$matched
  pair_keys <- paste(pairs$pubname, pairs$matched_name, sep = "\t")
  fingerprint <- digest::digest(
    paste(pair_keys, collapse = "\n"), algo = "sha256", serialize = FALSE
  )
  stopifnot(
    all(c("matched", "metadata") %in% names(raw)),
    all(c(
      "published_ret_scaled", "matched_ret_scaled",
      "matched_uncorr_ret_scaled"
    ) %in% names(raw$matched)),
    all(c(
      "pubname", "sweight", "matched_name", "sign",
      "rbar_insamp_matched", "sampstart", "sampend", "pubdate", "rho"
    ) %in% names(pairs)),
    metadata$pair_count == nrow(pairs),
    metadata$predictor_count == length(unique(raw$matched$pubname)),
    identical(metadata$pair_fingerprint_sha256, fingerprint),
    !anyDuplicated(as.data.frame(pairs)[c("pubname", "sweight", "matched_name")])
  )
}

model_path <- "../Data/Processed/mp_style_decay_models.RDS"
if (file.exists(model_path) && file.exists(raw_path) && file.exists(pair_path)) {
  models <- readRDS(model_path)
  stopifnot(
    identical(models$metadata$pair_fingerprint_sha256,
              raw$metadata$matched$pair_fingerprint_sha256),
    length(unique(vapply(c(models$main_scaled, models$main_unscaled),
                         stats::nobs, numeric(1)))) == 1L
  )
}
