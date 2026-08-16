# Estimate Section 3 MP-style decay regressions on the matched event-time panel.
#
# How to run: normally run through S3_Learning.R from flex-mining/.
# Inputs:  ../Data/Processed/raw_dm_benchmarks.RDS
# Outputs: ../Data/Processed/mp_style_decay_models.RDS
#
# S3b_MPStyleDecayTables.R renders the cached models into TeX.

rm(list = ls())
source("0_Environment.R")

benchmark_path <- "../Data/Processed/raw_dm_benchmarks.RDS"
benchmark <- readRDS(benchmark_path)
panel <- benchmark$matched
metadata <- benchmark$metadata$matched

stopifnot(
  metadata$predictor_count == dplyr::n_distinct(panel$pubname),
  !is.null(metadata$pair_fingerprint_sha256)
)
cat(
  "S3a matched-uncorr inputs:", metadata$pair_count, "pairs,",
  metadata$predictor_count, "predictors, fingerprint",
  metadata$pair_fingerprint_sha256, "\n"
)

regData <- panel %>%
  transmute(
    pubname, eventDate, calendarDate, sampstart, sampend, pubdate,
    ret = published_ret_scaled,
    matchRet = matched_uncorr_ret_scaled,
    ret_unscaled = published_ret_unscaled,
    matchRet_unscaled = matched_uncorr_ret_unscaled,
    postSample = ifelse(calendarDate >= sampend, 1, 0),
    postPub = ifelse(calendarDate >= pubdate, 1, 0)
  ) %>%
  mutate(
    diffRet = ret - matchRet,
    diffRet_unscaled = ret_unscaled - matchRet_unscaled
  ) %>%
  filter(
    calendarDate >= sampstart,
    complete.cases(ret, matchRet, ret_unscaled, matchRet_unscaled,
                   postSample, postPub)
  )

if (nrow(regData) == 0L) stop("The matched-uncorr regression panel is empty.")
cat(
  "Common regression sample:", nrow(regData), "signal-months and",
  dplyr::n_distinct(regData$pubname), "predictors\n"
)

etable_dict <- c(
  postSample = "Post-Sample",
  postPub = "Post-Pub",
  ret = "Return (scaled)",
  matchRet = "DM Matched Return (scaled)",
  diffRet = "Difference (scaled)",
  ret_unscaled = "Return (unscaled)",
  matchRet_unscaled = "DM Matched Return (unscaled)",
  diffRet_unscaled = "Difference (unscaled)",
  pubname = "Predictor",
  calendarDate = "Month",
  dmname = "DM strategy"
)

fit_outcome <- function(lhs, time_fe = FALSE) {
  fixed_effects <- if (time_fe) "pubname + calendarDate" else "pubname"
  fixest::feols(
    stats::as.formula(paste0(lhs, " ~ postSample + postPub | ", fixed_effects)),
    data = regData,
    cluster = ~pubname + calendarDate
  )
}

main_scaled <- list(
  fit_outcome("ret"), fit_outcome("ret", TRUE),
  fit_outcome("matchRet"), fit_outcome("matchRet", TRUE),
  fit_outcome("diffRet"), fit_outcome("diffRet", TRUE)
)
main_unscaled <- list(
  fit_outcome("ret_unscaled"), fit_outcome("ret_unscaled", TRUE),
  fit_outcome("matchRet_unscaled"), fit_outcome("matchRet_unscaled", TRUE),
  fit_outcome("diffRet_unscaled"), fit_outcome("diffRet_unscaled", TRUE)
)

main_fits <- c(main_scaled, main_unscaled)
fit_nobs <- vapply(main_fits, stats::nobs, numeric(1))
fit_predictors <- vapply(
  main_fits,
  function(fit) length(unique(fit$fixef_id$pubname)),
  integer(1)
)
stopifnot(
  length(unique(fit_nobs)) == 1L,
  fit_nobs[[1]] == nrow(regData),
  length(unique(fit_predictors)) == 1L,
  fit_predictors[[1]] == metadata$predictor_count
)

saveRDS(
  list(
    metadata = list(
      benchmark_path = benchmark_path,
      pair_count = metadata$pair_count,
      predictor_count = metadata$predictor_count,
      pair_fingerprint_sha256 = metadata$pair_fingerprint_sha256,
      regression_observation_count = nrow(regData),
      regression_predictors = sort(unique(regData$pubname))
    ),
    etable_dict = etable_dict,
    main_scaled = main_scaled,
    main_unscaled = main_unscaled
  ),
  "../Data/Processed/mp_style_decay_models.RDS"
)
