# Estimate Section 3 MP-style decay regressions on the matched event-time panel
# and compact retained-pair table produced by 3a_PrepDMBenchmarks.R.
#
# How to run: normally run through S3_Learning.R from flex-mining/.
# Inputs:  ../Data/Processed/{raw_dm_benchmarks,matched_uncorr_pairs}.RDS and
#          the versioned LongShort.RData
# Outputs: ../Data/Processed/mp_style_decay_models.RDS
#
# S3b_MPStyleDecayTables.R renders the cached models into TeX.

rm(list = ls())
source("0_Environment.R")

benchmark_path <- "../Data/Processed/raw_dm_benchmarks.RDS"
pair_path <- "../Data/Processed/matched_uncorr_pairs.RDS"
benchmark <- readRDS(benchmark_path)
panel <- benchmark$matched
metadata <- benchmark$metadata$matched
matched_pairs <- readRDS(pair_path) %>% setDT()
pair_keys <- paste(matched_pairs$pubname, matched_pairs$matched_name, sep = "\t")
pair_fingerprint <- digest::digest(
  paste(pair_keys, collapse = "\n"), algo = "sha256", serialize = FALSE
)

stopifnot(
  metadata$pair_count == nrow(matched_pairs),
  metadata$predictor_count == dplyr::n_distinct(panel$pubname),
  metadata$predictor_count == dplyr::n_distinct(matched_pairs$pubname),
  identical(metadata$pair_fingerprint_sha256, pair_fingerprint)
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

# Individual-DM appendix regressions use the same matched-uncorr pair universe.
# Cap at 100 pairs per predictor to keep this diagnostic model manageable.
max_strats_per_pub <- 100L
subsample_seed <- 42L
matchinfo <- matched_pairs %>%
  select(pubname, matched_name, sign, rbar_insamp_matched,
         sweight, sampstart, sampend, pubdate) %>%
  setDT()
set.seed(subsample_seed)
matchinfo <- matchinfo[, .SD[sample(.N, min(.N, max_strats_per_pub))], by = pubname]

dm_path <- paste0(
  "../Data/Processed/", globalSettings$dataVersion, " LongShort.RData"
)
dm_longshort <- readRDS(dm_path)
dm_rets <- dm_longshort$ret %>%
  left_join(
    dm_longshort$port_list %>% select(portid, sweight),
    by = "portid"
  ) %>%
  transmute(
    matched_name = signalid,
    sweight = tolower(sweight),
    calendarDate = yearm,
    raw_ret = ret
  ) %>%
  setDT()
rm(dm_longshort)
dm_rets <- dm_rets[unique(matchinfo[, .(matched_name, sweight)]),
                   on = c("matched_name", "sweight"), nomatch = 0]
dmPanel <- matchinfo[dm_rets, on = c("matched_name", "sweight"),
                     allow.cartesian = TRUE, nomatch = 0]
rm(dm_rets, matchinfo); gc()

dmPanel[, `:=`(
  dmname = matched_name,
  ret_scaled = raw_ret * sign / rbar_insamp_matched * 100,
  ret_unscaled = raw_ret * sign * 100,
  postSample = data.table::fifelse(calendarDate >= sampend, 1, 0),
  postPub = data.table::fifelse(calendarDate >= pubdate, 1, 0)
)]
dmPanel <- dmPanel[calendarDate >= sampstart]

fit_individual <- function(lhs, time_fe = FALSE) {
  fixed_effects <- if (time_fe) "dmname + calendarDate" else "dmname"
  fixest::feols(
    stats::as.formula(paste0(lhs, " ~ postSample + postPub | ", fixed_effects)),
    data = dmPanel,
    cluster = ~dmname + calendarDate
  )
}
individual_dm <- list(
  fit_individual("ret_scaled"), fit_individual("ret_scaled", TRUE),
  fit_individual("ret_unscaled"), fit_individual("ret_unscaled", TRUE)
)

saveRDS(
  list(
    metadata = list(
      benchmark_path = benchmark_path,
      pair_path = pair_path,
      pair_count = metadata$pair_count,
      predictor_count = metadata$predictor_count,
      pair_fingerprint_sha256 = metadata$pair_fingerprint_sha256,
      regression_observation_count = nrow(regData),
      regression_predictors = sort(unique(regData$pubname))
    ),
    etable_dict = etable_dict,
    main_scaled = main_scaled,
    main_unscaled = main_unscaled,
    individual_dm = individual_dm
  ),
  "../Data/Processed/mp_style_decay_models.RDS"
)
