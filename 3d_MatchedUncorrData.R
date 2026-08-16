# Build the canonical matched-uncorr benchmark shared by Figure 2(c) and
# Tables 3--4.
#
# How to run: normally run through 3_Precompute.R from flex-mining/.
# Inputs:  <dataVersion> MatchPub.RData, dmcomp_sumstats.RDS, and published
#          return/summary caches under ../Data/Processed
# Outputs: ../Data/Processed/matched_uncorr_benchmark.RDS
#
# "matched-uncorr" is the short code/artifact name. The benchmark matches each
# mined strategy's in-sample t-stat within 10% and mean return within 30% of
# its published predictor, requires 60 in-sample months, and retains pairwise
# signed-return correlations no greater than 10%.

rm(list = ls())
source("0_Environment.R")

cache_path <- "../Data/Processed/matched_uncorr_benchmark.RDS"
match_path <- paste0(
  "../Data/Processed/", globalSettings$dataVersion, " MatchPub.RData"
)
dm_sumstats_path <- "../Data/Processed/dmcomp_sumstats.RDS"

inclSignals <- restrictInclSignals(
  restrictType = globalSettings$restrictType,
  topT = globalSettings$topT
)

czret_dates <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  distinct(signalname, pubdate)
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep, signalname %in% inclSignals) %>%
  left_join(czret_dates, by = "signalname") %>%
  transmute(
    pubname = signalname, published_rbar = rbar,
    published_tstat = tstat, sampstart, sampend,
    sweight = tolower(sweight), pubdate
  ) %>%
  setDT()

message("Loading the matched candidate-return cache (large file)...")
match_cache <- readRDS(match_path)
candidate_returns <- match_cache$candidateReturns %>%
  filter(actSignal %in% czsum$pubname) %>%
  setDT()
rm(match_cache, czret_dates); gc()

# Pair-level diagnostics and the standing history floor. candidate_returns$ret
# is already signed to align each mined strategy with its published target.
pairs <- candidate_returns[samptype == "insamp", .(
  sign = data.table::first(sign),
  nmonth_insamp = sum(!is.na(ret)),
  rbar_insamp_matched = mean(ret),
  tstat_insamp_matched = {
    n <- sum(!is.na(ret))
    s <- stats::sd(ret, na.rm = TRUE)
    if (n > 1L && is.finite(s) && s > 0) mean(ret, na.rm = TRUE) / s * sqrt(n) else NA_real_
  }
), by = .(pubname = actSignal, matched_name = candSignalname)]

pairs <- merge(pairs, czsum, by = "pubname", all.x = TRUE)
pair_correlations <- readRDS(dm_sumstats_path)$insampsum %>%
  transmute(
    pubname, sweight = tolower(sweight), matched_name = dmname,
    rho = cor * sign(rbar)
  ) %>%
  setDT()
stopifnot(
  !anyDuplicated(pair_correlations, by = c("pubname", "sweight", "matched_name"))
)
pairs <- merge(
  pairs, pair_correlations,
  by = c("pubname", "sweight", "matched_name"), all.x = TRUE
)
pairs[, `:=`(
  mean_return_rel_distance = abs(rbar_insamp_matched - published_rbar) /
    abs(published_rbar),
  tstat_rel_distance = abs(tstat_insamp_matched - published_tstat) /
    abs(published_tstat),
  passes_history = nmonth_insamp >= globalSettings$match_nmonth_min,
  passes_correlation = !is.na(rho) & rho <= globalSettings$matched_uncorr_corr_max
)]
pairs[, keep_matched_uncorr := passes_history & passes_correlation]
data.table::setorder(pairs, pubname, matched_name)

matched_pairs <- pairs[passes_history == TRUE]
matched_uncorr_pairs <- pairs[keep_matched_uncorr == TRUE]
if (nrow(matched_uncorr_pairs) == 0L) {
  stop("The matched-uncorr screens retained no pairs.")
}

aggregate_pairs <- function(pair_set) {
  selected <- candidate_returns[
    pair_set[, .(actSignal = pubname, candSignalname = matched_name,
                 rbar_insamp_matched)],
    on = c("actSignal", "candSignalname"), nomatch = 0
  ]
  out <- selected[, .(
    ret_scaled = mean(100 * ret / rbar_insamp_matched, na.rm = TRUE),
    ret_unscaled = mean(100 * ret, na.rm = TRUE),
    n_matched_available = sum(!is.na(ret))
  ), by = .(pubname = actSignal, eventDate)]
  rm(selected); gc()
  out
}

message("Aggregating all history-qualified matched pairs...")
matched_panel <- aggregate_pairs(matched_pairs)
data.table::setnames(
  matched_panel,
  c("ret_scaled", "ret_unscaled", "n_matched_available"),
  c("matched_ret_scaled", "matched_ret_unscaled", "n_matched_available")
)

message("Aggregating matched-uncorr pairs...")
matched_uncorr_panel <- aggregate_pairs(matched_uncorr_pairs)
data.table::setnames(
  matched_uncorr_panel,
  c("ret_scaled", "ret_unscaled", "n_matched_available"),
  c("matched_uncorr_ret_scaled", "matched_uncorr_ret_unscaled",
    "n_matched_uncorr_available")
)

# Start with published returns, then restrict all outcomes to signal-months for
# which the matched-uncorr benchmark exists. This enforces a common regression
# sample for the academic, benchmark, and difference columns.
published_panel <- readRDS("../Data/Processed/ret_for_plot0.RDS") %>%
  transmute(
    pubname, eventDate, calendarDate,
    published_ret_scaled = ret,
    published_ret_unscaled = ret_unscaled
  )
# Preserve ret_for_plot0's row order. Besides making the cache easier to audit
# against its historical Tol30 builder, this keeps fixest's iterative FE
# calculations byte-stable with the previously checked-in Tol30 tables.
panel <- published_panel %>%
  inner_join(as_tibble(matched_uncorr_panel), by = c("pubname", "eventDate")) %>%
  left_join(as_tibble(matched_panel), by = c("pubname", "eventDate")) %>%
  left_join(as_tibble(czsum), by = "pubname")

surviving_predictors <- sort(unique(matched_uncorr_pairs$pubname))
pair_keys <- paste(matched_uncorr_pairs$pubname,
                   matched_uncorr_pairs$matched_name, sep = "\t")
metadata <- list(
  short_name = "matched-uncorr",
  specification = list(
    tstat_relative_tolerance = globalSettings$matched_uncorr_t_reltol,
    mean_return_relative_tolerance = globalSettings$matched_uncorr_r_reltol,
    minimum_insample_months = globalSettings$match_nmonth_min,
    maximum_pairwise_correlation = globalSettings$matched_uncorr_corr_max,
    normalization = "each matched strategy by its own in-sample mean"
  ),
  pair_count = nrow(matched_uncorr_pairs),
  predictor_count = length(surviving_predictors),
  panel_observation_count = nrow(panel),
  pair_fingerprint_sha256 = digest::digest(
    paste(pair_keys, collapse = "\n"), algo = "sha256", serialize = FALSE
  ),
  input_files = c(match_path, dm_sumstats_path,
                  "../Data/Processed/czsum_allpredictors.RDS",
                  "../Data/Processed/czret_keeponly.RDS",
                  "../Data/Processed/ret_for_plot0.RDS")
)

stopifnot(
  metadata$pair_count == sum(pairs$keep_matched_uncorr),
  metadata$predictor_count == data.table::uniqueN(panel$pubname),
  all(surviving_predictors == sort(unique(panel$pubname)))
)

saveRDS(
  list(
    metadata = metadata,
    pairs = pairs,
    surviving_predictors = surviving_predictors,
    panel = panel
  ),
  cache_path
)

message(
  "Wrote ", cache_path, ": ", metadata$pair_count, " pairs, ",
  metadata$predictor_count, " predictors, fingerprint ",
  metadata$pair_fingerprint_sha256
)
