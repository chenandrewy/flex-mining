# Estimate and render Appendix Table B.1 individual-DM decay regressions.
#
# How to run: normally run through SA_Appendices.R from flex-mining/.
# Inputs:  ../Data/Processed/{czsum_allpredictors,czret_keeponly,
#          dmcomp_sumstats,raw_dm_benchmarks}.RDS and the versioned
#          LongShort.RData
# Outputs: ../Results/Table_MPStyleRegsIndividualDM.tex

rm(list = ls())
source("0_Environment.R")

output_dir <- Sys.getenv(
  "MP_INDIVIDUAL_TABLE_OUTPUT_DIR",
  unset = "../Results"
)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

benchmark_path <- "../Data/Processed/raw_dm_benchmarks.RDS"
dmcomp_path <- "../Data/Processed/dmcomp_sumstats.RDS"
dm_path <- paste0(
  "../Data/Processed/", globalSettings$dataVersion, " LongShort.RData"
)

benchmark_metadata <- readRDS(benchmark_path)$metadata$matched
dmcomp <- readRDS(dmcomp_path)
inclSignals <- restrictInclSignals(
  restrictType = globalSettings$restrictType,
  topT = globalSettings$topT
)
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep, signalname %in% inclSignals) %>%
  setDT()
czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  filter(signalname %in% inclSignals)

published_metadata <- czsum %>%
  as_tibble() %>%
  left_join(
    czret %>% distinct(signalname, pubdate),
    by = "signalname"
  ) %>%
  transmute(
    pubname = signalname,
    published_rbar = rbar,
    published_tstat = tstat,
    sampstart,
    sampend,
    sweight = tolower(sweight),
    pubdate
  ) %>%
  setDT()
rm(czsum, czret, inclSignals)

message("Recomputing Appendix Table B.1 matched pairs in memory...")
pair_data <- build_matched_uncorr_pair_data(
  dmcomp$insampsum,
  published_metadata,
  dmcomp$name
)
matched_pairs <- pair_data$uncorr_pairs
pair_fingerprint <- matched_pair_fingerprint(matched_pairs)
stopifnot(
  benchmark_metadata$pair_count == nrow(matched_pairs),
  benchmark_metadata$predictor_count == data.table::uniqueN(matched_pairs$pubname),
  identical(benchmark_metadata$pair_fingerprint_sha256, pair_fingerprint),
  !anyDuplicated(
    matched_pairs,
    by = c("pubname", "sweight", "matched_name")
  )
)
cat(
  "Appendix Table B.1 matched-uncorr inputs:", nrow(matched_pairs),
  "pairs,", data.table::uniqueN(matched_pairs$pubname),
  "predictors, fingerprint", pair_fingerprint, "\n"
)

# Cap at 100 pairs per predictor to retain the established Table B.1 sample.
max_strats_per_pub <- 100L
subsample_seed <- 42L
matchinfo <- matched_pairs[, .(
  pubname,
  matched_name,
  sweight,
  rbar_insamp_matched,
  sampstart,
  sampend,
  pubdate
)]
set.seed(subsample_seed)
matchinfo <- matchinfo[
  , .SD[sample(.N, min(.N, max_strats_per_pub))],
  by = pubname
]

selected_returns <- pair_data$candidate_returns[
  matchinfo[, .(
    actSignal = pubname,
    candSignalname = matched_name,
    sweight
  )],
  on = c("actSignal", "candSignalname", "sweight"),
  nomatch = 0L
][, .(
  pubname = actSignal,
  matched_name = candSignalname,
  sweight,
  eventDate,
  signed_ret = ret
)]
dmPanel <- merge(
  matchinfo,
  selected_returns,
  by = c("pubname", "matched_name", "sweight"),
  all = FALSE,
  allow.cartesian = TRUE
)
rm(pair_data, matched_pairs, selected_returns, dmcomp, published_metadata)
gc()

dmPanel[, `:=`(
  dmname = matched_name,
  calendarDate = sampend + eventDate / 12,
  ret_scaled = signed_ret / rbar_insamp_matched * 100,
  ret_unscaled = signed_ret * 100
)]
dmPanel[, `:=`(
  postSample = data.table::fifelse(calendarDate >= sampend, 1, 0),
  postPub = data.table::fifelse(calendarDate >= pubdate, 1, 0)
)]
dmPanel <- dmPanel[calendarDate >= sampstart]
if (nrow(dmPanel) == 0L) {
  stop("The Appendix Table B.1 individual-DM panel is empty.")
}

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
stopifnot(length(unique(vapply(individual_dm, stats::nobs, numeric(1)))) == 1L)

etable_dict <- c(
  postSample = "Post-Sample",
  postPub = "Post-Pub",
  ret_scaled = "Return (scaled)",
  ret_unscaled = "Return (unscaled)",
  dmname = "DM strategy",
  calendarDate = "Month"
)
fixest::etable(
  individual_dm,
  tex = TRUE,
  dict = etable_dict,
  style.tex = fixest::style.tex("aer"),
  digits = 3,
  digits.stats = "r3",
  signif.code = NA,
  depvar = FALSE,
  headers = c(
    "Scaled returns", "Scaled returns",
    "Unscaled returns", "Unscaled returns"
  ),
  fitstat = ~ n + r2 + wr2,
  file = file.path(output_dir, "Table_MPStyleRegsIndividualDM.tex")
)
