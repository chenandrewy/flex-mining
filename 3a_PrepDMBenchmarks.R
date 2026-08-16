# Prepare raw data-mining event-return benchmarks used across exhibits.
#
# How to run: normally run through 3_Precompute.R from flex-mining/.
# Inputs:  cleaned published returns and chapter-2 mined-strategy caches
# Outputs: ../Data/Processed/{dmcomp,dmtic}_sumstats.RDS
#          ../Data/Processed/raw_dm_benchmarks.RDS
#          ../Data/Processed/ret_for_plot{0,1}.RDS

# Setup --------------------------------------------------------

rm(list = ls())

source("0_Environment.R")
library(doParallel)

dmcomp <- list()
dmtic <- list()
dmcomp$name <- paste0('../Data/Processed/',
                      globalSettings$dataVersion,
                      ' LongShort.RData')
dmtic$name <- "../Data/Processed/ticker_Harvey2017JF.RDS"

## Load Global Data -------------------------------------------

# these are treated as globals (don't modify pls)
inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType,
                                  topT = globalSettings$topT)

czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep) %>%
  filter(signalname %in% inclSignals) %>%
  setDT()

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
  select(signalname, theory) %>%
  filter(signalname %in% inclSignals)

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100) %>%
  filter(signalname %in% inclSignals)

# In-samp Sumstats (about 4 min on 4 cores) -------------------

## Run Function on Compustat DM --------------------------------

print("creating Compustat mining in-sample sumstats")
print("Takes about 4 minutes using 4 cores")
start_time <- Sys.time()
dmcomp$insampsum <- sumstats_for_DM_Strats(
  DMname = dmcomp$name,
  nsampmax = Inf
)
print("finished")
stop_time <- Sys.time()
stop_time - start_time

## Run Function on Ticker DM ----------------------------------------

print("creating ticker mining in-sample sumstats")
print("Takes about 20 sec using 9 cores")
start_time <- Sys.time()
dmtic$insampsum <- sumstats_for_DM_Strats(
  DMname = dmtic$name,
  nsampmax = Inf
)
print("end DM summary stats")
stop_time <- Sys.time()
stop_time - start_time

# Save to disk -----------------------------------------------------

saveRDS(dmcomp, "../Data/Processed/dmcomp_sumstats.RDS")
saveRDS(dmtic, "../Data/Processed/dmtic_sumstats.RDS")

# Generate Matched Returns (ret_for_plot[x]) ---------------------------------

## Default Match Settings --------------------------------------------------

npubmax <- Inf
use_sign_info <- TRUE
accounting_t2_screen <- list(
  # tolerance in levels
  t_tol = globalSettings$t_tol,
  r_tol = globalSettings$r_tol,
  # tolerance relative to op stat
  t_reltol = globalSettings$t_reltol,
  r_reltol = globalSettings$r_reltol,
  # alternative filtering
  t_min = globalSettings$t_min,
  t_max = globalSettings$t_max,
  t_rankpct_min = globalSettings$t_rankpct_min,
  minNumStocks = globalSettings$minNumStocks
)

## Shared data prep --------------------------------------------------
# here, matchRet uses accounting signals with t>2

# make event time returns for Compustat DM
accounting_t2_matched <- select_accounting_t2_pairs(
  dmcomp$insampsum,
  min_num_stocks = globalSettings$minNumStocks,
  t_threshold = globalSettings$t_min,
  minimum_months = 60L,
  required_final_year_months = 12L,
  pubnames = czsum$signalname
)
accounting_t2_metadata <- list(
  pair_count = nrow(accounting_t2_matched),
  predictor_count = data.table::uniqueN(accounting_t2_matched$pubname),
  sample_window_count = data.table::uniqueN(
    accounting_t2_matched[, .(sampstart, sampend)]
  ),
  pair_fingerprint_sha256 = accounting_t2_pair_fingerprint(
    accounting_t2_matched
  ),
  specification = list(
    oriented_raw_t_threshold = globalSettings$t_min,
    minimum_stocks_per_leg = globalSettings$minNumStocks / 2,
    minimum_insample_months = 60L,
    required_final_year_months = 12L,
    published_mean_return_tolerance = Inf,
    published_tstat_tolerance = Inf
  )
)

print("Making accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()
accounting_t2_event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = accounting_t2_matched,
  npubmax = npubmax, czsum = czsum, use_sign_info = use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

rm(accounting_t2_matched)

ret_for_plot0 = czret %>%
    transmute(pubname = signalname, eventDate, calendarDate = date,
              ret_unscaled = ret * 100, ret = ret_scaled, theory) %>%
    left_join(
        accounting_t2_event_time %>% transmute(pubname, eventDate,
                                                matchRet = dm_mean,
                                                matchRet_unscaled = dm_mean_unscaled),
        by = c("pubname", "eventDate")
    ) %>%
    select(eventDate, calendarDate, ret, ret_unscaled, matchRet, matchRet_unscaled, pubname, theory)

## Shared data prep 2 --------------------------------------------------
# adds to the previous ret_for_plot0
# creates matchRetAlt, which uses the top 5% t-stats

top5_screen <- accounting_t2_screen
top5_screen$t_min <- 0 # turn off t_min filter
top5_screen$t_rankpct_min <- 5 # add top 5% t filter

# make event time returns for Compustat DM
accounting_top5_matched <- SelectDMStrats(dmcomp$insampsum, top5_screen)

print("Making accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()
accounting_top5_event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = accounting_top5_matched,
  npubmax = npubmax, czsum = czsum, use_sign_info = use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

rm(accounting_top5_matched)

ret_for_plot1 = ret_for_plot0 %>%
  left_join(
    accounting_top5_event_time %>%
      transmute(pubname, eventDate, matchRetAlt = dm_mean),
    by = c("pubname", "eventDate")
  ) %>%
  select(eventDate, ret, matchRet, matchRetAlt, pubname, theory)

## Matched and matched-uncorrelated benchmark ------------------------------
# Keep the event-time benchmark with the other raw benchmark panels. Pair-level
# inputs are transient and are recomputed only by the Appendix Table B.1 script.

matched_czsum <- czsum %>%
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

message("Building matched candidate pairs and returns in memory...")
matched_pair_data <- build_matched_uncorr_pair_data(
  dmcomp$insampsum,
  matched_czsum,
  dmcomp$name
)
matched_candidate_returns <- matched_pair_data$candidate_returns
history_qualified_pairs <- matched_pair_data$history_pairs
matched_uncorr_pairs <- matched_pair_data$uncorr_pairs

aggregate_matched_pairs <- function(pair_set) {
  selected <- matched_candidate_returns[
    pair_set[, .(
      actSignal = pubname,
      candSignalname = matched_name,
      rbar_insamp_matched
    )],
    on = c("actSignal", "candSignalname"),
    nomatch = 0L
  ]
  selected[, .(
    ret_scaled = mean(100 * ret / rbar_insamp_matched, na.rm = TRUE),
    ret_unscaled = mean(100 * ret, na.rm = TRUE),
    n_available = sum(!is.na(ret))
  ), by = .(pubname = actSignal, eventDate)]
}

matched_event_panel <- aggregate_matched_pairs(history_qualified_pairs)
data.table::setnames(
  matched_event_panel,
  c("ret_scaled", "ret_unscaled", "n_available"),
  c("matched_ret_scaled", "matched_ret_unscaled", "n_matched_available")
)
matched_uncorr_event_panel <- aggregate_matched_pairs(matched_uncorr_pairs)
data.table::setnames(
  matched_uncorr_event_panel,
  c("ret_scaled", "ret_unscaled", "n_available"),
  c(
    "matched_uncorr_ret_scaled",
    "matched_uncorr_ret_unscaled",
    "n_matched_uncorr_available"
  )
)

matched_panel <- ret_for_plot0 %>%
  transmute(
    pubname,
    eventDate,
    calendarDate,
    published_ret_scaled = ret,
    published_ret_unscaled = ret_unscaled
  ) %>%
  inner_join(
    as_tibble(matched_uncorr_event_panel),
    by = c("pubname", "eventDate")
  ) %>%
  left_join(
    as_tibble(matched_event_panel),
    by = c("pubname", "eventDate")
  ) %>%
  left_join(as_tibble(matched_czsum), by = "pubname")

matched_metadata <- list(
  specification = list(
    tstat_relative_tolerance = globalSettings$matched_uncorr_t_reltol,
    mean_return_relative_tolerance = globalSettings$matched_uncorr_r_reltol,
    minimum_insample_months = globalSettings$match_nmonth_min,
    maximum_pairwise_correlation = globalSettings$matched_uncorr_corr_max,
    normalization = "each matched strategy by its own in-sample mean"
  ),
  pair_count = nrow(matched_uncorr_pairs),
  predictor_count = dplyr::n_distinct(matched_panel$pubname),
  panel_observation_count = nrow(matched_panel),
  pair_fingerprint_sha256 = matched_pair_fingerprint(matched_uncorr_pairs)
)
stopifnot(
  matched_metadata$pair_count == nrow(matched_uncorr_pairs),
  matched_metadata$predictor_count ==
    data.table::uniqueN(matched_uncorr_pairs$pubname),
  !anyDuplicated(
    matched_uncorr_pairs,
    by = c("pubname", "sweight", "matched_name")
  )
)

rm(
  matched_candidate_returns,
  matched_pair_data,
  history_qualified_pairs,
  matched_uncorr_pairs,
  matched_event_panel,
  matched_uncorr_event_panel,
  matched_czsum
)
gc()

## Raw benchmark contract -------------------------------------------------
# Figure 2 and future benchmark consumers use this calculation-oriented
# contract.  The ret_for_plot* files below remain compatibility artifacts.

ticker_top5_matched <- SelectDMStrats(dmtic$insampsum, top5_screen)

print("Making ticker top 5% event time returns")
start_time <- Sys.time()
ticker_top5_event_time <- make_DM_event_returns(
  DMname = dmtic$name, match_strats = ticker_top5_matched,
  npubmax = npubmax, czsum = czsum, use_sign_info = use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

published_benchmark <- ret_for_plot0 %>%
  transmute(pubname, eventDate, calendarDate, return = ret)
accounting_t2_benchmark <- published_benchmark %>%
  select(pubname, eventDate, calendarDate) %>%
  left_join(
    accounting_t2_event_time %>%
      transmute(
        pubname, eventDate, return = dm_mean,
        n_matches_available = dm_n
      ),
    by = c("pubname", "eventDate")
  ) %>%
  select(pubname, eventDate, calendarDate, return, n_matches_available)
accounting_top5_benchmark <- accounting_top5_event_time %>%
  transmute(
    pubname, eventDate,
    return = dm_mean, n_matches_available = dm_n
  ) %>%
  left_join(
    published_benchmark %>% select(pubname, eventDate, calendarDate),
    by = c("pubname", "eventDate")
  ) %>%
  select(pubname, eventDate, calendarDate, return, n_matches_available)
ticker_top5_benchmark <- ticker_top5_event_time %>%
  transmute(
    pubname, eventDate,
    return = dm_mean, n_matches_available = dm_n
  ) %>%
  left_join(
    published_benchmark %>% select(pubname, eventDate, calendarDate),
    by = c("pubname", "eventDate")
  ) %>%
  select(pubname, eventDate, calendarDate, return, n_matches_available)

raw_dm_benchmarks <- list(
  published = published_benchmark,
  accounting_t2 = accounting_t2_benchmark,
  accounting_top5 = accounting_top5_benchmark,
  ticker_top5 = ticker_top5_benchmark,
  matched = matched_panel,
  metadata = list(
    schema_version = 1L,
    normalization = "100 times return divided by the strategy in-sample mean",
    accounting_t2_screen = accounting_t2_screen,
    accounting_t2 = accounting_t2_metadata,
    top5_screen = top5_screen,
    mining_universes = c(
      accounting = dmcomp$name,
      ticker = dmtic$name
    ),
    matched = matched_metadata,
    source_files = c(
      "../Data/Processed/czsum_allpredictors.RDS",
      "../Data/Processed/czret_keeponly.RDS",
      dmcomp$name, dmtic$name
    )
  )
)

stopifnot(
  !anyDuplicated(as.data.frame(published_benchmark)[c("pubname", "eventDate")]),
  !anyDuplicated(as.data.frame(accounting_t2_benchmark)[c("pubname", "eventDate")]),
  !anyDuplicated(as.data.frame(accounting_top5_benchmark)[c("pubname", "eventDate")]),
  !anyDuplicated(as.data.frame(ticker_top5_benchmark)[c("pubname", "eventDate")])
)

rm(ticker_top5_event_time, ticker_top5_matched)

# Save to disk -----------------------------------------------------

saveRDS(raw_dm_benchmarks, "../Data/Processed/raw_dm_benchmarks.RDS")
saveRDS(ret_for_plot0, "../Data/Processed/ret_for_plot0.RDS")
saveRDS(ret_for_plot1, "../Data/Processed/ret_for_plot1.RDS")
