# Prepare all data-mined benchmark contracts in one Chapter 3 producer.
#
# How to run: normally run through 3_Precompute.R from flex-mining/.
#   For validation without overwriting production caches, set
#   PREP_DM_OUT_DIR=/some/dir before running.
# Inputs:  cleaned published returns/summaries (czret_keeponly, czsum_allpredictors),
#          the chapter-2 mined-return universes (<dataVersion> LongShort.RData,
#          ticker_Harvey2017JF.RDS), Fama-French factors, and DataInput CSVs.
#          It does NOT read the legacy MatchPub.RData,
#          PairwiseCorrelationsActualAndMatches.RDS, or MatchPubRiskAdjusted.RData
#          intermediates; it recomputes matching, pairwise correlations, and
#          risk adjustment from the mined-return universes.
# Outputs (in PREP_DM_OUT_DIR, default ../Data/Processed):
#          dmcomp_sumstats.RDS, dmtic_sumstats.RDS  (compat sumstats)
#          raw_dm_benchmarks.RDS                    (Figure 2 raw contract)
#          plotdat0.RDS, ret_for_plot0/1/_MaxPredictors.RDS  (compat display)
#          matched_uncorr_benchmark.RDS             (Figure 2c / Section 3)
#          risk_adjusted_dm_benchmarks.RDS          (factor-adjusted contract)
#
# This producer consolidates the calculations formerly split across
# 2b_MatchDataMinedToPub.R, 2d_RiskAdjustDataMinedSignals.R,
# 3a_ResearchVsDMPrep.R, 3d_MatchedUncorrData.R, and 3e_FactorAdjustedDMPrep.R.
# It runs in ordered phases and releases large objects between them:
#   Phase A  raw benchmarks and compatibility display caches
#   Phase B  matched candidate returns + pairwise correlations (in memory)
#   Phase C  matched-uncorr benchmark
#   Phase D  factor-adjusted (CAPM/FF3/FF4) benchmark

# Setup --------------------------------------------------------------------
rm(list = ls())
source("0_Environment.R")
source("helpers/risk_adjusted_helpers_tv.R")
library(doParallel)

out_dir <- Sys.getenv("PREP_DM_OUT_DIR", unset = "../Data/Processed")
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
outp <- function(name) file.path(out_dir, name)
message("Writing DM benchmark contracts to: ", normalizePath(out_dir))

ncores <- globalSettings$num_cores

dmcomp <- list(); dmtic <- list()
dmcomp$name <- paste0("../Data/Processed/", globalSettings$dataVersion, " LongShort.RData")
dmtic$name <- "../Data/Processed/ticker_Harvey2017JF.RDS"

inclSignals <- restrictInclSignals(
  restrictType = globalSettings$restrictType, topT = globalSettings$topT
)

# ==========================================================================
# Phase A: raw benchmarks and compatibility display caches
#   (formerly 3a_ResearchVsDMPrep.R)
# ==========================================================================
message("\n===== Phase A: raw benchmarks =====")

czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep) %>%
  filter(signalname %in% inclSignals) %>%
  setDT()

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
  select(signalname, Year, theory) %>%
  filter(signalname %in% inclSignals)

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100) %>%
  filter(signalname %in% inclSignals)

# In-sample sumstats -------------------------------------------------------
message("creating Compustat mining in-sample sumstats (~4 min on 4 cores)")
dmcomp$insampsum <- sumstats_for_DM_Strats(DMname = dmcomp$name, nsampmax = Inf)
message("creating ticker mining in-sample sumstats")
dmtic$insampsum <- sumstats_for_DM_Strats(DMname = dmtic$name, nsampmax = Inf)

saveRDS(dmcomp, outp("dmcomp_sumstats.RDS"))
saveRDS(dmtic, outp("dmtic_sumstats.RDS"))

# Matched returns (ret_for_plot[x]) ----------------------------------------
plotdat0 <- list()
plotdat0$name <- "t_min_2"
plotdat0$npubmax <- Inf
plotdat0$use_sign_info <- TRUE
plotdat0$matchset <- list(
  t_tol = globalSettings$t_tol, r_tol = globalSettings$r_tol,
  t_reltol = globalSettings$t_reltol, r_reltol = globalSettings$r_reltol,
  t_min = globalSettings$t_min, t_max = globalSettings$t_max,
  t_rankpct_min = globalSettings$t_rankpct_min,
  minNumStocks = globalSettings$minNumStocks
)

# accounting event-time returns with t>2
temp <- list()
temp$matched <- SelectDMStrats(dmcomp$insampsum, plotdat0$matchset)
message("Making accounting event time returns")
temp$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = temp$matched, npubmax = plotdat0$npubmax,
  czsum = czsum, use_sign_info = plotdat0$use_sign_info
)
plotdat0$comp_matched <- temp$matched
plotdat0$comp_event_time <- temp$event_time
rm(temp)

ret_for_plot0 <- czret %>%
  transmute(pubname = signalname, eventDate, calendarDate = date,
            ret_unscaled = ret * 100, ret = ret_scaled, theory) %>%
  left_join(
    plotdat0$comp_event_time %>% transmute(pubname, eventDate,
                                           matchRet = dm_mean,
                                           matchRet_unscaled = dm_mean_unscaled),
    by = c("pubname", "eventDate")
  ) %>%
  select(eventDate, calendarDate, ret, ret_unscaled, matchRet,
         matchRet_unscaled, pubname, theory)

# top-5% t-stat accounting event-time returns (matchRetAlt)
tempplotdat <- plotdat0
tempplotdat$matchset$t_min <- 0
tempplotdat$matchset$t_rankpct_min <- 5
temp <- list()
temp$matched <- SelectDMStrats(dmcomp$insampsum, tempplotdat$matchset)
message("Making accounting top-5% event time returns")
temp$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = temp$matched, npubmax = tempplotdat$npubmax,
  czsum = czsum, use_sign_info = tempplotdat$use_sign_info
)
tempplotdat$comp_matched <- temp$matched
tempplotdat$comp_event_time <- temp$event_time
rm(temp)

ret_for_plot1 <- ret_for_plot0 %>%
  left_join(
    tempplotdat$comp_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean),
    by = c("pubname", "eventDate")
  ) %>%
  select(eventDate, ret, matchRet, matchRetAlt, pubname, theory)

# Raw benchmark contract ---------------------------------------------------
ticker_top5_matched <- SelectDMStrats(dmtic$insampsum, tempplotdat$matchset)
message("Making ticker top-5% event time returns")
ticker_top5_event_time <- make_DM_event_returns(
  DMname = dmtic$name, match_strats = ticker_top5_matched,
  npubmax = tempplotdat$npubmax, czsum = czsum,
  use_sign_info = tempplotdat$use_sign_info
)

published_benchmark <- ret_for_plot0 %>%
  transmute(pubname, eventDate, calendarDate, return = ret)
accounting_t2_benchmark <- published_benchmark %>%
  select(pubname, eventDate, calendarDate) %>%
  left_join(
    plotdat0$comp_event_time %>%
      transmute(pubname, eventDate, return = dm_mean, n_matches_available = dm_n),
    by = c("pubname", "eventDate")
  ) %>%
  select(pubname, eventDate, calendarDate, return, n_matches_available)
accounting_top5_benchmark <- tempplotdat$comp_event_time %>%
  transmute(pubname, eventDate, return = dm_mean, n_matches_available = dm_n) %>%
  left_join(
    published_benchmark %>% select(pubname, eventDate, calendarDate),
    by = c("pubname", "eventDate")
  ) %>%
  select(pubname, eventDate, calendarDate, return, n_matches_available)
ticker_top5_benchmark <- ticker_top5_event_time %>%
  transmute(pubname, eventDate, return = dm_mean, n_matches_available = dm_n) %>%
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
  metadata = list(
    schema_version = 1L,
    normalization = "100 times return divided by the strategy in-sample mean",
    accounting_t2_screen = plotdat0$matchset,
    top5_screen = tempplotdat$matchset,
    mining_universes = c(accounting = dmcomp$name, ticker = dmtic$name),
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

# max-predictor variants ---------------------------------------------------
maxDMpredictors <- c(100, 1000)
ret_for_plot_MaxPredictors <- tibble()
for (rr in seq_along(maxDMpredictors)) {
  message("Making accounting event time returns with Max DM predictors: ",
          maxDMpredictors[rr])
  tempMatched <- plotdat0$comp_matched %>%
    filter(rank_tstat <= maxDMpredictors[rr] + 1)
  tempEvent_time <- make_DM_event_returns(
    DMname = dmcomp$name, match_strats = tempMatched, npubmax = plotdat0$npubmax,
    czsum = czsum, use_sign_info = plotdat0$use_sign_info
  )
  ret_for_plot_MaxPredictors <- czret %>%
    transmute(pubname = signalname, eventDate, ret = ret_scaled, theory) %>%
    left_join(
      tempEvent_time %>% transmute(pubname, eventDate, matchRet = dm_mean),
      by = c("pubname", "eventDate")
    ) %>%
    select(eventDate, ret, matchRet, pubname, theory) %>%
    mutate(maxDMpredictors = maxDMpredictors[rr]) %>%
    bind_rows(ret_for_plot_MaxPredictors)
}

saveRDS(raw_dm_benchmarks, outp("raw_dm_benchmarks.RDS"))
saveRDS(plotdat0, outp("plotdat0.RDS"))
saveRDS(ret_for_plot0, outp("ret_for_plot0.RDS"))
saveRDS(ret_for_plot1, outp("ret_for_plot1.RDS"))
saveRDS(ret_for_plot_MaxPredictors, outp("ret_for_plot_MaxPredictors.RDS"))

# Keep an in-memory copy of the raw published display panel for Phase C.
ret_for_plot0_A <- ret_for_plot0

rm(dmtic, ret_for_plot1, ret_for_plot_MaxPredictors, tempplotdat,
   published_benchmark, accounting_t2_benchmark, accounting_top5_benchmark,
   ticker_top5_benchmark, raw_dm_benchmarks)
# dmcomp$insampsum and plotdat0 are large; drop them now that Phase A is done.
dmcomp$insampsum <- NULL
rm(plotdat0)
gc()

# ==========================================================================
# Phase B: matched candidate returns + pairwise correlations
#   (formerly 2b_MatchDataMinedToPub.R)
# ==========================================================================
message("\n===== Phase B: matched candidate returns + correlations =====")

t_tol <- globalSettings$t_tol
r_tol <- globalSettings$r_tol
t_reltol <- globalSettings$matched_uncorr_t_reltol
r_reltol <- globalSettings$matched_uncorr_r_reltol
minNumStocks <- globalSettings$minNumStocks

# Chapter-2 matching used czsum WITHOUT the Keep filter (inclSignals only).
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(signalname %in% inclSignals)
czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  filter(signalname %in% inclSignals)

DMname <- dmcomp$name
bm_rets <- readRDS(DMname)$ret
bm_info <- readRDS(DMname)$port_list
bm_user <- readRDS(DMname)$user
bm_rets <- bm_rets %>%
  left_join(bm_info %>% select(portid, sweight), by = c("portid")) %>%
  transmute(sweight, dmname = signalid, yearm, ret, nstock_long, nstock_short)
setDT(bm_rets)
rm(bm_info)

samplist <- czsum %>% distinct(sampstart, sampend) %>% arrange(sampstart, sampend)

cl <- makePSOCKcluster(ncores); registerDoParallel(cl)
dm_insamp <- foreach(sampi = 1:dim(samplist)[1], .combine = rbind,
                     .packages = c("data.table", "tidyverse", "zoo")) %dopar% {
  sampcur <- samplist[sampi, ]
  sumcur <- bm_rets[
    yearm >= sampcur$sampstart & yearm <= sampcur$sampend & !is.na(ret),
    .(rbar = mean(ret), tstat = mean(ret) / sd(ret) * sqrt(.N),
      min_nstock_long = min(nstock_long), min_nstock_short = min(nstock_short)),
    by = c("sweight", "dmname")
  ]
  filtcur <- bm_rets[
    floor(yearm) == year(sampcur$sampend) & !is.na(ret),
    .(nlastyear = .N), by = c("sweight", "dmname")
  ]
  sumcur %>%
    left_join(filtcur, by = c("sweight", "dmname")) %>%
    mutate(sampstart = sampcur$sampstart, sampend = sampcur$sampend)
}
stopCluster(cl)

matchsum <- czsum %>%
  transmute(pubname = signalname, rbar_op = rbar, tstat_op = tstat,
            sampstart, sampend, sweight = tolower(sweight)) %>%
  left_join(dm_insamp, by = c("sampstart", "sampend", "sweight"),
            relationship = "many-to-many") %>%
  mutate(diff_rbar = abs(rbar * sign(rbar) - rbar_op),
         diff_tstat = abs(tstat * sign(rbar) - tstat_op)) %>%
  setDT()

setDT(czret)
cl <- makePSOCKcluster(ncores); registerDoParallel(cl)
candidateReturns <- foreach(pubi = 1:dim(czsum)[1], .combine = rbind,
                            .packages = c("data.table", "tidyverse", "zoo")) %dopar% {
  pubcur <- czsum[pubi, ]
  matchcur <- matchsum[
    pubname == pubcur$signalname &
      diff_rbar <= r_tol & diff_tstat <= t_tol &
      diff_rbar / abs(rbar_op) <= r_reltol &
      diff_tstat / abs(tstat_op) <= t_reltol &
      min_nstock_long >= minNumStocks / 2 &
      min_nstock_short >= minNumStocks / 2 &
      nlastyear == 12
  ] %>%
    transmute(sweight, dmname, sign = sign(rbar))
  bm_rets %>%
    inner_join(matchcur, by = c("sweight", "dmname")) %>%
    transmute(candSignalname = dmname,
              eventDate = as.integer(round(12 * (yearm - pubcur$sampend))),
              sign, ret = ret * sign,
              samptype = case_when(
                (yearm >= pubcur$sampstart) & (yearm <= pubcur$sampend) ~ "insamp",
                (yearm > pubcur$sampend) ~ "oos", TRUE ~ NA_character_
              )) %>%
    mutate(actSignal = pubcur$signalname)
}
stopCluster(cl)
setDT(candidateReturns)
rm(dm_insamp, matchsum, samplist)

# Pairwise in-sample correlations against the actual (published) returns.
keep_signals <- czsum %>% filter(Keep) %>% pull(signalname)
tmpCands <- candidateReturns[
  actSignal %in% keep_signals & samptype == "insamp",
  .(candSignalname, eventDate, ret, actSignal)
]
tmpCands <- merge(
  tmpCands, czret[, .(actSignal = signalname, eventDate, retActual = ret)],
  by = c("actSignal", "eventDate")
)
allRhos <- tmpCands[, .(rho = cor(ret, retActual)), by = .(actSignal, candSignalname)]
setnames(allRhos, "candSignalname", "candidateSignal")
rm(tmpCands)
gc()

# ==========================================================================
# Phase C: matched-uncorr benchmark  (formerly 3d_MatchedUncorrData.R)
# ==========================================================================
message("\n===== Phase C: matched-uncorr benchmark =====")

czret_dates <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  distinct(signalname, pubdate)
czsum_mu <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep, signalname %in% inclSignals) %>%
  left_join(czret_dates, by = "signalname") %>%
  transmute(pubname = signalname, published_rbar = rbar,
            published_tstat = tstat, sampstart, sampend, pubdate) %>%
  setDT()
rm(czret_dates)

candidate_returns <- candidateReturns[actSignal %in% czsum_mu$pubname]

pairs <- candidate_returns[samptype == "insamp", .(
  sign = data.table::first(sign),
  nmonth_insamp = sum(!is.na(ret)),
  rbar_insamp_matched = mean(ret),
  tstat_insamp_matched = {
    n <- sum(!is.na(ret)); s <- stats::sd(ret, na.rm = TRUE)
    if (n > 1L && is.finite(s) && s > 0) mean(ret, na.rm = TRUE) / s * sqrt(n) else NA_real_
  }
), by = .(pubname = actSignal, matched_name = candSignalname)]
pairs <- merge(pairs, czsum_mu, by = "pubname", all.x = TRUE)
all_rhos <- allRhos %>%
  transmute(pubname = actSignal, matched_name = candidateSignal, rho) %>% setDT()
pairs <- merge(pairs, all_rhos, by = c("pubname", "matched_name"), all.x = TRUE)
pairs[, `:=`(
  mean_return_rel_distance = abs(rbar_insamp_matched - published_rbar) / abs(published_rbar),
  tstat_rel_distance = abs(tstat_insamp_matched - published_tstat) / abs(published_tstat),
  passes_history = nmonth_insamp >= globalSettings$match_nmonth_min,
  passes_correlation = !is.na(rho) & rho <= globalSettings$matched_uncorr_corr_max
)]
pairs[, keep_matched_uncorr := passes_history & passes_correlation]
data.table::setorder(pairs, pubname, matched_name)

matched_pairs <- pairs[passes_history == TRUE]
matched_uncorr_pairs <- pairs[keep_matched_uncorr == TRUE]
if (nrow(matched_uncorr_pairs) == 0L) stop("The matched-uncorr screens retained no pairs.")

aggregate_pairs <- function(pair_set) {
  selected <- candidate_returns[
    pair_set[, .(actSignal = pubname, candSignalname = matched_name, rbar_insamp_matched)],
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

matched_panel <- aggregate_pairs(matched_pairs)
data.table::setnames(matched_panel,
  c("ret_scaled", "ret_unscaled", "n_matched_available"),
  c("matched_ret_scaled", "matched_ret_unscaled", "n_matched_available"))
matched_uncorr_panel <- aggregate_pairs(matched_uncorr_pairs)
data.table::setnames(matched_uncorr_panel,
  c("ret_scaled", "ret_unscaled", "n_matched_available"),
  c("matched_uncorr_ret_scaled", "matched_uncorr_ret_unscaled", "n_matched_uncorr_available"))

published_panel <- ret_for_plot0_A %>%
  transmute(pubname, eventDate, calendarDate,
            published_ret_scaled = ret, published_ret_unscaled = ret_unscaled)
panel <- published_panel %>%
  inner_join(as_tibble(matched_uncorr_panel), by = c("pubname", "eventDate")) %>%
  left_join(as_tibble(matched_panel), by = c("pubname", "eventDate")) %>%
  left_join(as_tibble(czsum_mu), by = "pubname")

surviving_predictors <- sort(unique(matched_uncorr_pairs$pubname))
pair_keys <- paste(matched_uncorr_pairs$pubname, matched_uncorr_pairs$matched_name, sep = "\t")
matched_uncorr_metadata <- list(
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
  input_files = c(
    dmcomp$name,
    "../Data/Processed/czsum_allpredictors.RDS",
    "../Data/Processed/czret_keeponly.RDS"
  )
)
stopifnot(
  matched_uncorr_metadata$pair_count == sum(pairs$keep_matched_uncorr),
  matched_uncorr_metadata$predictor_count == data.table::uniqueN(panel$pubname),
  all(surviving_predictors == sort(unique(panel$pubname)))
)
saveRDS(
  list(metadata = matched_uncorr_metadata, pairs = pairs,
       surviving_predictors = surviving_predictors, panel = panel),
  outp("matched_uncorr_benchmark.RDS")
)
message("Wrote matched_uncorr_benchmark.RDS: ", matched_uncorr_metadata$pair_count,
        " pairs, ", matched_uncorr_metadata$predictor_count, " predictors")

rm(pairs, matched_pairs, matched_uncorr_pairs, matched_panel, matched_uncorr_panel,
   panel, published_panel, ret_for_plot0_A, allRhos, all_rhos, czsum_mu)
gc()

# ==========================================================================
# Phase D: factor-adjusted benchmark  (formerly 2d + 3e)
# ==========================================================================
message("\n===== Phase D: factor-adjusted benchmark =====")

t_threshold <- 2
factors <- readRDS("../Data/Raw/FamaFrenchFactors.RData") %>% rename(date = yearm)

# --- Risk-adjust the matched candidate returns (formerly 2d) ---------------
czsum_ra <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(signalname %in% inclSignals, Keep) %>%
  select(signalname, sampstart, sampend)

candidateReturns_adj <- candidateReturns %>%
  left_join(czsum_ra, by = c("actSignal" = "signalname")) %>%
  mutate(date = sampend + eventDate / 12,
         samptype = ifelse(date >= sampstart & date <= sampend, "insamp", "oos")) %>%
  left_join(factors, by = "date")
setDT(candidateReturns_adj)
rm(candidateReturns, candidate_returns); gc()

dm_full <- candidateReturns_adj[date >= sampstart, .(
  beta_capm = extract_beta(ret, mktrf),
  ff3_coeffs = list(extract_ff3_coeffs(ret, mktrf, smb, hml)),
  ff4_coeffs = list(extract_ff4_coeffs(ret, mktrf, smb, hml, umd))
), by = .(actSignal, candSignalname)]
dm_is <- candidateReturns_adj[date >= sampstart & date <= sampend, .(
  beta_capm_is = extract_beta(ret, mktrf),
  ff3_coeffs_is = list(extract_ff3_coeffs(ret, mktrf, smb, hml)),
  ff4_coeffs_is = list(extract_ff4_coeffs(ret, mktrf, smb, hml, umd))
), by = .(actSignal, candSignalname)]
dm_post <- candidateReturns_adj[date > sampend, .(
  beta_capm_post = extract_beta(ret, mktrf),
  ff3_coeffs_post = list(extract_ff3_coeffs(ret, mktrf, smb, hml)),
  ff4_coeffs_post = list(extract_ff4_coeffs(ret, mktrf, smb, hml, umd))
), by = .(actSignal, candSignalname)]

# Unpack element k from EACH pair's coefficient vector (never unlist()).
unpack_coeffs <- function(coef_list, k) vapply(coef_list, function(z) as.numeric(z[k]), numeric(1))
dm_full[, c("beta_ff3", "s_ff3", "h_ff3") := lapply(1:3, function(k) unpack_coeffs(ff3_coeffs, k))][, ff3_coeffs := NULL]
dm_full[, c("beta_ff4", "s_ff4", "h_ff4", "u_ff4") := lapply(1:4, function(k) unpack_coeffs(ff4_coeffs, k))][, ff4_coeffs := NULL]
dm_is[, c("beta_ff3_is", "s_ff3_is", "h_ff3_is") := lapply(1:3, function(k) unpack_coeffs(ff3_coeffs_is, k))][, ff3_coeffs_is := NULL]
dm_is[, c("beta_ff4_is", "s_ff4_is", "h_ff4_is", "u_ff4_is") := lapply(1:4, function(k) unpack_coeffs(ff4_coeffs_is, k))][, ff4_coeffs_is := NULL]
dm_post[, c("beta_ff3_post", "s_ff3_post", "h_ff3_post") := lapply(1:3, function(k) unpack_coeffs(ff3_coeffs_post, k))][, ff3_coeffs_post := NULL]
dm_post[, c("beta_ff4_post", "s_ff4_post", "h_ff4_post", "u_ff4_post") := lapply(1:4, function(k) unpack_coeffs(ff4_coeffs_post, k))][, ff4_coeffs_post := NULL]

candidateReturns_adj <- candidateReturns_adj %>%
  left_join(dm_full, by = c("actSignal", "candSignalname")) %>%
  left_join(dm_is, by = c("actSignal", "candSignalname")) %>%
  left_join(dm_post, by = c("actSignal", "candSignalname")) %>%
  mutate(
    abnormal_capm = ret - beta_capm * mktrf,
    abnormal_ff3 = ret - (beta_ff3 * mktrf + s_ff3 * smb + h_ff3 * hml),
    abnormal_ff4 = ret - (beta_ff4 * mktrf + s_ff4 * smb + h_ff4 * hml + u_ff4 * umd),
    abnormal_capm_tv = case_when(
      date >= sampstart & date <= sampend ~ ret - beta_capm_is * mktrf,
      date > sampend ~ ret - beta_capm_post * mktrf, TRUE ~ NA_real_),
    abnormal_ff3_tv = case_when(
      date >= sampstart & date <= sampend ~ ret - (beta_ff3_is * mktrf + s_ff3_is * smb + h_ff3_is * hml),
      date > sampend ~ ret - (beta_ff3_post * mktrf + s_ff3_post * smb + h_ff3_post * hml), TRUE ~ NA_real_),
    abnormal_ff4_tv = case_when(
      date >= sampstart & date <= sampend ~ ret - (beta_ff4_is * mktrf + s_ff4_is * smb + h_ff4_is * hml + u_ff4_is * umd),
      date > sampend ~ ret - (beta_ff4_post * mktrf + s_ff4_post * smb + h_ff4_post * hml + u_ff4_post * umd), TRUE ~ NA_real_)
  )
setDT(candidateReturns_adj)
rm(dm_full, dm_is, dm_post); gc()

# --- Published-side factor adjustment + contract assembly (formerly 3e) ----
cache_path <- outp("risk_adjusted_dm_benchmarks.RDS")

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  filter(signalname %in% inclSignals) %>%
  left_join(factors, by = "date") %>% setDT()
setorder(czret, signalname, eventDate)

czret[date >= sampstart & date <= sampend, rbar_t := {
  m <- mean(ret, na.rm = TRUE); s <- sd(ret, na.rm = TRUE); n <- sum(!is.na(ret))
  if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
}, by = signalname]
czret[, rbar_t := nafill(rbar_t, "locf"), by = signalname]

betas_capm_is <- czret[date >= sampstart & date <= sampend & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_is = extract_beta(ret, mktrf)), by = signalname]
betas_capm_oos <- czret[date > sampend & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_oos = extract_beta(ret, mktrf)), by = signalname]
czret <- merge(czret, betas_capm_is, by = "signalname", all.x = TRUE)
czret <- merge(czret, betas_capm_oos, by = "signalname", all.x = TRUE)
czret[, beta_capm_tv := ifelse(date >= sampstart & date <= sampend, beta_capm_is, beta_capm_oos)]
czret[, abnormal_capm_tv := ret - beta_capm_tv * mktrf]
czret[date >= sampstart & date <= sampend, `:=`(
  abar_capm_tv = mean(abnormal_capm_tv, na.rm = TRUE),
  abar_capm_tv_t = {
    m <- mean(abnormal_capm_tv, na.rm = TRUE); s <- sd(abnormal_capm_tv, na.rm = TRUE)
    n <- sum(!is.na(abnormal_capm_tv)); if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_capm_tv := nafill(abar_capm_tv, "locf"), by = signalname]
czret[, abar_capm_tv_t := nafill(abar_capm_tv_t, "locf"), by = signalname]
czret[, abnormal_capm_tv_normalized := ifelse(
  abs(abar_capm_tv) > 1e-10, 100 * abnormal_capm_tv / abar_capm_tv, NA_real_)]

ff4_is <- czret[date >= sampstart & date <= sampend & !is.na(ret) & !is.na(mktrf) &
    !is.na(smb) & !is.na(hml) & !is.na(umd), {
      coeffs <- extract_ff4_coeffs(ret, mktrf, smb, hml, umd)
      .(beta_ff4_is = coeffs[1], s_ff4_is = coeffs[2], h_ff4_is = coeffs[3], u_ff4_is = coeffs[4])
    }, by = signalname]
ff4_oos <- czret[date > sampend & !is.na(ret) & !is.na(mktrf) &
    !is.na(smb) & !is.na(hml) & !is.na(umd), {
      coeffs <- extract_ff4_coeffs(ret, mktrf, smb, hml, umd)
      .(beta_ff4_oos = coeffs[1], s_ff4_oos = coeffs[2], h_ff4_oos = coeffs[3], u_ff4_oos = coeffs[4])
    }, by = signalname]
czret <- merge(czret, ff4_is, by = "signalname", all.x = TRUE)
czret <- merge(czret, ff4_oos, by = "signalname", all.x = TRUE)
for (coefficient in c("beta", "s", "h", "u")) {
  target <- paste0(coefficient, "_ff4_tv")
  is_col <- paste0(coefficient, "_ff4_is"); oos_col <- paste0(coefficient, "_ff4_oos")
  czret[, (target) := ifelse(date >= sampstart & date <= sampend, get(is_col), get(oos_col))]
}
czret[, abnormal_ff4_tv := ret - (beta_ff4_tv * mktrf + s_ff4_tv * smb + h_ff4_tv * hml + u_ff4_tv * umd)]
czret[date >= sampstart & date <= sampend, `:=`(
  abar_ff4_tv = mean(abnormal_ff4_tv, na.rm = TRUE),
  abar_ff4_tv_t = {
    m <- mean(abnormal_ff4_tv, na.rm = TRUE); s <- sd(abnormal_ff4_tv, na.rm = TRUE)
    n <- sum(!is.na(abnormal_ff4_tv)); if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_ff4_tv := nafill(abar_ff4_tv, "locf"), by = signalname]
czret[, abar_ff4_tv_t := nafill(abar_ff4_tv_t, "locf"), by = signalname]
czret[, abnormal_ff4_tv_normalized := ifelse(
  abs(abar_ff4_tv) > 1e-10, 100 * abnormal_ff4_tv / abar_ff4_tv, NA_real_)]

dm_stats_tv <- candidateReturns_adj[
  date >= sampstart & date <= sampend & !is.na(abnormal_capm_tv),
  .(abar_capm_tv_dm = mean(abnormal_capm_tv, na.rm = TRUE),
    n_capm_tv_dm = sum(!is.na(abnormal_capm_tv)),
    sd_capm_tv_dm = sd(abnormal_capm_tv, na.rm = TRUE),
    abar_ff4_tv_dm = mean(abnormal_ff4_tv, na.rm = TRUE),
    n_ff4_tv_dm = sum(!is.na(abnormal_ff4_tv)),
    sd_ff4_tv_dm = sd(abnormal_ff4_tv, na.rm = TRUE)),
  by = .(actSignal, candSignalname)
]
dm_stats_tv[, abar_capm_tv_dm_t := ifelse(n_capm_tv_dm > 1 & sd_capm_tv_dm > 0,
  abar_capm_tv_dm / sd_capm_tv_dm * sqrt(n_capm_tv_dm), NA_real_)]
dm_stats_tv[, abar_ff4_tv_dm_t := ifelse(n_ff4_tv_dm > 1 & sd_ff4_tv_dm > 0,
  abar_ff4_tv_dm / sd_ff4_tv_dm * sqrt(n_ff4_tv_dm), NA_real_)]

signals_raw_t2 <- unique(czret[rbar_t > t_threshold]$signalname)

build_model_panel <- function(model_key) {
  abnormal_col <- paste0("abnormal_", model_key, "_tv")
  published_mean_col <- paste0("abar_", model_key, "_tv")
  published_t_col <- paste0(published_mean_col, "_t")
  dm_t_col <- paste0("abar_", model_key, "_tv_dm_t")
  normalized_col <- paste0(abnormal_col, "_normalized")

  published_signals <- intersect(
    unique(czret[get(published_t_col) > t_threshold]$signalname), signals_raw_t2)
  eligible_pairs <- dm_stats_tv[
    !is.na(get(dm_t_col)) & get(dm_t_col) > t_threshold, .(actSignal, candSignalname)]
  dm_filtered <- candidateReturns_adj[eligible_pairs,
    on = c("actSignal", "candSignalname"), nomatch = 0]
  dm_agg <- normalize_and_aggregate_dm(dm_filtered, abnormal_col, model_key)
  setnames(dm_agg,
    c(paste0("matchRet_", model_key), paste0("n_matches_", model_key)),
    c("dm_return", "n_eligible_pairs"))
  dm_available <- dm_filtered[, .(
    n_pairs_available = uniqueN(candSignalname[!is.na(get(abnormal_col))])
  ), by = .(actSignal, eventDate)]

  panel <- czret[signalname %in% published_signals, .(
    pubname = signalname, eventDate, calendarDate = date,
    published_return = get(normalized_col)
  )] %>%
    inner_join(as_tibble(dm_agg), by = c("pubname" = "actSignal", "eventDate")) %>%
    left_join(as_tibble(dm_available), by = c("pubname" = "actSignal", "eventDate")) %>%
    filter(!is.na(dm_return))

  list(panel = panel, eligible_published_signals = sort(published_signals),
       eligible_pairs = as_tibble(eligible_pairs))
}

capm <- build_model_panel("capm")
ff4 <- build_model_panel("ff4")

published_stats <- unique(czret[, .(
  signalname, rbar_t, beta_capm_is, beta_capm_oos, abar_capm_tv, abar_capm_tv_t,
  beta_ff4_is, s_ff4_is, h_ff4_is, u_ff4_is,
  beta_ff4_oos, s_ff4_oos, h_ff4_oos, u_ff4_oos, abar_ff4_tv, abar_ff4_tv_t
)])
published_stats[, `:=`(
  eligible_raw_t2 = !is.na(rbar_t) & rbar_t > t_threshold,
  eligible_capm_t2 = !is.na(rbar_t) & rbar_t > t_threshold & !is.na(abar_capm_tv_t) & abar_capm_tv_t > t_threshold,
  eligible_ff4_t2 = !is.na(rbar_t) & rbar_t > t_threshold & !is.na(abar_ff4_tv_t) & abar_ff4_tv_t > t_threshold
)]
dm_stats_tv[, `:=`(
  eligible_capm_t2 = !is.na(abar_capm_tv_dm_t) & abar_capm_tv_dm_t > t_threshold,
  eligible_ff4_t2 = !is.na(abar_ff4_tv_dm_t) & abar_ff4_tv_dm_t > t_threshold
)]

risk_adjusted_result <- list(
  capm = capm, ff4 = ff4,
  published_stats = as_tibble(published_stats),
  pair_stats = as_tibble(dm_stats_tv),
  metadata = list(
    schema_version = 1L,
    coefficient_regimes = c("original sample", "post-sample"),
    minimum_factor_observations = 60L,
    raw_t_threshold = t_threshold, alpha_t_threshold = t_threshold,
    normalization = "each series by its own original-sample alpha mean",
    factor_models = list(capm = "Mkt-RF", ff4 = c("Mkt-RF", "SMB", "HML", "UMD")),
    input_files = c(dmcomp$name,
      "../Data/Processed/czret_keeponly.RDS",
      "../Data/Raw/FamaFrenchFactors.RData"),
    model_counts = list(
      capm = c(predictors = n_distinct(capm$panel$pubname), eligible_pairs = nrow(capm$eligible_pairs)),
      ff4 = c(predictors = n_distinct(ff4$panel$pubname), eligible_pairs = nrow(ff4$eligible_pairs))
    )
  )
)
stopifnot(
  !anyDuplicated(as.data.frame(risk_adjusted_result$capm$panel)[c("pubname", "eventDate")]),
  !anyDuplicated(as.data.frame(risk_adjusted_result$ff4$panel)[c("pubname", "eventDate")])
)
saveRDS(risk_adjusted_result, cache_path)
message("Wrote ", cache_path)
message("\nAll DM benchmark contracts written to ", normalizePath(out_dir))
