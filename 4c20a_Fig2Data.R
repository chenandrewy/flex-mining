# Build the plotted series for the reorganized Figure 2 (Spec 3, meeting notes 2026-07-23):
#   (a) Factor adjustments: CAPM pub/DM + FF3+Mom pub/DM (4 lines)
#   (b) Publication sample limits: annual-accounting-only pub/DM + pre-2003 pub/DM (4 lines)
#   (c) Controlling for sum-stats, excluding corr: pub + matched DM + matched & cor<=0.10 (3 lines)
#   (d) Alternative mining methods: pub + top 5% |t| accounting + top 5% |t| tickers (3 lines)
#
# Panel sources: (a) follows 4c4_RiskAdjustedResearchVsDMPlotsTV(FF4).R, (b) follows
# 4c6_AccountingOnlyPlots.R, (c) follows 4d_ResearchVsDMRobustnessCorrelationsEtc.R,
# (d) follows 4c3_ResearchVsAcctVsTicker.R. This script only assembles the long
# per-signal series and aggregates them; 4c20b_Fig2Plots.R renders the figures.
#
# Outputs: ../Data/Processed/fig2_panel_long.RDS  (label-level obs, for reaggregation)
#          ../Data/Processed/fig2_panel_agg.RDS   (rolling means + clustered SEs)

rm(list = ls())
source('0_Environment.R')
source('helpers/risk_adjusted_helpers_tv.R')
source('helpers/fig2_helpers.R')
library(doParallel)  # make_DM_event_returns uses makePSOCKcluster

t_threshold = 2

# Load global data --------------------------------------------------------

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType,
                                  topT = globalSettings$topT)

czsum <- readRDS('../Data/Processed/czsum_allpredictors.RDS') %>%
  filter(Keep) %>%
  filter(signalname %in% inclSignals) %>%
  setDT()

czcat <- fread('DataInput/SignalsTheoryChecked.csv') %>%
  select(signalname, Year, theory) %>%
  filter(signalname %in% inclSignals)

czret <- readRDS('../Data/Processed/czret_keeponly.RDS') %>%
  left_join(czcat, by = 'signalname') %>%
  mutate(retOrig = ret, ret_scaled = ret / rbar * 100) %>%
  filter(signalname %in% inclSignals)

# calendarDate lookup for series indexed by (pubname, eventDate)
caldate = czret %>% select(signalname, eventDate, date) %>% distinct()

fig2_long = list()

# Panel (b): publication sample limits ------------------------------------
# Annual-accounting-only published signals (selection copied from 4c6_AccountingOnlyPlots.R)

czacct = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>%
  left_join(fread('../Data/Raw/SignalDoc.csv') %>%
              transmute(Acronym, Cat.Data, Cat.Form, Def = tolower(`Detailed Definition`)),
            by = c('signalname' = 'Acronym')) %>%
  filter(signalname %in% inclSignals & Cat.Data == 'Accounting') %>%
  mutate(
    drop = FALSE
    , drop = if_else(grepl('quarter', Def), TRUE, drop)
    , drop = if_else(grepl('analyst|meanest|earningssurprise', paste(tolower(signalname), Def)), TRUE, drop)
    , drop = if_else(Cat.Form == 'discrete', TRUE, drop)
    , drop = if_else(signalname %in% c('ShareIss1Y', 'ShareIss5Y'), TRUE, drop)
  )
acct_signals = czacct[czacct$drop == FALSE, ]$signalname
print(paste0('Compustat Annual Accounting signals: ', length(acct_signals)))

# Pre-2003 publications ("Published pre-2003 only" in the meeting notes)
pre2003_signals = czcat %>% filter(Year < 2003) %>% pull(signalname)
print(paste0('Pre-2003 published signals: ', length(pre2003_signals)))
print(paste0('  (alternative, sampend <= Dec 2004 i.e. 20y post-sample: ',
             czsum[sampend <= as.yearmon('Dec 2004'), .N], ' signals)'))

ret_for_plot1 <- readRDS('../Data/Processed/ret_for_plot1.RDS')

panel_b_pair = function(signals, publab, dmlab) {
  dt = ret_for_plot1 %>%
    filter(pubname %in% signals, !is.na(matchRet)) %>%
    left_join(caldate, by = c('pubname' = 'signalname', 'eventDate')) %>%
    rename(calendarDate = date)
  bind_rows(
    dt %>% transmute(label = publab, pubname, eventDate, calendarDate, return = ret),
    dt %>% transmute(label = dmlab,  pubname, eventDate, calendarDate, return = matchRet)
  )
}

fig2_long$b = bind_rows(
  panel_b_pair(acct_signals,    'Pub, Annual Acct Only', 'DM, Annual Acct Pubs'),
  panel_b_pair(pre2003_signals, 'Pub, Pre-2003 Only',    'DM, Pre-2003 Pubs')
) %>% mutate(panel = 'b')

# Panel (d): alternative mining methods -----------------------------------
# Top 5% |t| accounting + top 5% |t| tickers (pipeline copied from 4c3_ResearchVsAcctVsTicker.R)

dmcomp <- readRDS('../Data/Processed/dmcomp_sumstats.RDS')
dmtic  <- readRDS('../Data/Processed/dmtic_sumstats.RDS')

matchset <- list(
  t_tol = globalSettings$t_tol,
  r_tol = globalSettings$r_tol,
  t_reltol = globalSettings$t_reltol,
  r_reltol = globalSettings$r_reltol,
  t_min = 0,               # pure top-x% screen, as in 4c3's t_rankpct_min block
  t_max = globalSettings$t_max,
  t_rankpct_min = 5,
  minNumStocks = globalSettings$minNumStocks
)

print('Making accounting top 5% event time returns (takes a few minutes)...')
comp_event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = SelectDMStrats(dmcomp$insampsum, matchset),
  npubmax = Inf, czsum = czsum, use_sign_info = TRUE
)

print('Making ticker top 5% event time returns...')
tic_event_time <- make_DM_event_returns(
  DMname = dmtic$name, match_strats = SelectDMStrats(dmtic$insampsum, matchset),
  npubmax = Inf, czsum = czsum, use_sign_info = TRUE
)

panel_d_wide <- czret %>%
  transmute(pubname = signalname, eventDate, calendarDate = date, ret = ret_scaled) %>%
  left_join(comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean),
            by = c('pubname', 'eventDate')) %>%
  left_join(tic_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean),
            by = c('pubname', 'eventDate')) %>%
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

fig2_long$d = bind_rows(
  panel_d_wide %>% transmute(label = 'Published', pubname, eventDate, calendarDate, return = ret),
  panel_d_wide %>% transmute(label = 'Top 5% |t| Mining Accounting', pubname, eventDate, calendarDate, return = matchRet),
  panel_d_wide %>% transmute(label = 'Top 5% |t| Mining Tickers', pubname, eventDate, calendarDate, return = matchRetAlt)
) %>% mutate(panel = 'd')

rm(comp_event_time, tic_event_time, dmcomp, dmtic); gc()

# Panel (c): sum-stat match, excluding correlated -------------------------
# Pipeline copied from 4d_ResearchVsDMRobustnessCorrelationsEtc.R (cc = 10 block)

matchname = paste0('../Data/Processed/', globalSettings$dataVersion, ' MatchPub.RData')
tmp = readRDS(matchname)
candidateReturns = tmp$candidateReturns %>%
  filter(actSignal %in% czsum$signalname)
rm(tmp); gc()

# matched DM, no correlation screen
rbar_pair = candidateReturns %>%
  filter(samptype == 'insamp') %>%
  group_by(actSignal, candSignalname) %>%
  summarise(rbar_insampMatched = mean(ret), .groups = 'drop')

matched_all = candidateReturns %>%
  left_join(rbar_pair, by = c('actSignal', 'candSignalname')) %>%
  mutate(ret_norm = 100 * ret / rbar_insampMatched) %>%
  group_by(actSignal, eventDate) %>%
  summarise(matchRet = mean(ret_norm, na.rm = TRUE), .groups = 'drop')

# matched DM, dropping pairs with correlation above 0.10
allRhos = readRDS('../Results/PairwiseCorrelationsActualAndMatches.RDS')

corCandidateReturns = candidateReturns %>%
  left_join(allRhos,
            by = c('candSignalname' = 'candidateSignal', 'actSignal' = 'actSignal')) %>%
  filter(rho <= 0.10)

print('Signals dropped entirely by the correlation restriction:')
print(setdiff(unique(candidateReturns$actSignal), unique(corCandidateReturns$actSignal)))

rbar_pair_cor = corCandidateReturns %>%
  filter(samptype == 'insamp') %>%
  group_by(actSignal, candSignalname) %>%
  summarise(rbar_insampMatched = mean(ret), .groups = 'drop')

matched_cor = corCandidateReturns %>%
  left_join(rbar_pair_cor, by = c('actSignal', 'candSignalname')) %>%
  mutate(ret_norm = 100 * ret / rbar_insampMatched) %>%
  group_by(actSignal, eventDate) %>%
  summarise(matchRetAlt = mean(ret_norm, na.rm = TRUE), .groups = 'drop')

panel_c_wide = czret %>%
  filter(Keep == 1) %>%
  transmute(pubname = signalname, eventDate, calendarDate = date, ret = ret_scaled) %>%
  left_join(matched_all, by = c('pubname' = 'actSignal', 'eventDate')) %>%
  left_join(matched_cor, by = c('pubname' = 'actSignal', 'eventDate')) %>%
  filter(!is.na(matchRetAlt))

fig2_long$c = bind_rows(
  panel_c_wide %>% transmute(label = 'Published', pubname, eventDate, calendarDate, return = ret),
  panel_c_wide %>% transmute(label = 'Matched on t-stat and mean return', pubname, eventDate, calendarDate, return = matchRet),
  panel_c_wide %>% transmute(label = 'Matched and excluding correlated', pubname, eventDate, calendarDate, return = matchRetAlt)
) %>% mutate(panel = 'c')

rm(candidateReturns, corCandidateReturns, matched_all, matched_cor, allRhos); gc()

# Panel (a): factor adjustments (CAPM + FF3+Mom, time-varying betas) -------
# Published side copied from 4c4_RiskAdjustedResearchVsDMPlotsTV(FF4).R; both
# factor models handled in one pass. DM side from MatchPubRiskAdjusted.RData
# (2d output, post FF3-recycling bugfix).

FamaFrenchFactors <- readRDS('../Data/Raw/FamaFrenchFactors.RData') %>%
  rename(date = yearm)

czret <- czret %>% left_join(FamaFrenchFactors, by = 'date')
setDT(czret)
setorder(czret, signalname, eventDate)

# raw in-sample t (for the baseline published filter)
czret[date >= sampstart & date <= sampend, rbar_t := {
  m <- mean(ret, na.rm = TRUE); s <- sd(ret, na.rm = TRUE); n <- sum(!is.na(ret))
  if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
}, by = signalname]
czret[, rbar_t := nafill(rbar_t, 'locf'), by = signalname]

# time-varying CAPM
betas_capm_is <- czret[date >= sampstart & date <= sampend & !is.na(ret) & !is.na(mktrf),
                       .(beta_capm_is = extract_beta(ret, mktrf)), by = signalname]
betas_capm_oos <- czret[date > sampend & !is.na(ret) & !is.na(mktrf),
                        .(beta_capm_oos = extract_beta(ret, mktrf)), by = signalname]
czret <- merge(czret, betas_capm_is, by = 'signalname', all.x = TRUE)
czret <- merge(czret, betas_capm_oos, by = 'signalname', all.x = TRUE)
czret[, beta_capm_tv := ifelse(date >= sampstart & date <= sampend, beta_capm_is, beta_capm_oos)]
czret[, abnormal_capm_tv := ret - beta_capm_tv * mktrf]
czret[date >= sampstart & date <= sampend, `:=`(
  abar_capm_tv = mean(abnormal_capm_tv, na.rm = TRUE),
  abar_capm_tv_t = {
    m <- mean(abnormal_capm_tv, na.rm = TRUE); s <- sd(abnormal_capm_tv, na.rm = TRUE)
    n <- sum(!is.na(abnormal_capm_tv))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_capm_tv := nafill(abar_capm_tv, 'locf'), by = signalname]
czret[, abar_capm_tv_t := nafill(abar_capm_tv_t, 'locf'), by = signalname]
czret[, abnormal_capm_tv_normalized := ifelse(abs(abar_capm_tv) > 1e-10,
                                              100 * abnormal_capm_tv / abar_capm_tv, NA_real_)]

# time-varying FF4 (FF3 + momentum)
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
czret <- merge(czret, ff4_is, by = 'signalname', all.x = TRUE)
czret <- merge(czret, ff4_oos, by = 'signalname', all.x = TRUE)
czret[, beta_ff4_tv := ifelse(date >= sampstart & date <= sampend, beta_ff4_is, beta_ff4_oos)]
czret[, s_ff4_tv := ifelse(date >= sampstart & date <= sampend, s_ff4_is, s_ff4_oos)]
czret[, h_ff4_tv := ifelse(date >= sampstart & date <= sampend, h_ff4_is, h_ff4_oos)]
czret[, u_ff4_tv := ifelse(date >= sampstart & date <= sampend, u_ff4_is, u_ff4_oos)]
czret[, abnormal_ff4_tv := ret - (beta_ff4_tv * mktrf + s_ff4_tv * smb + h_ff4_tv * hml + u_ff4_tv * umd)]
czret[date >= sampstart & date <= sampend, `:=`(
  abar_ff4_tv = mean(abnormal_ff4_tv, na.rm = TRUE),
  abar_ff4_tv_t = {
    m <- mean(abnormal_ff4_tv, na.rm = TRUE); s <- sd(abnormal_ff4_tv, na.rm = TRUE)
    n <- sum(!is.na(abnormal_ff4_tv))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_ff4_tv := nafill(abar_ff4_tv, 'locf'), by = signalname]
czret[, abar_ff4_tv_t := nafill(abar_ff4_tv_t, 'locf'), by = signalname]
czret[, abnormal_ff4_tv_normalized := ifelse(abs(abar_ff4_tv) > 1e-10,
                                             100 * abnormal_ff4_tv / abar_ff4_tv, NA_real_)]

# DM side: per-pair IS alpha t-stats, then normalize-and-aggregate
risk_adj_file <- paste0('../Data/Processed/', globalSettings$dataVersion,
                        ' MatchPubRiskAdjusted.RData')

print('Loading risk-adjusted DM returns (large file)...')
candidateReturns_adj <- readRDS(risk_adj_file)
setDT(candidateReturns_adj)

dm_stats_tv <- candidateReturns_adj[
  (date >= sampstart & date <= sampend) & !is.na(abnormal_capm_tv),
  .(
    abar_capm_tv_dm_t = {
      m <- mean(abnormal_capm_tv, na.rm = TRUE); s <- sd(abnormal_capm_tv, na.rm = TRUE)
      n <- sum(!is.na(abnormal_capm_tv))
      if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
    },
    abar_ff4_tv_dm_t = {
      m <- mean(abnormal_ff4_tv, na.rm = TRUE); s <- sd(abnormal_ff4_tv, na.rm = TRUE)
      n <- sum(!is.na(abnormal_ff4_tv))
      if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
    }
  ),
  by = .(actSignal, candSignalname)
]

signals_raw_t2 <- unique(czret[rbar_t > t_threshold]$signalname)
signals_capm_tv_t2 <- intersect(unique(czret[abar_capm_tv_t > t_threshold]$signalname), signals_raw_t2)
signals_ff4_tv_t2  <- intersect(unique(czret[abar_ff4_tv_t > t_threshold]$signalname), signals_raw_t2)

panel_a_pair = function(model_key, signals_pub, publab, dmlab) {
  abn_col = paste0('abnormal_', model_key, '_tv')
  dm_t_col = paste0('abar_', model_key, '_tv_dm_t')

  dm_filtered <- candidateReturns_adj %>%
    inner_join(dm_stats_tv %>% filter(.data[[dm_t_col]] > t_threshold) %>%
                 select(actSignal, candSignalname),
               by = c('actSignal', 'candSignalname'))

  dm_agg <- normalize_and_aggregate_dm(dm_filtered, abn_col, model_key) %>%
    rename(matchRet = !!sym(paste0('matchRet_', model_key)))

  dt = czret %>%
    filter(signalname %in% signals_pub) %>%
    transmute(pubname = signalname, eventDate, calendarDate = date,
              pub_abn = .data[[paste0(abn_col, '_normalized')]]) %>%
    inner_join(dm_agg %>% select(actSignal, eventDate, matchRet),
               by = c('pubname' = 'actSignal', 'eventDate')) %>%
    filter(!is.na(matchRet))

  print(paste0(model_key, ': pub signals = ', length(unique(dt$pubname)),
               ', DM pairs passing t>', t_threshold, ' = ',
               sum(dm_stats_tv[[dm_t_col]] > t_threshold, na.rm = TRUE)))

  bind_rows(
    dt %>% transmute(label = publab, pubname, eventDate, calendarDate, return = pub_abn),
    dt %>% transmute(label = dmlab,  pubname, eventDate, calendarDate, return = matchRet)
  )
}

fig2_long$a = bind_rows(
  panel_a_pair('capm', signals_capm_tv_t2, 'CAPM, Published',    'CAPM, Data-Mined'),
  panel_a_pair('ff4',  signals_ff4_tv_t2,  'FF3+Mom, Published', 'FF3+Mom, Data-Mined')
) %>% mutate(panel = 'a')

rm(candidateReturns_adj, dm_stats_tv); gc()

# Aggregate and save -------------------------------------------------------

fig2_long = bind_rows(fig2_long)
saveRDS(fig2_long, '../Data/Processed/fig2_panel_long.RDS')

print('Aggregating series (rolling means + clustered SEs)...')
fig2_agg = fig2_long %>%
  group_by(panel) %>%
  group_modify(~ fig2_aggregate_series(.x)) %>%
  ungroup()

saveRDS(fig2_agg, '../Data/Processed/fig2_panel_agg.RDS')

print('Series built:')
fig2_agg %>% group_by(panel, label) %>%
  summarise(n_months = sum(!is.na(roll_rbar)), .groups = 'drop') %>%
  print(n = 50)
