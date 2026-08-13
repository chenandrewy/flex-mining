# Alternative-specification versions of three Figure 2 panels (see 4c20a/4c20b
# for the primary versions):
#   (a-alt) FF3 proper instead of FF3+momentum: CAPM pub/DM + FF3 pub/DM
#   (b-alt) "long post-sample" read of pre-2003: sample ends by Dec 2004
#           (>=20 years of post-sample data) instead of publication year < 2003
#   (c-alt) matched on 10% relative tolerance in BOTH |t| and |rbar| (2b ships
#           10% t / 30% rbar; the extra 10% rbar screen is applied per pair here)
#
# Reuses the primary panels' CAPM and accounting-only series from
# fig2_panel_long.RDS, so 4c20a must have run first.
#
# Outputs: ../Data/Processed/fig2_alt_panel_{long,agg}.RDS and
#          ../Results/Fig2/Fig2{a,b,c}_*_{FF3,SampEnd2004,Tol10}{,_CI}.pdf

rm(list = ls())
source('0_Environment.R')
source('helpers/risk_adjusted_helpers_tv.R')
source('helpers/fig2_helpers.R')

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

caldate = czret %>% select(signalname, eventDate, date) %>% distinct()

main_long = readRDS('../Data/Processed/fig2_panel_long.RDS')

alt_long = list()

# (b-alt): sample ends by Dec 2004 (>= 20y post-sample) --------------------

sampend2004_signals = czsum[sampend <= as.yearmon('Dec 2004')]$signalname
print(paste0('Signals with sample ending by Dec 2004: ', length(sampend2004_signals)))

ret_for_plot1 <- readRDS('../Data/Processed/ret_for_plot1.RDS')

b_new = ret_for_plot1 %>%
  filter(pubname %in% sampend2004_signals, !is.na(matchRet)) %>%
  left_join(caldate, by = c('pubname' = 'signalname', 'eventDate')) %>%
  rename(calendarDate = date)

alt_long$b = bind_rows(
  main_long %>%
    filter(panel == 'b',
           label %in% c('Pub, Annual Acct Only', 'DM, Annual Acct Pubs')) %>%
    select(-panel),
  b_new %>% transmute(label = 'Pub, Sample Ended by 2004', pubname, eventDate,
                      calendarDate, return = ret),
  b_new %>% transmute(label = 'DM, Sample Ended by 2004', pubname, eventDate,
                      calendarDate, return = matchRet)
) %>% mutate(panel = 'b2004')

# (c-alt): matched on 10% t AND 10% rbar, excl corr ------------------------
# 2b already imposes diff_tstat/tstat_op <= 0.1; here each pair is additionally
# required to satisfy |rbar_dm_insamp - rbar_op| / rbar_op <= 0.1, where
# rbar_dm_insamp is the pair's signed in-sample mean (2b's rbar*sign(rbar)).

matchname = paste0('../Data/Processed/', globalSettings$dataVersion, ' MatchPub.RData')
tmp = readRDS(matchname)
candidateReturns = tmp$candidateReturns %>%
  filter(actSignal %in% czsum$signalname)
rm(tmp); gc()

rbar_pair = candidateReturns %>%
  filter(samptype == 'insamp') %>%
  group_by(actSignal, candSignalname) %>%
  summarise(rbar_insampMatched = mean(ret), .groups = 'drop') %>%
  left_join(czsum %>% transmute(actSignal = signalname, rbar_op = rbar),
            by = 'actSignal') %>%
  mutate(tight = abs(rbar_insampMatched - rbar_op) / rbar_op <= 0.1)

print(paste0('Pairs passing the extra 10% rbar screen: ', sum(rbar_pair$tight),
             ' of ', nrow(rbar_pair)))

tight_pairs = rbar_pair %>% filter(tight) %>% select(actSignal, candSignalname, rbar_insampMatched)

allRhos = readRDS('../Results/PairwiseCorrelationsActualAndMatches.RDS')

cand_tight = candidateReturns %>%
  inner_join(tight_pairs, by = c('actSignal', 'candSignalname'))

matched_all_10 = cand_tight %>%
  mutate(ret_norm = 100 * ret / rbar_insampMatched) %>%
  group_by(actSignal, eventDate) %>%
  summarise(matchRet = mean(ret_norm, na.rm = TRUE), .groups = 'drop')

matched_cor_10 = cand_tight %>%
  left_join(allRhos,
            by = c('candSignalname' = 'candidateSignal', 'actSignal' = 'actSignal')) %>%
  filter(rho <= 0.10) %>%
  mutate(ret_norm = 100 * ret / rbar_insampMatched) %>%
  group_by(actSignal, eventDate) %>%
  summarise(matchRetAlt = mean(ret_norm, na.rm = TRUE), .groups = 'drop')

c_new = czret %>%
  filter(Keep == 1) %>%
  transmute(pubname = signalname, eventDate, calendarDate = date, ret = ret_scaled) %>%
  left_join(matched_all_10, by = c('pubname' = 'actSignal', 'eventDate')) %>%
  left_join(matched_cor_10, by = c('pubname' = 'actSignal', 'eventDate')) %>%
  filter(!is.na(matchRetAlt))

print(paste0('Published signals in the 10%/10% panel: ', length(unique(c_new$pubname))))

alt_long$c = bind_rows(
  c_new %>% transmute(label = 'Published', pubname, eventDate, calendarDate, return = ret),
  c_new %>% transmute(label = 'Matched, 10% t-stat and mean return', pubname, eventDate,
                      calendarDate, return = matchRet),
  c_new %>% transmute(label = 'Matched and excluding correlated', pubname, eventDate,
                      calendarDate, return = matchRetAlt)
) %>% mutate(panel = 'c10')

rm(candidateReturns, cand_tight, matched_all_10, matched_cor_10, allRhos); gc()

# (a-alt): FF3 proper (no momentum) ----------------------------------------

FamaFrenchFactors <- readRDS('../Data/Raw/FamaFrenchFactors.RData') %>%
  rename(date = yearm)

czret <- czret %>% left_join(FamaFrenchFactors, by = 'date')
setDT(czret)
setorder(czret, signalname, eventDate)

czret[date >= sampstart & date <= sampend, rbar_t := {
  m <- mean(ret, na.rm = TRUE); s <- sd(ret, na.rm = TRUE); n <- sum(!is.na(ret))
  if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
}, by = signalname]
czret[, rbar_t := nafill(rbar_t, 'locf'), by = signalname]

ff3_is <- czret[date >= sampstart & date <= sampend & !is.na(ret) & !is.na(mktrf) &
                  !is.na(smb) & !is.na(hml), {
  coeffs <- extract_ff3_coeffs(ret, mktrf, smb, hml)
  .(beta_ff3_is = coeffs[1], s_ff3_is = coeffs[2], h_ff3_is = coeffs[3])
}, by = signalname]
ff3_oos <- czret[date > sampend & !is.na(ret) & !is.na(mktrf) &
                   !is.na(smb) & !is.na(hml), {
  coeffs <- extract_ff3_coeffs(ret, mktrf, smb, hml)
  .(beta_ff3_oos = coeffs[1], s_ff3_oos = coeffs[2], h_ff3_oos = coeffs[3])
}, by = signalname]
czret <- merge(czret, ff3_is, by = 'signalname', all.x = TRUE)
czret <- merge(czret, ff3_oos, by = 'signalname', all.x = TRUE)
czret[, beta_ff3_tv := ifelse(date >= sampstart & date <= sampend, beta_ff3_is, beta_ff3_oos)]
czret[, s_ff3_tv := ifelse(date >= sampstart & date <= sampend, s_ff3_is, s_ff3_oos)]
czret[, h_ff3_tv := ifelse(date >= sampstart & date <= sampend, h_ff3_is, h_ff3_oos)]
czret[, abnormal_ff3_tv := ret - (beta_ff3_tv * mktrf + s_ff3_tv * smb + h_ff3_tv * hml)]
czret[date >= sampstart & date <= sampend, `:=`(
  abar_ff3_tv = mean(abnormal_ff3_tv, na.rm = TRUE),
  abar_ff3_tv_t = {
    m <- mean(abnormal_ff3_tv, na.rm = TRUE); s <- sd(abnormal_ff3_tv, na.rm = TRUE)
    n <- sum(!is.na(abnormal_ff3_tv))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_ff3_tv := nafill(abar_ff3_tv, 'locf'), by = signalname]
czret[, abar_ff3_tv_t := nafill(abar_ff3_tv_t, 'locf'), by = signalname]
czret[, abnormal_ff3_tv_normalized := ifelse(abs(abar_ff3_tv) > 1e-10,
                                             100 * abnormal_ff3_tv / abar_ff3_tv, NA_real_)]

risk_adj_file <- paste0('../Data/Processed/', globalSettings$dataVersion,
                        ' MatchPubRiskAdjusted.RData')
print('Loading risk-adjusted DM returns (large file)...')
candidateReturns_adj <- readRDS(risk_adj_file)
setDT(candidateReturns_adj)

dm_stats_ff3 <- candidateReturns_adj[
  (date >= sampstart & date <= sampend) & !is.na(abnormal_ff3_tv),
  .(
    abar_ff3_tv_dm_t = {
      m <- mean(abnormal_ff3_tv, na.rm = TRUE); s <- sd(abnormal_ff3_tv, na.rm = TRUE)
      n <- sum(!is.na(abnormal_ff3_tv))
      if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
    }
  ),
  by = .(actSignal, candSignalname)
]

signals_raw_t2 <- unique(czret[rbar_t > t_threshold]$signalname)
signals_ff3_tv_t2 <- intersect(unique(czret[abar_ff3_tv_t > t_threshold]$signalname),
                               signals_raw_t2)

dm_filtered_ff3 <- candidateReturns_adj %>%
  inner_join(dm_stats_ff3 %>% filter(abar_ff3_tv_dm_t > t_threshold) %>%
               select(actSignal, candSignalname),
             by = c('actSignal', 'candSignalname'))

dm_agg_ff3 <- normalize_and_aggregate_dm(dm_filtered_ff3, 'abnormal_ff3_tv', 'ff3') %>%
  rename(matchRet = matchRet_ff3)

a_new = czret %>%
  filter(signalname %in% signals_ff3_tv_t2) %>%
  transmute(pubname = signalname, eventDate, calendarDate = date,
            pub_abn = abnormal_ff3_tv_normalized) %>%
  inner_join(dm_agg_ff3 %>% select(actSignal, eventDate, matchRet),
             by = c('pubname' = 'actSignal', 'eventDate')) %>%
  filter(!is.na(matchRet))

print(paste0('ff3: pub signals = ', length(unique(a_new$pubname)),
             ', DM pairs passing t>', t_threshold, ' = ',
             sum(dm_stats_ff3$abar_ff3_tv_dm_t > t_threshold, na.rm = TRUE)))

alt_long$a = bind_rows(
  main_long %>%
    filter(panel == 'a', label %in% c('CAPM, Published', 'CAPM, Data-Mined')) %>%
    select(-panel),
  a_new %>% transmute(label = 'FF3, Published', pubname, eventDate, calendarDate,
                      return = pub_abn),
  a_new %>% transmute(label = 'FF3, Data-Mined', pubname, eventDate, calendarDate,
                      return = matchRet)
) %>% mutate(panel = 'aff3')

rm(candidateReturns_adj, dm_stats_ff3, dm_filtered_ff3); gc()

# Aggregate and save -------------------------------------------------------

alt_long = bind_rows(alt_long)
saveRDS(alt_long, '../Data/Processed/fig2_alt_panel_long.RDS')

print('Aggregating series (rolling means + clustered SEs)...')
alt_agg = alt_long %>%
  group_by(panel) %>%
  group_modify(~ fig2_aggregate_series(.x)) %>%
  ungroup()

saveRDS(alt_agg, '../Data/Processed/fig2_alt_panel_agg.RDS')

# Render -------------------------------------------------------------------

outdir = '../Results/Fig2'
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

MATPURPLE = rgb(0.4940, 0.1840, 0.5560)
colors4 = c(colors[1], colors[2], MATPURPLE, colors[3])

fontsizeall = 28
linesizeall = 1.5
ylaball = 'Trailing 5-Year Return (bps pm)'
global_xl = -360
global_xh = 300

panels = list(
  aff3 = list(
    series = c('CAPM, Published', 'CAPM, Data-Mined',
               'FF3, Published', 'FF3, Data-Mined'),
    colors = c(colors[1], colors[1], colors[2], colors[2]),
    linetypes = c('solid', 'longdash', 'solid', 'longdash'),
    yaxislab = 'Trailing 5-Year Alpha (bps pm)',
    yl = 0, yh = 125, yh_ci = 150, legendpos = c(35, 20) / 100,
    file = 'Fig2a_FactorAdj_FF3'
  ),
  b2004 = list(
    series = c('Pub, Annual Acct Only', 'DM, Annual Acct Pubs',
               'Pub, Sample Ended by 2004', 'DM, Sample Ended by 2004'),
    colors = c(colors[1], colors[1], colors[2], colors[2]),
    linetypes = c('solid', 'longdash', 'solid', 'longdash'),
    yaxislab = ylaball,
    yl = 0, yh = 175, yh_ci = 200, legendpos = c(35, 20) / 100,
    file = 'Fig2b_PubSampleLimits_SampEnd2004'
  ),
  c10 = list(
    series = c('Published', 'Matched, 10% t-stat and mean return',
               'Matched and excluding correlated'),
    colors = colors,
    linetypes = c('solid', 'longdash', 'dashed'),
    yaxislab = ylaball,
    yl = -50, yh = 170, legendpos = c(40, 22) / 100,
    file = 'Fig2c_MatchedExclCorr_Tol10'
  )
)

for (pk in names(panels)) {
  p = panels[[pk]]
  agg = alt_agg %>% filter(panel == pk)

  # warn if a series would clip at the chosen limits
  rng = agg %>% filter(eventDate >= global_xl, eventDate <= global_xh) %>%
    summarise(lo = min(roll_rbar, na.rm = TRUE), hi = max(roll_rbar, na.rm = TRUE),
              lo_ci = min(lower, na.rm = TRUE), hi_ci = max(upper, na.rm = TRUE))
  if (rng$lo < p$yl | rng$hi > p$yh)
    warning(sprintf('%s: lines [%.1f, %.1f] exceed [%s, %s]', pk, rng$lo, rng$hi, p$yl, p$yh))
  yh_ci_eff = if (!is.null(p$yh_ci)) p$yh_ci else p$yh
  if (rng$lo_ci < p$yl | rng$hi_ci > yh_ci_eff)
    warning(sprintf('%s: CI ribbons [%.1f, %.1f] exceed [%s, %s]', pk, rng$lo_ci, rng$hi_ci, p$yl, yh_ci_eff))

  for (civ in c('none', 'all')) {
    yh_used = if (civ == 'all' && !is.null(p$yh_ci)) p$yh_ci else p$yh
    plt = fig2_overlay_plot(
      agg, series_labels = p$series, colors = p$colors, linetypes = p$linetypes,
      ci = civ, xl = global_xl, xh = global_xh, yl = p$yl, yh = yh_used,
      fontsize = fontsizeall, legendpos = p$legendpos,
      yaxislab = p$yaxislab, linesize = linesizeall
    )
    fname = paste0(outdir, '/', p$file, if (civ == 'all') '_CI' else '', '.pdf')
    ggsave(fname, plt, width = 10, height = 8)
    print(paste('Saved', fname))
  }
}
