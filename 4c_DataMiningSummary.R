# The OOS Sumstats can take several minutes

# Matching Summary XLSX ----------------------------------------------------------------

# Table 1: Unmatched predictors
tmpUnmatched = allRets %>% 
  group_by(signalname) %>% 
  filter(sum(is.na(matchRet)) == n()) %>% 
  ungroup() %>% 
  select(signalname) %>% 
  distinct()

tableUnmatched = czsum %>% 
  filter(signalname %in% tmpUnmatched$signalname) %>% 
  left_join(data.table::fread('../Data/Raw/SignalDoc.csv') %>% 
              as_tibble() %>% select(Acronym, Authors, Year, Journal),
            by = c('signalname' = 'Acronym')
  ) %>% 
  transmute(Authors, Year, Journal, rbar, tstat,
            SampleStartYear = year(sampstart),
            SampleEndYear   = year(sampend),
            theory1,
            sweight,
            signalname) %>% 
  arrange(Authors)


# Table 2: Matching summary
tmpUnmatchedByCat = czsum %>% 
  filter(!(signalname %in% unique(candidateReturns$actSignal))) %>% 
  group_by(theory1) %>% 
  count() %>% 
  transmute(theory1, SignalsUnmatched = n)

tmpMatchedByCat = czsum %>% 
  filter(signalname %in% unique(candidateReturns$actSignal)) %>% 
  group_by(theory1) %>% 
  count() %>% 
  transmute(theory1, SignalsMatched = n)

tmpNMatches = candidateReturns %>% 
  group_by(actSignal) %>% 
  summarise(nSignals = n_distinct(candSignalname)) %>% 
  ungroup() %>%
  left_join(czsum %>% select(signalname, theory1, sampstart, sampend), by = c('actSignal' = 'signalname')) %>% 
  group_by(theory1) %>% 
  summarise(medianStartYear = median(year(sampstart)),
            medianEndYear   = median(year(sampend)),
            min = min(nSignals), 
            Q25 = quantile(nSignals, .25), 
            Q50 = quantile(nSignals, .5),
            Q75 = quantile(nSignals, .75),
            max = max(nSignals))

tempsumRets = allRets %>% 
  filter(samptype == 'insamp', !(signalname %in% tmpUnmatched$signalname)) %>%
  group_by(theory1, signalname) %>% 
  summarise(retm = mean(retOrig),
            rett = mean(retOrig)/sd(retOrig)*sqrt(dplyr::n()),
            matchRet = mean(matchRetOrig, na.rm = TRUE)) %>% 
  group_by(theory1) %>% 
  summarize(rbar_insampAct = mean(retm),
            tstat_insampAct = mean(rett, na.rm = TRUE),
            rbar_insampMatched = mean(matchRet))

# Annoyingly complicated way to calculate average t-stat of matches
tempSumTstats = candidateReturns %>% 
  filter(samptype == 'insamp') %>%
  group_by(actSignal, candSignalname) %>% 
  summarise(tstat = mean(ret)/sd(ret)*sqrt(dplyr::n())) %>% 
  group_by(actSignal) %>% 
  summarise(tstat_insampMatched = mean(tstat)) %>% 
  left_join(czsum, by = c('actSignal' = 'signalname')) %>% 
  group_by(theory1) %>% 
  summarise(tstat_insampMatched = mean(tstat_insampMatched))

tempsumRets = tempsumRets %>% 
  left_join(tempSumTstats) %>% 
  select(theory1, starts_with('rbar'), starts_with('tstat')) # to change ordering


tableSumStats = tempsumRets %>% 
  left_join(tmpNMatches) %>% 
  left_join(tmpUnmatchedByCat) %>% 
  left_join(tmpMatchedByCat) %>% 
  arrange(desc(theory1)) %>% 
  # Change ordering
  select(theory1, medianStartYear, medianEndYear, rbar_insampAct, rbar_insampMatched,
         tstat_insampAct, tstat_insampMatched, min, Q25, Q50, Q75, max, SignalsUnmatched,
         SignalsMatched)

# Table 3: Significance of differences
# Mean comparisons in- and post-sample

tmpPredDecay = allRets %>% 
  filter(!(signalname %in% tmpUnmatched$signalname)) %>% # To exclude unmatched signals
  mutate(Sample = case_when(
    samptype == 'insamp'          ~ 'Insample', 
    eventDate >0 & eventDate <=60 ~  'Post1-5',
    eventDate > 60                ~ 'Post6+',
    TRUE ~ NA_character_)) %>% 
  group_by(signalname, Sample) %>% 
  summarise(meanAct = mean(ret),
            meanMatched = mean(matchRet, na.rm = TRUE)) %>% 
  ungroup() %>% 
  pivot_longer(cols = starts_with("mean"),
               names_prefix = 'mean',
               values_to = 'return') %>% 
  mutate(Predictor = ifelse(name == 'Act', 'Actual', 'Matched'))

tmpDecay = tmpPredDecay %>% 
  left_join(czsum) %>% 
  group_by(Sample, Predictor, theory1) %>% 
  summarise(meanReturn = mean(return, na.rm = TRUE),
            sdReturn = sd(return, na.rm = TRUE),
            seMean = sdReturn/sqrt(n())) 

tmpMeans = tmpDecay %>% 
  select(-sdReturn, -seMean) %>% 
  pivot_wider(names_from = c(theory1, Sample, Predictor),
              values_from = c(meanReturn))

tmpSEs = tmpDecay %>% 
  select(-sdReturn, -meanReturn) %>% 
  pivot_wider(names_from = c(theory1, Sample, Predictor),
              values_from = c(seMean))

tableSignificance = tmpMeans %>% 
  bind_rows(tmpSEs) %>% 
  # Change ordering
  select(starts_with('risk'),
         starts_with('mispricing'),
         starts_with('agnostic'))

# Save
toExcel = list(
  Unmatched    = tableUnmatched,
  SummaryTable = tableSumStats,
  Significance = tableSignificance
)

writexl::write_xlsx(toExcel, path = '../Results/MatchingSummary_DM.xlsx')


# Matching LaTeX output for overleaf -----------------------------------------------


#dm-sum part 1
tableSumStats %>% 
  transmute(Category = str_to_sentence(theory1),
            empty1 = NA_character_,
            medianStartYear = as.integer(medianStartYear),
            medianEndYear   = as.integer(medianEndYear),
            empty2 = NA_character_,
            rbar_insampAct  = round(100*rbar_insampAct, 1),
            rbar_insampMatched = round(100*rbar_insampMatched, 1),
            empty3 = NA_character_,
            tstat_insampAct = round(tstat_insampAct, 2),
            tstat_insampMatched = round(tstat_insampMatched,2)) %>% 
  xtable(digits = c(0, 0, 0, 0 ,0, 0, 1, 1, 0, 2, 2)) %>% 
  print(
    include.rownames = FALSE,
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = paste0('../Results/dm-sum1.tex')    
  )

#dm-sum part 2
tableSumStats %>% 
  mutate_at(.vars = vars(min, starts_with('Q'), max, starts_with("Signal")),
            .funs = list(~as.integer(.))) %>% 
  transmute(Category = str_to_sentence(theory1),
            empty1 = NA_character_,
            min, Q25, Q50, Q75, max,
            empty2 = NA_character_,
            SignalsUnmatched,
            SignalsMatched) %>% 
  xtable() %>% 
  print(
    include.rownames = FALSE,
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = paste0('../Results/dm-sum2.tex')    
  )


#Unmatched
signaldoc = read_csv('../Data/Raw/SignalDoc.csv')
tableUnmatched = tableUnmatched %>% 
  left_join(signaldoc %>% select(Acronym, LongDescription),
            by = c('signalname' = 'Acronym'))

for (rr in c('risk', 'mispricing', 'agnostic')) {
  
  tableUnmatched %>% 
    filter(theory1 == rr) %>% 
    arrange(Authors) %>% 
    transmute(Authors = paste0(Authors, ' (', as.character(Year), ')'), 
              LongDescription = str_to_sentence(LongDescription),
              theory1 = str_to_sentence(theory1), 
              rbar = round(100*rbar, 1), 
              tstat = round(tstat,2)) %>% 
    xtable(digits = c(0, 0, 0, 0, 1, 2)) %>% 
    print(
      include.rownames = FALSE,
      include.colnames = FALSE,
      hline.after = NULL,
      only.contents = TRUE,
      file = paste0('../Results/unmatched-', rr, '.tex')    
    )
}




# DM OOS Sumstats to CSV  ----------------------------
var_types <- c('vw', 'ew')
var_type <- var_types[1]


bm_rets = readRDS(DMname)$ret
bm_info = readRDS(DMname)$port_list

bm_rets = bm_rets %>% 
  left_join(
    bm_info %>% select(portid, sweight), by = c('portid')
  )  %>%
  transmute(
    sweight
    , signalname = signalid
    , yearm
    , ret
    , nstock) %>% 
  filter(signalname %in% filteredCandidates)


for (var_type in var_types) {
  
  str_to_add  <- var_type
  
  yz = bm_rets %>%
    filter(sweight == var_type) %>% 
    transmute(
      signalname, date = as.Date(yearm), ret
    )
  
  
  sumsignal_all = yz %>% 
    group_by(signalname) %>% 
    summarize(rbar = mean(ret), nmonth = n(), stdev = sd(ret),
              sharpe = f.sharp(ret),
              tstat = rbar/sd(ret)*sqrt(nmonth)) %>% 
    ungroup() %>% as.data.table()
  
  Summary_Statistics <- sumsignal_all %>% 
    summarise(across(where(is.numeric), .fns = 
                       list(Count =  ~  n(),
                            Mean = mean,
                            SD = sd,
                            Min = min,
                            q01 = ~quantile(., 0.01), 
                            q05 = ~quantile(., 0.01), 
                            q25 = ~quantile(., 0.25), 
                            Median = median,
                            q75 = ~quantile(., 0.75),
                            q95 = ~quantile(., 0.95),
                            q99 = ~quantile(., 0.99),
                            Max = max ))) %>%
    pivot_longer(everything(), names_sep = "_", names_to = c( "variable", ".value")) 
  # %>%  mutate_if(is.numeric, round, 2)
  
  fwrite(Summary_Statistics, glue::glue('../Results/Summary_StatisticsDM_{str_to_add}.csv'))
  
  Summary_Statistics
  
  print(xtable::xtable(Summary_Statistics, caption = 'Summary Statistics YZ All',
                       type = "latex", include.rownames=FALSE))
  
  
  ############################### # 
  # Table 1b
  ############################### #
  
  # Returns based on past returns
  # Basically creating a portfolio
  
  yz_dt <- yz %>% as.data.table() %>% setkey(signalname, date)
  
  yz_dt[, ret_30y_l := data.table::shift(frollmean(ret, 12*30, NA)), by = signalname]
  
  yz_dt[, t_30y_l   := data.table::shift(frollapply(ret, 12*30, f.custom.t, fill = NA)), by = signalname]
  
  yz_dt[, head(month(date))]
  
  yz_dt[month(date) != 6, t_30y_l := NA]
  
  ########################### #
  
  n_tiles <- 5
  
  name_var <- 'ret_30y_l'
  
  test <- f.ls.past.returns(n_tiles, name_var)
  
  print(xtable::xtable(test$sumsignal_oos, 
                       caption = 'Out-of-Sample Portfolios of Strategies Sorted on Past 30 Years of Returns',
                       type = "latex"), include.colnames=FALSE)
  
  fwrite(test$sumsignal_oos,  glue::glue('../Results/sumsignal_oos_30y_{str_to_add}_unit_level.csv'))
  fwrite(test$sumsignal_oos_pre_2003,  glue::glue('../Results/sumsignal_oos_30y_pre_2003_{str_to_add}_unit_level.csv'))
  fwrite(test$sumsignal_oos_post_2003,  glue::glue('../Results/sumsignal_oos_30y_post_2003_{str_to_add}_unit_level.csv'))
  
}


# DM OOS Sumstats to LaTeX ----------------------------------------------------


# to TeX
fs_ew = read_csv('../Results/sumsignal_oos_30y_ew_unit_level.csv')
fs_vw = read_csv('../Results/sumsignal_oos_30y_vw_unit_level.csv')

fs_ew = fs_ew %>% 
  transmute(bin = as.integer(bin),
            empty1 = NA_character_,
            rbar_is = round(100*rbar_is, 1),
            avg_tstat_is = round(avg_tstat_is, 2),
            empty2 = NA_character_,
            rbar_oos = round(100*rbar_oos, 1),
            Decay = ifelse(bin !=4, 
                           round(100*(1 - rbar_oos/rbar_is), 1),
                           NA_real_),
            empty3 = NA_character_
  )

fs_vw = fs_vw %>% 
  transmute(rbar_isvw = round(100*rbar_is, 1),
            avg_tstat_isvw = round(avg_tstat_is, 2),
            empty1vw = NA_character_,
            rbar_oosvw = round(100*rbar_oos, 1),
            Decayvw = ifelse(bin !=4, 
                             round(100*(1 - rbar_oos/rbar_is), 1),
                             NA_real_)
  )

bind_cols(fs_ew, fs_vw) %>% 
  xtable(digits = c(0, 0, 0, 1, 2,0, 1, 1, 0, 1, 2, 0, 1, 1)) %>% 
  print(
    include.rownames = FALSE,
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = paste0('../Results/dm-sortsFull.tex')
  )

# post 2003
fs_ew = read_csv('../Results/sumsignal_oos_30y_post_2003_ew_unit_level.csv')
fs_vw = read_csv('../Results/sumsignal_oos_30y_post_2003_vw_unit_level.csv')

fs_ew = fs_ew %>% 
  transmute(bin = as.integer(bin),
            empty1 = NA_character_,
            rbar_is = round(100*rbar_is, 1),
            avg_tstat_is = round(avg_tstat_is, 2),
            empty2 = NA_character_,
            rbar_oos = round(100*rbar_oos, 1),
            Decay = ifelse(bin !=4, 
                           round(100*(1 - rbar_oos/rbar_is), 1),
                           NA_real_),
            empty3 = NA_character_
  )

fs_vw = fs_vw %>% 
  transmute(rbar_isvw = round(100*rbar_is, 1),
            avg_tstat_isvw = round(avg_tstat_is, 2),
            empty1vw = NA_character_,
            rbar_oosvw = round(100*rbar_oos, 1),
            Decayvw = ifelse(bin !=4, 
                             round(100*(1 - rbar_oos/rbar_is), 1),
                             NA_real_)
  )

bind_cols(fs_ew, fs_vw) %>% 
  xtable(digits = c(0, 0, 0, 1, 2,0, 1, 1, 0, 1, 2, 0, 1, 1)) %>% 
  print(
    include.rownames = FALSE,
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = paste0('../Results/dm-sortsPost2003.tex')
  )

