# Matching Summary XLSX ----------------------------------------------------------------

# Table 1: Unmatched predictors ----
tmpUnmatched = allRets %>% 
  group_by(signalname) %>% 
  filter(sum(is.na(matchRet)) == n()) %>% 
  ungroup() %>% 
  select(signalname) %>% 
  distinct()

tableUnmatched = czsum %>% 
  filter(signalname %in% tmpUnmatched$signalname) %>% 
  left_join(data.table::fread('../Data/Raw/SignalDoc.csv') %>% 
              as_tibble() %>% select(Acronym, Authors, Journal),
            by = c('signalname' = 'Acronym')
  ) %>% 
  transmute(Authors, Year, Journal, rbar, tstat,
            SampleStartYear = year(sampstart),
            SampleEndYear   = year(sampend),
            theory1,
            sweight,
            signalname) %>% 
  arrange(Authors)


# Table 2: Matching summary ----
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

# Table 3: Significance of differences ----
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


## Table 4: Pairwise correlations between actual and DM/matched returns

# Compute correlations between each actual signal and each DM strategy (including unmatched)
# Note: This takes a very long time, I'm commenting this out for now, bc not sure it's worth it
# allRhos = tibble()
# num_cores = round(.4*detectCores())
# for (act in unique(czret$signalname)) {
#   print(act)
#   tmp = czret %>% 
#     filter(samptype == 'insamp') %>% 
#     filter(signalname == act) %>% 
#     select(date, retOrig) %>% 
#     left_join(dmdat$ret %>%   # This gives a very large table (1.3b rows)
#                 filter(portid == 1) %>% 
#                 select(yearm, ret, signalid),
#               by = c('date' = 'yearm'))
#   
#   
#   
#   tmpDMids = unique(tmp$signalid)
#   setDTthreads(1)
#   cl <- makePSOCKcluster(num_cores)
#   registerDoParallel(cl)
#   tmpRhos = foreach(signali=1:length(tmpDMids), 
#                        .combine = rbind,
#                        .packages = c('tidyverse')) %dopar% {
#                          
#                          tibble(actSignal = act,
#                                 candidateSignal = tmpDMids[signali],
#                                 rho = cor(tmp$ret[tmp$signalid == tmpDMids[signali]], 
#                                           tmp$retOrig[tmp$signalid == tmpDMids[signali]], 
#                                           use = 'complete.obs'))
# 
#                        }     
#   
#   stopCluster(cl)
#   setDTthreads()
#   
#   
#   # add to list
#   allRhos = bind_rows(
#     allRhos,
#     tmpRhos
#   )
#   
# }

# Pairwise correlations between actual signals and matched DM signals ----
tmpCands = candidateReturns %>% 
  filter(actSignal %in% czsum$signalname) %>% 
  filter(samptype == 'insamp') %>%  # interested in in-sample correlation with actual signals
  # Merge actual returns to candidate returns
  select(candSignalname, eventDate, ret, actSignal) %>% 
  inner_join(czret %>% 
               transmute(actSignal = signalname,
                         eventDate,
                         retActual = retOrig))

allRhos = tibble()

for (act in unique(tmpCands$actSignal)) {
  print(act)
  tmp = tmpCands %>% 
    filter(actSignal == act)
  
  # compute correlations for one actual signal
  tmpRhos = tibble()
  for (i in unique(tmp$candSignalname)) {
    
    tmpRhos = bind_rows(
      tmpRhos,
      tibble(actSignal = act,
             candidateSignal = i,
             rho = cor(tmp$ret[tmp$candSignalname == i], tmp$retActual[tmp$candSignalname == i]))
    )
    
  }
  
  # add to list
  allRhos = bind_rows(
    allRhos,
    tmpRhos
  )
  
}



# Plots and numbers

# Overall correlation relatively low on average
allRhos %>% 
  ggplot(aes(x = rho)) +
  geom_histogram() +
  theme_light(base_size = 18) +
  geom_vline(xintercept = median(allRhos$rho))


#quantile(allRhos$rho, probs = seq(0,1,.1))

saveRDS(allRhos, '../Results/PairwiseCorrelationsActualAndMatches.RDS')

# Create table
tmpCorrelations = allRhos %>% 
  left_join(czsum %>% select(signalname, theory1),
            by = c('actSignal' = 'signalname')) %>% 
  group_by(theory1) %>% 
  summarise(
    Min = min(rho),
    Q05 = quantile(rho, probs = 0.05, na.rm = TRUE),
    Q10 = quantile(rho, probs = 0.10, na.rm = TRUE),
    Q25 = quantile(rho, probs = 0.25, na.rm = TRUE),
    Q50 = quantile(rho, probs = 0.50, na.rm = TRUE),
    Q75 = quantile(rho, probs = 0.75, na.rm = TRUE),
    Q90 = quantile(rho, probs = 0.90, na.rm = TRUE),
    Q95 = quantile(rho, probs = 0.95, na.rm = TRUE),
    Max = max(rho)
  ) %>%
  arrange(desc(theory1)) %>% 
  tibble()


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


# Correlations
tmpCorrelations %>% 
  transmute(Category = str_to_sentence(theory1),
            empty1 = NA_character_,
            # Min,
            Q05,
            Q10, 
            Q25,
            Q50,
            Q75,
            Q90,
            Q95,
            # Max,
            empty2 = NA_character_) %>% 
  xtable(digits = c(0, 0, 2, 2, 2, 2, 2, 2, 2, 0, 0)) %>% 
  print(
    include.rownames = FALSE,
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = "../Results/allRhos-Cor.tex"  
  )
