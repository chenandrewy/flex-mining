# Plots for mispricing vs risk
# library(tidyverse)
# library(lubridate)
rm(list = ls())

source('0_Environment.R')

colors = c(rgb(0,0.4470,0.7410), # MATBLUE
           rgb(0.8500, 0.3250, 0.0980), # MATRED
           rgb(0.9290, 0.6940, 0.1250) # MATYELLOW
)


# Load data ---------------------------------------------------------------

# CZ data
tmp = readRDS('../Data/Processed/czdata.RDS')
czsum = tmp$czsum
czret = tmp$czret
rm(tmp)

# Matched returns
#candidateReturns = readRDS('../Data/Processed/MatchedData.RDS')
candidateReturns = readRDS('../Data/Processed/MatchedData2023-01-31 22h09m.RDS')


# Restrict to predictors in consideration
czsum = czsum %>% 
  filter(Keep == 1)

czret = czret %>% 
  filter(signalname %in% czsum$signalname)

candidateReturns = candidateReturns %>% 
  filter(actSignal %in% czsum$signalname)

# Section 2: Rolling returns by category ----------------------------------


# All Signals
ReturnPlotsNoDM(dt = czret %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory1),
                basepath = '../Results/Fig_PublicationsOverTime',
                suffix = 'AllSignals'
)

# Rep Qual at least fair
# ReturnPlotsNoDM(dt = czret %>% 
#                   filter(Rep_Quality %in% c('1_good', '2_fair')) %>% 
#                   transmute(eventDate,
#                             signalname,
#                             ret,
#                             catID = theory1),
#                 basepath = '../Results/Fig_PublicationsOverTime',
#                 suffix = 'RepQualGoodFair'
# )


# Keep equal 1
# ReturnPlotsNoDM(dt = czret %>% 
#                   filter(Keep == 1) %>% 
#                   transmute(eventDate,
#                             signalname,
#                             ret,
#                             catID = theory1),
#                 basepath = '../Results/Fig_PublicationsOverTime',
#                 suffix = 'KeepEqual1'
# )



# Section 3: Data-mining comparisons --------------------------------------

# Normalize candidate returns

# In-sample means
tempsumCand = candidateReturns %>% 
  filter(samptype == 'insamp') %>%
  group_by(actSignal, candSignalname) %>% 
  summarise(rbar_insampMatched = mean(ret)) %>% 
  ungroup()

# Rescale and average over all matched signals for each predictor and event date
tempCand = candidateReturns %>% 
  left_join(tempsumCand) %>% 
  mutate(retOrig = ret,
         ret = 100*ret/rbar_insampMatched) %>% 
  group_by(actSignal, eventDate) %>% 
  summarise(matchRet = mean(ret, na.rm = TRUE),
            matchRetOrig = mean(retOrig, na.rm = TRUE),
            nSignals = n()) %>% 
  ungroup()

# Combine with matched signals
allRets = czret %>% 
  left_join(tempCand, by = c('signalname' = 'actSignal', 'eventDate' = 'eventDate'))

rm(tempsumCand, tempCand)


# Plot re-scaled returns over time
ReturnPlotsWithDM(dt = allRets %>% 
                    filter(!is.na(matchRet), Keep == 1) %>% # To exclude unmatched signals
                    transmute(eventDate, ret, matchRet),
                  basepath = '../Results/Fig_PublicationsVsDataMining',
                  suffix = 'All_DM',
                  rollmonths = 60,
                  colors = colors)

# Plot re-scaled returns over time by category
for (jj in unique(allRets$theory1)) {
  print(jj)
  
  ReturnPlotsWithDM(dt = allRets %>% 
                      filter(!is.na(matchRet), theory1 == jj, Keep == 1) %>% # To exclude unmatched signals
                      select(eventDate, ret, matchRet),
                    basepath = '../Results/Fig_PublicationsVsDataMining',
                    suffix = paste0(jj, '_DM'),
                    colors = colors,
                    yl = -60, yh = 170, fig.width = 18) 
  
}


# Tables

# Table 1: Unmatched predictors
tmpUnmatched = allRets %>% 
  group_by(signalname) %>% 
  filter(sum(is.na(matchRet)) == n()) %>% 
  ungroup() %>% 
  select(signalname) %>% 
  distinct()

tableUnmatched = czsum %>% 
  filter(signalname %in% tmpUnmatched$signalname) %>% 
  left_join(data.table::fread('../Data/CZ/SignalDoc.csv') %>% 
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


