# Data-mining comparisons --------------------------------------
# this used to be the main figures, back in the day

rm(list = ls())
source('0_Environment.R')

# x-axis range for all plots
global_xl = -360  # x-axis lower bound
global_xh = 300   # x-axis upper bound

matchname = paste0('../Data/Processed/', globalSettings$dataVersion, ' MatchPub.RData')

# Import and Clean Matched Data ------------------------------------------------------

# CZ data
inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, theory) %>% 
  filter(signalname %in% inclSignals)

czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>% 
  filter(Keep) %>% 
  left_join(czcat, by = 'signalname') %>% 
  filter(signalname %in% inclSignals)

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(
    retOrig = ret
    , ret = ret/rbar*100
  ) %>% 
  filter(signalname %in% inclSignals)

# matched DM data
tmp = readRDS(matchname)
candidateReturns = tmp$candidateReturns
user = tmp$user
rm(tmp)

# filter for Keep only
candidateReturns = candidateReturns %>% 
  filter(actSignal %in% (czsum %>% filter(Keep) %>% pull(signalname)))

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
  # mutate(retOrig = ret,
  #        ret = 100*ret) %>%   
  group_by(actSignal, eventDate) %>% 
  summarise(matchRet = mean(ret, na.rm = TRUE),
            matchRetOrig = mean(retOrig, na.rm = TRUE),
            nSignals = n()) %>% 
  ungroup()

# Combine with matched signals
allRets = czret %>% 
  left_join(tempCand, by = c('signalname' = 'actSignal', 'eventDate' = 'eventDate'))

# Prepare data for the plotting function (which expects 'pubname' not 'signalname')
allRetsForPlot = allRets %>%
  rename(pubname = signalname, calendarDate = date)

rm(tempsumCand, tempCand)

# Plot re-scaled returns over time ----------------------------------------
ReturnPlotsWithDM_std_errors_indicators(dt = allRetsForPlot %>% 
                    filter(!is.na(matchRet), Keep == 1) %>% # To exclude unmatched signals
                    transmute(pubname, eventDate, ret, matchRet, calendarDate),
                  basepath = '../Results/Fig_PublicationsVsDataMining',
                  suffix = 'All_DM',
                  rollmonths = 60,
                  colors = colors,
                  xl = global_xl,
                  xh = global_xh,
                  labelmatch = TRUE
)

# Plot re-scaled returns over time by category
for (jj in unique(allRetsForPlot$theory)) {
  print(jj)
  
  ReturnPlotsWithDM_std_errors_indicators(dt = allRetsForPlot %>% 
                      filter(!is.na(matchRet), theory == jj, Keep == 1) %>% # To exclude unmatched signals
                      select(pubname, eventDate, ret, matchRet, calendarDate),
                    basepath = '../Results/Fig_PublicationsVsDataMining',
                    suffix = paste0(jj, '_DM'),
                    colors = colors,
                    xl = global_xl,
                    xh = global_xh,
                    yl = -90, yh = 170, fig.width = 18, fontsize = 28) 
  
}


# Excluding high correlations --------------------------------------------------
allRhos = readRDS('../Results/PairwiseCorrelationsActualAndMatches.RDS')

exclCorrelations = c(10)  # c(10, 20, 30, 40, 50)

for (cc in exclCorrelations) {
  
  # filter high correlations from candidate returns
  corCandidateReturns = candidateReturns %>% 
    left_join(allRhos, 
              by = c('candSignalname' = 'candidateSignal', 
                     'actSignal'      = 'actSignal')) %>% 
    filter(rho <= cc/100) %>% 
    select(-rho)
  
  print('The following actual signals are dropped by the correlation restriction')
  print(setdiff(unique(candidateReturns$actSignal), unique(corCandidateReturns$actSignal)))
  
  # Normalize candidate returns
  # In-sample means
  tempsumCand = corCandidateReturns %>% 
    filter(samptype == 'insamp') %>%
    group_by(actSignal, candSignalname) %>% 
    summarise(rbar_insampMatched = mean(ret)) %>% 
    ungroup()
  
  # Rescale and average over all matched signals for each predictor and event date, define matchRetAlt
  tempCand = corCandidateReturns %>% 
    left_join(tempsumCand) %>% 
    mutate(retOrig = ret,
           ret = 100*ret/rbar_insampMatched) %>%
    # mutate(retOrig = ret,
    #        ret = 100*ret) %>%   
    group_by(actSignal, eventDate) %>% 
    summarise(matchRetAlt = mean(ret, na.rm = TRUE),
              matchRetOrig = mean(retOrig, na.rm = TRUE),
              nSignals = n()) %>% 
    ungroup()
  
  # Combine with matched signals
  CorAllRets = czret %>% 
    left_join(tempCand, by = c('signalname' = 'actSignal', 'eventDate' = 'eventDate')) %>% 
    # Add matched returns without excluding correlations
    left_join(allRets %>% transmute(signalname, date, matchRet)) %>%
    # Rename columns for plotting function
    rename(pubname = signalname, calendarDate = date)  
  
  rm(tempsumCand, tempCand, corCandidateReturns)
  
  # Plot returns over time over all categories
  ReturnPlotsWithDM_std_errors_indicators(dt = CorAllRets %>% 
                      filter(!is.na(matchRetAlt), Keep == 1) %>% # To exclude unmatched signals
                      transmute(pubname, eventDate, ret, matchRet, matchRetAlt, calendarDate),
                    basepath = '../Results/Fig_PublicationsVsDataMining',
                    suffix = paste0('All_DM_Correlation', cc),
                    rollmonths = 60,
                    colors = colors,
                    xl = global_xl,
                    xh = global_xh,
                    legendlabels = c('Published'
                    ,'Matched on t-stat and mean return'
                    ,'Matched and excluding correlated'
                    ),
                    legendpos = c(35,45)/100,
                    fontsize = 24,
                    yl = -90, yh = 170
  )
  
  # Plot returns over time by category
  for (jj in unique(CorAllRets$theory)) {
    print(jj)
    
    ReturnPlotsWithDM_std_errors_indicators(dt = CorAllRets %>% 
                        filter(!is.na(matchRetAlt), theory == jj, Keep == 1) %>% # To exclude unmatched signals
                        select(pubname, eventDate, ret, matchRet, matchRetAlt, calendarDate),
                      basepath = '../Results/Fig_PublicationsVsDataMining',
                      suffix = paste0(jj, '_DM_Correlation', cc),
                      colors = colors,
                      xl = global_xl,
                      xh = global_xh,
                      legendlabels = c('Published'
                      ,'Matched on in-sample t-stat and mean return'
                      ,'Matched and excluding correlated'
                      ),
                      legendpos = c(35,45)/100,
                      fontsize = 24,
                      yl = -90, yh = 170
                      
    ) 
    
  }
  
} # end of loop over correlation restrictions


# Excluding high correlations and tighter returns --------------------------------------------------
allRhos = readRDS('../Results/PairwiseCorrelationsActualAndMatches.RDS')

exclCorrelations = c(10)  # c(10, 20, 30, 40, 50)
r_tol_cor <- 0.1
for (cc in exclCorrelations) {
  
  # filter high correlations from candidate returns
  corCandidateReturns = candidateReturns %>% 
    left_join(allRhos, 
              by = c('candSignalname' = 'candidateSignal', 
                     'actSignal'      = 'actSignal')) %>% 
    filter(rho <= cc/100) %>% 
    select(-rho) %>%
    left_join(czsum %>% transmute(actSignal = signalname, rbar_op = rbar))
  original_number <- unique(candidateReturns$actSignal)
  cor_filter_number <- unique(corCandidateReturns$actSignal)
  print('The following actual signals are dropped by the correlation restriction')
  print(setdiff(original_number, cor_filter_number))
  
  # Normalize candidate returns
  # In-sample means
  tempsumCand = corCandidateReturns %>% 
    group_by(candSignalname) %>%
    mutate(rbar_dm_is = mean(ret[samptype == "insamp"], na.rm = TRUE)) %>%
    ungroup() %>%
    mutate(diff_rbar = abs(rbar_dm_is*sign(rbar_dm_is) - rbar_op)) %>%
    filter(diff_rbar <= r_tol_cor) %>%
    filter(samptype == 'insamp') %>%
    group_by(actSignal, candSignalname) %>% 
    summarise(rbar_insampMatched = mean(ret)) %>% 
    ungroup()
  
  cor_filter_number_after_mean <- unique(tempsumCand$actSignal)
  print('The following actual signals are dropped by the correlation restriction and the 10%')
  print(setdiff(original_number, cor_filter_number_after_mean) %>% length())
  print(setdiff(original_number, cor_filter_number_after_mean))
  # Rescale and average over all matched signals for each predictor and event date
  tempCand = corCandidateReturns %>% 
    left_join(tempsumCand) %>% 
    mutate(retOrig = ret,
           ret = 100*ret/rbar_insampMatched) %>%
    # mutate(retOrig = ret,
    #        ret = 100*ret) %>%   
    group_by(actSignal, eventDate) %>% 
    summarise(matchRet = mean(ret, na.rm = TRUE),
              matchRetOrig = mean(retOrig, na.rm = TRUE),
              nSignals = n()) %>% 
    ungroup()
  
  # Combine with matched signals
  CorAllRets = czret %>% 
    left_join(tempCand, by = c('signalname' = 'actSignal', 'eventDate' = 'eventDate')) %>% 
    # Add matched returns without excluding correlations
    left_join(allRets %>% transmute(signalname, date, matchRetAlt = matchRet)) %>%
    # Rename columns for plotting function
    rename(pubname = signalname, calendarDate = date)
  
  rm(tempsumCand, tempCand, corCandidateReturns)
  
  # Plot returns over time over all categories
  ReturnPlotsWithDM_std_errors_indicators(dt = CorAllRets %>% 
                      filter(!is.na(matchRet), Keep == 1) %>% # To exclude unmatched signals
                      transmute(pubname, eventDate, ret, matchRet, matchRetAlt, calendarDate),
                    basepath = '../Results/Fig_PublicationsVsDataMining',
                    suffix = paste0('All_DM_Correlation_', cc, '_d_mean_', r_tol_cor),
                    rollmonths = 60,
                    colors = colors,
                    xl = global_xl,
                    xh = global_xh,
                    legendlabels = c('Published','Matched data-mined (excl correlated)','Matched data-mined (all)'),
                    legendpos = c(35,45)/100,
                    fontsize = 24,
                    yl = -90, yh = 170
  )
  
  # Plot returns over time by category
  for (jj in unique(CorAllRets$theory)) {
    print(jj)
    
    ReturnPlotsWithDM_std_errors_indicators(dt = CorAllRets %>% 
                        filter(!is.na(matchRet), theory == jj, Keep == 1) %>% # To exclude unmatched signals
                        select(pubname, eventDate, ret, matchRet, matchRetAlt, calendarDate),
                      basepath = '../Results/Fig_PublicationsVsDataMining',
                      suffix = paste0(jj, '_DM_Correlation', cc, '_d_mean_', r_tol_cor),
                      colors = colors,
                      xl = global_xl,
                      xh = global_xh,
                      legendlabels = c('Published','Matched data-mined (excl correlated)','Matched data-mined (all)'),
                      legendpos = c(35,45)/100,
                      fontsize = 24,
                      yl = -90, yh = 170
                      
    ) 
    
  }
  
}

