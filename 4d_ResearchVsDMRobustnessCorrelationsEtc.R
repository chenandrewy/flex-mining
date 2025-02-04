# Data-mining comparisons --------------------------------------
rm(list = ls())
source('0_Environment.R')

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

rm(tempsumCand, tempCand)


# Plot re-scaled returns over time ----------------------------------------
ReturnPlotsWithDM(dt = allRets %>% 
                    filter(!is.na(matchRet), Keep == 1) %>% # To exclude unmatched signals
                    transmute(eventDate, ret, matchRet),
                  basepath = '../Results/Fig_PublicationsVsDataMining',
                  suffix = 'All_DM',
                  rollmonths = 60,
                  colors = colors
                  , labelmatch = TRUE
)


# Plot re-scaled returns over time by category
for (jj in unique(allRets$theory)) {
  print(jj)
  
  ReturnPlotsWithDM(dt = allRets %>% 
                      filter(!is.na(matchRet), theory == jj, Keep == 1) %>% # To exclude unmatched signals
                      dplyr::select(eventDate, ret, matchRet),
                    basepath = '../Results/Fig_PublicationsVsDataMining',
                    suffix = paste0(jj, '_DM'),
                    colors = colors,
                    yl = -90, yh = 170, fig.width = 18, fontsize = 28) 
  
}



# Animations --------------------------------------------------------------

ReturnPlotsWithDM(dt = allRets %>% 
                    filter(!is.na(matchRet), Keep == 1) %>% # To exclude unmatched signals
                    transmute(eventDate, ret, matchRet),
                  basepath = '../Results/Extra/Fig_PublicationsVsDataMining',
                  suffix = 'All_DM',
                  rollmonths = 60,
                  colors = colors
                  , labelmatch = TRUE
                  , hideoos = FALSE
                  , filetype = '.png'
)


ReturnPlotsWithDM(dt = allRets %>% 
                    filter(!is.na(matchRet), Keep == 1) %>% # To exclude unmatched signals
                    transmute(eventDate, ret, matchRet),
                  basepath = '../Results/Extra/Fig_PublicationsVsDataMining',
                  suffix = 'All_DM_hide',
                  rollmonths = 60,
                  colors = colors
                  , labelmatch = TRUE
                  , hideoos = TRUE
                  , filetype = '.png'
)




# More Animations -------------------------------------------------------------------------

source('0_Environment.R')
# Plot re-scaled returns over time by category, hide
for (jj in unique(allRets$theory)) {
  print(jj)
  
  ReturnPlotsWithDM(dt = allRets %>% 
                      filter(!is.na(matchRet), theory == jj, Keep == 1) %>% # To exclude unmatched signals
                      dplyr::select(eventDate, ret, matchRet),
                    basepath = '../Results/Extra/Fig_PublicationsVsDataMining',
                    suffix = paste0(jj, '_DM_hide'),
                    colors = colors,
                    yl = -60, yh = 170, fig.width = 18, fontsize = 28
                    , hideoos = TRUE
                    , filetype = '.png') 
  
}

# Plot re-scaled returns over time by category
for (jj in unique(allRets$theory)) {
  print(jj)
  
  ReturnPlotsWithDM(dt = allRets %>% 
                      filter(!is.na(matchRet), theory == jj, Keep == 1) %>% # To exclude unmatched signals
                      dplyr::select(eventDate, ret, matchRet),
                    basepath = '../Results/Extra/Fig_PublicationsVsDataMining',
                    suffix = paste0(jj, '_DM'),
                    colors = colors,
                    yl = -60, yh = 170, fig.width = 18, fontsize = 28
                    , hideoos = FALSE
                    , filetype = '.png') 
  
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
    left_join(allRets %>% transmute(signalname, date, 
                                    matchRetAlt = matchRet))
  
  rm(tempsumCand, tempCand, corCandidateReturns)
  
  # Plot returns over time over all categories
  ReturnPlotsWithDM(dt = CorAllRets %>% 
                      filter(!is.na(matchRet), Keep == 1) %>% # To exclude unmatched signals
                      transmute(eventDate, ret, matchRet, matchRetAlt),
                    basepath = '../Results/Fig_PublicationsVsDataMining',
                    suffix = paste0('All_DM_Correlation', cc),
                    rollmonths = 60,
                    colors = colors,
                    legendlabels = c('Published','Matched data-mined (excl correlated)','Matched data-mined (all)'),
                    legendpos = c(35,45)/100,
                    fontsize = 24,
                    yl = -90, yh = 170
  )
  
  # Plot returns over time by category
  for (jj in unique(CorAllRets$theory)) {
    print(jj)
    
    ReturnPlotsWithDM(dt = CorAllRets %>% 
                        filter(!is.na(matchRet), theory == jj, Keep == 1) %>% # To exclude unmatched signals
                        dplyr::select(eventDate, ret, matchRet, matchRetAlt),
                      basepath = '../Results/Fig_PublicationsVsDataMining',
                      suffix = paste0(jj, '_DM_Correlation', cc),
                      colors = colors,
                      legendlabels = c('Published','Matched data-mined (excl correlated)','Matched data-mined (all)'),
                      legendpos = c(35,45)/100,
                      fontsize = 24,
                      yl = -90, yh = 170
                      
    ) 
    
  }
  
}


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
    left_join(allRets %>% transmute(signalname, date, 
                                    matchRetAlt = matchRet))
  
  rm(tempsumCand, tempCand, corCandidateReturns)
  
  # Plot returns over time over all categories
  ReturnPlotsWithDM(dt = CorAllRets %>% 
                      filter(!is.na(matchRet), Keep == 1) %>% # To exclude unmatched signals
                      transmute(eventDate, ret, matchRet, matchRetAlt),
                    basepath = '../Results/Fig_PublicationsVsDataMining',
                    suffix = paste0('All_DM_Correlation_', cc, '_d_mean_', r_tol_cor),
                    rollmonths = 60,
                    colors = colors,
                    legendlabels = c('Published','Matched data-mined (excl correlated)','Matched data-mined (all)'),
                    legendpos = c(35,45)/100,
                    fontsize = 24,
                    yl = -90, yh = 170
  )
  
  # Plot returns over time by category
  for (jj in unique(CorAllRets$theory)) {
    print(jj)
    
    ReturnPlotsWithDM(dt = CorAllRets %>% 
                        filter(!is.na(matchRet), theory == jj, Keep == 1) %>% # To exclude unmatched signals
                        dplyr::select(eventDate, ret, matchRet, matchRetAlt),
                      basepath = '../Results/Fig_PublicationsVsDataMining',
                      suffix = paste0(jj, '_DM_Correlation', cc, '_d_mean_', r_tol_cor),
                      colors = colors,
                      legendlabels = c('Published','Matched data-mined (excl correlated)','Matched data-mined (all)'),
                      legendpos = c(35,45)/100,
                      fontsize = 24,
                      yl = -90, yh = 170
                      
    ) 
    
  }
  
}

