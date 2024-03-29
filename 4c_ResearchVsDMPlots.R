# Data-mining comparisons --------------------------------------



# Plot re-scaled returns over time
source('0_Environment.R')

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
    left_join(tempCand, by = c('signalname' = 'actSignal', 'eventDate' = 'eventDate'))
  
  rm(tempsumCand, tempCand, corCandidateReturns)
  
  # Plot returns over time over all categories
  ReturnPlotsWithDM(dt = CorAllRets %>% 
                      filter(!is.na(matchRet), Keep == 1) %>% # To exclude unmatched signals
                      transmute(eventDate, ret, matchRet),
                    basepath = '../Results/Fig_PublicationsVsDataMining',
                    suffix = paste0('All_DM_Correlation', cc),
                    rollmonths = 60,
                    colors = colors,
                    yl = -90, yh = 170
  )
  
  # Plot returns over time by category
  for (jj in unique(CorAllRets$theory)) {
    print(jj)
    
    ReturnPlotsWithDM(dt = CorAllRets %>% 
                        filter(!is.na(matchRet), theory == jj, Keep == 1) %>% # To exclude unmatched signals
                        dplyr::select(eventDate, ret, matchRet),
                      basepath = '../Results/Fig_PublicationsVsDataMining',
                      suffix = paste0(jj, '_DM_Correlation', cc),
                      colors = colors,
                      yl = -90, yh = 170
                      
    ) 
    
  }
  
}

