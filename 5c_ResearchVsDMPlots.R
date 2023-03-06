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
for (jj in unique(allRets$theory1)) {
  print(jj)
  
  ReturnPlotsWithDM(dt = allRets %>% 
                      filter(!is.na(matchRet), theory1 == jj, Keep == 1) %>% # To exclude unmatched signals
                      dplyr::select(eventDate, ret, matchRet),
                    basepath = '../Results/Fig_PublicationsVsDataMining',
                    suffix = paste0(jj, '_DM'),
                    colors = colors,
                    yl = -60, yh = 170, fig.width = 18, fontsize = 28) 
  
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
for (jj in unique(allRets$theory1)) {
  print(jj)
  
  ReturnPlotsWithDM(dt = allRets %>% 
                      filter(!is.na(matchRet), theory1 == jj, Keep == 1) %>% # To exclude unmatched signals
                      dplyr::select(eventDate, ret, matchRet),
                    basepath = '../Results/Extra/Fig_PublicationsVsDataMining',
                    suffix = paste0(jj, '_DM_hide'),
                    colors = colors,
                    yl = -60, yh = 170, fig.width = 18, fontsize = 28
                    , hideoos = TRUE
                    , filetype = '.png') 
  
}

# Plot re-scaled returns over time by category
for (jj in unique(allRets$theory1)) {
  print(jj)
  
  ReturnPlotsWithDM(dt = allRets %>% 
                      filter(!is.na(matchRet), theory1 == jj, Keep == 1) %>% # To exclude unmatched signals
                      dplyr::select(eventDate, ret, matchRet),
                    basepath = '../Results/Extra/Fig_PublicationsVsDataMining',
                    suffix = paste0(jj, '_DM'),
                    colors = colors,
                    yl = -60, yh = 170, fig.width = 18, fontsize = 28
                    , hideoos = FALSE
                    , filetype = '.png') 
  
}
