
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


## Animations ====


ReturnPlotsNoDM(dt = czret %>% 
                  mutate(
                    ret = NA_real_
                  ) %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory1),
                basepath = '../Results/Anim-Pub-1',
                suffix = 'AllSignals',
                filetype = '.png'
)


ReturnPlotsNoDM(dt = czret %>% 
                  mutate(
                    ret = if_else(theory1 == 'risk', NA_real_, ret)
                  ) %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory1),
                basepath = '../Results/Anim-Pub-2',
                suffix = 'AllSignals',
                filetype = '.png'
)


ReturnPlotsNoDM(dt = czret %>% 
                  transmute(eventDate,
                            signalname,
                            ret,
                            catID = theory1),
                basepath = '../Results/Anim-Pub-3',
                suffix = 'AllSignals',
                filetype = '.png'
)

## post 2004 pubs only ====

temp = czret %>% 
  transmute(eventDate,
            signalname,
            ret,
            catID = theory1) %>% 
  inner_join(
    czsum %>% filter(sampend > 2000)
  )

# All Signals
ReturnPlotsNoDM(dt = temp,
                basepath = '../Results/Fig_PublicationsOverTime',
                suffix = 'SameEndAfter2000'
)


# Section 3: Data-mining comparisons --------------------------------------



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
                      dplyr::select(eventDate, ret, matchRet),
                    basepath = '../Results/Fig_PublicationsVsDataMining',
                    suffix = paste0(jj, '_DM'),
                    colors = colors,
                    yl = -60, yh = 170, fig.width = 18, fontsize = 28) 
  
}
