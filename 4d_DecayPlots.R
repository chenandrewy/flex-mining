
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

# Different filters for candidate returns
candidateReturns0 = candidateReturns

# Speeding up filtering below
candidateReturns0 = data.table(candidateReturns)
setkey(candidateReturns0, candSignalname)


# Set filters

## 1. At least 25% non-missing in CS in 1963
comp0 = readRDS('../Data/Raw/CompustatAnnual.RData')

fobs_list = comp0 %>% 
  filter(year(datadate)==1963) %>%
  arrange(gvkey, datadate) %>% 
  group_by(gvkey) %>% 
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  summarise(across(everything(), function(x) sum(!is.na(x) & x>0)/length(x)) ) %>% 
  pivot_longer(cols = everything())

fobs_list = fobs_list[1:243,]  # Rest is comp info from CCM
quantile(fobs_list$value, probs = seq(0, 1, .05))

# Keep if at least 25% non-missing
denomVars = fobs_list %>% 
  filter(value >=.25, !(name %in% c('gvkey'))) %>% 
  pull(name)

denomVars = c(denomVars, "me_datadate")

rm(comp0, fobs_list)

## 2. Only simple functions 'diff(v1)/lag(v2)' and 'v1/v2'

simpleFunctions = c('diff(v1)/lag(v2)', 'v1/v2')

## Apply filters
filteredCandidates = signal_list %>% 
  filter(
    v2 %in% denomVars,
    signal_form %in% simpleFunctions
  ) %>% 
  pull(signalid)

candidateReturns = candidateReturns0[.(filteredCandidates)]


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
