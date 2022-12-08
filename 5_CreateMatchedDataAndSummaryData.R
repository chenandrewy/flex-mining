# Create matched dataset and predictor summary dataset
# library(tidyverse)
# library(lubridate)

source('0_Environment.R')

DMStrategies = 'DM'  # Or YZ
t_tolerance = .3
r_tolerance = .3


# Load data ---------------------------------------------------------------

signalcat = readxl::read_xlsx('../Data/CZ/SignalsTheoryChecked.xlsx') %>% 
  transmute(signalname, 
            theory1 = theory,  # Based on Alejandros updated classification
            Keep)

signaldoc = data.table::fread('../Data/CZ/SignalDoc.csv') %>% 
  as_tibble() %>% 
  rename(signalname = Acronym) %>% 
  mutate(
    pubdate = as.Date(paste0(Year, '-12-31'))
    , sampend = as.Date(paste0(SampleEndYear, '-12-31'))
    , sampstart = as.Date(paste0(SampleStartYear, '-01-31'))  # Was -01-01 but slightly inaccurate
  ) %>% 
  transmute(signalname, Authors, Year, pubdate, sampend, sampstart
            , OP_pred = `Predictability in OP`, sweight = `Stock Weight`, Rep_Quality = `Signal Rep Quality`) %>% 
  left_join(signalcat) %>% 
  filter(
    OP_pred %in% c('1_clear','2_likely')
    , !is.na(theory1)
  )

# czret (monthly returns)
czret = data.table::fread("../Data/CZ/PredictorPortsFull.csv") %>% 
  as_tibble() %>% 
  filter(!is.na(ret), port == 'LS') %>%                                                           
  left_join(signaldoc) %>% 
  mutate(date = ceiling_date(date, unit = 'months') %m-% days(1)) %>% # Define dates consistently as last day of month!!
  mutate(
    samptype = case_when(
      (date >= sampstart) & (date <= sampend) ~ 'insamp'
      , (date > sampend) & (date <= pubdate) ~ 'oos'  #Q: AC had this as sampend + 36 months, why?
      , (date > pubdate) ~ 'postpub'
      , TRUE ~ NA_character_
    )
  ) %>% 
  select(signalname, date, ret, samptype, sampstart, sampend, theory1, Rep_Quality, Keep) %>% 
  filter(!is.na(samptype)) %>% 
  # Add event time
  mutate(eventDate = interval(sampend, date) %/% months(1))



# Data mining strategies
if (DMStrategies == 'DM') {
  
  
  stratData = c(list.files('../Data/LongShortPortfolios/',
                           full.names = TRUE,
                           pattern = c('5ew_')),
                list.files('../Data/LongShortPortfolios/',
                           full.names = TRUE,
                           pattern = c('5vw_'))
  )
  
  
  # Load in Loop
  bm_rets = tibble()
  
  for (ff in 1:length(stratData)) {
    
    # Load data
    print(stratData[ff])
    
    tmp = readRDS(stratData[ff]) 
    bm_rets = rbind(bm_rets,
                    tmp$ret %>% 
                      filter(complete.cases(.) == TRUE) %>% 
                      mutate(Weighting = tmp$sweight))
    rm(tmp)
  }
  
  bm_retsVW = bm_rets %>% 
    filter(Weighting == 'vw') %>% 
    transmute(
      signalname
      , date = zoo::as.Date(yearm, frac = 1)
      # , y = year(yearm)
      # , m = month(yearm)
      , ret)
  
  
  bm_retsEW = bm_rets %>% 
    filter(Weighting == 'ew') %>% 
    transmute(
      signalname
      , date = zoo::as.Date(yearm, frac = 1)
      # , y = year(yearm)
      # , m = month(yearm)
      , ret)
  
  rm(bm_rets)
  
} else if (DMStrategies == 'YZ') {
  
  
  bm_retsVW = readRDS('../Data/LongShortPortfolios/yz_ew.RData')$ret
  
  bm_retsEW = readRDS('../Data/LongShortPortfolios/yz_ew.RData')$ret
  
} else {
  
  message('DM strategies has to be one of DM or YZ')
}



# Compute summary stats and normalized returns for predictors ------------------

# Summary stats of predictors that we try to match
czsum = czret %>%
  filter(samptype == 'insamp') %>% 
  group_by(signalname) %>%
  summarize(
    rbar = mean(ret)
    #    , vol = sd(ret)
    , tstat = mean(ret)/sd(ret)*sqrt(dplyr::n())) %>% 
  left_join(signaldoc %>% select(signalname, sampstart, sampend, theory1, sweight, Rep_Quality, Keep))


# Compute in-sample means and rescale
tempsumAct = czret %>% 
  filter(samptype == 'insamp') %>%
  group_by(signalname) %>% 
  summarize(rbar_insampAct = mean(ret)) %>% 
  ungroup()

czret = czret %>% 
  left_join(tempsumAct) %>% 
  mutate(retOrig = ret,
         ret = 100*(ret/abs(rbar_insampAct)))


rm(tempsumAct)

# Save return summaries and returns dataset
saveRDS(
  list(czsum = czsum,
       czret = czret),
  file = '../Data/Processed/czdata.RDS'
)


# Find matches for risk signals -------------------------------------------


# signals = czsum %>% 
#   filter(rbar > .15, abs(tstat) > 1) # abs(rbar)?
# 
# # Excluded ones:
# czsum %>% 
#   filter(!(signalname %in% signals$signalname)) %>% 
#   select(signalname)

candidateReturns = tibble()
for (ii in 1:nrow(czsum)) {
  
  risksignal = czsum$signalname[ii]
  print(risksignal)
  
  # Extract some relevant info
  tmpSampleStart = czsum %>% 
    filter(signalname == risksignal) %>% 
    pull(sampstart)
  
  tmpSampleEnd = czsum %>% 
    filter(signalname == risksignal) %>% 
    pull(sampend)
  
  tmpTStat = czsum %>% 
    filter(signalname == risksignal) %>% 
    pull(tstat)
  
  tmpRbar = czsum %>% 
    filter(signalname == risksignal) %>% 
    pull(rbar)
  
  if (czsum$sweight[ii] == 'EW' | is.na(czsum$sweight[ii])) {  # DivYieldST is NA?
    tmpRets = bm_retsEW
  } else {
    tmpRets = bm_retsVW
  }
  
  
  # Find candidate returns
  
  tmp = matchedReturns(bm_rets = tmpRets,
                       actSignalname = risksignal, 
                       actSampleStart = tmpSampleStart, 
                       actSampleEnd = tmpSampleEnd, 
                       actTStat = tmpTStat,
                       actRBar = tmpRbar,
                       tol_t = t_tolerance,
                       tol_r = r_tolerance) 
  
  # Add to data
  candidateReturns = bind_rows(candidateReturns, tmp)
  
}

# Save
saveRDS(candidateReturns,
  file = '../Data/Processed/MatchedData.RDS'
)

