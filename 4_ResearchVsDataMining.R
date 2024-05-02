# Setup -------------------------------------------------------------------

rm(list = ls())

source('0_Environment.R')

DMname = paste0('../Data/Processed/',
                globalSettings$dataVersion, 
                ' LongShort.RData')

name = DMname %>% 
  str_remove('../Data/Processed/') %>% 
  str_remove(' LongShort.RData')

matchname = paste0('../Data/Processed/', name, ' MatchPub.RData')

# Import and Clean Matched Data ------------------------------------------------------

# CZ data
czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, theory)

czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>% 
  filter(Keep) %>% 
  left_join(czcat, by = 'signalname') 

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(
    retOrig = ret
    , ret = ret/rbar*100
  )

# DM data
dmdat = readRDS(DMname)

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

# Run Exhibits ---------------------------------------------

source('4a_DataMiningSummary.R', echo = T)  # this can take several minutes
source('4ab_DMCorrelationsPCASummary.R', echo = T)  # This might take around an hour or so
source('4b_MatchingSummary.R', echo = T)
source('4c_ResearchVsDMPlots.R', echo = T)
source('4d_InspectTables.R', echo = T)
