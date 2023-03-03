# Setup -------------------------------------------------------------------


rm(list = ls())

source('0_Environment.R')

DMname = '../Data/Processed/CZ-style-v4 LongShort.RData' # for autofill convenience

name = DMname %>% 
  str_remove('../Data/Processed/') %>% 
  str_remove(' LongShort.RData')

matchname = paste0('../Data/Processed/', name, ' MatchPub.RData')


# Import and Clean Matched Data ------------------------------------------------------

# import
tmp = readRDS(matchname)
czsum = tmp$czsum
czret = tmp$czret
candidateReturns = tmp$candidateReturns
user = tmp$user
rm(tmp)

# Restrict to predictors in consideration
czsum = czsum %>% 
  filter(Keep == 1)

czret = czret %>% 
  filter(signalname %in% czsum$signalname)

candidateReturns = candidateReturns %>% 
  filter(actSignal %in% czsum$signalname)

signal_list = readRDS(DMname)$signal_list

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


source('5a_DataMiningSummary.R', echo = T)  # this can take several minutes
source('5b_MatchingSummary.R', echo = T)
source('5c_ResearchVsDMPlots.R', echo = T)
source('5d_InspectTables.R', echo = T)
