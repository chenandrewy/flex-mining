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


# # load hand classifications
# signal_text <- fread('DataInput/SignalsTheoryChecked.csv')
# 
# # load classifications and text stats
# subset_text <- fread('DataIntermediate/TextClassification.csv')  %>% 
#   # dplyr::select(Authors, Year, Journal, file_names)%>%
#   mutate(Journal = gsub('^RF$', 'ROF', Journal)) %>%
#   mutate(Journal = gsub('^TAR$', 'AR', Journal)) %>%
#   mutate(Authors = gsub('et al.?|and |,', '', Authors)) %>%
#   mutate(FirstAuthor = word(Authors)) %>%
#   filter(Authors != 'Ang et al') %>%
#   filter(Authors != 'Chen Jegadeesh Lakonishok')


# Risk vs Mispricing Tables -----------------------------------------------

# this does not need the setup
source('4a_TextTables.R', echo = T)
source('4b_RegDecayTable.R', echo = T) # do we use signalkeep == 1 here?

# Research vs Data Mining Tables ---------------------------------------------


source('4c_DataMiningSummary.R', echo = T)
source('4d_DecayPlots.R', echo = T)
source('4e_InspectTables.R', echo = T)