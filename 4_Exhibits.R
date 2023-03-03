# Setup -------------------------------------------------------------------


rm(list = ls())

source('0_Environment.R')


DMname = '../Data/Processed/stratdat CZ-style-v4.RData'


# CZ data
tmp = readRDS('../Data/Processed/czdata.RDS')
czsum = tmp$czsum
czret = tmp$czret
rm(tmp)

# Matched returns
candidateReturns = readRDS('../Data/Processed/LastMatchedData.RDS')

# Restrict to predictors in consideration
czsum = czsum %>% 
  filter(Keep == 1)

czret = czret %>% 
  filter(signalname %in% czsum$signalname)

candidateReturns = candidateReturns %>% 
  filter(actSignal %in% czsum$signalname)

signal_list = readRDS(DMname)$signal_list


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


