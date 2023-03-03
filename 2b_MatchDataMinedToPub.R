# Alternative script for matching
# this one is much faster 
# and executes the scripts that make exhibits at the end

# takes about 2 min using 4 cores (which is fastest)

rm(list = ls())
tic0 = Sys.time()
# Setup -------------------------------------------------------------------

source('0_Environment.R')
library(doParallel)


DMStrategies = 'DM'  # DM Or YZ

DMname = '../Data/Processed/stratdat CZ-style-v4.RData'

# tolerance in levels  
#   use Inf to turn off
#   seems like reltol might be cleaner
t_tol = .1*Inf
r_tol = .3*Inf

# tolerance relative to op stat
t_reltol = 0.1
r_reltol = 0.3

minNumStocks = 20  # Minimum number of stocks in any month over the in-sample period to include a strategy
ncores = round(0.25*detectCores())

# Load data ---------------------------------------------------------------

signalcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  transmute(signalname, 
            theory1 = theory,  
            Keep)

signaldoc =  data.table::fread('../Data/Raw/SignalDoc.csv') %>% 
  as_tibble() %>% 
  rename(signalname = Acronym) %>% 
  mutate(
    pubdate = as.yearmon(paste0(Year, '-12'))
    , sampend = as.yearmon(paste0(SampleEndYear, '-12')) 
    , sampstart = as.yearmon(paste0(SampleStartYear, '-07'))  # ensures no weird early 1963 edge effects
  ) %>% 
  transmute(signalname, Authors, Year, pubdate, sampend, sampstart
            , OP_pred = `Predictability in OP`
            , sweight = tolower(`Stock Weight`)
            , Rep_Quality = `Signal Rep Quality`) %>% 
  left_join(signalcat) %>% 
  filter(
    OP_pred %in% c('1_clear','2_likely')
    , !is.na(theory1)
  ) 

# czret (monthly returns)
czret = data.table::fread("../Data/Raw/PredictorPortsFull.csv") %>% 
  as_tibble() %>% 
  filter(!is.na(ret), port == 'LS') %>%                                                           
  left_join(signaldoc) %>% 
  mutate(date = as.yearmon(date)) %>% 
  mutate(
    samptype = case_when(
      (date >= sampstart) & (date <= sampend) ~ 'insamp'
      , (date > sampend) & (date <= pubdate) ~ 'oos'  
      , (date > pubdate) ~ 'postpub'
      , TRUE ~ NA_character_
    )
  ) %>% 
  select(signalname, date, ret, samptype, sampstart, sampend, theory1, Rep_Quality, Keep) %>% 
  filter(!is.na(samptype)) %>% 
  # Add event time
  mutate(eventDate = interval(sampend, date) %/% months(1))


# Data mining strategies
bm_rets = readRDS(DMname)$ret
bm_info = readRDS(DMname)$port_list
bm_signal_info = readRDS(DMname)$signal_list

bm_rets = bm_rets %>% left_join(
  bm_info %>% select(portid, sweight), by = c('portid')
)  %>%
  transmute(
    sweight
    , dmname = signalid
    , yearm
    , ret
    , nstock)

setDT(bm_rets)
  



# Compute summary stats and normalized returns for predictors ------------------

# Summary stats of predictors that we try to match
czsum = czret %>%
  filter(samptype == 'insamp') %>% 
  group_by(signalname) %>%
  summarize(
    rbar = mean(ret)
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



# Find sum stats for dm in-sample -------------------------------------------

samplist = czsum %>% distinct(sampstart, sampend) %>% 
  arrange(sampstart, sampend)

tic = Sys.time()
dm_insamp = list()

cl <- makePSOCKcluster(ncores)
registerDoParallel(cl)
dm_insamp = foreach(sampi = 1:dim(samplist)[1], 
                    .combine = rbind,
                    .packages = c('data.table','tidyverse','zoo')) %dopar% {
                      
                      sampcur = samplist[sampi, ]
                      
                      # feedback
                      print(paste0(sampi , ' of ', dim(samplist)[1]))
                      
                      # find sum stats for the current sample
                      sumcur = bm_rets[
                        yearm >= sampcur$sampstart
                        & yearm <= sampcur$sampend
                        & !is.na(ret)
                        , .(
                          rbar = mean(ret), tstat = mean(ret)/sd(ret)*sqrt(.N)
                          , min_nstock = min(nstock)
                        )
                        , by = c('sweight','dmname')
                      ] 
                      
                      # find other stats for filtering
                      filtcur = bm_rets[
                        floor(yearm) == year(sampcur$sampend)
                        & !is.na(ret)
                        , .(nlastyear = .N)
                        , by = c('sweight','dmname')
                      ]
                      
                      # combine and save
                      sumcur %>% 
                        left_join(filtcur, by = c('sweight','dmname')) %>% 
                        mutate(
                          sampstart = sampcur$sampstart, sampend = sampcur$sampend
                        )
                    }
stopCluster(cl)


toc = Sys.time()
toc - tic


# Merge with czsum --------------------------------------------------------

matchsum = czsum %>% transmute(
  pubname = signalname, rbar_op = rbar,tstat_op = tstat, sampstart, sampend
  , sweight = tolower(sweight)
) %>% 
  left_join(
    dm_insamp, by = c('sampstart','sampend','sweight')) %>% 
  mutate(
    diff_rbar = abs(rbar*sign(rbar) - rbar_op)
    , diff_tstat = abs(tstat*sign(rbar) - tstat_op)
  ) %>% 
  setDT()



# Make matched panel ------------------------------------------------------


setDT(czret)

tic = Sys.time()
cl <- makePSOCKcluster(ncores)
registerDoParallel(cl)
candidateReturns =  foreach(pubi = 1:dim(czsum)[1], 
                            .combine = rbind,
                            .packages = c('data.table','tidyverse','zoo')) %dopar% {
                              
                              # feedback
                              print(paste0('pubi ', pubi , ' of ', dim(czsum)[1]))
                              
                              pubcur = czsum[pubi, ]
                              
                              matchcur = matchsum[
                                pubname == pubcur$signalname
                                & diff_rbar <= r_tol
                                & diff_tstat <= t_tol
                                & diff_rbar / rbar_op <= r_reltol
                                & diff_tstat / tstat_op <= t_reltol    
                                & min_nstock >= minNumStocks
                                & nlastyear == 12
                              ] %>%
                                transmute(sweight, dmname, sign = sign(rbar))
                              
                              pancur = bm_rets %>% 
                                inner_join(matchcur, by = c('sweight','dmname')) %>% 
                                transmute(candSignalname = dmname,
                                          eventDate = as.integer(round(12*(yearm-pubcur$sampend))),
                                          sign,
                                          # Sign returns
                                          ret = ret*sign,
                                          samptype = case_when(
                                            (yearm >= pubcur$sampstart) & (yearm <= pubcur$sampend) ~ 'insamp'
                                            , (yearm > pubcur$sampend) ~ 'oos'
                                            , TRUE ~ NA_character_
                                          )) %>% 
                                mutate(
                                  actSignal = pubcur$signalname
                                )
                              
                            }
stopCluster(cl)
toc = Sys.time()
toc - tic




# Save --------------------------------------------------------------------

saveRDS(candidateReturns,
        file = paste0('../Data/Processed/LastMatchedData.RDS')
)


toc0 = Sys.time()

toc0 - tic0
