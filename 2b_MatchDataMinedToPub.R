# Match mined strategies to published predictors on t-statistics and returns.
#
# How to run: normally run through 2_DataMining.R from flex-mining/.
# Inputs:  chapter-1 published summaries and chapter-2 mined strategies
# Outputs: ../Data/Processed/<dataVersion> MatchPub.RData

rm(list = ls())
tic0 = Sys.time()
# Setup -------------------------------------------------------------------

source('0_Environment.R')
library(doParallel)

DMname = paste0('../Data/Processed/',
                globalSettings$dataVersion, 
                ' LongShort.RData')

t_tol = globalSettings$t_tol
r_tol = globalSettings$r_tol

# Relative tolerances for the canonical matched-uncorr benchmark. Chapter 2
# constructs the candidate pair set; Chapter 3 adds its history/correlation
# screens once and caches the common panel used by Figure 2(c) and Tables 3--4.
t_reltol = globalSettings$matched_uncorr_t_reltol
r_reltol = globalSettings$matched_uncorr_r_reltol

minNumStocks = globalSettings$minNumStocks
ncores = globalSettings$num_cores

# Load data ---------------------------------------------------------------
inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)

czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>% 
  filter(signalname %in% inclSignals) 

# Data mining strategies
bm_rets = readRDS(DMname)$ret
bm_info = readRDS(DMname)$port_list
bm_signal_info = readRDS(DMname)$signal_list
bm_user = readRDS(DMname)$user

bm_rets = bm_rets %>% left_join(
  bm_info %>% select(portid, sweight), by = c('portid')
)  %>%
  transmute(
    sweight
    , dmname = signalid
    , yearm
    , ret
    , nstock_long
    , nstock_short)

setDT(bm_rets)
  

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
                          , min_nstock_long  = min(nstock_long)
                          , min_nstock_short = min(nstock_short)
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

# matchsum key is c(pubname,dmname). Each row is a dm strat that matches a pub
matchsum = czsum %>% transmute(
  pubname = signalname, rbar_op = rbar,tstat_op = tstat, sampstart, sampend
  , sweight = tolower(sweight)
) %>% 
  left_join(
    dm_insamp, by = c('sampstart','sampend','sweight')
    , relationship = 'many-to-many' # required to suppress warning
  ) %>% 
  mutate(
    diff_rbar = abs(rbar*sign(rbar) - rbar_op)
    , diff_tstat = abs(tstat*sign(rbar) - tstat_op)
  ) %>% 
  setDT()


# Make matched panel ------------------------------------------------------


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
                                & diff_rbar / abs(rbar_op) <= r_reltol
                                & diff_tstat / abs(tstat_op) <= t_reltol
                                & min_nstock_long  >= minNumStocks/2
                                & min_nstock_short >= minNumStocks/2
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

DMshortname = DMname %>% 
  str_remove('../Data/Processed/') %>% 
  str_remove(' LongShort.RData')

matchdat = list(
  candidateReturns = candidateReturns
  , user = bm_user
)

saveRDS(matchdat,
        file = paste0('../Data/Processed/', DMshortname, ' MatchPub.RData')
)


toc0 = Sys.time()

toc0 - tic0
