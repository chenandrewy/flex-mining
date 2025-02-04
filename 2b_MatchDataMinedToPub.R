# Alternative script for matching (for robustness figures that match on t-stats and mean returns)
# Output is used in 4e_ResearchVsDMRobustnessCorrelationsEtc.R

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

# tolerance relative to op stat
t_reltol = 0.1
r_reltol = 0.3

minNumStocks = globalSettings$minNumStocks
ncores = globalSettings$num_cores

# Load data ---------------------------------------------------------------
inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)

czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>% 
  filter(signalname %in% inclSignals) 

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, Year, theory) %>% 
  filter(signalname %in% inclSignals) 

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>% 
  left_join(czcat, by = 'signalname') %>% 
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


# Compute pairwise correlations between actual signals and matched DM signals -----

# filter for Keep only
candidateReturns = candidateReturns %>% 
  filter(actSignal %in% (czsum %>% filter(Keep) %>% pull(signalname)))


tmpCands = candidateReturns %>% 
  filter(actSignal %in% czsum$signalname) %>% 
  filter(samptype == 'insamp') %>%  # interested in in-sample correlation with actual signals
  # Merge actual returns to candidate returns
  select(candSignalname, eventDate, ret, actSignal) %>% 
  inner_join(czret %>% 
               transmute(actSignal = signalname,
                         eventDate,
                         retActual = ret))

allRhos = tibble()

for (act in unique(tmpCands$actSignal)) {
  print(act)
  tmp = tmpCands %>% 
    filter(actSignal == act)
  
  # compute correlations for one actual signal
  tmpRhos = tibble()
  for (i in unique(tmp$candSignalname)) {
    
    tmpRhos = bind_rows(
      tmpRhos,
      tibble(actSignal = act,
             candidateSignal = i,
             rho = cor(tmp$ret[tmp$candSignalname == i], tmp$retActual[tmp$candSignalname == i]))
    )
    
  }
  
  # add to list
  allRhos = bind_rows(
    allRhos,
    tmpRhos
  )
  
}

saveRDS(allRhos, '../Results/PairwiseCorrelationsActualAndMatches.RDS')

