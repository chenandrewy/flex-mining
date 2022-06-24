# Created 2022 06 from demo-yz-oos.R
# Rolling window strategy selection and testing


# Setup -------------------------------------------------------------------

# rm(list =ls())
source('0_Environment.R')
source('0_functions.R')
library(zoo) # for yearmonth

# Dataset Selection 
#   searches for patterns
#   use * to use everything
dataset_list = c('yz_ew', 'yz_vw')
# dataset_list = 
nstrat = 1000 # number of strategies to sample (fix me) 


# Rolling Window Settings
nmonth_past = 240 # number of past months to use
nmonth_min = 240 # with 240, real-time returns only start in 1994 or so
fixed_signals = T # T uses only signals in 1980
trade_months = c(6) # which months we evaluate past stats and trade
past_stat_lag_max = 12 # max number of months to use past stats for 
min_nsignal_each_tradedate = 20 # drop tradedates if less than these many signals


# Rolling Window Function ----------------------------------------------- 

RollingWindow = function(filename){
  
  ## Load stuff ----
  rets = readRDS(filename)$ret
  
  # data.table is much faster here
  setDT(rets)  
  
  # use only signals in 1980 if desired
  if (fixed_signals){
    signalselect2 = rets %>% 
      filter(!is.na(ret), year(date) == 1980) %>% 
      distinct(signalname) %>% 
      pull()
    
    rets = rets[signalname %in% signalselect2, ]
  }
  
  # sample if impatient
  signallistfull = rets$signalname %>% unique
  nstrat = min(nstrat, length(signallistfull))
  signalselect = sample(signallistfull, nstrat, replace = F)  
  rets = rets[signalname %in% signalselect]
  
  # reformat dates for clarity
  rets = rets[
    !is.na(ret)
  ][
    , yearm := as.yearmon(date)
  ][
    order(signalname, yearm)
  ] %>% 
    select(signalname, yearm, ret)
  
  
  ## Loop over "tradedates"  ----
  # please make me faster
  
  # for ease of renaming
  temprets = rets
  
  # maybe want to do each month eventually
  datelist = temprets %>% 
    distinct(yearm) %>% 
    filter(month(yearm) %in% trade_months) %>% 
    arrange(yearm) %>% 
    pull
  
  
  past_stat = tibble()
  for (i in 1:length(datelist)){
    
    currdate = datelist[i]
    
    print(currdate)
    
    samp = temprets %>% filter(yearm <= currdate, yearm >= currdate - nmonth_past/12 )
    
    # debug   sampsum %>% filter(signalname == 'ratio_ls_extremes_aco_act')
    
    sampsum = samp %>% 
      group_by(signalname) %>% 
      filter(!is.na(ret)) %>% 
      summarize(
        rbar = mean(ret), vol = sd(ret), ndate = sum(!is.na(ret)), tstat = rbar/vol*sqrt(ndate)
      ) 
    
    sampsum$tradedate = currdate # yearm when this info can be used to trade
    
    past_stat = rbind(past_stat, sampsum)
    
  } # for i
  
  ## find shrinkage ----
  
  past_shrink = past_stat %>% 
    group_by(tradedate) %>% 
    filter(ndate >= nmonth_min) %>% 
    summarize(
      shrink = (mean(tstat^2))^(-1)
      , muhat = mean(tstat)
      , nstratok = n()
      , mean_ndate = mean(ndate)
    ) 
  
  
  # Real-time returns -------------------------------------------------------
  
  ## Clean for data availability ----
  tempstat = past_stat
  
  # remove signalname-tradedate if not enough months of rets
  tempstat = tempstat %>% 
    filter(ndate >= nmonth_min)
  
  # then remove tradedates if not enough signalnames
  tempsum = tempstat %>% 
    group_by(tradedate) %>% 
    summarize(nsignal = n())
  
  tempstat = tempstat %>% 
    left_join(tempsum, by = 'tradedate') %>% 
    filter(
      nsignal >= min_nsignal_each_tradedate
    )
  
  past_stat2 = tempstat
  
  
  
  ## Expand stats and join w/ rets ----
  
  temprets = rets
  tempstat = past_stat2
  
  # set everything as data tables for speed
  temprets = setDT(temprets)
  setkey(temprets, signalname, yearm)
  setDT(tempstat)[ , yearm := tradedate + 1/12] # info @ tradedate is used to make rets in yearm and later
  setkey(tempstat, signalname, yearm)
  
  
  # allow tempstat to be used for one year
  tempstat2 = tempstat
  for (i in 1:(past_stat_lag_max-1)){
    tempstat2 = rbind(
      tempstat2
      , tempstat %>% mutate(yearm = yearm + i/12)
    )
  }
  setkey(tempstat2, signalname, yearm)
  
  # keep unique, most recent tradedate
  tempstat2 = tempstat2[ , statlag := yearm - tradedate][
    order(signalname, yearm, statlag)
  ]
  tempstat3 = unique(tempstat2, by = c('signalname','yearm'))
  
  # check
  tempstat3 %>% filter(statlag > 1, yearm <= 2020) %>% as.data.frame()
  
  # merge stats onto rets
  #   data.table is much faster here
  temprets[
    tempstat3, on = c('signalname','yearm')
    , ':=' (
      tradedate = tradedate
      , past_tstat = tstat
      , past_rbar  = rbar
      , past_ndate = ndate
    )
  ]
  
  rets2 = temprets %>% 
    filter(!is.na(tradedate))
  
  
  ## Add shrinkage ----
  
  temprets = rets2
  
  # find shrinkage each month
  tempshrink = temprets %>% 
    group_by(yearm) %>% 
    summarize(
      shrink = mean(past_tstat^2)^(-1)
    )
  
  # merge and shrink past rbar
  temprets = temprets %>% 
    left_join(tempshrink, by = 'yearm') %>% 
    mutate(
      past_rbar_shrink = past_rbar*(1-shrink)
    )
  
  rets3 = temprets
  
  return = rets3
  
} # End Rolling Window ------------------------------------------ #



# Finalize Dataset List ---------------------------------------------------

datanames_full = character()
for (name_curr in dataset_list){
  datanames_full = rbind(
    datanames_full
    , dir('../Data/LongShortPortfolios/', pattern = name_curr, full.names = T)
  )
}



# test --------------------------------------------------------------------

rets3 = RollingWindow(datanames_full[1])



# Plot time-series overview  -----------------------------------------------------------
# at this point all returns are valid
# improve meeee

nstrat_ts = rets3 %>% 
  distinct(signalname, tradedate, .keep_all = T) %>% 
  group_by(tradedate) %>% 
  summarize(
    nstrat_valid = n(), shrink = mean(shrink), past_ndate = mean(past_ndate)
  ) 

ggplot(nstrat_ts, aes(x=tradedate)) + geom_point(aes(y=nstrat_valid)) +
  geom_line(aes(y=shrink*4000)) +
  geom_line(aes(y=past_ndate*10))




# Past Stat Sorts  --------------------------------------------------------

# sort strats into portfolios based on past stats
# this is easier to conceptualize but hard to map to YZ and multiple testing

nport = 20

use_sign = 1

port_past_perf = rets3 %>% 
  group_by(yearm) %>% 
  mutate(
    sign_switch = sign(past_rbar)^use_sign
    , past_perf = past_rbar_shrink*sign_switch
    , port = ntile(past_perf, nport)
  ) %>% 
  filter(!is.na(port)) %>% 
  group_by(port,yearm) %>% 
  summarize(
    nstratok = sum(!is.na(ret))
    , ret = mean(ret*sign_switch)
    , past_perf = mean(past_perf)
    , past_rbar_shrink = mean(past_rbar_shrink*sign_switch)
  )

## plot ----

plotfun = function(yearmin, yearmax, title){
  p = port_past_perf %>% 
    filter(yearm >= yearmin, yearm <= yearmax) %>% 
    group_by(port) %>% 
    summarize(
      rbar = mean(ret)
      , past_rbar_shrink = mean(past_rbar_shrink), sd_shrink = sd(past_rbar_shrink)
      , vol = sd(ret), ndate = n(), tstat = rbar/vol*sqrt(ndate)
      , se = vol/sqrt(ndate)
    ) %>% 
    ggplot(aes(x=port)) +
    geom_point(
      aes(y=rbar), color = 'black', size = 4
    ) +
    geom_errorbar(
      aes(ymin = rbar - 2*se, ymax = rbar + 2*se)
    ) +
    geom_point(
      aes(y=past_rbar_shrink), color = 'blue', size = 3
    ) +
    geom_errorbar(
      aes(ymin = past_rbar_shrink - 2*sd_shrink, ymax = past_rbar_shrink + 2*sd_shrink)
      , color = 'blue', size = 3
    ) +    
    geom_abline(
      slope = 0
    ) +
    ggtitle(title)
  
  return = p
} # end plotfun

p1 = plotfun(1980,3000,'1980-2020')
p2 = plotfun(1980,2005, '1980-2005')
p3 = plotfun(1990,2000, '1990-2000')
p4 = plotfun(1990,2000, '1990-2005')
p5 = plotfun(1990,2020, '1990-2020')

p6 = plotfun(1980,1990, '1980-1990')


library(gridExtra)
grid.arrange(p1, p2, p3, p4, p5, p6, nrow = 3)

