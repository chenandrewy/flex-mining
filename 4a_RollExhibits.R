# Setup -------------------------------------------------------------------

rm(list =ls())
source('0_Environment.R')
source('0_functions.R')


# Portfolio Function ------------------------------------------------------


MakeRollPorts = function(rollstatok, rollshrink){
  # uses rets implicilty
  
  ## Clean for formatting and user selection ----
  setDT(famdat)
  
  # reformat dates for clarity
  famdat = famdat[!is.na(ret)
  ][
    , yearm := as.yearmon(date)
  ][
    order(signalname, yearm)
  ] %>% 
    select(signalname, yearm, ret)
  
  setkey(famdat, signalname, yearm)
  
  # use only signals in 1980 if desired
  if (fixed_signals){
    signalselect2 = famdat %>% 
      filter(!is.na(ret), year(yearm) == 1980) %>% 
      distinct(signalname) %>% 
      pull()
    
    famdat = famdat[signalname %in% signalselect2, ]
  }
  
  ## Join w/ rets ----
  
  # info @ tradedate is used to make rets in yearm and later
  tempstat = rollstatok %>% 
    mutate(yearm = tradedate + 1/12)
  
  # allow tempstat to be used for one year
  tempstat2 = tempstat
  for (i in 1:(past_stat_lag_max-1)){
    tempstat2 = rbind(
      tempstat2
      , tempstat %>% mutate(yearm = yearm + i/12)
    )
  }
  
  # remove stale (duplicate) data
  #   much faster in data.table
  setDT(tempstat2)
  tempstat2 = tempstat2[
    , staleness := yearm - tradedate
  ][
    order(signalname, yearm, staleness)
  ]
  tempstat3 = unique(tempstat2, by = c('signalname','yearm'))
  
  # merge stats onto rets
  #   data.table is much faster here
  famdat[
    tempstat3
    , on = c('signalname','yearm')
    , ':=' (
      tradedate = tradedate
      , past_tstat = tstat
      , past_rbar  = rbar
      , past_ndate = ndate
    )
  ]
  
  # drop fam strat if no ok data 
  famdat = famdat %>% filter(!is.na(tradedate))
  
  ## Add shrinkage ----
  famdat = famdat %>% 
    select(-contains('shrink')) %>% 
    left_join(
      rollshrink, by = 'tradedate'
    ) %>% 
    mutate(
      past_rbar_shrink = past_rbar*(1-shrink)
    )
  
  ## Combine into Portfolios -----
  famdat = famdat %>% 
    mutate(
      sign_switch = sign(past_rbar)^use_sign
      , past_perf = past_rbar_shrink*sign_switch    
    ) %>% 
    group_by(yearm) %>% 
    mutate(
      port = ntile(past_perf, nport)
    ) %>% 
    setDT()
      
  # DT is much faster
  rollport = famdat[
    , by = .(port,yearm)
    , .(
      nstratok = sum(!is.na(ret))
      , ret = mean(ret*sign_switch)
      , past_perf = mean(past_perf)
      , past_rbar_shrink = mean(past_rbar_shrink*sign_switch)
    )
  ]
  

  
  ## output ----
  return = rollport
  
} # end function MakeRollPorts ---



# Prep Data -------------------------------------------------------------------------


## user spec ----
specname = 'stratdat_ratio_ls_extremes10ew_ScaleVars'
# specname = 'stratdat_ratio_ls_extremes5ew_ScaleVars'
# specname = 'yz_vw'

# data avail settings
nmonth_min = 120 # with 240, real-time returns only start in 1994 or so
fixed_signals = F # T uses only signals in 1980 (this addsa lot of time)
past_stat_lag_max = 12 # max number of months to use past stats for 
min_nsignal_each_tradedate = 500 # drop tradedates if less than these many signals


# direct portfolio settings
nport = 40
use_sign = 1

## read in stuff ----
# read in data on the strategy "family"
famdat = readRDS(paste0(
  '../Data/LongShortPortfolios/', specname, '.RData'
))$ret
setDT(famdat)


# read in data on rolling stats and filter
rollstat = readRDS(paste0(
  '../Data/RollingStats/', specname, '.RData'
))$past_stat 

# remove signalname-tradedate if not enough months of rets
rollstatok = rollstat %>% filter(ndate >= nmonth_min) 

# then remove tradedates if not enough signalnames
rollstatok = rollstatok %>% 
  left_join(
    rollstatok %>% group_by(tradedate) %>% summarize(nsignal = n())
    , by = 'tradedate'
  ) %>% 
  filter(
    nsignal >= min_nsignal_each_tradedate
  )

## find shrinkage ----
rollshrink = rollstatok %>% 
  group_by(tradedate) %>% 
  filter(ndate >= nmonth_min) %>% 
  summarize(
    shrink = min((mean(tstat^2))^(-1), 1.0)
    , muhat = mean(tstat)
    , nstratok = n()
    , mean_ndate = mean(ndate)
  ) 
setDT(rollshrink)

## finally make roll ports ----
# make portfolios from the rolling stats
#   implicitly uses famdat
rollport = MakeRollPorts(rollstatok, rollshrink)

# Plots --------------------------------------------------------------------




## time-series overview  ----

rollshrink %>% 
  select(-muhat) %>% 
  pivot_longer(-c('tradedate')) %>% 
  ggplot(aes(x=tradedate)) + 
  geom_point(aes(y=value)) +
  facet_wrap(~name, scale = 'free_y', nrow = 3)



## Vol over time ----

plotme = rollport %>% 
  group_by(port) %>% 
  arrange(port, yearm) %>% 
  mutate(
    vol = rollmean(ret, k=12*5, fill = NA, align = 'right')
    , port = factor(port)
  ) %>%
  filter(!is.na(vol))

ggplot(plotme, aes(x=yearm, group  = port)) +
  geom_line(aes(y=vol, color = port), size = 1) +
  scale_color_hue()


## X-sec of ports by subsamp ----

plotfun = function(yearmin, yearmax){
  
  yearmin = max(min(rollport$yearm), yearmin)
  yearmax = min(max(rollport$yearm), yearmax)
  
  samp = rollport %>% 
    filter(yearm >= yearmin, yearm <= yearmax) %>% 
    group_by(port) %>% 
    arrange(port,yearm) %>% 
    rename(rbar_shrink = past_rbar_shrink)
  
  sampshrink = samp %>% summarize(rbar_shrink = mean(rbar_shrink)) %>% 
    mutate(shrink_time = 'mean') %>% 
    rbind(
      samp %>% filter(row_number() == 1) %>% transmute(port, rbar_shrink, shrink_time = 'begin')
    ) %>% 
    rbind(
      samp %>% filter(row_number() == n()) %>% transmute(port, rbar_shrink, shrink_time = 'end')
    ) %>% 
    mutate(
      shrink_time = factor(shrink_time, levels = c('begin','mean','end'))
    )
  
  samprealtime = samp %>% 
    group_by(port) %>% 
    summarize(
      rbar = mean(ret)
      , vol = sd(ret), ndate = n(), tstat = rbar/vol*sqrt(ndate)
      , se = vol/sqrt(ndate)
    )
  
  p = samprealtime %>% 
    ggplot(aes(x=port)) +
    # real time returns
    geom_point(
      aes(y=rbar), color = 'black', size = 4
    ) +
    geom_errorbar(
      aes(ymin = rbar - 2*se, ymax = rbar + 2*se)
    ) +
    # shrinkage stuff
    geom_line(
      data = sampshrink
      , aes(y=rbar_shrink, group = shrink_time, color = shrink_time)
      , size = 1.5
    )  +
    geom_abline(
      slope = 0
    ) +
    ggtitle(paste0(yearmin, '-', yearmax))
  
  return = p
} # end plotfun

p1 = plotfun(1900,2030)
p2 = plotfun(1900,2005)
p3 = plotfun(2005,2030)


grid.arrange(p1, p2, p3, nrow = 2)

