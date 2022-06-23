# Created 2022 06 from demo-yz-oos.R
# Rolling window strategy selection and testing


# Setup -------------------------------------------------------------------

# rm(list =ls())
source('0_Environment.R')
source('0_functions.R')
library(zoo) # for yearmonth

useData = list(
  dataset = 'all_data'  # all_data
  , signal_form = 'ratio'
  , portnum = 10
  , weights = 'ew'
  , longshort_form = 'ls_extremes'
  , scale_vars = 'ScaleVars' # 'Scalevars' or 'NoScaleVars'
)

# number of strategies to sample 
nstrat = 100*1000

# number of past months to use
nmonth_past = 240


# Prepare data ------------------------------------------------------------

## Load data ----

if (useData$dataset == 'all_data') {
  
  tmp = readRDS(
    paste0('../Data/LongShortPortfolios/stratdat_',
           useData$signal_form, '_', 
           useData$longshort_form,
           useData$portnum, 
           useData$weights, '_',
           useData$scale_vars, '.RData')
  )
  
  rets = tmp$ret %>% 
    transmute(signalname, 
              date,
              ret)
  
  plotString = paste0(useData$dataset, '_',  # For saving plots
                      useData$signal_form, '_', 
                      useData$longshort_form,
                      useData$portnum, 
                      useData$weights)
  
  # yz_data
} else if (useData$dataset == 'yz_data') {
  
  temp = read_sas('../Data Yan-Zheng/Yan_Zheng_RFS_Data.sas7bdat')
  
  # clean, select ew or vw
  temp1 = temp %>%
    mutate(
      signalname = paste(transformation, fsvariable, sep = '.')
    ) %>%
    transmute(
      signalname, date = DATE, ret = 100*ddiff_ew
    )
  
  # sample strats (aka signals)
  stratlist = temp1$signalname %>% unique()
  nstrat = min(nstrat, length(stratlist))
  stratselect = tibble(
    signalname = sample(stratlist, nstrat, replace = F)
    , strati = 1:nstrat
  )
  
  
  rets = temp1 %>%
    inner_join(
      stratselect, by = 'signalname'
    ) %>%
    select(signalname, date, ret)
  
  plotString = paste0(useData$dataset, '_',
                      useData$signal_form, '_', 
                      useData$longshort_form,
                      useData$portnum, 
                      useData$weights)
  
  # clean up
  rm(list = ls(pattern = 'temp'))
  gc()
  
} else {
  
  message(paste('dataset must be one of yz_data or all_data'))
  
}




## Clean ----

# use only signals in 1980 for stability (?)
signalselect = rets %>% 
  filter(!is.na(ret), year(date) == 1980) %>% 
  distinct(signalname) %>% 
  pull()


# maybe we should do this earlier
retsclean = rets %>% 
  filter(!is.na(ret)) %>% 
  filter(signalname %in% signalselect) %>% 
  mutate(yearm = as.yearmon(date)) %>% 
  select(signalname, yearm, ret)


# Rolling window stats -----------------------------------------------

# for ease of renaming
temprets = retsclean
  

## Evaluate strategies  ----

# maybe want to do each month eventually
datelist = temprets %>% 
  distinct(yearm) %>% 
  filter(month(yearm) %in% c(6)) %>% 
  arrange(yearm) %>% 
  pull


past_stat = tibble()
for (i in 1:length(datelist)){
  
  
  currdate = datelist[i]
  
  print(currdate)
  
  samp = temprets %>% filter(yearm <= currdate, yearm >= currdate - nmonth_past/12 )
  
  sampsum = samp %>% 
    group_by(signalname) %>% 
    filter(!is.na(ret)) %>% 
    summarize(
      rbar = mean(ret), vol = sd(ret), ndate = sum(!is.na(ret)), tstat = rbar/vol*sqrt(ndate)
    ) 
  
  sampsum$tradedate = currdate # yearm when this info can be used to trade
  
  past_stat = rbind(past_stat, sampsum)
  
} # for i

# rename for sanity
past_stat = 
  past_stat %>% 
  rename(
    past_rbar = rbar, past_tstat = tstat, past_vol = vol, past_ndate = ndate
  )


# add past stats to rets, careful with timing
# do this early cuz it's slow
temprets2 = temprets %>% 
  left_join(
    past_stat %>% mutate(yearm = tradedate + 1/12) # stats from tradedate used for returns next month
    , by = c('signalname', 'yearm')
  )  %>% 
  arrange(
    signalname, yearm
  )


rets3 = temprets2


## Add shrinkage and fill  ----

temp = past_stat %>% 
  group_by(tradedate) %>% 
  filter(past_ndate >= 120) %>% 
  summarize(
    past_shrink = (mean(past_tstat^2))^(-1)
    , past_muhat = mean(past_tstat)
    , past_nstratok = n()
  ) 

temp %>% ggplot(aes(x=tradedate)) + geom_point(aes(y=past_shrink))
temp %>% ggplot(aes(x=tradedate)) + geom_point(aes(y=past_nstratok))

# this is a little slow, but not that bad
rets4 = rets3 %>% 
  left_join(
    temp, by = c('tradedate')
  ) %>% 
  arrange(signalname,yearm) %>% 
  group_by(signalname) %>% 
  fill(starts_with('past_'), tradedate) %>% 
  filter(
    !is.na(ret*past_rbar*past_shrink)
  ) %>% 
  mutate(
    past_rbar_shrink = (1-past_shrink)*past_rbar
  )


# Clean rolling windows  -----------------------------------------------------------


## eyeball nstrats and shrinkage comp ----

# eyeball number of strategies
#   seems like a lot are added in 1985.  
#   So only data post 1986
nstrat_ts = rets4 %>% 
  filter(!is.na(ret)) %>% 
  group_by(yearm) %>% 
  summarize(nstrat = n(), past_shrink = mean(past_shrink)) 

ggplot(nstrat_ts, aes(x=yearm)) + geom_point(aes(y=nstrat)) +
  geom_line(aes(y=past_shrink*4000))


## debug ----

temp = rets4 %>% 
  group_by(yearm) %>% 
  summarize(past_ndate = mean(past_ndate), nsignal = n())  

ggplot(temp, aes(x=yearm)) + geom_point(aes(y=past_ndate)) +
  geom_line(aes(y=nsignal/10))

# Past Stat Sorts  --------------------------------------------------------

# sort strats into portfolios based on past stats
# this is easier to conceptualize but hard to map to YZ and multiple testing

nport = 20

use_sign = 1

port_past_perf = rets4 %>% 
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

