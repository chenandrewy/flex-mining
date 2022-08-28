# Created 2022 06 from demo-yz-oos.R
# Rolling window strategy selection and testing


# Setup -------------------------------------------------------------------

rm(list =ls())
source('0_Environment.R')



# Dataset Selection 
#   searches for patterns
#   use * to use everything

# dataset_list = '*NoScaleVars.RData'
# dataset_list = c('ratio_ls')
dataset_list = 'stratdat_ratio_ls_extremes10ew_NoScaleVars'


nstrat = 20*1000 # number of strategies to sample if impatient

# past stat  settings
nmonth_list = c(60,120,240) # list of number of past months to use
trade_months = c(6) # which months we evaluate past stats and trade



# RollingStats Function ----------------------------------------------- 

RollingStats = function(filename){
  
  ## Load large sample ----
  rets = readRDS(filename)$ret
  
  # data.table is much faster here
  setDT(rets)  

  # sample if impatient
  signallistfull = rets$signalname %>% unique
  nstrat = min(nstrat, length(signallistfull))
  signalselect = sample(signallistfull, nstrat, replace = F)  
  rets = rets[signalname %in% signalselect]
  
  # reformat dates for clarity
  rets = rets[
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
    
    # feedback
    print(currdate)
    
    samp = temprets %>% filter(yearm <= currdate, yearm >= currdate - max(nmonth_list)/12 )
    
    sampsum = tibble()
    for (nmonth_past in nmonth_list){    
      temp = samp %>% 
        filter(yearm >= currdate - nmonth_past/12) %>% 
        group_by(signalname) %>% 
        filter(!is.na(ret)) %>% 
        summarize(
          rbar = mean(ret), vol = sd(ret), nmonth = sum(!is.na(ret)), tstat = rbar/vol*sqrt(nmonth)
        ) %>% 
        mutate(nmonth_past = nmonth_past) 
      
      sampsum = rbind(sampsum, temp)
    } # end for nmonth_past
    
    # clean up
    sampsum = sampsum %>% 
      mutate(tradedate = currdate) %>% 
      select(
        signalname, tradedate, nmonth_past, everything()
      )
    
    past_stat = rbind(past_stat, sampsum)
    
  } # for i
  
  
  ## Write to disk ----
  shortname = filename %>% 
    str_remove('../Data/LongShortPortfolios/') %>% 
    str_remove('.RData')
  
  roll_stat = list(
    past_stat = past_stat
    , setting = NULL
  )  
  
  saveRDS(roll_stat
    , paste0('../Data/RollingStats/', shortname, '.RData'))
  
  return = roll_stat
  
} # End Rolling Window ------------------------------------------ #



# Loop over Large Samples -------------------------------------------------



## finalize dataset list ---------------------------------------------------

datanames_full = character()
for (name_curr in dataset_list){
  datanames_full = c(
    datanames_full
    , dir('../Data/LongShortPortfolios/', pattern = name_curr, full.names = T)
  )
}
datanames_full = unique(datanames_full)

print('datanames_full is ')
print(datanames_full)



## actual loop --------------------------------------------------------------------

for (filename in datanames_full){
  shortname = filename %>% 
    str_remove('../Data/LongShortPortfolios/') %>% 
    str_remove('.RData')
  
  print(paste0('Rolling Window for ', shortname))
  roll_stat = RollingStats(filename)
  
  gc() # get some weird elapsed time limit errors, maybe this will help?
}


