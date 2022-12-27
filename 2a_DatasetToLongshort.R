tic = Sys.time()

# Setup  ----------------------------------------------------------------

# rm(list = ls())

source('0_Environment.R')
library(doParallel)
env <- foreach:::.foreachGlobals # https://stackoverflow.com/questions/64519640/error-in-summary-connectionconnection-invalid-connection
rm(list=ls(name=env), pos=env)

## Parallel settings ----
num_cores <- round(.5*detectCores())  # Adjust number of cores used as you see fit
num_cores = 1 # use num_cores = 1 for serial
threads_fst(1) # since fst is used inside foreach, might want to limit cpus, though this doesn't seem to help

## Output settings ----
user = list()
user$name = 'yz_rep' # name of output file

# signal choices
#   really not sure that xnames and scaling_variables should be defined this way
temp_denom = compnames$yz.denom
# temp_denom = c("at", "act",  "invt", "ppent", "lt", "lct", "dltt",
#                "ceq", "seq", "icapt", "sale", "cogs", "xsga", "emp", 'me')

user$signal = list(
  form = c('ratio', 'ratioChange', 'ratioChangePct',
                  'levelChangePct', 'levelChangeScaled', 'levelsChangePct_Change') # 'noise'
  , xnames = unique(c(compnames$yz.numer,temp_denom))  # must include scaling variables
  , scaling_variables = temp_denom
  , signalnum   = 1000 # number of signals to sample or Inf for all
  , seednumber  = 1235 # seed sampling
)

# portfolio choices
user$port = list(
  longshort_form = 'ls_extremes'
  , portnum        = c(10)
  , sweight        = c('ew','vw') 
  , trim           = NA_real_  # NA or some quantile e.g. .005
  
)

# data lag choices
user$data = list(
  reup_months    = c(6) # stocks are traded using new data at end of these months
  , toostale_months = 12 
  , data_avail_lag = 6 # months
)


# Data Prep ---------------------------------------------------------------
# use as function for easy skipping and debugging

tic = Sys.time()

prepare_data = function(){
  
  # do everything in data.table for efficiency  
  
  # safety delete
  file.remove('../Data/tmpAllDat.fst')
  
  ## import and merge ====
  
  ## import compustat, convert to monthly
  comp0 = readRDS('../Data/Intermediate/CompustatAnnual.RData')
  setDT(comp0)
  comp0[ , c(
    'conm','tic','cusip','cik','sic','naics','linkprim','linktype','liid','lpermco','linkdt','linkenddt'
    , 'gvkey','datadate'
  ) := NULL
  ]
  
  
  # lag signal, then keep around for toostale_months
  #   see here: https://stackoverflow.com/questions/11121385/repeat-rows-of-a-data-frame
  #   signalyearm are yearmons when investor has access to signal & signal is not stale
  comp1 = comp0 %>% slice(rep(1:n(), each = user$data$toostale_months)) 
  comp1$temp_lag_months = rep(user$data$data_avail_lag + 0:(user$data$toostale_months-1), dim(comp0)[1])
  comp1[ 
    , signalyearm := datayearm + temp_lag_months/12
  ]
  
  # remove duplicates
  comp1 = comp1[
    order(permno, signalyearm, datayearm)
  ][
    , head(.SD, 1), by = .(permno, signalyearm)
  ]
  
  
  ## import crsp data
  # date refers to ret date, everything else is lagged
  crsp <- readRDS('../Data/Intermediate/crspm.RData') %>% 
    transmute(
      permno, ret_yearm = yearm, ret, me
    )
  setDT(crsp)
  
  crsp[ , me := c(NA, me[-.N]), by = permno] # lags me by one month
  
  # merge, signals available at signalyearm get used for returns in ret_yearm
  comp1[ , ret_yearm := signalyearm + 1/12]
  alldat = merge(crsp, comp1, all.x=TRUE, by = c('permno','ret_yearm'))
  
  # fill NA with most recent x by permno
  #   this takes the most time 
  allxnames = unique(c(user$signal$xnames, user$signal$scaling_variables, 'me'))
  alldat[ , (allxnames) := nafill(.SD, 'locf'), by = .(permno), .SDcols = allxnames]
  
  ## enforce updating frequency (aka rebalancing) ====
  
  # make separate me that is always updated monthly (me_monthly is not in allxnames)
  alldat[ , me_monthly := me ]
  alldat[
    , temp_month := as.numeric(12*(signalyearm %% 1))
  ][
    , reup_id := if_else(temp_month %in% user$data$reup_months, 1, NA_real_)
  ][
    , (allxnames) := lapply(.SD, function(x) x*reup_id ), .SDcols = allxnames 
  ]
  
  # fill NA with most recent x by permno
  #   this takes the most time 
  alldat[ , (allxnames) := nafill(.SD, 'locf'), by = .(permno), .SDcols = allxnames]
  
  # Save to fst such that parallelization works without having to load big file onto each worker
  fst::write_fst(alldat, '../Data/tmpAllDat.fst')
  
  # used later
  ret_dates = alldat %>% transmute(date = ret_yearm) %>% as_tibble() %>%  distinct() %>% arrange()
  
  
  rm(comp0,comp1,crsp,alldat)
  gc()
  
} # end prepare_data

# prepare_data()

toc = Sys.time()
toc - tic



# Sample Strategies -------------------------------------------------------

## Draw lists of signals and ports ------------------------------------------

signal_list = make_signal_list(signal_form =  user$signal$form,
                               xvars = user$signal$xnames, 
                               signalnum = user$signal$signalnum,
                               scale_vars = user$signal$scaling_variables,                         
                               rs = user$signal$seednumber)

port_list = expand.grid(longshort_form = user$port$longshort_form, 
                        portnum = user$port$portnum, 
                        sweight = user$port$sweight,
                        trim = user$port$trim,
                        stringsAsFactors = FALSE)

## Internal Function ---------------------------------------------------------------
debugSource('0_Environment.R')
make_many_ls = function(){
  ### make one portdat ===
  
  # extract current settings settings
  signal_cur = signal_list[signali,]
  
  # import small dataset with return, me, xusedcurr, and add signal
  if (is.na(signal_cur$v2)) { # If only one variable needed to construct signal
    smalldat = fst::read_fst('../Data/tmpAllDat.fst', 
                             columns = c('permno', 'ret_yearm', 'ret', 'me_monthly',
                                         signal_cur$v1)) %>%
      as_tibble()
  } else {
    smalldat = fst::read_fst('../Data/tmpAllDat.fst', 
                             columns = c('permno', 'ret_yearm', 'ret', 'me_monthly',
                                         signal_cur$v1, signal_cur$v2)) %>%
      as_tibble()
  }
  smalldat = smalldat %>% mutate(ret_yearm = as.yearmon(ret_yearm))
  
  tic = Sys.time() #
  smalldat$signal = dataset_to_signal(form = signal_cur$signal_form, 
                                      dt = smalldat, 
                                      v1 =  signal_cur$v1,
                                      v2 = signal_cur$v2) # makes a signal
  toc = Sys.time() #
  print('signal done')
  print(toc - tic) #
  
  
  tic = Sys.time() #
  # assign to portfolios
  portdat = tibble()
  for (porti in 1:dim(port_list)[1]){
    tempport = signal_to_ports(dt = smalldat, 
                               form = port_list[porti,]$longshort_form, 
                               portnum = port_list[porti,]$portnum, 
                               sweight = port_list[porti,]$sweight,
                               trim = port_list[porti,]$trim)
    tempport = tempport %>% mutate(
      # longshort_form = port_list[porti,]$longshort_form, # not currently used
      portnum = port_list[porti,]$portnum, 
      sweight = port_list[porti,]$sweight,      
    )
    portdat = rbind(portdat, tempport)
  }
  
  toc = Sys.time() #
  print('ports done')
  print(toc - tic)  #
  
  # CLEAN UP AND SAVE ==== (in signal loop rather than after to accommodate parallelization)
  signame = ifelse(is.na(signal_cur$v2),
                   paste0(signal_cur$signal_form, '_', signal_cur$v1),
                   paste0(signal_cur$signal_form, '_', signal_cur$v1, '_', signal_cur$v2))
  
  
  ls_dat = portdat %>% 
    mutate(signalname = signame) 
  
  # feedback
  print(paste0(
    'signali = ', signali, ' of ', nrow(signal_list)
    , ' | signalform = ', signal_cur$signal_form
    , ' | v1 = ', signal_cur$v1
    , ' | v2 = ', signal_cur$v2
    #      , ' | Var(tstat) = ', round(var_tstat,2)
  ))
  
  ## end make one portdat ===
  
  return(ls_dat)
  

}


## Loop over signals -------------------------------------------------------
# call make_many_ls (this is where the action is) 

if (num_cores > 1){
  setDTthreads(1)
  cl <- makePSOCKcluster(num_cores)
  registerDoParallel(cl)
  ls_dat_all = foreach(signali=1:nrow(signal_list), 
                       .combine = rbind,
                       .packages = c('tidyverse','zoo')) %dopar% {
                         
                         log.text <- paste0(Sys.time(), " processing loop run ", signali)
                         write.table(log.text, "../Data/loop-log.txt", append = TRUE, row.names = FALSE, col.names = FALSE)
                         
                         ls_dat = make_many_ls() # make strats
                         
                         
                       } # end for signali
  stopCluster(cl)
  setDTthreads()
} else {
  ls_dat_all = foreach(signali=1:nrow(signal_list), 
                       .combine = rbind) %do% {
                         ls_dat = make_many_ls()
                       } # end for signali    
} # if num_cores



# Organize and Save -------------------------------------------------------

# compile all info into one list
stratdat = list(
  ret = ls_dat_all
  , user = user
  , name = user$name
)

# save
saveRDS(stratdat, paste0('../Data/LongShortPortfolios/stratdat_',
                         stratdat$name, '_',
                         '.RData'))

toc = Sys.time()
print(toc - tic)


# Debug: Compare to YZ -------------------------------------------------------------------------


yz = readRDS('../Data/LongShortPortfolios/yz_ew.RData')$ret %>% 
  mutate(sweight = 'ew') %>% 
  rbind(
    readRDS('../Data/LongShortPortfolios/yz_vw.RData')$ret %>% 
      mutate(sweight = 'vw') 
  ) %>% 
  mutate(
    yearm = as.yearmon(date), source = 'yz'
  ) %>% 
  select(source, sweight, signalname, yearm, ret)

temp = yz %>% 
  rbind(
    stratdat$ret %>% transmute(
      source = 'us', sweight, signalname, yearm, ret
    )
  ) %>% 
  filter(yearm >= 1963.6, yearm <= 2014) 


sum1 = temp %>% 
  group_by(source, sweight, signalname) %>% 
  summarize(rbar = mean(ret), nmonth = n(), tstat = mean(ret)/sd(ret)*sqrt(n())) 

q = c(5, 10, 25, 50, 75, 90, 95)/100
sum1 %>%
  filter(sweight == 'ew') %>% 
  group_by(source) %>% 
  summarize(
    q = q, rbar_q = quantile(rbar, q), nmonth_q = quantile(nmonth, q), tstat_q = quantile(tstat, q)
  ) %>% 
  pivot_wider(
    names_from = 'source', values_from = ends_with('_q')
  )


sum1 %>%
  filter(sweight == 'vw') %>% 
  group_by(source) %>% 
  summarize(
    q = q, rbar_q = quantile(rbar, q), nmonth_q = quantile(nmonth, q), tstat_q = quantile(tstat, q)
  ) %>% 
  pivot_wider(
    names_from = 'source', values_from = ends_with('_q')
  )
