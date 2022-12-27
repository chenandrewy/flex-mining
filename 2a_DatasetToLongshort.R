tic = Sys.time()

# Settings ----------------------------------------------------------------

rm(list = ls())

## Environment ----
source('0_Environment.R')

# safety delete
file.remove('../Data/tmpAllDat.fst')

# parallel setup
library(doParallel)
env <- foreach:::.foreachGlobals # https://stackoverflow.com/questions/64519640/error-in-summary-connectionconnection-invalid-connection
rm(list=ls(name=env), pos=env)

## User settings ----
stratname = 'yz_rep' # name of output file


no_cores <- round(.5*detectCores())  # Adjust number of cores used as you see fit
# no_cores = 3 # use no_cores = 1 for serial
no_cores = 10
threads_fst(1) # since fst is used inside foreach, might want to limit cpus, though this doesn't seem to help


# data lag choices
data_avail_lag = 6 # months
toostale_months = 12 

# signal choices
signal_form = c('ratio', 'ratioChange', 'ratioChangePct',
                'levelChangePct', 'levelChangeScaled', 'levelsChangePct_Change') # 'noise'

xnames = unique(c(compnames$yz.numer, compnames$yz.denom))
scaling_variables = compnames$yz.denom

signalnum   = Inf # number of signals to sample or Inf for all
seednumber  = 1235 # seed sampling



# portfolio choices
reup_months    = c(6) # stocks are traded using new data at end of these months
longshort_form = 'ls_extremes'
portnum        = c(10)
sweight        = c('ew','vw') 
trim           = NULL  # or some quantile e.g. .005

## Draw list of signals ====
signal_list = make_signal_list(signal_form =  signal_form,
                               xvars = xnames, 
                               signalnum = signalnum,
                               scale_vars = scaling_variables,                         
                               rs = seednumber)

port_list = expand.grid(longshort_form = longshort_form, 
                        portnum = portnum, 
                        sweight = sweight,
                        stringsAsFactors = FALSE)


# Internal Functions ---------------------------------------------------------------

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
  print(toc - tic) #
  
  
  tic = Sys.time() #
  # assign to portfolios
  portdat = tibble()
  for (porti in 1:dim(port_list)[1]){
    tempport = signal_to_longshort(dt = smalldat, 
                                   form = port_list[porti,]$longshort_form, 
                                   portnum = port_list[porti,]$portnum, 
                                   sweight = port_list[porti,]$sweight,
                                   trim = trim)
    tempport = tempport %>% mutate(
      longshort_form = port_list[porti,]$longshort_form, 
      portnum = port_list[porti,]$portnum, 
      sweight = port_list[porti,]$sweight,      
    )
    portdat = rbind(portdat, tempport)
  }
  
  toc = Sys.time() #
  print(toc - tic)  #
  
  # CLEAN UP AND SAVE ==== (in signal loop rather than after to accommodate parallelization)
  signame = ifelse(is.na(signal_cur$v2),
                   paste0(signal_cur$signal_form, '_', longshort_form, '_', signal_cur$v1),
                   paste0(signal_cur$signal_form, '_', longshort_form, '_', signal_cur$v1, '_', signal_cur$v2))
  
  
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
  
  # return(NULL)
}



# Data Prep ---------------------------------------------------------------
# do everything in data.table for efficiency

tic = Sys.time()


## import and merge ====

## import compustat, convert to monthly
comp0 = readRDS('../Data/Intermediate/CompustatAnnual.RData')
setDT(comp0)
comp0 = comp0 %>% select(permno, datayearm, all_of(xnames))

# lag signal, then keep around for toostale_months
#   see here: https://stackoverflow.com/questions/11121385/repeat-rows-of-a-data-frame
#   signalyearm are yearmons when investor has access to signal & signal is not stale
comp1 = comp0 %>% slice(rep(1:n(), each = toostale_months)) 
comp1$temp_lag_months = rep(data_avail_lag + 0:(toostale_months-1), dim(comp0)[1])
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

# Add me to xnames
xnames = c(xnames, "me")
# fill NA with most recent x by permno
#   this takes the most time 
alldat[ , (xnames) := nafill(.SD, 'locf'), by = .(permno), .SDcols = xnames]

## enforce updating frequency (aka rebalancing) ====

# make separate me that is always updated monthly (me_monthly is not in xnames2)
alldat[ , me_monthly := me ]
alldat[
  , temp_month := as.numeric(12*(signalyearm %% 1))
][
  , reup_id := if_else(temp_month %in% reup_months, 1, NA_real_)
][
  , (xnames) := lapply(.SD, function(x) x*reup_id ), .SDcols = xnames 
]

# fill NA with most recent x by permno
#   this takes the most time 
alldat[ , (xnames) := nafill(.SD, 'locf'), by = .(permno), .SDcols = xnames]

# Save to fst such that parallelization works without having to load big file onto each worker
fst::write_fst(alldat, '../Data/tmpAllDat.fst')

# used later
ret_dates = alldat %>% transmute(date = ret_yearm) %>% as_tibble() %>%  distinct() %>% arrange()

toc = Sys.time()
toc - tic

rm(comp0,comp1,crsp)
gc()






# Sample Strategies -------------------------------------------------------
# call make_many_ls (this is where the action is) 

if (no_cores > 1){
  cl <- makePSOCKcluster(no_cores)
  registerDoParallel(cl)
  ls_dat_all = foreach(signali=1:nrow(signal_list), 
                       .combine = rbind,
                       .packages = c('tidyverse','zoo')) %dopar% {
                         ls_dat = make_many_ls()
                       } # end for signali
  stopCluster(cl)  
} else {
  ls_dat_all = foreach(signali=1:nrow(signal_list), 
                       .combine = rbind) %do% {
                         ls_dat = make_many_ls()
                       } # end for signali    
} # if no_cores




# Organize and Save -------------------------------------------------------

# compile all info into one list
stratdat = list(
  ret = ls_dat_all
  , signal_form = signal_form
  , longshort_form = longshort_form
  , portnum = portnum
  , trim = trim
  , reup_months = reup_months
  , sweight = sweight
  , data_avail_lag = data_avail_lag
  , scaling_variables = scaling_variables
  , usedData = 'allData'
)

# save
saveRDS(stratdat, paste0('../Data/LongShortPortfolios/stratdat_',
                         stratname,
                         '.RData'))

toc = Sys.time()
print(toc - tic)


# Debug: Compare to YZ -------------------------------------------------------------------------


yz = readRDS('../Data/LongShortPortfolios/yz_ew.RData')$ret %>% 
  mutate(sweight = 'ew') %>% 
  rbind(
    readRDS('../Data/LongShortPortfolios/yz_vw.RData')$ret %>% 
      mutate(sweight = 'vw')
  )



yz %>% group_by(signalname, sweight) %>% 
  summarize(tstat = mean(ret)/sd(ret)*sqrt(n())) %>% 
  group_by(sweight) %>% 
  summarize(quantile(tstat), sd(tstat))


stratdat$ret %>% filter(yearm >= 1963.6, yearm <= 2014) %>% group_by(signalname, sweight) %>% 
  summarize(tstat = mean(ret)/sd(ret)*sqrt(n())) %>% 
  group_by(sweight) %>% 
  summarize(quantile(tstat), sd(tstat))
