tic = Sys.time()

# rm(list = ls())

# Setup  ----------------------------------------------------------------


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

# name of output file (by default use time and date)
user$name = Sys.time() %>% substr(1,17) 
substr(user$name, 17,17) = 'm'
substr(user$name, 14,14) = 'h'
user$name

# signal choices
#   really not sure that xnames and scaling_variables should be defined this way
#   default is me aligned with datadate, which is 6 months lagged
temp_denom = compnames$yz.denom

user$signal = list(
  signalnum   = Inf # number of signals to sample or Inf for all
  , form = c('ratio', 'ratioChange', 'ratioChangePct',
                  'levelChangePct', 'levelChangeScaled', 'levelsChangePct_Change') # 'noise'
  , xnames = unique(c(compnames$yz.numer,temp_denom))  # must include scaling variables
  , scaling_variables = temp_denom
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

# actually run function (comment out for debugging)
prepare_data()

toc = Sys.time()
print('done prepping data')
toc - tic



# Sample Strategies -------------------------------------------------------

## Draw lists of signals and ports ------------------------------------------

# ac: this function doesn't seem to work right
# signal_list = make_signal_list(signal_form =  user$signal$form,
#                                xvars = user$signal$xnames, 
#                                signalnum = user$signal$signalnum,
#                                scale_vars = user$signal$scaling_variables,                         
#                                rs = user$signal$seednumber)

# ac: this works to replicate yz strat list
# first make 240*76 = 18,240 combinations
# use yz.denom for me_datadate, yz.denom_alt for me (most recent)
signal_list = expand.grid(
  signal_form = user$signal$form
  , v1 = compnames$yz.numer
  , v2 = compnames$yz.denom_alt
  , stringsAsFactors = F
) %>% 
  mutate(
    v2 = if_else(signal_form == 'levelChangePct', NA_character_, v2)
  ) %>% 
  distinct(across(everything()), .keep_all = T) %>% 
  # remove 13 vboth x 5 two variable fun where v1 == v2 leads to constant signals  
  mutate(
    dropme = v1 %in% intersect(compnames$yz.numer, compnames$yz.denom) 
      & signal_form != 'levelChangePct' 
      &  v1 == v2 
  ) %>% 
  # remove selected strategies (2 vodd x 31 pd_var funs) based on yz sas data
  mutate(
    dropme2 = v1 %in% c('rdip', 'txndbr')  
      & signal_form %in% c('ratioChangePct','levelChangePct','levelsChangePct_Change')
  ) %>%
  filter(!(dropme | dropme2)) %>% 
  select(-starts_with('drop')) %>% 
  as_tibble()


# check dimensions at console
signal_list %>% dim()

# sample and add id
signal_list = signal_list %>% 
  sample_n(min(dim(signal_list)[1],user$signal$signalnum)) %>% 
  arrange(across(everything())) %>% 
  mutate(signalid = row_number()) %>% 
  select(signalid, everything())  




# port list
port_list = expand.grid(longshort_form = user$port$longshort_form, 
                        portnum = user$port$portnum, 
                        sweight = user$port$sweight,
                        trim = user$port$trim,
                        stringsAsFactors = FALSE) %>% 
  arrange(across(everything())) %>% 
  mutate(portid = row_number()) %>% 
  select(portid, everything())  

# debug
signal_list0 = signal_list
port_list0 = port_list


## Internal Function ---------------------------------------------------------------

make_many_ls = function(){
  ### make one portdat ===
  
  # extract current settings 
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
    tempport = tempport %>% mutate(portid = porti)
    portdat = rbind(portdat, tempport)
  }
  
  toc = Sys.time() #
  print('ports done')
  print(toc - tic)  #
  
  # Clean up and save
  ls_dat = portdat %>% mutate(signalid = signali) 
  
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
  
} # make_many_ls



## Loop over signals -------------------------------------------------------
# # call make_many_ls (this is where the action is) 
# 
# tic_loop = Sys.time()
# 
# if (num_cores > 1){
#   setDTthreads(1)
#   cl <- makePSOCKcluster(num_cores)
#   registerDoParallel(cl)
#   file.remove('../Data/make_many_ls.log')
#   ls_dat_all = foreach(signali=1:nrow(signal_list), 
#                        .combine = rbind,
#                        .packages = c('tidyverse','zoo')) %dopar% {
#                          
#                          if (signali %% 100 == 0){
#                            log.text <- paste0(
#                              Sys.time()
#                              , " signali = ", signali
#                              , " of ", dim(signal_list)[1]
#                              , " minutes elapsed = ", round(as.numeric(Sys.time() - tic_loop, units = 'mins'), 1)
#                            )
#                            write.table(log.text, "../Data/make_many_ls.log", append = TRUE, row.names = FALSE, col.names = FALSE)
#                          }
#                          ls_dat = make_many_ls() # make strats
#                          
#                          
#                        } # end for signali
#   stopCluster(cl)
#   setDTthreads()
# } else {
#   ls_dat_all = foreach(signali=1:nrow(signal_list), 
#                        .combine = rbind) %do% {
#                          ls_dat = make_many_ls()
#                        } # end for signali    
# } # if num_cores



# Load yz data ------------------------------------------------------------

# yz
yzraw = read_sas('../Data Yan-Zheng/Yan_Zheng_RFS_Data.sas7bdat')

yz_signal_list = yzraw %>% distinct(transformation,fsvariable) %>% 
  arrange(transformation, fsvariable) %>% 
  mutate(signalid = row_number()) 

yz = yzraw %>% 
  mutate(yearm = as.yearmon(DATE), ret= ddiff_ew*100) %>% 
  mutate(
    v2 = str_remove(transformation, 'pd_var_')
    , v2 = str_remove(v2, 'd_var_')
    , v2 = str_remove(v2, 'var_')
    , form = case_when(
      grepl('pd_var', transformation) ~ 'pd_var'
      , grepl('d_var', transformation) ~ 'd_var'
      , grepl('var', transformation) ~ 'var'
    )
  ) %>% 
  transmute(yearm, v1 = fsvariable, form, v2, ret, transformation)
  

# Test One signal -------------------------------------------------------------------------

signal_list0$signal_form %>% unique

signal_list = signal_list0 %>% filter(
  v1 == 'at', signal_form == 'levelChangePct'
) 

port_list = port_list0 %>% filter(sweight == 'ew')

debugSource('0_Environment.R')

num_cores = 1

signali = 1
ls_dat = make_many_ls()

ls_dat %>% print(n=200)

## examine result ----
ls_dat %>% 
  filter(yearm >= 1965, yearm <= 2014) %>% 
  summarize(
    nmonth = n(), rbar = mean(ret), tstat = rbar/sd(ret)*sqrt(nmonth)
    , mean_nstock = mean(nstock)
  )

ls_dat %>% 
  filter(yearm >= 1968, yearm <= 2003) %>% 
  summarize(
    nmonth = n(), rbar = mean(ret), tstat = rbar/sd(ret)*sqrt(nmonth)
    , mean_nstock = mean(nstock)
  )


## compare with yz ----

yz %>% 
  filter(v1 == 'lt', form == 'd_var', v2 == 'at') %>% 
  group_by(form) %>% 
  filter(yearm >= 1965, yearm <= 2014) %>% 
  summarize(
    nmonth = n(), rbar = mean(ret), tstat = rbar/sd(ret)*sqrt(nmonth)
  )  %>% 
  arrange(tstat) %>% 
  print(n=20)


