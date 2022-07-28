# Takes in a dataset and constructs a sample of long-short strategies
# Parallelized without much feedback (see 2a_DatasetToLongshort.R for the non-parallelized
# version with more verbose feedback)

# ASSUMPTIONS ON DATASET MADE FOR SPEED
#   1) everything except for returns is lagged
#   2) data is sorted by c(permno, date)

# ENVIRONMENT ====
source('0_Environment.R')
source('0_functions.R')
library(doParallel)
no_cores <- round(.6*detectCores())  # Adjust number of cores used as you see fit

# Settings ----------------------------------------------------------------

# data lag choices
data_avail_lag = 6 # months
toostale_months = 12 

# signal choices
signal_form = c('ratio', 'ratioChange', 'ratioChangePct',
                'levelChangePct', 'levelChangeScaled', 'levelsChangePct_Change') # 'noise'
signalnum   = TRUE # number of signals to sample or TRUE for all
seednumber  = 1235 # seed sampling

# portfolio choices
reup_months    = c(6) # stocks are traded using new data at end of these months
longshort_form = 'ls_extremes'
portnum        = c(10) # '5'
sweight        = c('ew') # 'vw'
trim           = NULL  # or some quantile e.g. .005

# variable choices
# scaling_variables <- c("at", "act",  "invt", "ppent", "lt", "lct", "dltt",
#                       "ceq", "seq", "icapt", "sale", "cogs", "xsga", "me", "emp")

scaling_variables = NULL

dataCombinations = expand.grid(signal_form = signal_form,
                               longshort_form = longshort_form, 
                               portnum = portnum, 
                               sweight = sweight,
                               stringsAsFactors = FALSE)

# DATA PREP (about 2 minutes) ====

## import compustat, convert to monthly
comp0 = read_fst('../Data/Intermediate/a_aCompustat.fst') 

# do everything in data.table for efficiency
tic = Sys.time()

# lag signal, then keep around for toostale_months
#   see here: https://stackoverflow.com/questions/11121385/repeat-rows-of-a-data-frame
#   signaldate are dates when investor has access to signal & signal is not stale
comp1 = comp0 %>% slice(rep(1:n(), each = 12)) %>% as.data.table()
comp1$temp_lag_months = rep(data_avail_lag + 0:(toostale_months-1), dim(comp0)[1])
comp1[ 
  , signaldate := datadate %m+% months(temp_lag_months)
]

## merge onto returns data
# date refers to ret date, everything else is lagged
crsp <- read_fst('../Data/Intermediate/crspm.fst', columns = c('permno','date','ret','me')) %>% 
  as.data.table() 
setnames(crsp, old = 'date', new = 'ret_date')
crsp[ , me := c(NA, me[-.N]), by = permno]

# signals are available at signaldate get used for returns in ret_yearm
comp1[
  , ret_yearm := signaldate %m+% months(1)
][ 
  , ret_yearm := year(ret_yearm)*100 + month(ret_yearm)
]
crsp[, ret_yearm := year(ret_date)*100 + month(ret_date)]

# merge
alldat = merge(crsp, comp1, all.x=TRUE, by = c('permno','ret_yearm'))

## enforce updating frequency (aka rebalancing) 
# make separate me that is always updated monthly (me_monthly is not in xnames2)
alldat[ , me_monthly := me]

# all the x vars under consideration
xnames = setdiff(names(alldat), c('gvkey', 'datadate', 'conm', 'fyear', 'tic', 'cusip',
                                  'naicsh', 'sich', "timeLinkStart_d", "timeLinkEnd_d", "time_avail_d", "time_avail_y", 'permno',
                                  'ret_yearm', 'ret_date', 'ret', 'temp_lag_months', 'signaldate', 'me_monthly'))


# force x to NA in non-reup-months
alldat[
  , reup_id := month(signaldate) %in% reup_months
][
  , reup_id := if_else(reup_id, 1, NA_real_)
][
  , (xnames) := lapply(.SD, function(x) x*reup_id ), .SDcols = xnames 
]

# fill NA with most recent x by permno
#   this takes the most time 
alldat[ , (xnames) := nafill(.SD, 'locf'), by = .(permno), .SDcols = xnames]
# Save to fst such that parallelization works without having to load big file onto each worker
fst::write_fst(alldat, '../Data/tmpAllDat.fst')

ret_dates = alldat %>%  pull(ret_date) %>% unique() %>% sort() # used later

toc = Sys.time()
toc - tic

rm(comp0,comp1,crsp, alldat)
gc()

# SAMPLE STRATEGIES ====

# Loop over data configurations
for (ii in 1:nrow(dataCombinations)) {

  signal_form = dataCombinations$signal_form[ii]
  longshort_form = dataCombinations$longshort_form[ii]
  sweight = dataCombinations$sweight[ii]
  portnum = dataCombinations$portnum[ii]
  
  ## draw sample of variable combinations ====
  
  if (signal_form == 'levelChangePct') { # If only one variable needed to construct signal
    xused_list = tibble(v1 = xnames, v2 = NA_real_)
  } else {
    xused_list = strategy_list(xvars = xnames, 
                               signalnum = signalnum,
                               scale_vars = scaling_variables,
                               rs = seednumber)
  }
  

  ## loop over signals  ====
  cl <- makePSOCKcluster(no_cores)
  registerDoParallel(cl)
  
  ls_dat_all = foreach(signali=1:nrow(xused_list), 
                       .combine = rbind,
                       .packages = c('tidyverse')) %dopar% {

                         # initialize retmat data
                         #retmat = matrix(nrow = length(ret_dates), ncol = 1)
                         
                         ### make one portdat ====
                         
                         # import small dataset with return, me, xusedcurr, and add signal
                         if (signal_form == 'levelChangePct') { # If only one variable needed to construct signal
                           smalldat = fst::read_fst('../Data/tmpAllDat.fst', 
                                                    columns = c('permno', 'ret_yearm', 'ret', 'me_monthly','ret_date',
                                                                xused_list$v1[signali])) %>%
                             as_tibble()
                         } else {
                           smalldat = fst::read_fst('../Data/tmpAllDat.fst', 
                                                    columns = c('permno', 'ret_yearm', 'ret', 'me_monthly','ret_date',
                                                                xused_list$v1[signali], xused_list$v2[signali])) %>%
                             as_tibble()
                         }

                         #ret_dates = smalldat %>%  pull(ret_date) %>% unique() %>% sort()
                         
                         smalldat$signal = dataset_to_signal(form = signal_form, 
                                                             dt = smalldat, 
                                                             v1 =  xused_list$v1[signali],
                                                             v2 = xused_list$v2[signali]) # makes a signal
                         
                         # assign to portfolios
                         portdat = signal_to_longshort(dt = smalldat, 
                                                       form = longshort_form, 
                                                       portnum = portnum, 
                                                       sweight = sweight,
                                                       trim = trim)
                         
                         #retmat[, 1] = portdat$ret_ls
                         
                         # CLEAN UP AND SAVE ==== (in signal loop rather than after to accommodate parallelization)
                         signame = ifelse(is.na(xused_list$v2[signali]),
                                          paste0(signal_form, '_', longshort_form, '_', xused_list$v1[signali]),
                                          paste0(signal_form, '_', longshort_form, '_', xused_list$v1[signali], '_', xused_list$v2[signali]))
                         
                         
                         ls_dat = portdat %>% 
                           mutate(signalname = signame) 
                        # cbind(ret_dates, as.data.frame(retmat)) %>% 
                         #   rename(date = ret_dates) %>% 
                         #   pivot_longer(
                         #     cols = -date, names_to = 'signali', names_prefix = 'V', values_to = 'ret') %>%
                           # mutate(signalname = signame) %>% 
                           # select(-signali)
                         
                         ## end make one portdat ====
                         
                       } # end for signali
  stopCluster(cl)
  
  # compile all info into one list
  stratdat = list(
    ret = ls_dat_all %>%
      rename(ret = ret_ls) %>% 
      filter(!is.na(ret))  # To save some space
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
  if (is.null(scaling_variables)) {
    saveRDS(stratdat, paste0('../Data/LongShortPortfolios/stratdat_',
                             signal_form, '_', longshort_form, portnum, sweight, 
                             '_NoScaleVars.RData'))
    
  } else {
    saveRDS(stratdat, paste0('../Data/LongShortPortfolios/stratdat_',
                             signal_form, '_', longshort_form, portnum, sweight, 
                             '_ScaleVars.RData'))
  }
  
  # Housekeeping for next set of strategies
  rm(ls_dat_all, stratdat, signal_form, longshort_form,
     sweight, portnum, xused_list)
}
