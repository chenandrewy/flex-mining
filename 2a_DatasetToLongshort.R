# Takes in a dataset and constructs a sample of long-short strategies

# ASSUMPTIONS ON DATASET MADE FOR SPEED
#   1) everything except for returns is lagged
#   2) data is sorted by c(permno, date)

# do we want to move to doRNG? https://cran.r-project.org/web/packages/doRNG/vignettes/doRNG.pdf

# Settings ----------------------------------------------------------------

## Environment ----
source('0_Environment.R')
source('0_functions.R')

# parallel setup
library(doParallel)
env <- foreach:::.foreachGlobals # https://stackoverflow.com/questions/64519640/error-in-summary-connectionconnection-invalid-connection
rm(list=ls(name=env), pos=env)

## User settings ----
# no_cores <- round(.6*detectCores())  # Adjust number of cores used as you see fit
no_cores = 1 # use no_cores = 1 for serial

# data lag choices
data_avail_lag = 6 # months
toostale_months = 12 

# signal choices
# signal_form = c('ratio', 'ratioChange', 'ratioChangePct',
#                 'levelChangePct', 'levelChangeScaled', 'levelsChangePct_Change') # 'noise'
signal_form = c('ratio')
signalnum   = 100 # number of signals to sample or TRUE for all
seednumber  = 1235 # seed sampling

# portfolio choices
reup_months    = c(6) # stocks are traded using new data at end of these months
longshort_form = 'ls_extremes'
portnum        = c(5, 10)
sweight        = c('ew', 'vw') 
trim           = NULL  # or some quantile e.g. .005

# variable choices
# scaling_variables <- c("at", "act",  "invt", "ppent", "lt", "lct", "dltt",
#                       "ceq", "seq", "icapt", "sale", "cogs", "xsga", "me", "emp")

scaling_variables = NULL

# accounting_variables <- c("acchg", "aco", "acox", "act", "am", "ao", "aoloch", "aox", "ap", "apalch", 
#                           "aqc", "aqi", "aqs", "at", "bast", "caps", "capx", "capxv", "ceq", "ceql", "ceqt", "ch", "che", "chech",
#                           "cld2", "cld3", "cld4", "cld5", "cogs", "cstk", "cstkcv", "cstke", "dc", "dclo", "dcom", "dcpstk", 
#                           "dcvsr", "dcvsub", "dcvt", "dd", "dd1", "dd2", "dd3", "dd4", "dd5", "dfs", "dfxa", "diladj", "dilavx",
#                           "dlc", "dlcch", "dltis", "dlto", "dltp", "dltr", "dltt", "dm", "dn", "do", "donr", "dp", "dpact", "dpc",
#                           "dpvieb", "dpvio", "dpvir", "drc", "ds", "dudd", "dv", "dvc", "dvp", "dvpa", "dvpibb", "dvt", "dxd2", "dxd3",
#                           "dxd4", "dxd5", "ebit", "ebitda", "esopct", "esopdlt", "esopt", "esub", "esubc", "exre", "fatb", "fatc", "fate",
#                           "fatl", "fatn", "fato", "fatp", "fiao", "fincf", "fopo", "fopox", "fopt", "fsrco", "fsrct", "fuseo", "fuset", "gdwl",
#                           "gp", "ib", "ibadj", "ibc", "ibcom", "icapt", "idit", "intan", "intc", "intpn", "invch", "invfg", "invo", "invrm", 
#                           "invt", "invwip", "itcb", "itci", "ivaco", "ivaeq", "ivao", "ivch", "ivncf", "ivst", "ivstch", "lco", "lcox", 
#                           "lcoxdr", "lct", "lifr", "lo", "lt", "mib", "mii", "mrc1", "mrc2", "mrc3", "mrc4", "mrc5", "mrct", "msa", "ni",
#                           "niadj", "nieci", "nopi", "nopio", "np", "oancf", "ob", "oiadp", "pi", "pidom", "pifo", "ppegt", "ppenb",
#                           "ppenc", "ppenli", "ppenme", "ppennr", "ppeno", "ppent", "ppevbb", "ppeveb", "ppevo", "ppevr", "prstkc",
#                           "pstk", "pstkc", "pstkl", "pstkn", "pstkr", "pstkrv", "rdip", "re", "rea", "reajo", "recch", "recco", "recd", "rect",
#                           "recta", "rectr", "reuna", "sale", "seq", "siv", "spi", "sppe", "sppiv", "sstk", "tlcf", "tstk", "tstkc", "tstkp", 
#                           "txach", "txbco", "txc", "txdb", "txdba", "txdbca", "txdbcl", "txdc", "txdfed", "txdfo", "txdi", "txditc", 
#                           "txds", "txfed", "txfo", "txndb", "txndba", "txndbl", "txndbr", "txo", "txp", "txpd", "txr", "txs", "txt", "txw",
#                           "wcap", "wcapc", "wcapch", "xacc", "xad", "xdepl", "xi", "xido", "xidoc", "xint", "xopr", "xpp", "xpr", "xrd", "xrent",
#                           "xsga")


dataCombinations = expand.grid(signal_form = signal_form,
                               longshort_form = longshort_form, 
                               portnum = portnum, 
                               sweight = sweight,
                               stringsAsFactors = FALSE)


# Internal Functions ---------------------------------------------------------------

make_many_ls = function(){
  # initialize retmat data
  #retmat = matrix(nrow = length(ret_dates), ncol = 1)
  
  ### make one portdat ===
  
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
  
  # feedback
  print(paste0(
    'signali = ', signali, ' of ', nrow(xused_list)
    , ' | signalform = ', signal_form
    , ' | v1 = ', xused_list$v1[signali]
    , ' | v2 = ', xused_list$v2[signali]
    #      , ' | Var(tstat) = ', round(var_tstat,2)
  ))
  
  ## end make one portdat ===
  
  return = ls_dat
}

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

if (no_cores > 1){
  # Save to fst such that parallelization works without having to load big file onto each worker
  fst::write_fst(alldat, '../Data/tmpAllDat.fst')
}

# sed later
ret_dates = alldat %>% transmute(date = ret_yearm) %>% as_tibble() %>%  distinct() %>% arrange()

toc = Sys.time()
toc - tic

rm(comp0,comp1,crsp)
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
  
  # initialize retmat data
  retmat = matrix(nrow = length(ret_dates), ncol = nrow(xused_list))
  
  # call make_many_ls_* ====
  # ls_dat_all = make_many_ls_serial()
  # ls_dat_all = make_many_ls_parallel()
  

  if (no_cores > 1){
    cl <- makePSOCKcluster(no_cores)
    registerDoParallel(cl)
    ls_dat_all = foreach(signali=1:nrow(xused_list), 
                         .combine = rbind,
                         .packages = c('tidyverse')) %dopar% {
                           
                           ls_dat = make_many_ls()
                           
                         } # end for signali
    stopCluster(cl)  
  } else {
    ls_dat_all = foreach(signali=1:nrow(xused_list), 
                         .combine = rbind) %do% {
                           ls_dat = make_many_ls()
                         } # end for signali    
  } # if no_cores
  

  # finish up ====

  
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
  if (is.null(scaling_variables)) {
    saveRDS(stratdat, paste0('../Data/LongShortPortfolios/stratdat_',
                             signal_form, '_', longshort_form, portnum, sweight, 
                             '_NoScaleVars.RData'))
    
  } else {
    saveRDS(stratdat, paste0('../Data/LongShortPortfolios/stratdat_',
                             signal_form, '_', longshort_form, portnum, sweight, 
                             '_ScaleVars.RData'))
  }
  
}



# debug -------------------------------------------------------------------

stratdat$ret %>% 
  filter(!is.na(ret_ls)) %>% 
  group_by(signalname) %>% 
  summarize(tstat = mean(ret_ls)/sd(ret_ls)*sqrt(n())) %>% 
  pull(tstat) %>% 
  hist()
