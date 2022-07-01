# Takes in a dataset and constructs a sample of long-short strategies

# ASSUMPTIONS ON DATASET MADE FOR SPEED
#   1) everything except for returns is lagged
#   2) data is sorted by c(permno, date)

# ENVIRONMENT ====
source('0_Environment.R')
source('0_functions.R')


# Settings ----------------------------------------------------------------

# data lag choices
data_avail_lag = 6 # months
toostale_months = 12 

# signal choices
signal_form = c('ratio', 'ratiodiff')
signalnum   = TRUE # number of signals to sample or TRUE for all (need at least 200, it seems)
seednumber  = 1235 # seed sampling

# portfolio choices
reup_months    = c(6) # stocks are traded using new data at end of these months
longshort_form = 'ls_extremes'
portnum        = c(5, 10)
sweight        = c('ew', 'vw') 
trim           = NULL  # or some quantile e.g. .005

# variable choices
#scaling_variables <- c("at", "act",  "invt", "ppent", "lt", "lct", "dltt",
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
                               sweight = sweight)

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
  xused_list = strategy_list(xvars = xnames, 
                             signalnum = signalnum,
                             scale_vars = scaling_variables,
                             rs = seednumber)
  
  # initialize retmat data
  ret_dates = alldat %>%  pull(ret_date) %>% unique() %>% sort()
  retmat = matrix(nrow = length(ret_dates), ncol = nrow(xused_list))
  
  ## loop over signals  ====
  
  for (signali in 1:nrow(xused_list)){
    
    ### make one portdat ====
    # # select xused for making signal 
    # xusednamecurr = xused_list[signali, ] %>% as.matrix %>% as.character()
    
    # import small dataset with return, me, xusedcurr, and add signal
    smalldat = alldat %>% select(permno, ret_yearm, ret, me_monthly, 
                                 xused_list$v1[signali], xused_list$v2[signali]) %>% 
      as_tibble()
    
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
    
    retmat[, signali] = portdat$ret_ls
    
    # feedback ===
    
    tstat = colMeans(retmat, na.rm=TRUE)/apply(retmat,2,sd,na.rm=T)*sqrt(apply(!is.na(retmat), 2, sum))
    var_tstat = var(tstat, na.rm =T)
    
    print(paste0(
      'signali = ', signali, ' of ', nrow(xused_list)
      , ' | v1 = ', xused_list$v1[signali]
      , ' | v2 = ', xused_list$v2[signali]
      , ' | Var(tstat) = ', round(var_tstat,2)
    ))
    
    # hist(tstat)
    
    ## end make one portdat ====
    
  } # end for signali
  
  
  # CLEAN UP AND SAVE ====
  ls_dat = cbind(ret_dates, as.data.frame(retmat)) %>% 
    rename(date = ret_dates) %>% 
    pivot_longer(
      cols = -date, names_to = 'signali', names_prefix = 'V', values_to = 'ret'
    ) %>% 
    left_join(
      xused_list %>% 
        mutate(signali = row_number() %>% as.character(),
               signalname = paste0(signal_form, '_', longshort_form, '_', v1, '_', v2))
    ) %>% 
    select(-signali, -v1, -v2)
  
  
  # compile all info into one list
  stratdat = list(
    ret = ls_dat
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
