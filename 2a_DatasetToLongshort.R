# 2023 03: takes in a dataset and constructs a sample of long-short strategies

# ASSUMPTIONS ON DATASET MADE FOR SPEED
#   1) everything except for returns is lagged
#   2) data is sorted by c(permno, date)

# TO DO: 
#   move some data cleaning earlier

# ENVIRONMENT ====
source('0_Environment.R')

scaling_variables <- c("at", "act",  "invt", "ppent", "lt", "lct", "dltt",
                       "ceq", "seq", "icapt", "sale", "cogs", "xsga", "me", "emp")

accounting_variables <- c("acchg", "aco", "acox", "act", "am", "ao", "aoloch", "aox", "ap", "apalch", 
                          "aqc", "aqi", "aqs", "at", "bast", "caps", "capx", "capxv", "ceq", "ceql", "ceqt", "ch", "che", "chech",
                          "cld2", "cld3", "cld4", "cld5", "cogs", "cstk", "cstkcv", "cstke", "dc", "dclo", "dcom", "dcpstk", 
                          "dcvsr", "dcvsub", "dcvt", "dd", "dd1", "dd2", "dd3", "dd4", "dd5", "dfs", "dfxa", "diladj", "dilavx",
                          "dlc", "dlcch", "dltis", "dlto", "dltp", "dltr", "dltt", "dm", "dn", "do", "donr", "dp", "dpact", "dpc",
                          "dpvieb", "dpvio", "dpvir", "drc", "ds", "dudd", "dv", "dvc", "dvp", "dvpa", "dvpibb", "dvt", "dxd2", "dxd3",
                          "dxd4", "dxd5", "ebit", "ebitda", "esopct", "esopdlt", "esopt", "esub", "esubc", "exre", "fatb", "fatc", "fate",
                          "fatl", "fatn", "fato", "fatp", "fiao", "fincf", "fopo", "fopox", "fopt", "fsrco", "fsrct", "fuseo", "fuset", "gdwl",
                          "gp", "ib", "ibadj", "ibc", "ibcom", "icapt", "idit", "intan", "intc", "intpn", "invch", "invfg", "invo", "invrm", 
                          "invt", "invwip", "itcb", "itci", "ivaco", "ivaeq", "ivao", "ivch", "ivncf", "ivst", "ivstch", "lco", "lcox", 
                          "lcoxdr", "lct", "lifr", "lo", "lt", "mib", "mii", "mrc1", "mrc2", "mrc3", "mrc4", "mrc5", "mrct", "msa", "ni",
                          "niadj", "nieci", "nopi", "nopio", "np", "oancf", "ob", "oiadp", "pi", "pidom", "pifo", "ppegt", "ppenb",
                          "ppenc", "ppenli", "ppenme", "ppennr", "ppeno", "ppent", "ppevbb", "ppeveb", "ppevo", "ppevr", "prstkc",
                          "pstk", "pstkc", "pstkl", "pstkn", "pstkr", "pstkrv", "rdip", "re", "rea", "reajo", "recch", "recco", "recd", "rect",
                          "recta", "rectr", "reuna", "sale", "seq", "siv", "spi", "sppe", "sppiv", "sstk", "tlcf", "tstk", "tstkc", "tstkp", 
                          "txach", "txbco", "txc", "txdb", "txdba", "txdbca", "txdbcl", "txdc", "txdfed", "txdfo", "txdi", "txditc", 
                          "txds", "txfed", "txfo", "txndb", "txndba", "txndbl", "txndbr", "txo", "txp", "txpd", "txr", "txs", "txt", "txw",
                          "wcap", "wcapc", "wcapch", "xacc", "xad", "xdepl", "xi", "xido", "xidoc", "xint", "xopr", "xpp", "xpr", "xrd", "xrent",
                          "xsga")


# data lag choices
data_avail_lag = 6 # months
toostale_months = 12 

# signal choices
#   for random ratios, use filterstr = TRUE
#   for yz style, use filterstr = '(V1 %in% scaling_variables) | (V2 %in% scaling_variables)' 
xnames = unique(c(accounting_variables, scaling_variables)) # all the x's under consideration
signal_form = 'ratio' # 'ratio' or 'product' or 'sin+cos' or 'noise'
xusednum = 2 # number of x's used in function
filterstr = TRUE
signalnum = 200 # number of signals to sample (need at least 200, it seems)
seednumber = 1235 # seed sampling

# portfolio choices
reup_months = c(6) # stocks are traded using new data at end of these months
longshort_form = 'ls_extremes'
portnum = 10
sweight = 'ew' # 'ew' or 'vw'


# function for turning xused into a signal
#  pass in alldat as global to save memory
dataset_to_signal = function(form, xusednamecurr){
  
  # make matrix from xused
  xusedcurr = smalldat %>% select(xusednamecurr) %>% as.matrix()
  
  if (form == 'ratio'){ 
    # ratio (for accounting)
    return = xusedcurr[,1]/xusedcurr[,2]

  } else if (form == 'product'){ 
    # product (for nonsense stuff? or maybe not?)
    return = xusedcurr[,1]*xusedcurr[,2]
    
  } else if (form == 'sin+cos'){ 
    # sin + cos  (for really nonsense stuff ??)
    return = sin(xusedcurr[,1])+cos(xusedcurr[,2])
    
  } else if (form == 'noise'){ 
    # pure noise
    return = runif(dim(xusedcurr)[1])
    
  } # end if form
  
} # end dataset_to_signal

signal_to_longshort = function(form, portnum, sweight){
  
  if (form == 'ls_extremes'){
    
    # sweight is zero if data is missing
    if (sweight == 'ew'){
      smalldat$weight = !is.na(smalldat$ret)
    } else if (sweight == 'vw'){
      smalldat$weight = smalldat$me_monthly
      smalldat$weight[is.na(smalldat$weight)] = 0
    }
    
    # find portfolio, rename date (only ret is still left)
    portdat = smalldat %>%
      mutate(port = ntile(signal, portnum)) %>%
      group_by(ret_yearm, port) %>%
      summarize(
        ret = weighted.mean(ret,weight, na.rm=T), .groups = 'drop'
      ) %>%
      rename(date = ret_yearm) %>%
      pivot_wider(
        id_cols = c(port,date), names_from = port, values_from = ret, names_prefix = 'port'
      ) %>%
      mutate(
        ret_ls = (!!as.name(paste0('port', portnum))) - (!!as.name('port1'))
      )
  } # if form
  
} # end signal_to_longshort


# DATA PREP (about 2 minutes) ====
# perhaps should be moved earlier

# do everything in data.table for efficiency
tic = Sys.time()

## import compustat, convert to monthly
comp0 = read_fst('../Data/Intermediate/a_aCompustat.fst') 

# just in case xnames don't line up with comp0
xnames2 = colnames(comp0)
xnames2 = xnames2[xnames2 %in% xnames]

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

# force to x to NA in non-reup-months
alldat[
  , reup_id := month(signaldate) %in% reup_months
][
  , reup_id := if_else(reup_id, 1, NA_real_)
][
  , (xnames2) := lapply(.SD, function(x) x*reup_id ), .SDcols = xnames2 
]

# fill NA with most recent x by permno
#   this takes the most time 
alldat[ , (xnames2) := nafill(.SD, 'locf'), by = .(permno), .SDcols = xnames2]


toc = Sys.time()
toc - tic

rm(comp0,comp1,crsp)
gc()

# SAMPLE STRATEGIES ====
set.seed(seednumber)

## draw sample of xused from full list ====

# make list of all possible xused combinations
full_xused_list = combn(xnames, xusednum) %>% t() %>% as_tibble()

# filter xused (e.g. for economically sensible scaling variables, as in yz)
evalme = paste0(
  'full_xused_list = full_xused_list %>% filter('
  , filterstr
  , ')'
)
eval(parse(text=evalme))


# sample
signalnum = min(signalnum, dim(full_xused_list)[1])
id = sample(1:nrow(full_xused_list), signalnum, replace = F)
xused_list = full_xused_list[id, ] 
# xused_list = xused_list %>% arrange_at(vars(everything())) # sort if desired

# initialize retmat data
ret_dates = alldat %>%  pull(ret_date) %>% unique() %>% sort()
retmat = matrix(nrow = length(ret_dates), ncol = signalnum)

## loop over signals  ====

# loop over sample of xusedsignals
for (signali in 1:signalnum){
  
  ### make one portdat ====
  # select xused for making signal 
  xusednamecurr = xused_list[signali, ] %>% as.matrix %>% as.character()
  
  # import small dataset with return, me, xusedcurr, and add signal
  smalldat = alldat %>% select(permno, ret_yearm, ret, me_monthly, all_of(xusednamecurr))
  smalldat$signal = dataset_to_signal(signal_form, xusednamecurr) # makes a signal

  # assign to portfolios
  #   we should wrap this in a function and give choices
  #   there are many ways to turn signals into a return (or returns)
  portdat = signal_to_longshort(longshort_form, portnum, sweight)

  retmat[, signali] = portdat$ret_ls
  
  # feedback ===
  
  tstat = colMeans(retmat, na.rm=TRUE)/apply(retmat,2,sd,na.rm=T)*sqrt(apply(!is.na(retmat), 2, sum))
  var_tstat = var(tstat, na.rm =T)
  
  print(paste0(
    'signali = ', signali, ' of ', signalnum
    , ' | xname = ', paste(xusednamecurr, collapse = ', ')
    , ' | Var(tstat) = ', round(var_tstat,2)
  ))
  
  hist(tstat)
  
  ## end make one portdat ====
  
} # end for signali


# CLEAN UP AND SAVE ====
ls_dat = cbind(ret_dates, as.data.frame(retmat)) %>% 
  rename(date = ret_dates) %>% 
  pivot_longer(
    cols = -date, names_to = 'signali', names_prefix = 'V', values_to = 'ret'
  )


# compile all info into one list
stratdat = list(
  signal_form = signal_form
  , xused_filter = filterstr
  , longshort_form = longshort_form
  , xused = tibble(signalid = 1:signalnum) %>% cbind(xused_list)
  , ret = ls_dat
)

# save
saveRDS(stratdat, paste0('../Data/LongShortPortfolios/temp_stratdat.RData'))


# Demo results ====

sumstat = stratdat$ret %>% 
  filter(!is.na(ret)) %>% 
  filter(year(date) >= 1963, year(date)<=1993) %>% 
  group_by(signali) %>% 
  summarize(
    rbar = mean(ret), vol = sd(ret), T = n(), tstat = rbar/vol*sqrt(T)
  ) 

hist(sumstat$tstat)

var(sumstat$tstat)

# check alphas don't matter ====
library(lme4)

ffdat <- read_fst('../Data/Intermediate/m_FamaFrenchFactors.fst') %>% 
  as_tibble() 

ffdat


strat_ff = stratdat$ret %>% 
  filter(!is.na(ret)) %>% 
  mutate(yyyymm = year(date)*100 + month(date)) %>% 
  left_join(ffdat)

factor_bench = 'mktrf'

model = as.formula(
  paste0(
    'ret ~ 1'
    ,' + ', paste(factor_bench, collapse = ' + ')
    ,' | signali'
  )
)


# fit model
fit = lmList(model , data=strat_ff) 

coef = summary(fit)$coefficients[ , , 1] # element 1 is for the intercept
port = rownames(coef)

# package nicely
alpha = coef %>% as_tibble 
colnames(alpha) = c('est','se','tstat','pval') # rename for convenience
alpha$port = port
alpha = alpha %>% select(port, everything())
alpha

hist(alpha$tstat)

var(alpha$tstat)
