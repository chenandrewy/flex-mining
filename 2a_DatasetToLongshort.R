tic = Sys.time()

rm(list = ls())

# Setup  ----------------------------------------------------------------


debugSource('0_Environment.R')
library(doParallel)
env <- foreach:::.foreachGlobals # https://stackoverflow.com/questions/64519640/error-in-summary-connectionconnection-invalid-connection
rm(list=ls(name=env), pos=env)

threads_fst(1) # since fst is used inside foreach, might want to limit cpus, though this doesn't seem to help

## Output settings ----
user = list()

# name of output file (by default use time and date)
user$name = Sys.time() %>% substr(1,17) 
substr(user$name, 17,17) = 'm'
substr(user$name, 14,14) = 'h'
# user$name = 'deleteme2'

# signal choices
user$signal = list(
  signalnum   = Inf # number of signals to sample or Inf for all
  , form = c('v1/v2', 'diff(v1/v2)', 'pdiff(v1/v2)',
                  'pdiff(v1)', 'diff(v1)/lag(v2)', 'pdiff(v1)-pdiff(v2)')
  , x1code = 'yz.numer'
  , x2code = 'yz.denom'
  , seednumber  = 1235 # seed sampling
)

# portfolio choices
user$port = list(
  longshort_form = 'ls_extremes'
  , portnum        = c(10)
  , sweight        = c('ew','vw') 
  , trim           = NA_real_  # NA or some quantile e.g. .005
  
)

# data basic choices
user$data = list(
  backfill_dropyears = 1 # number of years to drop for backfill bias adj
  , reup_months    = 6 # stocks are traded using new data at end of these months
  , data_avail_lag = 6 # months
  , toostale_months = 18 # months after datadate to keep signal for  
  , delist_adj = 'none' # 'none' or 'ghz'
  , crsp_filter = 'shrcd %in% c(10,11) & abs(prc) >= 1 & floor(siccd/1000) != 6' # use NA_character_ for no filter
)

# debugging
debugset = list(
  prep_data = T
  , num_cores = round(.5*detectCores())  # Adjust number of cores used as you see fit
  # , num_cores = 1 # use num_cores = 1 for serial  
  , shortlist = F
)

## Check ----
cat('\n\n\n\n\n\n\n\n\n')
cat('About to run:')
print(user$signal %>% t())
print(user$port %>% t())
print(user$data %>% t())
print(user$name) 
print(debugset %>% t())
keyin = readline('type q to abort')
if (keyin == 'q') stop('stopping')

# Data Prep ---------------------------------------------------------------
tic = Sys.time()


## prep varlist ------------------------------------------------------------

if (debugset$shortlist == F){
  varlist = list(
    x1 = compnames[[user$signal$x1code]]
    , x2 = compnames[[user$signal$x2code]]
  )
} else {
  varlist = list(
    x1 = c('fopt')
    , x2 = c('at')
  )  
}
varlist$xall = unique(c(varlist$x1, varlist$x2))


## prep crsp-comp ----------------------------------------------------------

if (debugset$prep_data){  
  # do everything in data.table for efficiency  
  
  # safety delete
  file.remove('../Data/tmpAllDat.fst')
  
  # import and merge ===
  ## import compustat, convert to monthly
  comp0 = readRDS('../Data/Intermediate/CompustatAnnual.RData')
  comp1 = copy(comp0 %>% select(all_of(varlist$xall), gvkey, permno, datayearm))
  
  #Yan and Zheng (2017): To mitigate a backfilling bias, we require that a firm be listed on Compustat for two years
  #before it is included in our sample (Fama and French 1993)
  setorder(comp1, gvkey, permno, datayearm)
  comp1[, years_on_comp := datayearm - min(datayearm), by = 'gvkey']
  comp1 = comp1[years_on_comp >= user$data$backfill_dropyears][, !c('years_on_comp')]
  
  # keep only if permno is ok (must be done after backfill adjustment)
  comp1 = comp1[!is.na(permno)]
  
  # # debug 
  # comp1 = comp1[gvkey %in% c('013007', '012994') & datayearm <= 1990]

  # lag signal, then keep around until signalyearm == datadate + toostale_months
  comp1[ , signalyearm := datayearm + user$data$data_avail_lag/12]
  
  comp2 = copy(comp1)
  for (lagi in (user$data$data_avail_lag+1):user$data$toostale_months){
    temp = copy(comp1)
    temp[ , signalyearm := datayearm + lagi/12]
    comp2 = rbind(comp2, temp)
  }
  
  # remove duplicate permno-signalyearm, keep most recent datayearm
  setorder(comp2, permno, signalyearm, -datayearm)
  comp2 = comp2[, head(.SD, 1), by = .(permno, signalyearm)]
  
  # enforce updating frequency (aka rebalancing) 
  comp2[
    , temp_month := as.numeric(12*(signalyearm %% 1)+1)
    ][
    , reup_id := if_else(temp_month %in% user$data$reup_months, 1, NA_real_)
  ][
    , (varlist$xall) := lapply(.SD, function(x) x*reup_id ), .SDcols = varlist$xall 
  ]
  
  # fill NA with most recent x by permno 
  setorder(comp2, permno, signalyearm, -datayearm)
  comp2[ , (varlist$xall) := nafill(.SD, 'locf'), by = .(permno, datayearm), .SDcols = varlist$xall]
  comp2[ , (c('temp_month', 'reup_id')) := NULL]
  
  # import crsp data
  crsp = readRDS('../Data/Intermediate/crspm.RData')
  setDT(crsp)
  
  # timing adjustments
  setorder(crsp, permno, yearm)
  lagme = setdiff(names(crsp), c('permno','yearm','ret','retx','dlret'))
  crsp[ , (lagme) := shift(.SD, n=1, type = 'lag'), by = permno, .SDcols = lagme]
  setnames(crsp, old = 'yearm', new = 'ret_yearm')  

  # filters
  if (!is.na(user$data$crsp_filter)) {
    crsp = crsp %>%
      filter(eval(parse(text=user$data$crsp_filter)))
  }
  
  # incorporate delisting return
  # GHZ cite Johnson and Zhao (2007), Shumway and Warther (1999)
  # but the way HXZ does this might be a bit better
  if (user$data$delist_adj == 'ghz') {
    crsp = crsp %>%
      mutate(
        dlret = ifelse(
          is.na(dlret)
          & (dlstcd == 500 | (dlstcd >=520 & dlstcd <=584))
          & (exchcd == 1 | exchcd == 2)
          , -35
          , dlret
        )
        , dlret = ifelse(
          is.na(dlret)
          & (dlstcd == 500 | (dlstcd >=520 & dlstcd <=584))
          & (exchcd == 3)
          , -55
          , dlret
        )
        , dlret = ifelse(
          dlret < -100 & !is.na(dlret)
          , -100
          , dlret
        )
        , dlret = ifelse(
          is.na(dlret)
          , 0
          , dlret
        )
        , ret = (1+ret/100)*(1+dlret/100)-1
        , ret = ifelse(
          is.na(ret) & ( dlret != 0)
          , dlret
          , ret
        )
      )
  }
  
  # clean up crsp
  crsp = crsp %>%
    transmute(
      permno, ret_yearm, ret, me_monthly = me
    )
  
  # merge, signals available at signalyearm get used for returns in ret_yearm
  comp2[ , ret_yearm := signalyearm + 1/12]
  alldat = merge(crsp, comp2, all.x=TRUE, by = c('permno','ret_yearm'))

  # Save to fst such that parallelization works without having to load big file onto each worker
  fst::write_fst(alldat, '../Data/tmpAllDat.fst')
  
  # used later
  ret_dates = alldat %>% transmute(date = ret_yearm) %>% as_tibble() %>%  distinct() %>% arrange()
  
  
  rm(comp0,comp1,comp2,crsp,alldat)
  gc()
  
} # end prepare_data



toc = Sys.time()
print('done prepping data')
toc - tic



# Sample Strategies -------------------------------------------------------

## Draw lists of signals and ports ------------------------------------------

# ac 2022 12: this function doesn't seem to work right
# signal_list = make_signal_list(signal_form =  user$signal$form,
#                                xvars = user$signal$x1list,
#                                signalnum = user$signal$signalnum,
#                                scale_vars = user$signal$x2list,
#                                rs = user$signal$seednumber)

signal_list = make_signal_list_yz(signal_form = user$signal$form
                               , x1list = varlist$x1
                               , x2list = varlist$x2
                               , signalnum = user$signal$signalnum
                               , seed = user$signal$seednumber)

# port list
port_list = expand.grid(longshort_form = user$port$longshort_form, 
                        portnum = user$port$portnum, 
                        sweight = user$port$sweight,
                        trim = user$port$trim,
                        stringsAsFactors = FALSE) %>% 
  arrange(across(everything())) %>% 
  mutate(portid = row_number()) %>% 
  select(portid, everything())  

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
# call make_many_ls (this is where the action is) 

tic_loop = Sys.time()

if (debugset$num_cores > 1){
  setDTthreads(1)
  cl <- makePSOCKcluster(num_cores)
  registerDoParallel(cl)
  file.remove('../Data/make_many_ls.log')
  ls_dat_all = foreach(signali=1:nrow(signal_list), 
                       .combine = rbind,
                       .packages = c('tidyverse','zoo')) %dopar% {
                         
                         if (signali %% 100 == 0){
                           min_elapsed = round(as.numeric(Sys.time() - tic_loop, units = 'mins'), 1)
                           i_remain = nrow(signal_list) - signali
                           log.text <- paste0(
                             Sys.time()
                             , " signali = ", signali
                             , " of ", dim(signal_list)[1]
                             , ",  min elapsed = ", round(as.numeric(Sys.time() - tic_loop, units = 'mins'), 1)
                             , ",  min per 1000 signals = ", round(min_elapsed / signali *1000, 2) 
                             , ",  min remain = ", round(min_elapsed / signali * i_remain, 1)
                           )
                           write.table(log.text, "../Data/make_many_ls.log", append = TRUE, row.names = FALSE, col.names = FALSE)
                         }
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
  , signal_list = signal_list
  , port_list = port_list
  , user = user
  , name = user$name
)

# save
saveRDS(stratdat, paste0('../Data/LongShortPortfolios/stratdat ',
                         stratdat$name, 
                         '.RData'))

toc = Sys.time()
print(toc - tic)






# Check on Console --------------------------------------------------------


q = c(1, 5, 10, 25, 50)/100
q = c(q, 1-q) %>% sort() %>% unique()



stratdat$ret %>% 
  filter(yearm >= 1964, yearm <= 2012) %>% 
  left_join(
    port_list, by = 'portid'
  ) %>% 
  group_by(signalid, sweight) %>% 
  summarize(
    rbar = mean(ret), nmonth = n(), tstat = rbar/sd(ret)*sqrt(nmonth)
  ) %>% 
  filter(nmonth >= 2) %>% 
  group_by(sweight) %>% 
  summarize(
    q = q, rbar_q = quantile(rbar, q), nmonth_q = quantile(nmonth, q), tstat_q = quantile(tstat, q)
  ) %>% 
  pivot_wider(
    names_from = 'sweight', values_from = ends_with('_q')
  )

user$data %>% t()
