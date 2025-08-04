tic = Sys.time()

# takes about 3 hours using 14 cores
# check Data/make_many_ls.log for progress

# Setup  ----------------------------------------------------------------
library(doParallel)
env <- foreach:::.foreachGlobals # https://stackoverflow.com/questions/64519640/error-in-summary-connectionconnection-invalid-connection
rm(list=ls(name=env), pos=env)

threads_fst(1) # since fst is used inside foreach, might want to limit cpus, though this doesn't seem to help

## Output settings ----
user = list()

# name of output file (by default use time and date)
# user$name = Sys.time() %>% substr(1,17) 
# substr(user$name, 17,17) = 'm'
# substr(user$name, 14,14) = 'h'
user$name = globalSettings$dataVersion

# signal choices
user$signal = list(
  signalnum = globalSettings$signalnum
  , form    = globalSettings$form
  , denom_min_fobs = globalSettings$denom_min_fobs
)

# portfolio choices
user$port = list(
  longshort_form = globalSettings$longshort_form
  , portnum = globalSettings$portnum
  , sweight = globalSettings$sweight 
  , trim    = globalSettings$trim
)

# data basic choices
user$data = list(
  backfill_dropyears = globalSettings$backfill_dropyears
  , reup_months     = globalSettings$reup_months
  , data_avail_lag  = globalSettings$data_avail_lag
  , toostale_months = globalSettings$toostale_months   
  , delist_adj      = globalSettings$delist_adj
  , crsp_filter     = globalSettings$crsp_filter 
)

# debugging
debugset = list(
  prep_data   = globalSettings$prep_data
  , num_cores = globalSettings$num_cores
  , shortlist = globalSettings$shortlist
)

## prep varlist ------------------------------------------------------------

numer_ok = readxl::read_excel('DataInput/Yan-Zheng-Compustat-Vars.xlsx') %>% 
  janitor::clean_names() %>% 
  filter(in_yz_table_a_1 == 1 | in_yz_table_a_2 == 1) %>% 
  mutate(name = tolower(acronym))

denom_ok = fread('DataIntermediate/freq_obs_1963.csv') %>% 
  filter(freq_obs_1963 > user$signal$denom_min_fobs) 

validDenoms = read_csv('DataIntermediate/validDenomsCombinations.csv')

if (debugset$shortlist == F){
  varlist = list(
    x1 = numer_ok$name
    , x2 = denom_ok$name
  )
} else {
  varlist = list(
    x1 = c('invt')
    , x2 = c('at')
  )  
}

varlist$xall = unique(c(varlist$x1, varlist$x2))

## prep lists of signals and ports ------------------------------------------

signal_list = make_signal_list(signal_form = user$signal$form,
                               xvars       = varlist$x1,
                               scale_vars  = varlist$x2,
                               validDenoms = validDenoms)

# port list
port_list = expand.grid(longshort_form = user$port$longshort_form, 
                        portnum = user$port$portnum, 
                        sweight = user$port$sweight,
                        trim = user$port$trim,
                        stringsAsFactors = FALSE) %>% 
  arrange(across(everything())) %>% 
  mutate(portid = row_number()) %>% 
  select(portid, everything())  

# sanity check
n1 = varlist$x1 %>% length()
n2 = varlist$x2 %>% length()

## Check user is OK ----
cat('\n\n\n\n\n\n\n\n\n')
cat('About to run:')
print(user$signal %>% t())
print(user$port %>% t())
print(user$data %>% t())
print(debugset %>% t())
print('number of signals is ')
print(nrow(signal_list))
print(n1 * n2 + (n1-n2)*n2 + choose(n2,2))
print(n1)
print(n2)
print(user$name) 
if (globalSettings$interactive_mode) {
  keyin = readline('type q to abort')
  if (keyin == 'q') stop('stopping')
}


# Create big temp data ---------------------------------------------------------------
tic = Sys.time()

if (debugset$prep_data){  
  # do everything in data.table for efficiency  
  
  # safety delete
  file.remove('../Data/tmpAllDat.fst')
  
  # import and merge ===
  ## import compustat, convert to monthly
  comp0 = readRDS('../Data/Raw/CompustatAnnual.RData')
  comp1 = copy(comp0 %>% select(all_of(varlist$xall), gvkey, permno, datayearm))
  
  #Yan and Zheng (2017): To mitigate a backfilling bias, we require that a firm be listed on Compustat for two years
  #before it is included in our sample (Fama and French 1993)
  setorder(comp1, gvkey, permno, datayearm)
  comp1[, years_on_comp := datayearm - min(datayearm), by = 'gvkey']
  comp1 = comp1[years_on_comp >= user$data$backfill_dropyears][, !c('years_on_comp')]
  
  # keep only if permno is ok (must be done after backfill adjustment)
  comp1 = comp1[!is.na(permno)]
  
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
  #   this takes all the time
  setorder(comp2, permno, signalyearm, -datayearm)
  comp2[ , (varlist$xall) := nafill(.SD, 'locf'), by = .(permno, datayearm), .SDcols = varlist$xall]
  comp2[ , (c('temp_month', 'reup_id')) := NULL]
  
  # import crsp data
  crsp = readRDS('../Data/Raw/crspm.RData')
  setDT(crsp)
  
  # timing adjustments
  setorder(crsp, permno, yearm)
  lagme = setdiff(names(crsp), c('permno','yearm','ret','retx','dlret'))
  crsp[ , (lagme) := data.table::shift(.SD, n=1, type = 'lag'), by = permno, .SDcols = lagme]
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
        , ret = 100*((1+ret/100)*(1+dlret/100)-1)
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

## Loop over signals -------------------------------------------------------
# call make_many_ls (this is where the action is) 

print('Constructing long-short portfolios')
print('Check ../Data/make_many_ls.log for progress')

tic_loop = Sys.time()

if (debugset$num_cores > 1){
  setDTthreads(1)
  cl <- makePSOCKcluster(debugset$num_cores)

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
saveRDS(stratdat, paste0('../Data/Processed/',
                         stratdat$name, 
                         ' LongShort.RData'))

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
