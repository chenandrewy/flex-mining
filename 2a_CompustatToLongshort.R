# Construct the Compustat long-short strategy universe.
#
# How to run: normally run through 2_DataMining.R from the flex-mining/
#   working directory; it can also be run directly with Rscript.
# Inputs:  cleaned Compustat and CRSP files (../Data/Raw). Valid-denominator
#          metadata is derived in memory here (see derive_valid_denoms).
# Outputs: ../Data/Processed/<dataVersion> LongShort.RData
#          ../Data/tmpAllDat.fst and ../Data/make_many_ls.log (temporary/progress)

source("0_Environment.R")

make_signal_list = function(signal_form, xvars, scale_vars, validDenoms = NULL) {

  #' @param xvars Unique names of variables used for creating strategies
  #' @param scale_vars Scaling variables used in ratios (or NULL for unrestricted)
  #' @param validDenoms Dataset of valid denominator for each combination of signals (from derive_valid_denoms())

  # make list of all possible xused combinations
  tmp = expand.grid(signal_form = signal_form,
                    v1 = xvars,
                    v2 = scale_vars, stringsAsFactors = FALSE) %>%
    as_tibble()

  # Remove v1=v2 for functions where this does not make sense
  # and remove inverse  (e.g. keep only v1/v2 not v2/v1)
  if (!is.null(validDenoms)) {  # new version that removes based on non-zero freq in denominator

    tmp = tmp %>%
      # paste name alphabetically
      mutate(combName = ifelse(v1 < v2, paste(v1, v2, sep='|'),
                               paste(v2, v1, sep='|')) %>% as.character()) %>%
      left_join(validDenoms)  %>%
      mutate(keep = case_when(
        # Keep all growth rate signals
        signal_form  %in% c('diff(v1)/lag(v2)') ~ 1,
        # For ratio signals, keep version with more non-zeros in denominator
        !(v1 %in% scale_vars) ~ 1,
        v1 %in% scale_vars & v2 == denom ~ 1,
        TRUE ~ 0
      )) %>%
      filter(keep == 1) %>%
      select(signal_form, v1, v2)

  } else {  # old version that just removes alphabetically

    tmp = tmp %>%
      mutate(keep = case_when(
        signal_form  %in% c('diff(v1)/lag(v2)') ~ 1,
        # For ratio signals
        v1 %in% scale_vars & v1<= v2 ~ 0,
        TRUE ~ 1)
      ) %>%
      filter(keep == 1) %>%
      select(-keep)
  }

  # remove v2 for signal_forms that use only 1 variable
  tmp = tmp %>%
    mutate(v2 = if_else(signal_form %in% c('pdiff(v1)'), NA_character_, v2)) %>%
    arrange(signal_form, v1, v2) %>%
    distinct(signal_form, v1, v2, .keep_all = T)

  # # sample from full list
  # signalnum = min(signalnum, nrow(tmp))
  # tmp = tmp %>% sample_n(signalnum)

  # clean up
  tmp = tmp %>%
    arrange(across(everything())) %>%
    mutate(signalid = row_number()) %>%
    select(signalid, everything())

  return(tmp)
}







# Derive valid-denominator metadata in memory from raw Compustat.
# Formerly the standalone 1a_ValidDenoms.R, which round-tripped the results
# through DataIntermediate CSVs. 2a is the only consumer, so it is kept in
# memory here: the metadata always matches the raw vintage 2a is mining, and
# there is no regenerable artifact to fall out of sync (~2 min to compute).
#
#' @param comp0 Raw annual Compustat (../Data/Raw/CompustatAnnual.RData).
#' @param keepnames Accounting/CRSP variable names to retain (numer + denom).
#' @return list(freq_obs, validDenoms): 1963 non-missing frequencies used for
#'   denominator eligibility, and the preferred denominator for each variable
#'   pair (the variable with more non-missing obs in the first year both appear).
derive_valid_denoms = function(comp0, keepnames) {

  # fraction of first-per-gvkey rows that are present and non-zero, by year
  year_freq = function(yy) {
    comp0 %>%
      filter(year(datadate) == yy, !is.na(permno)) %>%
      arrange(gvkey, datadate) %>%
      group_by(gvkey) %>%
      filter(row_number() == 1) %>%
      ungroup() %>%
      summarise(across(everything(),
                       function(x) sum(!is.na(x) & x != 0) / length(x))) %>%
      pivot_longer(cols = everything(), names_to = 'name', values_to = 'freq_obs')
  }

  # 1963 frequencies: denominator eligibility screen
  freq_obs = year_freq(1963) %>%
    transmute(name, freq_obs_1963 = freq_obs) %>%
    filter(name %in% keepnames) %>%
    arrange(-freq_obs_1963)

  # annual frequencies for every year, long form
  fobs_list_annual = lapply(1963:max(year(comp0$datadate)), function(yy) {
    year_freq(yy) %>% mutate(year = yy)
  }) %>%
    bind_rows() %>%
    filter(name %in% keepnames)

  # for each ordered variable pair, in the first year both are non-missing,
  # the denominator is whichever has more non-missing obs (ties: alphabetical)
  validDenoms = fobs_list_annual %>%
    full_join(fobs_list_annual, by = 'year', relationship = 'many-to-many') %>%
    filter(name.x != name.y) %>%
    filter(freq_obs.x > 0, freq_obs.y > 0) %>%
    group_by(name.x, name.y) %>%
    filter(row_number() == 1) %>%
    ungroup() %>%
    mutate(combName = ifelse(name.x < name.y, paste(name.x, name.y, sep = '|'),
                             paste(name.y, name.x, sep = '|')) %>% as.character()) %>%
    mutate(denom = ifelse(freq_obs.x > freq_obs.y |
                            (freq_obs.x == freq_obs.y & name.x < name.y),
                          name.x, name.y)) %>%
    select(combName, year, denom) %>%
    distinct()

  list(freq_obs = freq_obs, validDenoms = validDenoms)
}


# function for turning xused into a signal
dataset_to_signal = function(form, dt, v1, v2){

  stopifnot("form must be one of ratio, ratioChange, ratioChangePct,
            levelChangePct, levelChangeScaled, levelsChangePct_Change, noise" =
              form %in% c('v1/v2', 'diff(v1/v2)', 'pdiff(v1/v2)',
                          'pdiff(v1)', 'diff(v1)/lag(v2)', 'pdiff(v1)-pdiff(v2)', 'noise'))

  if (form == 'v1/v2'){

    dt[,'tmp'] = dt[, 'v1']/dt[, 'v2']
    return(dt %>% pull(tmp))

  } else if (form == 'diff(v1/v2)') {

    dt[,'tmp'] = dt[, 'v1']/dt[, 'v2']
    return(
      dt %>%
        arrange(permno, ret_yearm) %>%
        group_by(permno) %>%
        mutate(tmp2 = tmp - lag(tmp, 12)) %>%
        ungroup() %>%
        pull(tmp2)
    )

  } else if (form == 'pdiff(v1/v2)') {

    dt[,'tmp'] = dt[, 'v1']/dt[, 'v2']
    return(
      dt %>%
        arrange(permno, ret_yearm) %>%
        group_by(permno) %>%
        mutate(tmp2 = 100*(tmp - lag(tmp, 12))/lag(tmp, 12)) %>%
        ungroup() %>%
        pull(tmp2)
    )

  } else if (form == 'diff(v1)/lag(v2)') {
    dt[,'tmp'] = dt[, 'v1']
    dt[,'tmp2'] = dt[, 'v2']
    return(
      dt %>%
        arrange(permno, ret_yearm) %>%
        group_by(permno) %>%
        mutate(tmp3 = (tmp - lag(tmp, 12))/lag(tmp2, 12)) %>%
        ungroup() %>%
        pull(tmp3)
    )

  } else if (form == 'pdiff(v1)-pdiff(v2)') {
    dt[,'tmp'] = dt[, 'v1']
    dt[,'tmp2'] = dt[, 'v2']
    return(
      dt %>%
        arrange(permno, ret_yearm) %>%
        group_by(permno) %>%
        mutate(tmp3 = 100*(tmp - lag(tmp, 12))/lag(tmp, 12),
               tmp4 = 100*(tmp2 - lag(tmp2, 12))/lag(tmp2, 12)) %>%
        ungroup() %>%
        mutate(tmp5 = tmp3 - tmp4) %>%
        pull(tmp5)
    )


  } else if (form == 'pdiff(v1)') {

    dt[,'tmp'] = dt[, 'v1']
    return(
      dt %>%
        arrange(permno, ret_yearm) %>%
        group_by(permno) %>%
        mutate(tmp2 = 100*(tmp - lag(tmp, 12))/lag(tmp, 12)) %>%
        ungroup() %>%
        pull(tmp2)
    )

  } else if (form == 'noise'){
    # pure noise
    return = runif(dim(xusedcurr)[1])
  } # end if form

} # end dataset_to_signal




signal_to_ports = function(dt0, form, portnum, sweight, trim = NULL){

  dt = dt0 %>% filter(!is.na(signal), !is.na(ret), is.finite(signal))

  if (form == 'ls_extremes'){

    # sweight is zero if data is missing
    if (sweight == 'ew'){
      dt$weight = !is.na(dt$ret)
    } else if (sweight == 'vw'){
      dt$weight = dt$me_monthly
      dt$weight[is.na(dt$weight)] = 0
    }

    # Potential preprocessing of signal values
    if (!is.na(trim)) {

      dt = dt %>%
        filter(signal >= quantile(dt$signal, trim, na.rm = TRUE),
               signal <= quantile(dt$signal, 1-trim, na.rm = TRUE)
        )
    }

    # # find breakpoints
    # # based on email with LingLing Zheng 2023 01
    # # she used proc rank with group output and ties = min
    # # see https://blogs.sas.com/content/sgf/2019/07/19/how-the-rank-procedure-calculates-ranks-with-groups-and-ties/
    dt = dt %>%
      group_by(ret_yearm) %>%
      mutate(
        rank = rank(signal, ties.method = 'min')
        , group = floor(rank*portnum / (n()+1)) + 1
        , port = case_when(
          group == 1 ~ 'short'
          , group == portnum ~ 'long'
        )
      ) %>%
      ungroup()

    # find long-short return, rename date (only ret is still left)
    dt = dt %>%
      filter(!is.na(port)) %>%
      group_by(ret_yearm, port) %>%
      summarize(
        ret = weighted.mean(ret,weight, na.rm=T)
        , nstock = n()
        , .groups = 'drop'
      ) %>%
      rename(yearm = ret_yearm)

    # more error checking
    if (dim(dt %>% filter(port == 'short'))[1] == 0){
      print('No short portfolios, returning empty tibble')
      return(tibble())
    }
    if (dim(dt %>% filter(port == 'long'))[1] == 0){
      print('No long portfolios, returning empty tibble')
      return(tibble())
    }

    # find long-short return
    dt_ls = dt %>%
      select(yearm, port, ret, nstock) %>%
      pivot_wider(names_from = port, values_from = c(ret, nstock)) %>%
      mutate(ret_ls = ret_long - ret_short) %>%
      filter(!is.na(ret_ls)) %>%
      transmute(yearm, ret = ret_ls, nstock_long, nstock_short)

    return(dt_ls)

  } # if form

} # end signal_to_ports

make_many_ls = function(){
  ### make one portdat ===

  # extract current settings
  signal_cur = signal_list[signali,]

  # import small dataset with return, me, xusedcurr, and add signal
  if (is.na(signal_cur$v2) | signal_cur$v1 == signal_cur$v2) { # If only one variable needed to construct signal
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

  # Unify column names for processing
  if (is.na(signal_cur$v2)) {
    colnames(smalldat) = c('permno', 'ret_yearm', 'ret', 'me_monthly', 'v1')
  } else if (signal_cur$v1 == signal_cur$v2) {
    colnames(smalldat) = c('permno', 'ret_yearm', 'ret', 'me_monthly', 'v1')
    smalldat = smalldat %>% mutate(v2 = v1)
  } else {
    colnames(smalldat) = c('permno', 'ret_yearm', 'ret', 'me_monthly', 'v1', 'v2')
  }

  tic = Sys.time() #
  smalldat$signal = dataset_to_signal(form = signal_cur$signal_form,
                                      dt = smalldat) # makes a signal
  toc = Sys.time() #
  print('signal done')
  print(toc - tic) #


  tic = Sys.time() #
  # assign to portfolios
  portdat = tibble()
  for (porti in 1:dim(port_list)[1]){
    tempport = signal_to_ports(dt0 = smalldat,
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

# "form portfolios based on the first, second, and third letters of the ticker symbol"

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

# Valid-denominator metadata, derived in memory from raw Compustat (see
# derive_valid_denoms above). comp0 stays loaded and is reused by the data-prep
# block below, so CompustatAnnual.RData is read only once.
comp0 = readRDS('../Data/Raw/CompustatAnnual.RData')
valid_denom_meta = derive_valid_denoms(
  comp0, keepnames = union(compnames$yz.numer, compnames$yz.denom)
)

denom_ok = valid_denom_meta$freq_obs %>%
  filter(freq_obs_1963 > user$signal$denom_min_fobs)

validDenoms = valid_denom_meta$validDenoms

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
  ## comp0 (raw annual Compustat) is already in memory from the
  ## valid-denominator step above; reuse it rather than re-reading the file.
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
