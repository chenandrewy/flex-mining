# Setup -------------------------------------------------------------------------

rm(list = ls())

source("0_Environment.R")

library("dplyr")
library("tidyverse")
library("magrittr")
library(doParallel)

# settings
ncores <- round(detectCores() / 2)

dmcomp = list()
dmtic  = list()
dmcomp$name = "../Data/Processed/CZ-style-v6 LongShort.RData"
dmtic$name = '../Data/Processed/ticker_Harvey2017JF.RDS'

## Load Global Data ---------------------------------------------------------------

# these are treated as globals (don't modify pls)
czsum <- readRDS("../Data/Processed/czsum_all207.RDS") %>% 
  filter(Keep)

czcat <- fread("DataIntermediate/TextClassification.csv") %>%
  select(signalname, Year, theory1, misprice_risk_ratio)

czret <- readRDS("../Data/Processed/czret.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100)

# In-samp Sumstats ------------------------------------------------------
## Declare function --------------------------------------------------------

# function for computing DM strat sumstats in pub samples
sumstats_for_DM_Strats = function(
    DMname = "../Data/Processed/CZ-style-v6 LongShort.RData"
    , nsampmax = Inf
    ){
  
  # read in DM strats
  dm_rets <- readRDS(DMname)$ret
  dm_info <- readRDS(DMname)$port_list
  
  dm_rets <- dm_rets %>%
    left_join(
      dm_info %>% select(portid, sweight),
      by = c("portid")
    ) %>%
    transmute(
      sweight,
      dmname = signalid,
      yearm,
      ret,
      nstock
    ) %>% 
    setDT()
  
  # Finds sum stats for dm in each pub sample
  # the output for this can be used for all dm selection methods
  samplist <- czsum %>%
    distinct(sampstart, sampend) %>%
    arrange(sampstart, sampend)
  
  # set up for parallel
  cl <- makePSOCKcluster(ncores)
  registerDoParallel(cl)
  
  # loop setup
  nsamp <- dim(samplist)[1] 
  nsamp = min(nsamp,nsampmax)
  dm_insamp <- list()
  
  # dopar in a function needs some special setup 
  # https://stackoverflow.com/questions/6689937/r-problem-with-foreach-dopar-inside-function-called-by-optim
  dm_insamp <- foreach(
    sampi = 1:nsamp,
    .combine = rbind,
    .packages = c("data.table", "tidyverse", "zoo"),
    .export = ls(envir = globalenv())
  ) %dopar% {
    
    # feedback
    print(paste0("DM sample stats for sample ", sampi, " of ", nsamp))
    
    # find sum stats for the current sample
    sampcur <- samplist[sampi, ]
    sumcur <- dm_rets[
      yearm >= sampcur$sampstart &
        yearm <= sampcur$sampend &
        !is.na(ret),
      .(
        rbar = mean(ret), tstat = mean(ret) / sd(ret) * sqrt(.N),
        min_nstock = min(nstock)
      ),
      by = c("sweight", "dmname")
    ]
    
    # find number of obs in the last year of the sample
    filtcur <- dm_rets[
      floor(yearm) == year(sampcur$sampend) &
        !is.na(ret),
      .(nlastyear = .N),
      by = c("sweight", "dmname")
    ]
    
    # combine and save
    sumcur = sumcur %>%
      left_join(filtcur, by = c("sweight", "dmname")) %>%
      mutate(
        sampstart = sampcur$sampstart, sampend = sampcur$sampend
      )
    
    return(sumcur)
  } # end dm_insamp loop
  stopCluster(cl)
  
  # Merge with czsum
  # insampsum key is c(pubname,dmname). Each row is a dm strat that matches a pub
  insampsum <- czsum %>%
    transmute(
      pubname = signalname, rbar_op = rbar, tstat_op = tstat, sampstart, sampend,
      sweight = tolower(sweight)
    ) %>%
    left_join(
      dm_insamp,
      by = c("sampstart", "sampend", "sweight"),
      relationship = "many-to-many" # required to suppress warning
    ) %>%
    arrange(pubname, desc(tstat))
  
  setDT(insampsum)
  
  return(insampsum)
}  # end Sumstats function

## Run Function on Compustat DM ----------------------------------------

print("creating Compustat mining in-sample sumstats")
print('Takes about 4 minutes using 4 cores')
start_time <- Sys.time()
dmcomp$insampsum = sumstats_for_DM_Strats(
  DMname = dmcomp$name
  , nsampmax = Inf
)
print("finished")
stop_time <- Sys.time()
stop_time - start_time

## Run Function on Ticker DM ----------------------------------------

print("creating ticker mining in-sample sumstats")
print('Takes about 20 sec using 9 cores')
start_time <- Sys.time()
dmtic$insampsum = sumstats_for_DM_Strats(
  DMname = dmtic$name
  , nsampmax = Inf
)
print("end DM summary stats")
stop_time <- Sys.time()
stop_time - start_time

# Convenience Save --------------------------------------------------------
save.image('../Data/tmpAltDMPlots.RData')

# Convenience Load --------------------------------------------------------
load('../Data/tmpAltDMPlots.RData')

# Functions for making event time returns ---------------------------------

SelectDMStrats <- function(insampsum, settings) {
  # input:
  #     insampsum = summary stats for each pubname, dmname combination
  #     dmset = settings for selection
  # output: matchcur = all pubname, dmname that satisfy dmset
  
  # add derivative statistics
  insampsum <- insampsum %>%
    group_by(sweight, sampstart, sampend) %>%
    arrange(desc(abs(tstat))) %>%
    mutate(rank_tstat = row_number()) %>%
    arrange(desc(abs(rbar))) %>%
    mutate(rank_rbar = row_number(), n_dm_tot = n()) %>% 
    mutate(
      diff_rbar = abs(rbar * sign(rbar) - rbar_op),
      diff_tstat = abs(tstat * sign(rbar) - tstat_op)
    ) %>% 
    setDT()
    
  # filter
  matchcur <- insampsum[
    diff_rbar <= settings$r_tol &
      diff_tstat <= settings$t_tol &
      diff_rbar / rbar_op <= settings$r_reltol &
      diff_tstat / tstat_op <= settings$t_reltol &
      min_nstock >= settings$minNumStocks &
      nlastyear == 12 &
      abs(tstat) >= settings$t_min &
      rank_tstat / n_dm_tot <= settings$t_top_pct / 100
  ]

  print("summary of matching:")
  matchcur[, .(n_dm_match = .N, sampstart = min(sampstart), sampend = min(sampend)), by = "pubname"] %>%
    arrange(-n_dm_match) %>%
    print()

  return(matchcur)

  print("end selectStrats")
}

make_DM_event_returns <- function(
    match_strats
    , DMname = "../Data/Processed/CZ-style-v6 LongShort.RData"
    , npubmax = Inf
    , czsum
    ) {
  # input: match_strats = summary stats for each selected pubname, dmname pair
  #     outname = name of RDS output
  # you need to pass in czsum (can't use the global) because of
  # a mysterious dopar error (object 'czum' not found)
  # output: for each pubname-eventDate, average dm returns
  gc()
  
  # read in DM strats
  dm_rets <- readRDS(DMname)$ret
  dm_info <- readRDS(DMname)$port_list
  
  dm_rets <- dm_rets %>%
    left_join(
      dm_info %>% select(portid, sweight),
      by = c("portid")
    ) %>%
    transmute(
      sweight,
      dmname = signalid,
      yearm,
      ret,
      nstock
    ) %>% 
    setDT()  

  cl <- makePSOCKcluster(ncores)
  registerDoParallel(cl)
  npub <- dim(czsum)[1] 
  npub = min(npub,npubmax)
  event_dm_scaled <- foreach(
    pubi = 1:npub,
    .combine = rbind,
    .packages = c("data.table", "tidyverse", "zoo")
  ) %dopar% {
    
    # feedback
    print(paste0("pubi ", pubi, " of ", npub))

    pubcur <- czsum[pubi, ]

    # select matching dm strats for the current pubname
    matchcur <- match_strats[pubname == pubcur$signalname]

    matchcur <- matchcur %>%
      transmute(sweight, dmname, sign = sign(rbar), rbar)

    # make an event time panel
    eventpan <- dm_rets %>%
      inner_join(matchcur, by = c("sweight", "dmname")) %>%
      transmute(
        candSignalname = dmname,
        eventDate = as.integer(round(12 * (yearm - pubcur$sampend))),
        sign,
        # Sign returns and scale
        ret_scaled = ret * sign / abs(rbar) * 100,
        samptype = case_when(
          (yearm >= pubcur$sampstart) & (yearm <= pubcur$sampend) ~ "insamp",
          (yearm > pubcur$sampend) ~ "oos",
          TRUE ~ NA_character_
        )
      )

    # average down to one matched return per event date
    eventsumscaled <- eventpan[, .(dm_mean = mean(ret_scaled), dm_sd = sd(ret_scaled), dm_n = .N),
      by = c("eventDate")
    ] %>%
      mutate(
        pubname = pubcur$signalname
      )

    return(eventsumscaled)
  } # end do pubi = 1:npub

  stopCluster(cl)

  return(event_dm_scaled)
} # end MakeMatchedPanel

# wrapper function for selection and event returns
select_and_make_panel = function(matchset, dmname, insampsum){
  output = list()
  
  output$matched <- SelectDMStrats(insampsum, matchset)
  
  print("Making event time returns")
  print('Takes up to 1 minute on 4 cores')
  start_time = Sys.time()
  output$event_time <- make_DM_event_returns(
    output$matched, dmname, npubmax = Inf, czsum = czsum
  )
  stop_time <- Sys.time()
  stop_time - start_time  
  
  return(output)
}

# t_min plots ---------------------------------------------------------------

## Plot abs(t) > 2 ----------------------------------------------------------
plotdat = list()

plotdat$name = 't_min_2'

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf, 
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 2.0, # Default = 0, minimum data mined t-stat
  t_top_pct = 100, # top x% of data mined t-stats (set to 10 or less)
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

# make event time returns for Compustat DM
temp = select_and_make_panel(plotdat$matchset, dmcomp$name, dmcomp$insampsum)
plotdat$comp_matched = temp$matched
plotdat$comp_event_time = temp$event_time

# make event time returns for ticker DM
temp = select_and_make_panel(plotdat$matchset, dmtic$name, dmtic$insampsum)
plotdat$tic_matched = temp$matched
plotdat$tic_event_time = temp$event_time

# join and reformat for plotting function
# it wants columns:  (eventDate, ret, matchRet, matchRetAlt)
# dm_mean returns are already scaled (see above)
ret_for_plotting = czret %>% 
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>% 
  left_join(
    plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
  ) %>% 
  left_join(
    plotdat$tic_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)
  )  %>% 
  select(eventDate, ret, matchRet, matchRetAlt, pubname)  %>% 
  # keep only rows where both matchrets are observed
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

# plot
legprefix = paste0('|t|>', plotdat$matchset$t_min)
ReturnPlotsWithDM(
  dt = ret_for_plotting,
  basepath = "../Results/Fig_AltDM",
  suffix = plotdat$name,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -30,
  legendlabels = 
  c(paste0('Pub w/ Mining Match (', length(unique(ret_for_plotting$pubname)), ')'),
   paste0(legprefix, ' Mining Compustat'), paste0(legprefix, ' Mining Tickers')),
   legendpos = c(82,90)/100
)

## Plot abs(t) > 3 ----------------------------------------------------------
plotdat = list()

plotdat$name = 't_min_3'

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf, 
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 3.0, # Default = 0, minimum data mined t-stat
  t_top_pct = 100, # top x% of data mined t-stats (set to 10 or less)
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

# make event time returns for Compustat DM
temp = select_and_make_panel(plotdat$matchset, dmcomp$name, dmcomp$insampsum)
plotdat$comp_matched = temp$matched
plotdat$comp_event_time = temp$event_time

# make event time returns for ticker DM
temp = select_and_make_panel(plotdat$matchset, dmtic$name, dmtic$insampsum)
plotdat$tic_matched = temp$matched
plotdat$tic_event_time = temp$event_time

# join and reformat for plotting function
# it wants columns:  (eventDate, ret, matchRet, matchRetAlt)
# dm_mean returns are already scaled (see above)
ret_for_plotting = czret %>% 
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>% 
  left_join(
    plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
  ) %>% 
  left_join(
    plotdat$tic_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)
  )  %>% 
  select(eventDate, ret, matchRet, matchRetAlt, pubname)  %>% 
  # keep only rows where both matchrets are observed
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

# plot
legprefix = paste0('|t|>', plotdat$matchset$t_min)
ReturnPlotsWithDM(
  dt = ret_for_plotting,
  basepath = "../Results/Fig_AltDM",
  suffix = plotdat$name,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -30,
  legendlabels = 
  c(paste0('Pub w/ Mining Match (', length(unique(ret_for_plotting$pubname)), ')'),
   paste0(legprefix, ' Mining Compustat'), paste0(legprefix, ' Mining Tickers')),
   legendpos = c(82,90)/100
)

# t_top_pct plots ---------------------------------------------------------------

## Plot top 1% of abs(t) ----------------------------------------------------------
plotdat = list()

plotdat$name = 't_top_pct_1'

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf, 
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0, # Default = 0, minimum data mined t-stat
  t_top_pct = 1, # top x% of data mined t-stats (set to 10 or less)
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

# make event time returns for Compustat DM
temp = select_and_make_panel(plotdat$matchset, dmcomp$name, dmcomp$insampsum)
plotdat$comp_matched = temp$matched
plotdat$comp_event_time = temp$event_time

# make event time returns for ticker DM
temp = select_and_make_panel(plotdat$matchset, dmtic$name, dmtic$insampsum)
plotdat$tic_matched = temp$matched
plotdat$tic_event_time = temp$event_time

# join and reformat for plotting function
# it wants columns:  (eventDate, ret, matchRet, matchRetAlt)
# dm_mean returns are already scaled (see above)
ret_for_plotting = czret %>% 
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>% 
  left_join(
    plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
  ) %>% 
  left_join(
    plotdat$tic_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)
  )  %>% 
  select(eventDate, ret, matchRet, matchRetAlt, pubname)  %>% 
  # keep only rows where both matchrets are observed
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

# plot
legprefix = paste0('top ', plotdat$matchset$t_top_pct, '% |t|')
ReturnPlotsWithDM(
  dt = ret_for_plotting,
  basepath = "../Results/Fig_AltDM",
  suffix = plotdat$name,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -30,
  legendlabels = 
  c(paste0('Pub w/ Mining Match (', length(unique(ret_for_plotting$pubname)), ')'),
   paste0(legprefix, ' Mining Compustat'), paste0(legprefix, ' Mining Tickers')),
   legendpos = c(82,90)/100
)

## Plot top 0.1% of abs(t) ----------------------------------------------------------
plotdat = list()

plotdat$name = 't_top_pct_0.1'

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf, 
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0, # Default = 0, minimum data mined t-stat
  t_top_pct = 0.1, # top x% of data mined t-stats (set to 10 or less)
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

# make event time returns for Compustat DM
temp = select_and_make_panel(plotdat$matchset, dmcomp$name, dmcomp$insampsum)
plotdat$comp_matched = temp$matched
plotdat$comp_event_time = temp$event_time

# make event time returns for ticker DM
temp = select_and_make_panel(plotdat$matchset, dmtic$name, dmtic$insampsum)
plotdat$tic_matched = temp$matched
plotdat$tic_event_time = temp$event_time

# join and reformat for plotting function
# it wants columns:  (eventDate, ret, matchRet, matchRetAlt)
# dm_mean returns are already scaled (see above)
ret_for_plotting = czret %>% 
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>% 
  left_join(
    plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
  ) %>% 
  left_join(
    plotdat$tic_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)
  )  %>% 
  select(eventDate, ret, matchRet, matchRetAlt, pubname)  %>% 
  # keep only rows where both matchrets are observed
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

# plot
legprefix = paste0('top ', plotdat$matchset$t_top_pct, '% |t|')
ReturnPlotsWithDM(
  dt = ret_for_plotting,
  basepath = "../Results/Fig_AltDM",
  suffix = plotdat$name,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -30,
  legendlabels = 
  c(paste0('Pub w/ Mining Match (', length(unique(ret_for_plotting$pubname)), ')'),
   paste0(legprefix, ' Mining Compustat'), paste0(legprefix, ' Mining Tickers')),
   legendpos = c(82,90)/100
)


## Plot top 10% of abs(t) ----------------------------------------------------------
plotdat = list()

plotdat$name = 't_top_pct_10'

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf, 
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0, # Default = 0, minimum data mined t-stat
  t_top_pct = 10, # top x% of data mined t-stats (set to 10 or less)
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

# make event time returns for Compustat DM
temp = select_and_make_panel(plotdat$matchset, dmcomp$name, dmcomp$insampsum)
plotdat$comp_matched = temp$matched
plotdat$comp_event_time = temp$event_time

# make event time returns for ticker DM
temp = select_and_make_panel(plotdat$matchset, dmtic$name, dmtic$insampsum)
plotdat$tic_matched = temp$matched
plotdat$tic_event_time = temp$event_time

# join and reformat for plotting function
# it wants columns:  (eventDate, ret, matchRet, matchRetAlt)
# dm_mean returns are already scaled (see above)
ret_for_plotting = czret %>% 
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>% 
  left_join(
    plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
  ) %>% 
  left_join(
    plotdat$tic_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)
  )  %>% 
  select(eventDate, ret, matchRet, matchRetAlt, pubname)  %>% 
  # keep only rows where both matchrets are observed
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

# plot
legprefix = paste0('top ', plotdat$matchset$t_top_pct, '% |t|')
ReturnPlotsWithDM(
  dt = ret_for_plotting,
  basepath = "../Results/Fig_AltDM",
  suffix = plotdat$name,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -30,
  legendlabels = 
  c(paste0('Pub w/ Mining Match (', length(unique(ret_for_plotting$pubname)), ')'),
   paste0(legprefix, ' Mining Compustat'), paste0(legprefix, ' Mining Tickers')),
   legendpos = c(82,90)/100
)

