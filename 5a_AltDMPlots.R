# Setup -------------------------------------------------------------------------

rm(list = ls())

source("0_Environment.R")

library("dplyr")
library("tidyverse")
library("magrittr")
library(doParallel)

# settings
ncores <- round(detectCores() / 2)

dmcomp <- list()
dmtic <- list()
dmcomp$name <- "../Data/Processed/CZ-style-v6 LongShort.RData"
dmtic$name <- "../Data/Processed/ticker_Harvey2017JF.RDS"

## Load Global Data ---------------------------------------------------------------

# these are treated as globals (don't modify pls)
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep)

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
  select(signalname, Year, theory)

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100)

# In-samp Sumstats ------------------------------------------------------
## Declare function --------------------------------------------------------

# function for computing DM strat sumstats in pub samples
sumstats_for_DM_Strats <- function(
    DMname = "../Data/Processed/CZ-style-v6 LongShort.RData",
    nsampmax = Inf) {
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
  nsamp <- min(nsamp, nsampmax)
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
    sumcur <- sumcur %>%
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
} # end Sumstats function

## Run Function on Compustat DM ----------------------------------------

print("creating Compustat mining in-sample sumstats")
print("Takes about 4 minutes using 4 cores")
start_time <- Sys.time()
dmcomp$insampsum <- sumstats_for_DM_Strats(
  DMname = dmcomp$name,
  nsampmax = Inf
)
print("finished")
stop_time <- Sys.time()
stop_time - start_time

## Run Function on Ticker DM ----------------------------------------

print("creating ticker mining in-sample sumstats")
print("Takes about 20 sec using 9 cores")
start_time <- Sys.time()
dmtic$insampsum <- sumstats_for_DM_Strats(
  DMname = dmtic$name,
  nsampmax = Inf
)
print("end DM summary stats")
stop_time <- Sys.time()
stop_time - start_time

# Convenience Save --------------------------------------------------------
save.image("../Data/tmpAltDMPlots.RData")

# Convenience Load --------------------------------------------------------
load("../Data/tmpAltDMPlots.RData")

source('0_Environment.R') # in case there are function updates

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
      abs(tstat) > settings$t_min &
      abs(tstat) < settings$t_max &
      rank_tstat / n_dm_tot <= settings$t_rankpct_min / 100
  ]

  print("summary of matching:")
  matchcur[, .(n_dm_match = .N, sampstart = min(sampstart), sampend = min(sampend)), by = "pubname"] %>%
    arrange(-n_dm_match) %>%
    print()

  return(matchcur)

  print("end selectStrats")
}

make_DM_event_returns <- function(
    match_strats,
    DMname = "../Data/Processed/CZ-style-v6 LongShort.RData",
    npubmax = Inf,
    czsum,
    use_sign_info = TRUE
    ) {
  # input: match_strats = summary stats for each selected pubname, dmname pair
  #     outname = name of RDS output
  # you need to pass in czsum (can't use the global) because of
  # a mysterious dopar error (object 'czsum' not found)
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
  npub <- min(npub, npubmax)
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
        # scale returns
        ret_scaled = ret * sign / abs(rbar) * 100,
        # # sign returns (sanity check)
        # ret_scaled = ifelse(use_sign_info, sign*ret_scaled, ret_scaled),
        samptype = case_when(
          (yearm >= pubcur$sampstart) & (yearm <= pubcur$sampend) ~ "insamp",
          (yearm > pubcur$sampend) ~ "oos",
          TRUE ~ NA_character_
        )
      )
    
    if (use_sign_info==FALSE){
      # remove sign_info if requested (for testing)
      eventpan[ , ret_scaled := sign*ret_scaled]
    }

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


# Plots v2 ----------------------------------------------------------------


## Shared setup ------------------------------------------------------------

# shared aesthetic settings
fontsizeall = 28
legposall = c(30,15)/100
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5


plot_one_setting = function(plotdat){
  
  # make event time returns for Compustat DM
  temp = list()
  temp$matched <- SelectDMStrats(dmcomp$insampsum, plotdat$matchset)
  
  print("Making accounting event time returns")
  print("Can take a few minutes...")
  start_time <- Sys.time()
  temp$event_time <- make_DM_event_returns(
    DMname = dmcomp$name, match_strats = temp$matched, npubmax = plotdat$npubmax, 
    czsum = czsum, use_sign_info = plotdat$use_sign_info
  )
  stop_time <- Sys.time()
  stop_time - start_time
  
  plotdat$comp_matched <- temp$matched
  plotdat$comp_event_time <- temp$event_time
  
  # make event time returns for ticker DM
  temp = list()
  temp$matched <- SelectDMStrats(dmtic$insampsum, plotdat$matchset)
  
  print("Making ticker event time returns")
  start_time <- Sys.time()
  temp$event_time <- make_DM_event_returns(
    DMname = dmtic$name, match_strats = temp$matched, npubmax = plotdat$npubmax, 
    czsum = czsum, use_sign_info = plotdat$use_sign_info
  )
  stop_time <- Sys.time()
  stop_time - start_time
  
  plotdat$tic_matched <- temp$matched
  plotdat$tic_event_time <- temp$event_time
  
  # join and reformat for plotting function
  # it wants columns:  (eventDate, ret, matchRet, matchRetAlt)
  # dm_mean returns are already scaled (see above)
  ret_for_plotting <- czret %>%
    transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
    left_join(
      plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
    ) %>%
    left_join(
      plotdat$tic_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)
    ) %>%
    select(eventDate, ret, matchRet, matchRetAlt, pubname) %>%
    # keep only rows where both matchrets are observed
    filter(!is.na(matchRet) & !is.na(matchRetAlt))
  
  # plot
  ReturnPlotsWithDM(
    dt = ret_for_plotting,
    basepath = "../Results/Fig_AltDM",
    suffix = plotdat$name,
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl = -30,
    legendlabels =
      c(
        paste0("Pub w/ Mining Matches (", length(unique(ret_for_plotting$pubname)), ")"),
        paste0(plotdat$legprefix, " Mining Accounting"), 
        paste0(plotdat$legprefix, " Mining Tickers")
      ),
    legendpos = legposall,
    fontsize = fontsizeall,
    yaxislab = ylaball,
    linesize = linesizeall
  )
  
} # end plot_one_setting


## t_min plots ---------------------------------------------------------------

### Plot abs(t) > 1 ----------------------------------------------------------

plotdat <- list()

plotdat$name <- "t_min_1"
plotdat$legprefix = "|t|>1.0"
plotdat$npubmax = Inf
plotdat$use_sign_info = TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 1, # Default = 0, minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 100, # top x% of data mined t-stats, 100% for off
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)
plot_one_setting(plotdat)

### Plot abs(t) > 2 ----------------------------------------------------------

plotdat <- list()

plotdat$name <- "t_min_2"
plotdat$legprefix = "|t|>2.0"
plotdat$npubmax = Inf
plotdat$use_sign_info = TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 2, # Default = 0, minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 100, # top x% of data mined t-stats, 100% for off
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)
plot_one_setting(plotdat)

### Plot abs(t) > 3 ----------------------------------------------------------

plotdat <- list()

plotdat$name <- "t_min_3"
plotdat$legprefix = "|t|>3.0"
plotdat$npubmax = Inf
plotdat$use_sign_info = TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 3, # Default = 0, minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 100, # top x% of data mined t-stats, 100% for off
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

plot_one_setting(plotdat)


## t_rankpct_min plots ---------------------------------------------------------------

### Plot top 90% of abs(t) ----------------------------------------------------------
plotdat <- list()

plotdat$name <- "t_rankpct_min_90"
plotdat$legprefix = "top 90% |t|"
plotdat$npubmax = Inf
plotdat$use_sign_info = TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0, # minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 90, # minimum t-stat rank (in pctile)
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

plot_one_setting(plotdat)


### Plot top 75% of abs(t) ----------------------------------------------------------
plotdat <- list()

plotdat$name <- "t_rankpct_min_75"
plotdat$legprefix = "top 75% |t|"
plotdat$npubmax = Inf
plotdat$use_sign_info = TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0, # minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 75, # minimum t-stat rank (in pctile)
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

plot_one_setting(plotdat)


### Plot top 50% of abs(t) ----------------------------------------------------------
plotdat <- list()

plotdat$name <- "t_rankpct_min_50"
plotdat$legprefix = "top 50% |t|"
plotdat$npubmax = Inf
plotdat$use_sign_info = TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0, # minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 50, # minimum t-stat rank (in pctile)
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

plot_one_setting(plotdat)


### Plot top 5% of abs(t) ----------------------------------------------------------
plotdat <- list()

plotdat$name <- "t_rankpct_min_5"
plotdat$legprefix = "top 5% |t|"
plotdat$npubmax = Inf
plotdat$use_sign_info = TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0, # minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 5, # minimum t-stat rank (in pctile)
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

plot_one_setting(plotdat)


### Plot top 0.5% of abs(t) ----------------------------------------------------------
plotdat <- list()

plotdat$name <- "t_rankpct_min_0.5"
plotdat$legprefix = "top 0.5% |t|"
plotdat$npubmax = Inf
plotdat$use_sign_info = TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0, # minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 5, # minimum t-stat rank (in pctile)
  minNumStocks = 10 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

plot_one_setting(plotdat)

