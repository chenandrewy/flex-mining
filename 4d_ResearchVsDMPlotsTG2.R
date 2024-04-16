# Data-mining comparisons --------------------------------------
# These are the new main plots as of 2024 04.

# Setup --------------------------------------------------------

rm(list = ls())

source("0_Environment.R")

library("dplyr")
library("tidyverse")
library("magrittr")
library(doParallel)

# settings
ncores <- round(detectCores() / 2)
# ncores = 4

dmcomp <- list()
dmtic <- list()
dmcomp$name <- paste0('../Data/Processed/',
                      globalSettings$dataVersion, 
                      ' LongShort.RData')
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

# In-samp Sumstats (about 4 min on 4 cores) -------------------
## Declare function --------------------------------------------

# function for computing DM strat sumstats in pub samples
sumstats_for_DM_Strats <- function(
    DMname = paste0('../Data/Processed/',
                    globalSettings$dataVersion, 
                    ' LongShort.RData'),
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
      nstock_long,
      nstock_short
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
        min_nstock_long = min(nstock_long),
        min_nstock_short = min(nstock_short),
        nmonth = sum(!is.na(ret))
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

## Run Function on Compustat DM --------------------------------

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

# Convenience Save ---------------------------------------------
save.image("../Data/tmpAltDMPlots.RData")

# Convenience Load --------------------------------------------
load("../Data/tmpAltDMPlots.RData")

source('0_Environment.R') # in case there are function updates

# Functions for making event time returns ---------------------

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
      min_nstock_long >= settings$minNumStocks/2 &
      min_nstock_short >= settings$minNumStocks/2 &
      abs(tstat) > settings$t_min &
      abs(tstat) < settings$t_max &
      rank_tstat / n_dm_tot <= settings$t_rankpct_min / 100 &
      nlastyear == 12 &   # tbc: make flexible
      nmonth >= 5*12 # tbc: make flexible
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
    DMname = paste0('../Data/Processed/',
                    globalSettings$dataVersion, 
                    ' LongShort.RData'),
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
      nstock_long,
      nstock_short
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
    eventsumscaled <- eventpan[, .(dm_mean = mean(ret_scaled), 
                                   dm_sd = sd(ret_scaled), dm_n = .N),
                               by = c("eventDate",'samptype')
    ] %>%
      mutate(pubname = pubcur$signalname)
    
    return(eventsumscaled)
  } # end do pubi = 1:npub
  
  stopCluster(cl)
  
  return(event_dm_scaled)
} # end MakeMatchedPanel

make_ret_for_plotting <- function(plotdat, theory_filter = NULL){
  # takes in acct dm and ticker dm data and returns
  # data for an event time plot

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
  print(stop_time - start_time)
  
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
  print(stop_time - start_time)
  
  plotdat$tic_matched <- temp$matched
  plotdat$tic_event_time <- temp$event_time 

  # join and reformat for plotting function
  # it wants columns:  (eventDate, ret, matchRet, matchRetAlt)
  # dm_mean returns are already scaled (see above)
  if(is.null(theory_filter)){
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
  }else{
    if(theory_filter == 'all'){
      ret_for_plotting <- czret %>%
        transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
        left_join(
          plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
        ) %>%
        select(eventDate, ret, matchRet, pubname) %>%
        # keep only rows where DM matchrets are observed
        filter(!is.na(matchRet) )
    }else{    
      ret_for_plotting <- czret %>%
      filter(theory == theory_filter) %>%
      transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
      left_join(
        plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
      ) %>%
      select(eventDate, ret, matchRet, pubname) %>%
      # keep only rows where DM matchrets are observed
      filter(!is.na(matchRet) )}

  }

  return(ret_for_plotting)

} # end make_ret_for_plotting

# Shared Settings --------------------------------------------------
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
  minNumStocks = globalSettings$minNumStocks
)

# aesthetic settings
fontsizeall = 28
legposall = c(30,15)/100
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5

# Intro Plot --------------------------------------------------

## Make data --------------------------------------------------
ret_for_plotting = make_ret_for_plotting(plotdat)

## plot --------------------------------------------------

printme = ReturnPlotsWithDM(
  dt = ret_for_plotting %>% select(-matchRetAlt),
  basepath = "../Results/Fig_DM",
  suffix = plotdat$name,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  legendlabels =
    c(
      paste0("Published (and Peer Reviewed)"),
      paste0("Data-Mined for |t|>2.0 in Original Sample"),
      paste0(plotdat$legprefix, " Mining Tickers")
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

# custom edits 
printme2 = printme + theme(
  legend.background = element_rect(fill = "white", color = "black"
    , size = 0.3)
  # remove space where legend would be
  , legend.margin = margin(-0.7, 0.5, 0.5, 0.5, "cm")
  , legend.position  = c(44,15)/100
  # add space between legend items
  , legend.spacing.y = unit(0.2, "cm")
  ) +
  guides(color = guide_legend(byrow = TRUE))

# save (again)
ggsave(paste0("../Results/Fig_DM_", plotdat$name, '.pdf'), width = 10, height = 8)

# numbers for intro
ret_for_plotting[eventDate>0 & eventDate <= Inf, .(mean(ret), mean(matchRet))]

ret_for_plotting %>% distinct(pubname)
czret %>% distinct(signalname)

# Plot by Theory Category -------------------------------------

# loop over theory categories
for (jj in unique(czret$theory)) {
  print(jj)
  plotdat$name <- paste0("t_min_2_cat_", jj)

  ret_for_plotting = make_ret_for_plotting(plotdat, theory_filter = jj)
  
  # Plot
  plt = ReturnPlotsWithDM(
    dt = ret_for_plotting,
    basepath = "../Results/Fig_DM",
    suffix = plotdat$name,
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl =-100, yh = 175,
    fig.width = 18,
    fontsize = 44,
    legendlabels =
      c(
        paste0("Published"),
        paste0(plotdat$legprefix, " Mining Accounting"), 
        paste0(plotdat$legprefix, " Mining Tickers")
      ),
    yaxislab = 'Trailing 5-Year Return',
    legendpos = c(25,20)/100,
  )  
  
}

