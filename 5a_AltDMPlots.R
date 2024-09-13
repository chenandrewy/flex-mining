# Setup -------------------------------------------------------------------------

rm(list = ls())

source("0_Environment.R")

library("dplyr")
library("tidyverse")
library("magrittr")
library(doParallel)

# settings
# ncores <- round(detectCores() / 2)
ncores <- 2 # really, ram is the limiting factor...

dmcomp <- list()
dmtic <- list()
dmcomp$name <- paste0(
  "../Data/Processed/",
  globalSettings$dataVersion,
  " LongShort.RData"
)
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

source("0_Environment.R") # in case there are function updates

# Functions for making event time returns ---------------------------------

# Shared Plot Setup (plot_one_setting) -------------------------------------------

# shared aesthetic settings
fontsizeall <- 28
legposall <- c(30, 15) / 100
ylaball <- "Trailing 5-Year Return (bps pm)"
linesizeall <- 1.5
plot_one_setting <- function(plotdat) {
  # make event time returns for Compustat DM
  temp <- list()
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
  temp <- list()
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
    yl = -50,
    yh = 140,
    legendlabels =
      c(
        paste0("Published"),
        paste0(plotdat$legprefix, " Mining Accounting"),
        paste0(plotdat$legprefix, " Mining Tickers")
      ),
    legendpos = legposall,
    fontsize = fontsizeall,
    yaxislab = ylaball,
    linesize = linesizeall
  )
} # end plot_one_setting

# Main Plots ----------------------------------------------------------------

## Plot abs(t) > 2 ----------------------------------------------------------

plotdat <- list()

plotdat$name <- "t_min_2"
plotdat$legprefix <- "|t|>2.0"
plotdat$npubmax <- Inf
plotdat$use_sign_info <- TRUE

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
plot_one_setting(plotdat)

## numbers for intro ----------------------------------------------------------

# make event time returns for Compustat DM
temp <- list()
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

# summarize
temp$event_time[
  !is.na(samptype), .(dm_rbar = mean(dm_mean)),
  by = c("pubname", "samptype")
] %>%
  group_by(samptype) %>%
  summarize(mean(dm_rbar))

# compare to czret
czret %>%
  mutate(
    samptype = if_else(samptype %in% c("postpub", "oos"), "postsamp", samptype)
  ) %>%
  group_by(signalname, samptype) %>%
  summarise(rbar = mean(ret_scaled)) %>%
  group_by(samptype) %>%
  summarize(mean(rbar))



## Plot top 5% of abs(t) ----------------------------------------------------------
plotdat <- list()

plotdat$name <- "t_rankpct_min_5"
plotdat$legprefix <- "top 5% |t|"
plotdat$npubmax <- Inf
plotdat$use_sign_info <- TRUE

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
  minNumStocks = globalSettings$minNumStocks
)

plot_one_setting(plotdat)

## Plot abs(t) > 0.5 ----------------------------------------------------------

plotdat <- list()

plotdat$name <- "t_min_0.5"
plotdat$legprefix <- "|t|>0.5"
plotdat$npubmax <- Inf
plotdat$use_sign_info <- TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0.5, # Default = 0, minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 100, # top x% of data mined t-stats, 100% for off
  minNumStocks = globalSettings$minNumStocks
)
plot_one_setting(plotdat)

## Plot top 90% of abs(t) ----------------------------------------------------------
plotdat <- list()

plotdat$name <- "t_rankpct_min_90"
plotdat$legprefix <- "top 90% |t|"
plotdat$npubmax <- Inf
plotdat$use_sign_info <- TRUE

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
  minNumStocks = globalSettings$minNumStocks
)

# Extra t_min plots ---------------------------------------------------------------

## Plot abs(t) > 1 ----------------------------------------------------------

plotdat <- list()

plotdat$name <- "t_min_1"
plotdat$legprefix <- "|t|>1.0"
plotdat$npubmax <- Inf
plotdat$use_sign_info <- TRUE

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
  minNumStocks = globalSettings$minNumStocks
)

plot_one_setting(plotdat)

## Plot abs(t) > 3 ----------------------------------------------------------

plotdat <- list()

plotdat$name <- "t_min_3"
plotdat$legprefix <- "|t|>3.0"
plotdat$npubmax <- Inf
plotdat$use_sign_info <- TRUE

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
  minNumStocks = globalSettings$minNumStocks
)

plot_one_setting(plotdat)

# Extra t_rankpct_min plots ---------------------------------------------------------------

## Plot top 75% of abs(t) ----------------------------------------------------------
plotdat <- list()

plotdat$name <- "t_rankpct_min_75"
plotdat$legprefix <- "top 75% |t|"
plotdat$npubmax <- Inf
plotdat$use_sign_info <- TRUE

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
  minNumStocks = globalSettings$minNumStocks
)

plot_one_setting(plotdat)

### Plot top 50% of abs(t) ----------------------------------------------------------
plotdat <- list()

plotdat$name <- "t_rankpct_min_50"
plotdat$legprefix <- "top 50% |t|"
plotdat$npubmax <- Inf
plotdat$use_sign_info <- TRUE

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
  minNumStocks = globalSettings$minNumStocks
)

plot_one_setting(plotdat)

## Plot top 0.5% of abs(t) ----------------------------------------------------------
plotdat <- list()

plotdat$name <- "t_rankpct_min_0.5"
plotdat$legprefix <- "top 0.5% |t|"
plotdat$npubmax <- Inf
plotdat$use_sign_info <- TRUE

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
  minNumStocks = globalSettings$minNumStocks
)

plot_one_setting(plotdat)

# Plot selected lines for slides ---------------------------------------------

# published
# top 5% mining accounting
# t>2 mining tickers

plotdat <- list()

plotdat$name <- "custom_slides"
plotdat$npubmax <- Inf
plotdat$use_sign_info <- TRUE


plotdat$matchset_rankmin <- list(
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
  minNumStocks = globalSettings$minNumStocks
)

plotdat$matchset_t2 <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf,
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 2, # minimum screened t-stat
  t_max = Inf, # maximum screened t-stat
  t_rankpct_min = 100, # top x% of data mined t-stats, 100% for off
  minNumStocks = globalSettings$minNumStocks
)


# make event time returns for Compustat DM
temp <- list()
temp$matched <- SelectDMStrats(dmcomp$insampsum, plotdat$matchset_rankmin)

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
temp <- list()
temp$matched <- SelectDMStrats(dmtic$insampsum, plotdat$matchset_t2)

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
  yl = -50,
  yh = 140,
  legendlabels =
    c(
      paste0("Published"),
      paste0("top 5% |t| Mining Accounting"),
      paste0("|t|>2.0 Mining Tickers")
    ),
  legendpos = legposall,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)
