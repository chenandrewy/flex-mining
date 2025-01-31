# Research vs DM Accounting vs DM Ticker
# separated from ResearchVSDMPlots.R for clarity
# For much of the analysis, we don't want to look at tickers (sheesh)


# Setup --------------------------------------------------------

rm(list = ls())

source("0_Environment.R")
library(doParallel)

## Load Global Data -------------------------------------------

# these are treated as globals (don't modify pls)
inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)

czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep) %>% 
  filter(signalname %in% inclSignals) %>% 
  setDT()

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
  select(signalname, Year, theory) %>% 
  filter(signalname %in% inclSignals)

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100) %>% 
  filter(signalname %in% inclSignals)

# Load pre-computed dm sumstats
dmcomp <- readRDS("../Data/Processed/dmcomp_sumstats.RDS")
dmtic <- readRDS("../Data/Processed/dmtic_sumstats.RDS")

# Specialized plot function -------------------------------------------

# Wrapper for construction of main and robustness figures
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



# Shared Settings --------------------------------------------------
plotdat <- list()

plotdat$name <- "t_min_2"
plotdat$legprefix = "|t|>2.0"
plotdat$npubmax = Inf
plotdat$use_sign_info = TRUE

plotdat$matchset <- list(
  # tolerance in levels
  t_tol = globalSettings$t_tol,
  r_tol = globalSettings$r_tol,
  # tolerance relative to op stat
  t_reltol = globalSettings$t_reltol,
  r_reltol = globalSettings$r_reltol,
  # alternative filtering
  t_min = globalSettings$t_min,
  t_max = globalSettings$t_max, 
  t_rankpct_min = globalSettings$t_rankpct_min,
  minNumStocks = globalSettings$minNumStocks
)

# aesthetic settings
fontsizeall = 28
legposall = c(30,15)/100
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5


# t_min plots ---------------------------------------------------------------

## Plot abs(t) > 0.5 ----------------------------------------------------------
plotdat$name <- "t_min_0.5"
plotdat$legprefix = "|t|>0.5"

plotdat$matchset$t_min = 0.5

plot_one_setting(plotdat)

plotdat$matchset$t_min = globalSettings$t_min # back to default

## Plot abs(t) > 1 ----------------------------------------------------------

plotdat$name <- "t_min_1"
plotdat$legprefix = "|t|>1.0"

plotdat$matchset$t_min = 1

plot_one_setting(plotdat)

plotdat$matchset$t_min = globalSettings$t_min # back to default

## Plot abs(t) > 2 ----------------------------------------------------------

plotdat$name <- "t_min_2"
plotdat$legprefix = "|t|>2.0"

plotdat$matchset$t_min = 2

plot_one_setting(plotdat)

plotdat$matchset$t_min = globalSettings$t_min # back to default

## Plot abs(t) > 3 ----------------------------------------------------------
plotdat$name <- "t_min_3"
plotdat$legprefix = "|t|>3.0"

plotdat$matchset$t_min = 3

plot_one_setting(plotdat)

plotdat$matchset$t_min = globalSettings$t_min # back to default

# t_rankpct_min plots ---------------------------------------------------------------

## Plot top 90% of abs(t) ----------------------------------------------------------
plotdat$name <- "t_rankpct_min_90"
plotdat$legprefix = "top 90% |t|"

plotdat$matchset$t_rankpct_min = 90

plot_one_setting(plotdat)

plotdat$matchset$t_rankpct_min = globalSettings$t_rankpct_min # back to default

## Plot top 75% of abs(t) ----------------------------------------------------------
plotdat$name <- "t_rankpct_min_75"
plotdat$legprefix = "top 75% |t|"

plotdat$matchset$t_rankpct_min = 75

plot_one_setting(plotdat)

plotdat$matchset$t_rankpct_min = globalSettings$t_rankpct_min # back to default

## Plot top 50% of abs(t) ----------------------------------------------------------
plotdat$name <- "t_rankpct_min_50"
plotdat$legprefix = "top 50% |t|"

plotdat$matchset$t_rankpct_min = 50

plot_one_setting(plotdat)

plotdat$matchset$t_rankpct_min = globalSettings$t_rankpct_min # back to default

## Plot top 5% of abs(t) ----------------------------------------------------------
plotdat$name <- "t_rankpct_min_5"
plotdat$legprefix = "top 5% |t|"

plotdat$matchset$t_rankpct_min = 5 

plot_one_setting(plotdat)

plotdat$matchset$t_rankpct_min = globalSettings$t_rankpct_min # back to default

## Plot top 0.5% of abs(t) ----------------------------------------------------------
plotdat$name <- "t_rankpct_min_0.5"
plotdat$legprefix = "top 0.5% |t|"

plotdat$matchset$t_rankpct_min = 0.5 #Typo in previous version? Was 5?

plot_one_setting(plotdat)

plotdat$matchset$t_rankpct_min = globalSettings$t_rankpct_min # back to default



