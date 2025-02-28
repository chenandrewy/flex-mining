# 2024 10 this prep is used in many, many plots now, and is compute heavy

# Setup --------------------------------------------------------

rm(list = ls())

source("0_Environment.R")
library(doParallel)

# settings
ncores = globalSettings$num_cores

dmcomp <- list()
dmtic <- list()
dmcomp$name <- paste0('../Data/Processed/',
                      globalSettings$dataVersion, 
                      ' LongShort.RData')
dmtic$name <- "../Data/Processed/ticker_Harvey2017JF.RDS"

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

# In-samp Sumstats (about 4 min on 4 cores) -------------------

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

# Save to disk -----------------------------------------------------

saveRDS(dmcomp, "../Data/Processed/dmcomp_sumstats.RDS")
saveRDS(dmtic, "../Data/Processed/dmtic_sumstats.RDS")

# Generate Matched Returns (ret_for_plot[x]) ---------------------------------

## Default Match Settings --------------------------------------------------
# this should be cleaned up, ideally

plotdat0 <- list()

plotdat0$name <- "t_min_2"
plotdat0$npubmax = Inf
plotdat0$use_sign_info = TRUE

plotdat0$matchset <- list(
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

## Shared data prep --------------------------------------------------
# here, matchRet uses accounting signals with t>2

# make event time returns for Compustat DM
temp = list()
temp$matched <- SelectDMStrats(dmcomp$insampsum, plotdat0$matchset)

print("Making accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()
temp$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = temp$matched, npubmax = plotdat0$npubmax, 
  czsum = czsum, use_sign_info = plotdat0$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat0$comp_matched <- temp$matched
plotdat0$comp_event_time <- temp$event_time
rm(temp)

ret_for_plot0 = czret %>%
    transmute(pubname = signalname, eventDate, calendarDate = date, ret = ret_scaled, theory) %>%
    left_join(
        plotdat0$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean),
        by = c("pubname", "eventDate")
    ) %>%
    select(eventDate, calendarDate, ret, matchRet, pubname, theory) 

## Shared data prep 2 --------------------------------------------------
# adds to the previous ret_for_plot0
# creates matchRetAlt, which uses the top 5% t-stats

tempplotdat = plotdat0
tempplotdat$matchset$t_min = 0 # turn off t_min filter
tempplotdat$matchset$t_rankpct_min = 5 # add top 5% t filter

# make event time returns for Compustat DM
temp = list()
temp$matched <- SelectDMStrats(dmcomp$insampsum, tempplotdat$matchset)

print("Making accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()
temp$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = temp$matched, npubmax = tempplotdat$npubmax, 
  czsum = czsum, use_sign_info = tempplotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

tempplotdat$comp_matched <- temp$matched
tempplotdat$comp_event_time <- temp$event_time
rm(temp)

ret_for_plot1 = ret_for_plot0 %>%
  left_join(
    tempplotdat$comp_event_time %>% transmute(pubname, eventDate, matchRetAlt = dm_mean),
    by = c("pubname", "eventDate")
  ) %>% 
  select(eventDate, ret, matchRet, matchRetAlt, pubname, theory)

## Shared data prep 3 --------------------------------------------------
# an alternative to ret_for_plot0 and ret_for_plot1
# uses top maxDMpredictors[rr] accounting signals

# max number of DM predictors per paper
maxDMpredictors = c(100, 1000)

ret_for_plot_MaxPredictors = tibble()
for (rr in 1:length(maxDMpredictors)) {
  
  print(paste0("Making accounting event time returns with Max DM predictors: ", maxDMpredictors[rr]))
  
  tempMatched <- plotdat0$comp_matched %>% 
    filter(rank_tstat <= maxDMpredictors[rr] + 1) 
  
  print("Can take a few minutes...")
  start_time <- Sys.time()
  tempEvent_time <- make_DM_event_returns(
    DMname = dmcomp$name, match_strats = tempMatched, npubmax = plotdat0$npubmax, 
    czsum = czsum, use_sign_info = plotdat0$use_sign_info
  )
  stop_time <- Sys.time()
  print(stop_time - start_time)
  
  ret_for_plot_MaxPredictors = czret %>%
    transmute(pubname = signalname, eventDate, ret = ret_scaled, theory) %>%
    left_join(
      tempEvent_time %>% transmute(pubname, eventDate, matchRet = dm_mean),
      by = c("pubname", "eventDate")
    ) %>%
    select(eventDate, ret, matchRet, pubname, theory) %>% 
    mutate(maxDMpredictors = maxDMpredictors[rr]) %>% 
    bind_rows(ret_for_plot_MaxPredictors)
  
}

# Save to disk -----------------------------------------------------

saveRDS(plotdat0, "../Data/Processed/plotdat0.RDS")
saveRDS(ret_for_plot0, "../Data/Processed/ret_for_plot0.RDS")
saveRDS(ret_for_plot1, "../Data/Processed/ret_for_plot1.RDS")
saveRDS(ret_for_plot_MaxPredictors, "../Data/Processed/ret_for_plot_MaxPredictors.RDS")
