 # Setup -------------------------------------------------------------------------

rm(list = ls())

source("0_Environment.R")

library("dplyr")
library("tidyverse")
library("magrittr")
library(doParallel)

# settings

# technical
ncores <- 9
DMname <- "../Data/Processed/CZ-style-v6 LongShort.RData" # autofill

## Load Data ---------------------------------------------------------------

# these are treated as globals (don't modify pls)
czsum <- readRDS("../Data/Processed/czsum_all207.RDS")

czcat <- fread("DataIntermediate/TextClassification.csv") %>%
  select(signalname, Year, theory1, misprice_risk_ratio)

czret <- readRDS("../Data/Processed/czret.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100)

# Data mining strategies
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list
dm_signal_info <- readRDS(DMname)$signal_list
dm_user <- readRDS(DMname)$user

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
  )

setDT(dm_rets)
print("finished loading data")

# Data mining summary stats -------------------------------------------------

# Finds sum stats for dm in each pub sample
# the output for this can be used for all dm selection methods
samplist <- czsum %>%
  distinct(sampstart, sampend) %>%
  arrange(sampstart, sampend)

print("creating DM summary stats for each pub sample")
tic <- Sys.time()

cl <- makePSOCKcluster(ncores)
registerDoParallel(cl)

nsamp <- dim(samplist)[1] # reduce for debugging
dm_insamp <- list()

# dopar in a function needs some special setup (but not using this in a function any more)
# https://stackoverflow.com/questions/6689937/r-problem-with-foreach-dopar-inside-function-called-by-optim
dm_insamp <- foreach(
  sampi = 1:nsamp,
  .combine = rbind,
  .packages = c("data.table", "tidyverse", "zoo")
  # .export = ls(envir = globalenv())
) %dopar% {
  sampcur <- samplist[sampi, ]

  # feedback
  print(paste0("DM sample stats for sample ", sampi, " of ", nsamp))

  # find sum stats for the current sample
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

  # find other stats for filtering
  filtcur <- dm_rets[
    floor(yearm) == year(sampcur$sampend) &
      !is.na(ret),
    .(nlastyear = .N),
    by = c("sweight", "dmname")
  ]

  # combine and save
  sumcur %>%
    left_join(filtcur, by = c("sweight", "dmname")) %>%
    mutate(
      sampstart = sampcur$sampstart, sampend = sampcur$sampend
    )
}
stopCluster(cl)

# add ranking statistics
dm_insamp <- dm_insamp %>%
  group_by(sweight, sampstart, sampend) %>%
  arrange(desc(abs(tstat))) %>%
  mutate(rank_tstat = row_number()) %>%
  arrange(desc(abs(rbar))) %>%
  mutate(rank_rbar = row_number(), n_dm_tot = n())

# Merge with czsum
# matchsum key is c(pubname,dmname). Each row is a dm strat that matches a pub
matchsum <- czsum %>%
  transmute(
    pubname = signalname, rbar_op = rbar, tstat_op = tstat, sampstart, sampend,
    sweight = tolower(sweight)
  ) %>%
  left_join(
    dm_insamp,
    by = c("sampstart", "sampend", "sweight"),
    relationship = "many-to-many" # required to suppress warning
  ) %>%
  mutate(
    diff_rbar = abs(rbar * sign(rbar) - rbar_op),
    diff_tstat = abs(tstat * sign(rbar) - tstat_op)
  ) %>%
  arrange(pubname, desc(tstat))

setDT(matchsum)

print("end DM summary stats")
toc <- Sys.time()
toc - tic

# Declare Functions -------------------------------------------------------

SelectDMStrats <- function(matchsum, settings) {
  # input:
  #     matchsum = summary stats for each pubname, dmname combination
  #     dmset = settings for selection
  # output: matchcur = all pubname, dmname that satisfy dmset

  matchcur <- matchsum[
    diff_rbar <= settings$r_tol &
      diff_tstat <= settings$t_tol &
      diff_rbar / rbar_op <= settings$r_reltol &
      diff_tstat / tstat_op <= settings$t_reltol &
      min_nstock >= settings$minNumStocks &
      nlastyear == 12 &
      abs(tstat) >= settings$t_min &
      abs(tstat) <= settings$t_min2 &
      rank_tstat / n_dm_tot <= settings$t_top_pct / 100
  ]

  print("summary of matching:")
  matchcur[, .(n_dm_match = .N, sampstart = min(sampstart), sampend = min(sampend)), by = "pubname"] %>%
    arrange(-n_dm_match) %>%
    print()

  return(matchcur)

  print("end selectStrats")
}

make_DM_event_returns <- function(dmselect) {
  # input: dmselect = summary stats for each selected pubname, dmname pair
  #     outname = name of RDS output
  # output: for each pubname-eventDate, average dm returns
  gc()

  tic <- Sys.time()
  cl <- makePSOCKcluster(ncores)
  registerDoParallel(cl)
  npub <- dim(czsum)[1] # reduce to debug
  event_dm_scaled <- foreach(
    pubi = 1:npub,
    .combine = rbind,
    .packages = c("data.table", "tidyverse", "zoo")
  ) %do% {
    # feedback
    print(paste0("pubi ", pubi, " of ", npub))

    pubcur <- czsum[pubi, ]

    # select matching dm strats for the current pubname
    matchcur <- dmselect[pubname == pubcur$signalname]

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
  toc <- Sys.time()
  toc - tic

  return(event_dm_scaled)
} # end MakeMatchedPanel

add_pub_and_graph <- function(event_dm_scaled, plotname) {
  # merge and rename to match ReturnPlotsWithDM
  allRets <- czret %>%
    transmute(pubname = signalname, eventDate, ret = ret_scaled, samptype) %>%
    left_join(
      event_dm_scaled %>% transmute(pubname, eventDate, matchRet = dm_mean),
      by = c("pubname", "eventDate")
    ) %>%
    left_join(
      czsum %>% transmute(pubname = signalname, Keep),
      by = "pubname"
    ) %>%
    filter(!is.na(matchRet), Keep == 1) %>% # To exclude unmatched signals
    select(eventDate, ret, matchRet) # strictly needs these three columns

  # Graph
  ReturnPlotsWithDM(
    dt = allRets,
    basepath = "../Results/Fig_PublicationsVsDataMining",
    suffix = plotname,
    rollmonths = 60,
    colors = colors,
    labelmatch = TRUE
  )
} # end add_pub_and_graph


# 1st graph -------------------------------------------------------------------

# choose all settings at once
name1 <- "0.03"
dmset1 <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf, 
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0, # Default = 0, minimum data mined t-stat
  t_min2 = 25,
  t_top_pct = 0.03, # top x% of data mined t-stats (set to 10 or less)
  minNumStocks = 20 # Minimum number of stocks in any month over the in-sample period to include a strategy
)

print(paste0('making first graph, name = ', name1, ' ======================'))

core <- ls()
core <- append(core, c("SelectDMStrats", "MakeMatchedPanel", "create_allRets_and_graph")) # functions

dm_strats <- SelectDMStrats(matchsum, dmset1)
print("func1.2")

event_dm_scaled <- make_DM_event_returns(dm_strats)
print("func1.3")

add_pub_and_graph(event_dm_scaled, name1)
print("func1.4")

rm(list = setdiff(ls(), core))
gc()


# 2nd graph -------------------------------------------------------------------



# choose all settings at once
name2 <- "0.02"
dmset2 <- list(
  # tolerance in levels
  t_tol = .1 * Inf,
  r_tol = .3 * Inf, 
  # tolerance relative to op stat
  t_reltol = 0.1 * Inf,
  r_reltol = 0.3 * Inf,
  # alternative filtering
  t_min = 0, #  minimum data mined t-stat, 0 to ignore
  t_min2 = 25,
  t_top_pct = 0.02, # top x% of data mined t-stats, 100 to ignore
  minNumStocks = 20 # Minimum number of stocks in  any month over the in-sample period to include a strategy
)
print(paste0('making first graph, name = ', name2, ' ======================'))

core <- ls()
core <- append(core, c("SelectDMStrats", "MakeMatchedPanel", "create_allRets_and_graph")) # functions

dm_strats <- SelectDMStrats(matchsum, dmset2)
print("func2.2")

event_dm_scaled <- make_DM_event_returns(dm_strats)
print("func2.3")

add_pub_and_graph(event_dm_scaled, name2)
print("func2.4")

rm(list = setdiff(ls(), core))
gc()


# debug -----------------------------------------------------------------------

event_dm_scaled  %>% 
  group_by(pubname)  %>% summarize(dm_n = mean(dm_n))  %>% 
  arrange(dm_n)  %>% print(n=Inf)

matchsum  %>% group_by(pubname,sweight)  %>% summarize(n = n())  %>% print(n=Inf)