# Data-mining comparisons 
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

## Load Global Data -------------------------------------------

# these are treated as globals (don't modify pls)
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep) %>% 
  setDT()

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
  select(signalname, Year, theory)

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100)

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

# Convenience Save ---------------------------------------------
save.image("../Data/tmpTG2Plots.RData")

# Convenience Load --------------------------------------------
load("../Data/tmpTG2Plots.RData")

source('0_Environment.R') # in case there are function updates

# Declare Function  ---------------------

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
    fontsize = 38,
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

