# Data-mining comparisons 
# These are the new main plots as of 2024 04.

# Setup --------------------------------------------------------

rm(list = ls())

source("0_Environment.R")
library(doParallel)

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

# Load pre-computed dm sumstats
dmcomp <- readRDS("../Data/Processed/dmcomp_sumstats.RDS")
dmtic <- readRDS("../Data/Processed/dmtic_sumstats.RDS")

# Specialized data prep function -------------------------------

make_ret_for_plotting <- function(plotdat, theory_filter = NULL){
  # takes dm selection criteria and returns data for an event time plot
  
  # make event time returns for Compustat DM
  temp = list()
  temp$matched <- SelectDMStrats(dmcomp$insampsum, plotdat$matchset)
  
  # tbc: see if make_DM_event_returns should called just once 
  # tbc: see if this function makes any sense at all now
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
  
  # join and reformat for plotting function
  # it wants columns:  (eventDate, ret, matchRet, matchRetAlt)
  # dm_mean returns are already scaled (see above)
  if(is.null(theory_filter)){
    ret_for_plotting <- czret %>%
      transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
      left_join(
        plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
      ) %>%
      select(eventDate, ret, matchRet, pubname) %>%
      # keep only rows where DM matchrets are observed
      filter(!is.na(matchRet) )
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

# Intro Plot --------------------------------------------------

## Make data --------------------------------------------------
ret_for_plotting = make_ret_for_plotting(plotdat)

## plot --------------------------------------------------

printme = ReturnPlotsWithDM(
  dt = ret_for_plotting,
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
(
  printme + theme(
  legend.background = element_rect(fill = "white", color = "black"
    , size = 0.3)
  # remove space where legend would be
  , legend.margin = margin(-1.0, 0.5, 0.5, 0.5, "cm")
  , legend.position  = c(44,15)/100
  # add space between legend items
  , legend.spacing.y = unit(0.2, "cm")
  ) +
  guides(color = guide_legend(byrow = TRUE))
) %>% 
  ggsave(filename = paste0("../Results/Fig_DM_", plotdat$name, '.pdf'), width = 10, height = 8)

# numbers for intro
ret_for_plotting[eventDate>0 & eventDate <= Inf, .(mean(ret), mean(matchRet))]

ret_for_plotting %>% distinct(pubname)
czret %>% distinct(signalname)

## Plot for slide animation --------------------------------------------------

plotdat$name <- "t_min_2_0"

printme = ReturnPlotsWithDM(
  dt = ret_for_plotting %>% select(-matchRet),
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
(
  printme + theme(
  legend.background = element_rect(fill = "white", color = "black"
    , size = 0.3)
  # remove space where legend would be
  , legend.margin = margin(-1.0, 0.5, 0.5, 0.5, "cm")
  , legend.position  = c(44,15)/100
  # add space between legend items
  , legend.spacing.y = unit(0.2, "cm")
  ) +
  guides(color = guide_legend(byrow = TRUE))
) %>% 
  ggsave(filename = paste0("../Results/Fig_DM_", plotdat$name, '.pdf'), width = 10, height = 8)



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

## Plot with legend label for slides ------------------------------------
jj = 'risk'

plotdat$name <- paste0("t_min_2_cat_", jj, "_slides")
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
      paste0("Published with Risk Explanation"),
      paste0(plotdat$legprefix, " Mining Accounting"), 
      paste0(plotdat$legprefix, " Mining Tickers")
    ),
  yaxislab = 'Trailing 5-Year Return',
  legendpos = c(25,20)/100,
  filetype = '.png'
)  



# Accounting variables only plot  ----------------------------

signaldoc =  data.table::fread('../Data/Raw/SignalDoc.csv') %>% 
  filter(Cat.Data == 'Accounting')

plotdat$name <- "t_min_2AccountingOnly"

ret_for_plottingAnnualAccounting = ret_for_plotting %>% 
  select(-matchRetAlt) %>% 
  filter(pubname %in% unique(signaldoc$Acronym)) %>% 
  # Remove quarterly Compustat and a few others not constructed via annual CS
  filter(!(pubname %in% c("Cash", "ChTax", "EarningsSurprise", 
                          "NumEarnIncrease", "RevenueSurprise", "roaq",
                          'EarnSupBig',
                          'EarningsStreak',
                          'ShareIss1Y',
                          'ShareIss5Y')))

printme = ReturnPlotsWithDM(
  dt = ret_for_plottingAnnualAccounting,
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

ret_for_plottingAnnualAccounting[eventDate>0 & eventDate <= Inf & pubname %in% unique(signaldoc$Acronym), .(mean(ret), mean(matchRet))]

ret_for_plottingAnnualAccounting %>% filter(pubname %in% unique(signaldoc$Acronym)) %>% 
  distinct(pubname)

# tbc: Trailing N-year return plots --------------------------------------


