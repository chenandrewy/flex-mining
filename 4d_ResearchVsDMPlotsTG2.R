# Data-mining comparisons 
# These are the new main plots as of 2024 04.

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

# Convenience Save 
save.image("../Data/tmpTG2Plots.RData")

# Convenience Load 
load("../Data/tmpTG2Plots.RData")


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

# save for slides
ggsave(paste0("../Results/Fig_DM_", plotdat$name, '.png'), width = 10, height = 8)

# numbers for intro
ret_for_plotting[eventDate>0 & eventDate <= Inf, .(mean(ret), mean(matchRet))]

ret_for_plotting %>% distinct(pubname)
czret %>% distinct(signalname)

## Plot for slide animation --------------------------------------------------

printme = ReturnPlotsWithDM(
  dt = ret_for_plotting %>% select(-matchRetAlt, -matchRet),
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

# save for slides
ggsave(paste0("../Results/Fig_DM_", plotdat$name, '_0.png'), width = 10, height = 8)


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



