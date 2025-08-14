# Data-mining comparisons 
# These are the new main plots as of 2024 04.
# This file is extremely slow unless you have a lot of RAM, it seems.

# Fast Setup --------------------------------------------------------

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
    select(signalname, Year, theory, Journal) %>% 
    filter(signalname %in% inclSignals)

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  left_join(czcat, by = "signalname") %>%
  mutate(ret_scaled = ret / rbar * 100) %>% 
  filter(signalname %in% inclSignals)

# Load pre-computed dm sumstats
dmcomp <- readRDS("../Data/Processed/dmcomp_sumstats.RDS")
dmtic <- readRDS("../Data/Processed/dmtic_sumstats.RDS")

# Load pre-computed matched returns
ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")
ret_for_plot1 <- readRDS("../Data/Processed/ret_for_plot1.RDS")
ret_for_plot_MaxPredictors <- readRDS("../Data/Processed/ret_for_plot_MaxPredictors.RDS")

# Default Plot Settings --------------------------------------------------

# aesthetic settings
fontsizeall = 28
legposall = c(30,15)/100
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5

# x-axis range for all plots
global_xl = -360  # x-axis lower bound
global_xh = 300   # x-axis upper bound

# Intro Plot --------------------------------------------------

## plot --------------------------------------------------

tempsuffix = "t_min_2"

printme = ReturnPlotsWithDM(
  dt = ret_for_plot0 %>% filter(!is.na(matchRet)),
  basepath = "../Results/temp_",
  suffix = tempsuffix,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  xl = global_xl,
  xh = global_xh,
  legendlabels =
    c(
      paste0("Published (and Peer Reviewed)"),
      paste0("Data-Mined for |t|>2.0 in Original Sample"),
      'N/A'
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
  ggsave(filename = paste0("../Results/Fig_DM_", tempsuffix, '.pdf'), width = 10, height = 8)

file.remove(paste0("../Results/temp__", tempsuffix, ".pdf"))

## numbers for intro --------------------------------------------------

sink(paste0("../Results/Fig_DM_", tempsuffix, '.md'))

print(paste0("Mean post-samp return in this figure: "))
ret_for_plot0 %>% 
              filter(eventDate > 0 & eventDate <= Inf) %>% 
              summarize(
    pub_mean_pool = mean(ret), dm_mean_pool = mean(matchRet)
  ) %>% print()

print(paste0("number of published signals by eventDate: "))
ret_for_plot0 %>% 
  filter(eventDate%%60 == 0, eventDate >= 0) %>% 
  group_by(eventDate) %>% 
  summarize(
    n_pub = n_distinct(pubname)
  ) %>% print()

sink()  

name1 = ret_for_plot0 %>% filter(eventDate ==-60) %>% pull(pubname) %>% unique()
name2 = ret_for_plot0 %>% filter(eventDate ==0) %>% pull(pubname) %>% unique()

setdiff(name2, name1)

temp = ret_for_plot0[pubname=='ProbInformedTrading']
View(temp)


## plot with Calendar SE --------------------------------------------------

tempsuffix = "t_min_2_se_indicators_calendar"

printme = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot0 %>% filter(!is.na(matchRet)),
  basepath = "../Results/temp_",
  suffix = tempsuffix,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  xl = global_xl,
  xh = global_xh,
  legendlabels =
    c(
      paste0("Published (and Peer Reviewed)"),
      paste0("Data-Mined for |t|>2.0 in Original Sample"),
      'N/A'
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
  ggsave(filename = paste0("../Results/Fig_DM_", tempsuffix, '.pdf'), width = 10, height = 8)

file.remove(paste0("../Results/temp__", tempsuffix, ".pdf"))

## Plot for slide animation --------------------------------------------------

tempname = "t_min_2_0"

printme = ReturnPlotsWithDM(
  dt = ret_for_plot0 %>% filter(!is.na(matchRet)) %>% select(-matchRet),
  basepath = "../Results/temp_Fig_DM",
  suffix = tempname,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  xl = global_xl,
  xh = global_xh,
  legendlabels =
    c(
      paste0("Published (and Peer Reviewed)"),
      paste0("Data-Mined for |t|>2.0 in Original Sample"),
      'N/A'
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
  ggsave(filename = paste0("../Results/Fig_DM_", tempname, '.pdf'), width = 10, height = 8)

file.remove("../Results/temp_Fig_DM_t_min_2_0.pdf")

# Plot by Theory Category -------------------------------------

# loop over theory categories
for (jj in unique(czret$theory)) {
  print(jj)
  tempname <- paste0("t_min_2_cat_", jj)
  
  # ret_for_plotting = make_ret_for_plotting(plotdat, theory_filter = jj)
  tempret = ret_for_plot0 %>% filter(theory == jj) %>% 
    filter(!is.na(matchRet))
  
  # Plot
  plt = ReturnPlotsWithDM(
    dt = tempret,  
    basepath = "../Results/Fig_DM",
    suffix = tempname,
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl =-100, yh = 175,
    xl = global_xl, xh = global_xh,
    fig.width = 18,
    fontsize = 38,
    legendlabels =
      c(
        paste0("Published"),
        paste0("|t|>2.0 Mining Accounting"),
        'N/A'
      ),
    yaxislab = 'Trailing 5-Year Return',
    legendpos = c(25,20)/100,
  )  

  # Plot with Calendar SE
  plt_cal = ReturnPlotsWithDM_std_errors_indicators(
    dt = tempret,
    basepath = "../Results/Fig_DM_SE_Calendar",
    suffix = tempname,
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl =-100, yh = 175,
    xl = global_xl, xh = global_xh,
    fig.width = 18,
    fontsize = 38,
    legendlabels =
      c(
        paste0("Published with ", str_to_title(jj), " Explanation"),
        paste0("|t|>2.0 Mining Accounting"),
        'N/A'
      ),
    yaxislab = 'Trailing 5-Year Return',
    legendpos = c(25,20)/100,
  )
  
}

# Trailing N-year return plots --------------------------------------

## Plot ====

# loop over n_year_roll
n_year_list = c(1, 3, 5, 10)

for (n_year_roll in n_year_list) {
  vlabel = if (n_year_roll == 1){
    "1 year post-samp"
  }else{
    paste0(n_year_roll, " years post-samp")
  }
  (ReturnPlotsWithDM(
    dt = ret_for_plot1 %>% filter(!is.na(matchRet)),
    basepath = "../Results/temp_Fig_DM",
    suffix = "temp",
    rollmonths = n_year_roll * 12,
    colors = colors,
    labelmatch = FALSE,
    xl = global_xl, xh = global_xh,
    yl = -0, yh = 150,
    legendlabels =
      c(
        paste0("Published"),
        paste0("|t|>2.0 Mining Accounting"),
        paste0("Top 5% |t| Mining Accounting")
      ),
    legendpos = c(30,20)/100,
    fontsize = fontsizeall,
    yaxislab = paste0("Trailing ", n_year_roll, "-Year Return (bps pm)"),
    linesize = linesizeall
  ) +
      geom_vline(xintercept = n_year_roll * 12, linetype = "dashed",
                 color = "magenta") +
      annotate("text", x = n_year_roll * 12 + 6, y = 140, 
               label = vlabel,
               vjust = 1.5, hjust = 0, size = 6, color = "magenta")
  ) %>% 
    ggsave(filename = paste0("../Results/Fig_DM_Roll", n_year_roll, ".pdf"),
           width = 10, height = 8)
} # end loop over n_year_roll

# Accounting variables only plot  ----------------------------

# get accounting signals
czacct = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>% 
  left_join(fread('../Data/Raw/SignalDoc.csv') %>% 
    transmute(Acronym, Cat.Data, Cat.Form, Def = tolower(`Detailed Definition`)) 
    , by = c('signalname' = 'Acronym')) %>% 
  filter(signalname %in% inclSignals & Cat.Data == 'Accounting')

# find accounting signals to drop
czacct = czacct %>% 
  mutate(
    drop = FALSE
    # drop quarterly compustat
    , drop = if_else(grepl('quarter', Def), TRUE, drop)
    # drop analyst forecasts
    , drop = if_else(grepl('analyst|meanest|earningssurprise', Def), TRUE, drop)
    # drop discrete signals
    , drop = if_else(Cat.Form == 'discrete', TRUE, drop)
    # drop ShareIss1Y and ShareIss5Y because they use only crsp data
    , drop = if_else(signalname %in% c('ShareIss1Y', 'ShareIss5Y'), TRUE, drop)
  ) %>% 
  select(signalname, drop, everything()) %>% 
  arrange(-drop) 


ret_for_plottingAnnualAccounting = ret_for_plot1 %>% 
  # keep only published signals that use annual compustat, no double sorts, no weird discrete signals
  filter(pubname %in% czacct[czacct$drop==FALSE, ]$signalname) %>% 
  select(-matchRet) %>% 
  # select top 5% t-stats as the matchRet (bad notation, sorry)
  rename(matchRet = matchRetAlt) %>%
  filter(!is.na(matchRet))


# Add calendar date to the dataset for clustering standard errors
ret_for_plottingAnnualAccounting = ret_for_plottingAnnualAccounting %>%
  left_join(
    czret %>% select(signalname, eventDate, date),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  ) %>%
  rename(calendarDate = date)

# Use the calendar time-based standard errors function
printme = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plottingAnnualAccounting,
  basepath = "../Results/Fig_DM",
  suffix = "t_top5Pct_AccountingOnly_CalendarSE",
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  xl = global_xl,
  xh = global_xh,
  legendlabels =
    c(
      paste0("Pub Compustat Annual"),
      paste0("Data-Mined for Top 5% |t| in Original Sample"),
      'N/A'
    ),
  legendpos = c(42,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

# custom edits 
printme2 = printme + theme(
  legend.background = element_rect(fill = "white", color = "black", size = 0.3),
  # remove space where legend would be
  legend.margin = margin(-0.7, 0.5, 0.5, 0.5, "cm"),
  legend.position = c(44,15)/100,
  # add space between legend items
  legend.spacing.y = unit(0.2, "cm")
) +
  guides(color = guide_legend(byrow = TRUE))

# save
ggsave(paste0("../Results/Fig_DM_t_top5Pct_AccountingOnly_CalendarSE.pdf"), width = 10, height = 8)


# Restricting the number of DM predictors per paper -----------------------
# Note: The normalization of DM strategies to have an in-sample mean of 100
# occurs in the `make_DM_event_returns` function in `0_Environment.R`.

# Prep for plotting
ret_for_plot_MaxPredictors1 = ret_for_plot_MaxPredictors %>%
  filter(!is.na(matchRet), maxDMpredictors == 100) %>%
  left_join(
    ret_for_plot_MaxPredictors %>% 
      filter(!is.na(matchRet), maxDMpredictors == 1000) %>%
      transmute(eventDate, pubname, matchRetAlt = matchRet),
    by = c("pubname", "eventDate")
  )

# Plot
printme = ReturnPlotsWithDM(
  dt = ret_for_plot_MaxPredictors1 %>% filter(!is.na(matchRet)),
  basepath = "../Results/Fig_DM",
  suffix = "MaxDMpredsPerPublished",
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  xl = global_xl,
  xh = global_xh,
  legendlabels =
    c(
      paste0("Published (and Peer Reviewed)"),
      paste0("Data-Mined for Top 100 |t| in Original Sample"),
      paste0("Data-Mined for Top 1000 |t| in Original Sample")
    ),
  legendpos = c(47,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

# Use journal definitions from globalSettings ------------------------
top_finance = globalSettings$top3Finance
top_accounting = globalSettings$top3Accounting

# Add journal classifications to ret_for_plot0
ret_for_plot_journal <- ret_for_plot0 %>%
  left_join(
    czcat %>% 
      # Exclude top econ journals from the analysis entirely # Note: AER, Econometrica, REStud not in data
      filter(!Journal %in% c('QJE', 'JPE')) %>%
      mutate(journaltype = case_when(
        Journal %in% top_finance ~ 'Top 3 Finance',
        Journal %in% top_accounting ~ 'Top 3 Accounting',
        TRUE ~ 'Other'
      )) %>%
      mutate(journaltype = factor(journaltype, 
        levels = c('Top 3 Finance', 'Top 3 Accounting', 'Other')
      )) %>%
      select(signalname, journaltype),
    by = c("pubname" = "signalname")
  )

# Plot by Journal Category
for (jj in levels(ret_for_plot_journal$journaltype)) {
  print(jj)
  tempname <- paste0("t_min_2_journal_", gsub(" ", "", jj))

  tempret = ret_for_plot_journal %>% 
    filter(journaltype == jj) %>% 
    filter(!is.na(matchRet))
  
  # Count unique signals in this category
  n_signals = length(unique(tempret$pubname))
  
  # Plot
  plt = ReturnPlotsWithDM(
    dt = tempret,  
    basepath = "../Results/Fig_DM",
    suffix = tempname,
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl = -0, yh = 125,
    xl = global_xl, xh = global_xh,
    fig.width = 18,
    fontsize = 38,
    legendlabels =
      c(
        paste0("Published in ", jj, " (N=", n_signals, ")"),
        paste0("|t|>2.0 Mining Accounting"),
        'N/A'
      ),
    yaxislab = 'Trailing 5-Year Return',
    legendpos = c(25,20)/100,
  )  
}
