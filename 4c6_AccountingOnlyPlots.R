# Accounting Only Plots - Controlling for Data Source
# Creates parallel exhibits for |t|>2 and top 5% |t| filters
# Restricted to published signals using Compustat Annual Accounting

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")

## Load Global Data -------------------------------------------

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

# Load pre-computed matched returns
ret_for_plot1 <- readRDS("../Data/Processed/ret_for_plot1.RDS")

# Default Plot Settings --------------------------------------------------

fontsizeall = 28
legposall = c(30,15)/100
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5

global_xl = -360
global_xh = 300

# Identify Accounting-Only Signals ----------------------------

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

# Filter to accounting-only published signals
acct_signals = czacct[czacct$drop==FALSE, ]$signalname

print(paste0("Number of Compustat Annual Accounting signals: ", length(acct_signals)))

# Plot 1: |t|>2 AccountingOnly ----------------------------

ret_for_plotting_t2 = ret_for_plot1 %>%
  filter(pubname %in% acct_signals) %>%
  select(-matchRetAlt) %>%  # remove top 5% column, keep only |t|>2
  filter(!is.na(matchRet)) %>%
  left_join(
    czret %>% select(signalname, eventDate, date),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  ) %>%
  rename(calendarDate = date)

print(paste0("Number of signals with |t|>2 matches: ", length(unique(ret_for_plotting_t2$pubname))))

printme = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plotting_t2,
  basepath = "../Results/Fig_DM",
  suffix = "t_min_2_AccountingOnly_CalendarSE",
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
      paste0("Data-Mined for |t|>2.0 in Original Sample"),
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
  legend.margin = margin(-0.7, 0.5, 0.5, 0.5, "cm"),
  legend.position = c(44,15)/100,
  legend.spacing.y = unit(0.2, "cm")
) +
  guides(color = guide_legend(byrow = TRUE))

ggsave(paste0("../Results/Fig_DM_t_min_2_AccountingOnly_CalendarSE.pdf"), width = 10, height = 8)

# Plot 2: Top 5% |t| AccountingOnly ----------------------------

ret_for_plotting_top5 = ret_for_plot1 %>%
  filter(pubname %in% acct_signals) %>%
  select(-matchRet) %>%
  rename(matchRet = matchRetAlt) %>%  # matchRetAlt is top 5% |t|
  filter(!is.na(matchRet)) %>%
  left_join(
    czret %>% select(signalname, eventDate, date),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  ) %>%
  rename(calendarDate = date)

print(paste0("Number of signals with top 5% |t| matches: ", length(unique(ret_for_plotting_top5$pubname))))

printme = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plotting_top5,
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
  legend.margin = margin(-0.7, 0.5, 0.5, 0.5, "cm"),
  legend.position = c(44,15)/100,
  legend.spacing.y = unit(0.2, "cm")
) +
  guides(color = guide_legend(byrow = TRUE))

ggsave(paste0("../Results/Fig_DM_t_top5Pct_AccountingOnly_CalendarSE.pdf"), width = 10, height = 8)

print("Done! Created two AccountingOnly figures:")
print("  1. Fig_DM_t_min_2_AccountingOnly_CalendarSE.pdf")
print("  2. Fig_DM_t_top5Pct_AccountingOnly_CalendarSE.pdf")
