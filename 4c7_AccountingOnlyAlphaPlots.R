# Accounting Only Alpha Plots - Controlling for Data Source with Risk Adjustment
# Creates parallel exhibits using CAPM and FF4 alphas (TIME-VARYING betas)
# Restricted to published signals using Compustat Annual Accounting
# Consistent with 4c4 approach: IS betas for IS period, OOS betas for OOS period

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")
source("helpers/risk_adjusted_helpers_tv.R")

t_threshold = 2

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

# Load Fama-French factors
FamaFrenchFactors <- readRDS('../Data/Raw/FamaFrenchFactors.RData') %>%
  rename(date = yearm)

czret <- czret %>%
  left_join(FamaFrenchFactors, by = 'date')

# Load pre-computed risk-adjusted DM returns
DMname = paste0('../Data/Processed/',
                globalSettings$dataVersion,
                ' LongShort.RData')
DMshortname = DMname %>%
  str_remove('../Data/Processed/') %>%
  str_remove(' LongShort.RData')

risk_adj_file <- paste0('../Data/Processed/', DMshortname, ' MatchPubRiskAdjusted.RData')

if (!file.exists(risk_adj_file)) {
  stop("Risk-adjusted DM file not found. Run 2d_RiskAdjustDataMinedSignals.R first.")
}

candidateReturns_adj <- readRDS(risk_adj_file)

# Default Plot Settings --------------------------------------------------

fontsizeall = 28
legposall = c(30,15)/100
ylaball = 'Trailing 5-Year Alpha (bps pm)'
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
    , drop = if_else(grepl('quarter', Def), TRUE, drop)
    , drop = if_else(grepl('analyst|meanest|earningssurprise', Def), TRUE, drop)
    , drop = if_else(Cat.Form == 'discrete', TRUE, drop)
    , drop = if_else(signalname %in% c('ShareIss1Y', 'ShareIss5Y'), TRUE, drop)
  ) %>%
  select(signalname, drop, everything()) %>%
  arrange(-drop)

acct_signals = czacct[czacct$drop==FALSE, ]$signalname
print(paste0("Number of Compustat Annual Accounting signals: ", length(acct_signals)))

# Compute Published Signal Alphas -------------------------------------------

czret %>% setDT()
data.table::setorder(czret, signalname, eventDate)

# Compute raw return t-stats first (for additional filter)
czret[date >= sampstart & date <= sampend, `:=`(
  rbar_t = {
    m <- mean(ret, na.rm = TRUE)
    s <- sd(ret, na.rm = TRUE)
    n <- sum(!is.na(ret))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, rbar_t := nafill(rbar_t, 'locf'), by = .(signalname)]

# Get signals with raw t > threshold (for intersection with alpha filter)
signals_raw_t2 <- unique(czret[rbar_t > t_threshold]$signalname)
print(paste0("Signals with raw t>", t_threshold, ": ", length(signals_raw_t2)))

# Time-varying CAPM betas (IS betas for IS period, OOS betas for OOS period)
betas_capm_is <- czret[date >= sampstart & date <= sampend & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_is = extract_beta(ret, mktrf)), by = signalname]
betas_capm_oos <- czret[date > sampend & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_oos = extract_beta(ret, mktrf)), by = signalname]
czret <- merge(czret, betas_capm_is, by = 'signalname', all.x = TRUE)
czret <- merge(czret, betas_capm_oos, by = 'signalname', all.x = TRUE)
czret[, beta_capm_tv := ifelse(date >= sampstart & date <= sampend, beta_capm_is, beta_capm_oos)]
czret[, abnormal_capm_tv := ret - beta_capm_tv * mktrf]

# Compute in-sample alpha stats for filtering
czret[date >= sampstart & date <= sampend, `:=`(
  abar_capm_tv = mean(abnormal_capm_tv, na.rm = TRUE),
  abar_capm_tv_t = {
    m <- mean(abnormal_capm_tv, na.rm = TRUE)
    s <- sd(abnormal_capm_tv, na.rm = TRUE)
    n <- sum(!is.na(abnormal_capm_tv))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_capm_tv := nafill(abar_capm_tv, 'locf'), by = .(signalname)]
czret[, abar_capm_tv_t := nafill(abar_capm_tv_t, 'locf'), by = .(signalname)]
czret[, abnormal_capm_tv_normalized := ifelse(abs(abar_capm_tv) > 1e-10, 100 * abnormal_capm_tv / abar_capm_tv, NA_real_)]

# Time-varying FF4 betas (IS betas for IS period, OOS betas for OOS period)
ff4_is <- czret[date >= sampstart & date <= sampend & !is.na(ret) & !is.na(mktrf) & !is.na(smb) & !is.na(hml) & !is.na(umd), {
  coeffs <- extract_ff4_coeffs(ret, mktrf, smb, hml, umd)
  .(beta_ff4_is = coeffs[1], s_ff4_is = coeffs[2], h_ff4_is = coeffs[3], u_ff4_is = coeffs[4])
}, by = signalname]
ff4_oos <- czret[date > sampend & !is.na(ret) & !is.na(mktrf) & !is.na(smb) & !is.na(hml) & !is.na(umd), {
  coeffs <- extract_ff4_coeffs(ret, mktrf, smb, hml, umd)
  .(beta_ff4_oos = coeffs[1], s_ff4_oos = coeffs[2], h_ff4_oos = coeffs[3], u_ff4_oos = coeffs[4])
}, by = signalname]
czret <- merge(czret, ff4_is, by = 'signalname', all.x = TRUE)
czret <- merge(czret, ff4_oos, by = 'signalname', all.x = TRUE)
czret[, beta_ff4_tv := ifelse(date >= sampstart & date <= sampend, beta_ff4_is, beta_ff4_oos)]
czret[, s_ff4_tv := ifelse(date >= sampstart & date <= sampend, s_ff4_is, s_ff4_oos)]
czret[, h_ff4_tv := ifelse(date >= sampstart & date <= sampend, h_ff4_is, h_ff4_oos)]
czret[, u_ff4_tv := ifelse(date >= sampstart & date <= sampend, u_ff4_is, u_ff4_oos)]
czret[, abnormal_ff4_tv := ret - (beta_ff4_tv * mktrf + s_ff4_tv * smb + h_ff4_tv * hml + u_ff4_tv * umd)]

czret[date >= sampstart & date <= sampend, `:=`(
  abar_ff4_tv = mean(abnormal_ff4_tv, na.rm = TRUE),
  abar_ff4_tv_t = {
    m <- mean(abnormal_ff4_tv, na.rm = TRUE)
    s <- sd(abnormal_ff4_tv, na.rm = TRUE)
    n <- sum(!is.na(abnormal_ff4_tv))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_ff4_tv := nafill(abar_ff4_tv, 'locf'), by = .(signalname)]
czret[, abar_ff4_tv_t := nafill(abar_ff4_tv_t, 'locf'), by = .(signalname)]
czret[, abnormal_ff4_tv_normalized := ifelse(abs(abar_ff4_tv) > 1e-10, 100 * abnormal_ff4_tv / abar_ff4_tv, NA_real_)]

# Compute DM Signal Alpha Stats -------------------------------------------

dm_stats <- candidateReturns_adj[
  (date >= sampstart & date <= sampend) & !is.na(abnormal_capm_tv),
  .(
    abar_capm_dm_tv = mean(abnormal_capm_tv, na.rm = TRUE),
    abar_capm_dm_tv_t = {
      m <- mean(abnormal_capm_tv, na.rm = TRUE)
      s <- sd(abnormal_capm_tv, na.rm = TRUE)
      n <- sum(!is.na(abnormal_capm_tv))
      if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
    },
    abar_ff4_dm_tv = mean(abnormal_ff4_tv, na.rm = TRUE),
    abar_ff4_dm_tv_t = {
      m <- mean(abnormal_ff4_tv, na.rm = TRUE)
      s <- sd(abnormal_ff4_tv, na.rm = TRUE)
      n <- sum(!is.na(abnormal_ff4_tv))
      if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
    }
  ),
  by = .(actSignal, candSignalname)
]

# Plot 1: CAPM Alpha AccountingOnly |t|>2 ----------------------------

print("Creating CAPM Alpha AccountingOnly plot...")

# Filter published signals: accounting-only AND CAPM alpha t > threshold AND raw t > threshold
signals_capm_acct = intersect(
  acct_signals,
  unique(czret[abar_capm_tv_t > t_threshold]$signalname)
)
# Additionally require raw t-stat filter for consistency with 4c5
signals_capm_acct = intersect(signals_capm_acct, signals_raw_t2)
print(paste0("Accounting signals with CAPM alpha t>", t_threshold, " AND raw t>", t_threshold, ": ", length(signals_capm_acct)))

# Filter DM signals by CAPM alpha t > threshold
dm_filtered_capm <- candidateReturns_adj %>%
  inner_join(
    dm_stats %>% filter(abar_capm_dm_tv_t > t_threshold),
    by = c("actSignal", "candSignalname")
  )

# Normalize and aggregate DM alphas
dm_capm_agg <- dm_filtered_capm %>%
  group_by(actSignal, candSignalname) %>%
  mutate(
    abar_is = mean(abnormal_capm_tv[date >= sampstart & date <= sampend], na.rm = TRUE),
    abnormal_normalized = ifelse(abs(abar_is) > 1e-10, 100 * abnormal_capm_tv / abar_is, NA_real_)
  ) %>%
  ungroup() %>%
  group_by(actSignal, eventDate) %>%
  summarize(
    dm_mean = mean(abnormal_normalized, na.rm = TRUE),
    .groups = 'drop'
  )

# Create plot data
ret_for_plot_capm = czret %>%
  filter(signalname %in% signals_capm_acct) %>%
  transmute(pubname = signalname, eventDate, ret = abnormal_capm_tv_normalized,
            calendarDate = date, theory) %>%
  left_join(
    dm_capm_agg %>% transmute(pubname = actSignal, eventDate, matchRet = dm_mean),
    by = c("pubname", "eventDate")
  ) %>%
  filter(!is.na(matchRet))

print(paste0("Signals with CAPM matches: ", length(unique(ret_for_plot_capm$pubname))))

# Create plot
printme = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot_capm,
  basepath = "../Results/Fig_DM",
  suffix = "CAPM_AccountingOnly_CalendarSE",
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  xl = global_xl,
  xh = global_xh,
  legendlabels =
    c(
      paste0("Pub Compustat Annual (CAPM Alpha)"),
      paste0("Data-Mined for |t|>2.0 (CAPM Alpha)"),
      'N/A'
    ),
  legendpos = c(42,20)/100,
  fontsize = fontsizeall,
  yaxislab = 'Trailing 5-Year CAPM Alpha (bps pm)',
  linesize = linesizeall
)

printme2 = printme + theme(
  legend.background = element_rect(fill = "white", color = "black", size = 0.3),
  legend.margin = margin(-0.7, 0.5, 0.5, 0.5, "cm"),
  legend.position = c(44,15)/100,
  legend.spacing.y = unit(0.2, "cm")
) +
  guides(color = guide_legend(byrow = TRUE))

ggsave(paste0("../Results/Fig_DM_CAPM_tv_AccountingOnly_CalendarSE.pdf"), width = 10, height = 8)

# Plot 2: FF4 Alpha AccountingOnly |t|>2 ----------------------------

print("Creating FF4 Alpha AccountingOnly plot...")

# Filter published signals: accounting-only AND FF4 alpha t > threshold AND raw t > threshold
signals_ff4_acct = intersect(
  acct_signals,
  unique(czret[abar_ff4_tv_t > t_threshold]$signalname)
)
# Additionally require raw t-stat filter for consistency with 4c5
signals_ff4_acct = intersect(signals_ff4_acct, signals_raw_t2)
print(paste0("Accounting signals with FF4 alpha t>", t_threshold, " AND raw t>", t_threshold, ": ", length(signals_ff4_acct)))

# Filter DM signals by FF4 alpha t > threshold
dm_filtered_ff4 <- candidateReturns_adj %>%
  inner_join(
    dm_stats %>% filter(abar_ff4_dm_tv_t > t_threshold),
    by = c("actSignal", "candSignalname")
  )

# Normalize and aggregate DM alphas
dm_ff4_agg <- dm_filtered_ff4 %>%
  group_by(actSignal, candSignalname) %>%
  mutate(
    abar_is = mean(abnormal_ff4_tv[date >= sampstart & date <= sampend], na.rm = TRUE),
    abnormal_normalized = ifelse(abs(abar_is) > 1e-10, 100 * abnormal_ff4_tv / abar_is, NA_real_)
  ) %>%
  ungroup() %>%
  group_by(actSignal, eventDate) %>%
  summarize(
    dm_mean = mean(abnormal_normalized, na.rm = TRUE),
    .groups = 'drop'
  )

# Create plot data
ret_for_plot_ff4 = czret %>%
  filter(signalname %in% signals_ff4_acct) %>%
  transmute(pubname = signalname, eventDate, ret = abnormal_ff4_tv_normalized,
            calendarDate = date, theory) %>%
  left_join(
    dm_ff4_agg %>% transmute(pubname = actSignal, eventDate, matchRet = dm_mean),
    by = c("pubname", "eventDate")
  ) %>%
  filter(!is.na(matchRet))

print(paste0("Signals with FF4 matches: ", length(unique(ret_for_plot_ff4$pubname))))

# Create plot
printme = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot_ff4,
  basepath = "../Results/Fig_DM",
  suffix = "FF4_AccountingOnly_CalendarSE",
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  xl = global_xl,
  xh = global_xh,
  legendlabels =
    c(
      paste0("Pub Compustat Annual (FF4 Alpha)"),
      paste0("Data-Mined for |t|>2.0 (FF4 Alpha)"),
      'N/A'
    ),
  legendpos = c(42,20)/100,
  fontsize = fontsizeall,
  yaxislab = 'Trailing 5-Year FF4 Alpha (bps pm)',
  linesize = linesizeall
)

printme2 = printme + theme(
  legend.background = element_rect(fill = "white", color = "black", size = 0.3),
  legend.margin = margin(-0.7, 0.5, 0.5, 0.5, "cm"),
  legend.position = c(44,15)/100,
  legend.spacing.y = unit(0.2, "cm")
) +
  guides(color = guide_legend(byrow = TRUE))

ggsave(paste0("../Results/Fig_DM_FF4_tv_AccountingOnly_CalendarSE.pdf"), width = 10, height = 8)

print("Done! Created AccountingOnly alpha figures (time-varying betas):")
print("  1. Fig_DM_CAPM_tv_AccountingOnly_CalendarSE.pdf")
print("  2. Fig_DM_FF4_tv_AccountingOnly_CalendarSE.pdf")
