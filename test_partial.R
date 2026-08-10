# Risk-Adjusted Data-mining comparisons 
# Based on 4c2_ResearchVsDMPlots.R but with CAPM and FF3 adjustments
# This file compares raw vs risk-adjusted returns for published vs data-mined signals

# Setup ----------------------------------------------------------------
rm(list = ls())
source("0_Environment.R")
library(doParallel)

# Create results subfolder for risk-adjusted analysis
results_dir <- "../Results/RiskAdjusted"
if (!dir.exists(results_dir)) {
  dir.create(results_dir, recursive = TRUE)
  cat("Created directory:", results_dir, "\n")
} else {
  cat("Using existing directory:", results_dir, "\n")
}

# Check if risk-adjusted DM files exist
DMname = paste0('../Data/Processed/',
                globalSettings$dataVersion, 
                ' LongShort.RData')

DMshortname = DMname %>% 
  str_remove('../Data/Processed/') %>% 
  str_remove(' LongShort.RData')

risk_adj_file <- paste0('../Data/Processed/', DMshortname, ' MatchPubRiskAdjusted.RData')
summary_file <- paste0('../Data/Processed/', DMshortname, ' MatchedRiskAdjSummary.RData')

if (!file.exists(risk_adj_file) | !file.exists(summary_file)) {
  cat("Risk-adjusted DM files not found. Running 2d_RiskAdjustDataMinedSignals.R...\n")
  source("2d_RiskAdjustDataMinedSignals.R")
}

# Helper functions -----------------------------------------------------
extract_beta <- function(x, y) {
  model <- lm(y ~ x)
  bet <- coef(model)[2]
  return(bet)
}

extract_ff3_coeffs <- function(ret, mktrf, smb, hml) {
  model <- lm(ret ~ mktrf + smb + hml)
  coeffs <- coef(model)
  return(coeffs[2:4])  # Return beta, s, and h coefficients
}

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
  mutate(
    retOrig = ret,
    ret_scaled = ret / rbar * 100
  ) %>% 
  filter(signalname %in% inclSignals)

# Load pre-computed dm sumstats
dmcomp <- readRDS("../Data/Processed/dmcomp_sumstats.RDS")
dmtic <- readRDS("../Data/Processed/dmtic_sumstats.RDS")

# Load pre-computed matched returns
ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")
ret_for_plot1 <- readRDS("../Data/Processed/ret_for_plot1.RDS")

# Load pre-computed risk-adjusted DM returns
# Load individual DM returns for t-stat computation
candidateReturns_adj <- readRDS(risk_adj_file)

# Load aggregated summary (we'll recompute this with filtering)
matched_risk_adj <- readRDS(summary_file)

# Load FF factors and join ------------------------------------------------
FamaFrenchFactors <- readRDS('../Data/Raw/FamaFrenchFactors.RData') %>%
  rename(date = yearm)

czret <- czret %>% 
  left_join(FamaFrenchFactors, by = c('date'))

ret_for_plot0 <- ret_for_plot0 %>%
  left_join(
    czret %>% select(signalname, eventDate, date, mktrf, smb, hml),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  )

ret_for_plot1 <- ret_for_plot1 %>%
  left_join(
    czret %>% select(signalname, eventDate, date, mktrf, smb, hml),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  )

# Compute Risk Adjustments ------------------------------------------------

## CAPM adjustments - full sample betas
czret %>% setDT()

# Full-sample betas for published signals
czret[, beta_capm := extract_beta(ret_scaled, mktrf*100), by = signalname]
czret[, abnormal_capm := ret_scaled - beta_capm*mktrf*100]

# Normalize abnormal returns by in-sample mean and compute t-stats
czret[samptype == 'insamp', `:=`(
  abar_capm = mean(abnormal_capm, na.rm = TRUE),
  abar_capm_t = mean(abnormal_capm, na.rm = TRUE) / sd(abnormal_capm, na.rm = TRUE) * sqrt(.N)
), by = signalname]
czret[, abar_capm := nafill(abar_capm, "locf"), by = .(signalname)]
czret[, abar_capm_t := nafill(abar_capm_t, "locf"), by = .(signalname)]
czret[, abnormal_capm_normalized := 100*abnormal_capm/abar_capm]

## FF3 adjustments - full sample coefficients  
czret[, c("beta_ff3", "s_ff3", "h_ff3") := {
  coeffs <- extract_ff3_coeffs(ret_scaled, mktrf*100, smb*100, hml*100)
  list(coeffs[1], coeffs[2], coeffs[3])
}, by = signalname]

czret[, abnormal_ff3 := ret_scaled - (beta_ff3*mktrf*100 + s_ff3*smb*100 + h_ff3*hml*100)]

# Normalize FF3 abnormal returns and compute t-stats
czret[samptype == 'insamp', `:=`(
  abar_ff3 = mean(abnormal_ff3, na.rm = TRUE),
  abar_ff3_t = mean(abnormal_ff3, na.rm = TRUE) / sd(abnormal_ff3, na.rm = TRUE) * sqrt(.N)
), by = signalname]
czret[, abar_ff3 := nafill(abar_ff3, "locf"), by = .(signalname)]
czret[, abar_ff3_t := nafill(abar_ff3_t, "locf"), by = .(signalname)]
czret[, abnormal_ff3_normalized := 100*abnormal_ff3/abar_ff3]

# Also compute raw return t-stats for comparison
czret[samptype == 'insamp', `:=`(
  rbar_scaled = mean(ret_scaled, na.rm = TRUE),
  rbar_scaled_t = mean(ret_scaled, na.rm = TRUE) / sd(ret_scaled, na.rm = TRUE) * sqrt(.N)
), by = signalname]
czret[, rbar_scaled_t := nafill(rbar_scaled_t, "locf"), by = .(signalname)]

# Compute normalized DM abnormal returns ---------------------------------

# Add samptype to matched_risk_adj for normalization
matched_risk_adj <- matched_risk_adj %>%
  left_join(
    czret %>% select(signalname, eventDate, samptype) %>% distinct(),
    by = c("actSignal" = "signalname", "eventDate" = "eventDate")
  )

# Compute in-sample means, t-stats, and normalize
matched_risk_adj <- matched_risk_adj %>%
  group_by(actSignal) %>%
  mutate(
    # Compute in-sample means and t-stats
    abar_capm_dm = mean(matchRet_capm[samptype == "insamp"], na.rm = TRUE),
    abar_ff3_dm = mean(matchRet_ff3[samptype == "insamp"], na.rm = TRUE),
    abar_capm_dm_t = mean(matchRet_capm[samptype == "insamp"], na.rm = TRUE) / 
                     sd(matchRet_capm[samptype == "insamp"], na.rm = TRUE) * 
                     sqrt(sum(samptype == "insamp" & !is.na(matchRet_capm))),
    abar_ff3_dm_t = mean(matchRet_ff3[samptype == "insamp"], na.rm = TRUE) / 
                    sd(matchRet_ff3[samptype == "insamp"], na.rm = TRUE) * 
                    sqrt(sum(samptype == "insamp" & !is.na(matchRet_ff3))),
    # Forward fill the in-sample means
    abar_capm_dm = ifelse(is.na(abar_capm_dm), NA, abar_capm_dm),
    abar_ff3_dm = ifelse(is.na(abar_ff3_dm), NA, abar_ff3_dm),
    # Normalize
    matchRet_capm_normalized = 100 * matchRet_capm / abar_capm_dm,
    matchRet_ff3_normalized = 100 * matchRet_ff3 / abar_ff3_dm
  ) %>%
  ungroup()

# Add risk adjustments to plotting data ----------------------------------

# Function to add risk adjustments to ret_for_plot data
add_risk_adjustments <- function(plot_data, czret_data, dm_risk_adj) {
  plot_data %>%
    left_join(
      czret_data %>% select(signalname, eventDate, abnormal_capm_normalized, abnormal_ff3_normalized,
                           rbar_scaled_t, abar_capm_t, abar_ff3_t),
      by = c("pubname" = "signalname", "eventDate" = "eventDate")
    ) %>%
    left_join(
      dm_risk_adj %>% select(actSignal, eventDate, matchRet_capm_normalized, matchRet_ff3_normalized),
      by = c("pubname" = "actSignal", "eventDate" = "eventDate")
    )
}

ret_for_plot0_adj <- add_risk_adjustments(ret_for_plot0, czret, matched_risk_adj)
ret_for_plot1_adj <- add_risk_adjustments(ret_for_plot1, czret, matched_risk_adj)

# Default Plot Settings --------------------------------------------------
fontsizeall = 28
legposall = c(30,15)/100
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5
global_xl = -360  
global_xh = 300   

print("Risk adjustments computed successfully!")
print(paste("Number of signals with CAPM adjustments:", sum(!is.na(czret$beta_capm))))
print(paste("Number of signals with FF3 adjustments:", sum(!is.na(czret$beta_ff3))))

# Risk-Adjusted Plots ----------------------------------------------------

## 1. Raw Returns (baseline from 4c2) ------------------------------------
tempsuffix = "raw_returns"

printme_raw = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, eventDate, date) %>% distinct(), by = c("pubname" = "signalname", "eventDate" = "eventDate")) %>%
    rename(calendarDate = date),
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
      paste0("Published (Raw Returns)"),
      paste0("Data-Mined for |t|>2.0 (Raw)"),
      'N/A'
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_", tempsuffix, '.pdf'), 
       printme_raw, width = 10, height = 8)

# Print summary statistics
cat("\n=== RAW RETURNS PLOT STATISTICS ===\n")
ret_for_plot0 %>% 
  filter(!is.na(matchRet)) %>%
  summarise(
    pub_mean_insamp = mean(ret[eventDate <= 0], na.rm = TRUE),
    pub_mean_oos = mean(ret[eventDate > 0], na.rm = TRUE),
    dm_mean_insamp = mean(matchRet[eventDate <= 0], na.rm = TRUE),
    dm_mean_oos = mean(matchRet[eventDate > 0], na.rm = TRUE)
  ) %>% print()

cat("\nNumber of signals:", length(unique(ret_for_plot0$pubname[!is.na(ret_for_plot0$matchRet)])), "\n")

## 2. CAPM-Adjusted Returns ----------------------------------------------
tempsuffix = "capm_adjusted"

# Create CAPM-adjusted version of plotting data
ret_for_plot0_capm <- ret_for_plot0_adj %>%
  filter(!is.na(abnormal_capm_normalized), !is.na(matchRet_capm_normalized))

printme_capm = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot0_capm %>% 
    transmute(eventDate, pubname, theory, ret = abnormal_capm_normalized, matchRet = matchRet_capm_normalized) %>%
    left_join(czret %>% select(signalname, eventDate, date) %>% distinct(), by = c("pubname" = "signalname", "eventDate" = "eventDate")) %>%
    rename(calendarDate = date),
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
      paste0("Published (CAPM Alpha)"),
      paste0("Data-Mined for |t|>2.0 (CAPM Alpha)"),
      'N/A'
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = "Trailing 5-Year CAPM Alpha (% of In-Sample Alpha)",
  linesize = linesizeall
)

ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_", tempsuffix, '.pdf'), 
       printme_capm, width = 10, height = 8)

# Print summary statistics
cat("\n=== CAPM NORMALIZED ALPHA PLOT STATISTICS ===\n")
ret_for_plot0_capm %>% 
  summarise(
    pub_mean_insamp = mean(abnormal_capm_normalized[eventDate <= 0], na.rm = TRUE),
    pub_mean_oos = mean(abnormal_capm_normalized[eventDate > 0], na.rm = TRUE),
    dm_mean_insamp = mean(matchRet_capm_normalized[eventDate <= 0], na.rm = TRUE),
    dm_mean_oos = mean(matchRet_capm_normalized[eventDate > 0], na.rm = TRUE)
  ) %>% print()
print("Partial test completed")
