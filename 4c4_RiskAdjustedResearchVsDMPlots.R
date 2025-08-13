# Risk-Adjusted Data-mining comparisons 
# Based on 4c2_ResearchVsDMPlots.R but with CAPM and FF3 adjustments
# This file compares raw vs risk-adjusted returns for published vs data-mined signals

# Setup ----------------------------------------------------------------
rm(list = ls())
source("0_Environment.R")
source("helpers/risk_adjusted_helpers.R")

t_threshold = 2
# For raw returns: threshold is in basis points (e.g., 15 = 0.15% per month)
# For CAPM/FF3 alphas: values are already normalized to ~100, so use 15 for consistency
return_threshold = 0.3  # basis points for raw returns/alphas. Returns and FF5 factors are in percentages so 0.3 = 30 bps per month

# Filter type: "tstat" for t-stat filtering, "return" for return filtering
# Check for environment variable first, otherwise use default
filter_type <- Sys.getenv("FILTER_TYPE", "tstat")  # Default to t filtering

# Create results subfolder for risk-adjusted analysis
base_results_dir <- "../Results/RiskAdjusted"
if (filter_type == "return") {
  # Format threshold for directory name (e.g., 15 -> "15", 0.15 -> "0.15")
  threshold_str <- if (return_threshold < 1) paste0("0.", return_threshold * 100) else as.character(return_threshold)
  results_dir <- file.path(base_results_dir, paste0("ReturnFilter_", threshold_str))
} else {
  results_dir <- file.path(base_results_dir, "TstatFilter")
}

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


# Helper function to normalize and aggregate DM returns with t-stat filtering

# Helper function to create filtered plot data

# NEW: Helper to aggregate DM without normalization (raw units)

# Helper function to create and save risk-adjusted plots

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

# Load pre-computed matched returns
ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")

# Load pre-computed risk-adjusted DM returns
# Load individual DM returns for t-stat computation
candidateReturns_adj <- readRDS(risk_adj_file)

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

## CAPM adjustments - full sample betas
czret %>% setDT()
# Ensure stable ordering before any LOCF fills
data.table::setorder(czret, signalname, eventDate)

print(czret[, .(
  ret_scaled_min = min(ret_scaled, na.rm = TRUE),
  ret_scaled_p05 = quantile(ret_scaled, 0.05, na.rm = TRUE),
  ret_scaled_p25 = quantile(ret_scaled, 0.25, na.rm = TRUE),
  ret_scaled_p75 = quantile(ret_scaled, 0.75, na.rm = TRUE),
  ret_scaled_p95 = quantile(ret_scaled, 0.95, na.rm = TRUE),
  ret_scaled_max = max(ret_scaled, na.rm = TRUE),
  ret_min = min(ret, na.rm = TRUE),
  ret_p05 = quantile(ret, 0.05, na.rm = TRUE),
  ret_p25 = quantile(ret, 0.25, na.rm = TRUE),
  ret_p75 = quantile(ret, 0.75, na.rm = TRUE),
  ret_p95 = quantile(ret, 0.95, na.rm = TRUE),
  ret_max = max(ret, na.rm = TRUE),
  mktrf_min = min(mktrf, na.rm = TRUE),
  mktrf_p05 = quantile(mktrf, 0.05, na.rm = TRUE),
  mktrf_p25 = quantile(mktrf, 0.25, na.rm = TRUE),
  mktrf_p75 = quantile(mktrf, 0.75, na.rm = TRUE),
  mktrf_p95 = quantile(mktrf, 0.95, na.rm = TRUE),
  mktrf_max = max(mktrf, na.rm = TRUE),
  smb_min = min(smb, na.rm = TRUE),
  smb_p05 = quantile(smb, 0.05, na.rm = TRUE),
  smb_p25 = quantile(smb, 0.25, na.rm = TRUE),
  smb_p75 = quantile(smb, 0.75, na.rm = TRUE),
  smb_p95 = quantile(smb, 0.95, na.rm = TRUE),
  smb_max = max(smb, na.rm = TRUE),
  hml_min = min(hml, na.rm = TRUE),
  hml_p05 = quantile(hml, 0.05, na.rm = TRUE),
  hml_p25 = quantile(hml, 0.25, na.rm = TRUE),
  hml_p75 = quantile(hml, 0.75, na.rm = TRUE),
  hml_p95 = quantile(hml, 0.95, na.rm = TRUE),
  hml_max = max(hml, na.rm = TRUE)
)])

# Full-sample betas for published signals
czret[, beta_capm := extract_beta(ret = ret, mktrf = mktrf), by = signalname]
czret[, abnormal_capm := ret - beta_capm*mktrf]

# Normalize abnormal returns by in-sample mean and compute t-stats
czret[samptype == 'insamp', `:=`(
  abar_capm = mean(abnormal_capm, na.rm = TRUE),
  abar_capm_t = {
    m <- mean(abnormal_capm, na.rm = TRUE)
    s <- sd(abnormal_capm, na.rm = TRUE)
    n <- sum(!is.na(abnormal_capm))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_capm := nafill(abar_capm, "locf"), by = .(signalname)]
czret[, abar_capm_t := nafill(abar_capm_t, "locf"), by = .(signalname)]

# Fix: Add protection against division by zero
czret[, abnormal_capm_normalized := ifelse(abs(abar_capm) > 1e-10, 100*abnormal_capm/abar_capm, NA)]

## FF3 adjustments - full sample coefficients  
czret[, c("beta_ff3", "s_ff3", "h_ff3") := {
  coeffs <- extract_ff3_coeffs(ret = ret, mktrf = mktrf, smb = smb, hml = hml)
  list(coeffs[1], coeffs[2], coeffs[3])
}, by = signalname]

czret[, abnormal_ff3 := ret - (beta_ff3*mktrf + s_ff3*smb + h_ff3*hml)]

# Normalize FF3 abnormal returns and compute t-stats
czret[samptype == 'insamp', `:=`(
  abar_ff3 = mean(abnormal_ff3, na.rm = TRUE),
  abar_ff3_t = {
    m <- mean(abnormal_ff3, na.rm = TRUE)
    s <- sd(abnormal_ff3, na.rm = TRUE)
    n <- sum(!is.na(abnormal_ff3))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_ff3 := nafill(abar_ff3, "locf"), by = .(signalname)]
czret[, abar_ff3_t := nafill(abar_ff3_t, "locf"), by = .(signalname)]

# Fix: Add protection against division by zero
czret[, abnormal_ff3_normalized := ifelse(abs(abar_ff3) > 1e-10, 100*abnormal_ff3/abar_ff3, NA)]

# Compute raw return averages and t-stats on actual returns (not scaled)
czret[samptype == 'insamp', `:=`(
  rbar_avg = mean(ret, na.rm = TRUE),  # Average actual return in basis points
  rbar_t = {
    m <- mean(ret, na.rm = TRUE)
    s <- sd(ret, na.rm = TRUE) 
    n <- sum(!is.na(ret))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, rbar_avg := nafill(rbar_avg, "locf"), by = .(signalname)]
czret[, rbar_t := nafill(rbar_t, "locf"), by = .(signalname)]

# NEW: Compute published-side time-varying CAPM/FF3 betas and normalized alphas
# Use IS betas within IS period and OOS betas within OOS period; fallback to full-sample if unavailable
betas_capm_is <- czret[samptype == 'insamp' & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_is = extract_beta(ret, mktrf)), by = signalname]
betas_capm_oos <- czret[samptype == 'oos' & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_oos = extract_beta(ret, mktrf)), by = signalname]
czret <- merge(czret, betas_capm_is, by = 'signalname', all.x = TRUE)
czret <- merge(czret, betas_capm_oos, by = 'signalname', all.x = TRUE)
czret[, beta_capm_tv := ifelse(samptype == 'insamp', beta_capm_is, beta_capm_oos)]
czret[is.na(beta_capm_tv), beta_capm_tv := beta_capm]
czret[, abnormal_capm_tv := ret - beta_capm_tv * mktrf]
czret[samptype == 'insamp', `:=`(
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

ff3_is <- czret[samptype == 'insamp' & !is.na(ret) & !is.na(mktrf) & !is.na(smb) & !is.na(hml), {
  coeffs <- extract_ff3_coeffs(ret, mktrf, smb, hml)
  .(beta_ff3_is = coeffs[1], s_ff3_is = coeffs[2], h_ff3_is = coeffs[3])
}, by = signalname]
ff3_oos <- czret[samptype == 'oos' & !is.na(ret) & !is.na(mktrf) & !is.na(smb) & !is.na(hml), {
  coeffs <- extract_ff3_coeffs(ret, mktrf, smb, hml)
  .(beta_ff3_oos = coeffs[1], s_ff3_oos = coeffs[2], h_ff3_oos = coeffs[3])
}, by = signalname]
czret <- merge(czret, ff3_is, by = 'signalname', all.x = TRUE)
czret <- merge(czret, ff3_oos, by = 'signalname', all.x = TRUE)
czret[, beta_ff3_tv := ifelse(samptype == 'insamp', beta_ff3_is, beta_ff3_oos)]
czret[, s_ff3_tv := ifelse(samptype == 'insamp', s_ff3_is, s_ff3_oos)]
czret[, h_ff3_tv := ifelse(samptype == 'insamp', h_ff3_is, h_ff3_oos)]
czret[is.na(beta_ff3_tv), beta_ff3_tv := beta_ff3]
czret[is.na(s_ff3_tv), s_ff3_tv := s_ff3]
czret[is.na(h_ff3_tv), h_ff3_tv := h_ff3]
czret[, abnormal_ff3_tv := ret - (beta_ff3_tv * mktrf + s_ff3_tv * smb + h_ff3_tv * hml)]
czret[samptype == 'insamp', `:=`(
  abar_ff3_tv = mean(abnormal_ff3_tv, na.rm = TRUE),
  abar_ff3_tv_t = {
    m <- mean(abnormal_ff3_tv, na.rm = TRUE)
    s <- sd(abnormal_ff3_tv, na.rm = TRUE)
    n <- sum(!is.na(abnormal_ff3_tv))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_ff3_tv := nafill(abar_ff3_tv, 'locf'), by = .(signalname)]
czret[, abar_ff3_tv_t := nafill(abar_ff3_tv_t, 'locf'), by = .(signalname)]
czret[, abnormal_ff3_tv_normalized := ifelse(abs(abar_ff3_tv) > 1e-10, 100 * abnormal_ff3_tv / abar_ff3_tv, NA_real_)]

# Compute normalized DM abnormal returns ---------------------------------

# First, create the risk-adjusted data for plotting by joining with `czret`
ret_for_plot0_adj <- ret_for_plot0 %>%
  left_join(
    czret %>% select(signalname, eventDate,
                      abnormal_capm, abnormal_ff3, abnormal_capm_tv, abnormal_ff3_tv,
                      abnormal_capm_normalized, abnormal_ff3_normalized,
                      abnormal_capm_tv_normalized, abnormal_ff3_tv_normalized),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  )

# Default Plot Settings --------------------------------------------------
fontsizeall = 28
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5
global_xl = -360  
global_xh = 300   

print("Risk adjustments computed successfully!")
print(paste("Number of signals with CAPM adjustments:", sum(!is.na(czret$beta_capm))))
print(paste("Number of signals with FF3 adjustments:", sum(!is.na(czret$beta_ff3))))

# Risk-Adjusted Plots ----------------------------------------------------

## 1. Raw Returns (baseline from 4c2) ------------------------------------
# Filter published signals by raw t-stat threshold for raw plot
signals_raw_t2_plot <- unique(czret[rbar_t >= t_threshold]$signalname)
tempsuffix = paste0("raw_returns_t", t_threshold)

printme_raw = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot0 %>% filter(!is.na(matchRet), pubname %in% signals_raw_t2_plot) %>% 
      transmute(eventDate, pubname, theory, ret, matchRet) %>%
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
      paste0("Published (Raw, t>=", t_threshold, ")"),
      paste0("Data-Mined (Raw, t>=", t_threshold, ")"),
      'N/A'
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_", tempsuffix, '.pdf'), 
       printme_raw, width = 10, height = 8)

# Print summary statistics (filtered by raw t)
cat("\n=== RAW RETURNS PLOT STATISTICS (t >= ", t_threshold, ") ===\n", sep = "")
ret_for_plot0 %>% 
  filter(!is.na(matchRet), pubname %in% signals_raw_t2_plot) %>%
  summarise(
    pub_mean_insamp = mean(ret[eventDate <= 0], na.rm = TRUE),
    pub_mean_oos = mean(ret[eventDate > 0], na.rm = TRUE),
    dm_mean_insamp = mean(matchRet[eventDate <= 0], na.rm = TRUE),
    dm_mean_oos = mean(matchRet[eventDate > 0], na.rm = TRUE)
  ) %>% print()

cat("\nNumber of signals:", length(unique(ret_for_plot0$pubname[!is.na(ret_for_plot0$matchRet) & ret_for_plot0$pubname %in% signals_raw_t2_plot])), "\n")


# Filtered versions (t-stat or return) -----------------------------------------------

if (filter_type == "return") {
  cat("\n\n=== RETURN FILTERED ANALYSIS (avg return/alpha >=", return_threshold, ") ===\n")
} else {
  cat("\n\n=== T-STAT FILTERED ANALYSIS (t >=", t_threshold, ") ===\n")
}

cat("\nComputing statistics for individual DM signals...\n")
filters <- prepare_dm_filters(
  candidateReturns_adj = candidateReturns_adj,
  czret = czret,
  filter_type = filter_type,
  t_threshold = t_threshold,
  return_threshold = return_threshold
)

dm_stats <- filters$dm_stats
dm_filtered_capm <- filters$dm_filtered_capm
dm_filtered_ff3 <- filters$dm_filtered_ff3
signals_raw_t2 <- filters$signals_raw
signals_capm_t2 <- filters$signals_capm
signals_ff3_t2 <- filters$signals_ff3

if (filter_type == "return") {
  cat("\nNumber of PUBLISHED signals with avg return/alpha >=", return_threshold, ":\n")
  cat("Raw returns:", length(signals_raw_t2), "\n")
  cat("CAPM alpha:", length(signals_capm_t2), "\n")
  cat("FF3 alpha:", length(signals_ff3_t2), "\n")
  
  cat("\nNumber of DM signals with avg return/alpha >=", return_threshold, ":\n")
  cat("CAPM alpha:", sum(dm_stats$abar_capm_dm >= return_threshold, na.rm = TRUE), "\n")
  cat("FF3 alpha:", sum(dm_stats$abar_ff3_dm >= return_threshold, na.rm = TRUE), "\n")
} else {
  cat("\nNumber of PUBLISHED signals with t >=", t_threshold, ":\n")
  cat("Raw returns:", length(signals_raw_t2), "\n")
  cat("CAPM alpha:", length(signals_capm_t2), "\n")
  cat("FF3 alpha:", length(signals_ff3_t2), "\n")
  
  cat("\nNumber of DM signals with t >=", t_threshold, ":\n")
  cat("CAPM alpha:", sum(dm_stats$abar_capm_dm_t >= t_threshold, na.rm = TRUE), "\n")
  cat("FF3 alpha:", sum(dm_stats$abar_ff3_dm_t >= t_threshold, na.rm = TRUE), "\n")
}

# Create filtered plots for CAPM
if (filter_type == "return") {
  cat("\n=== CAPM FILTERED (avg alpha >=", return_threshold, ") STATISTICS ===\n")
} else {
  cat("\n=== CAPM FILTERED (t >=", t_threshold, ") STATISTICS ===\n")
}

# Use helper function to normalize and aggregate
dm_capm_aggregated <- normalize_and_aggregate_dm(
  dm_filtered_capm, 
  "abnormal_capm", 
  "capm_t2_normalized"
)

# Create filtered plot data
ret_for_plot0_capm_t2 <- create_filtered_plot_data(
  ret_for_plot0_adj,
  signals_capm_t2,
  dm_capm_aggregated,
  "abnormal_capm_normalized",
  "matchRet_capm_t2_normalized",
  "capm_t2_normalized"
)

cat("Published signals with CAPM t >=", t_threshold, ":", length(signals_capm_t2), "\n")
cat("Published signals with filtered DM matches:", length(unique(ret_for_plot0_capm_t2$pubname)), "\n")

# Create and save plot
printme_capm_t2 <- create_risk_adjusted_plot(
  ret_for_plot0_capm_t2,
  "abnormal_capm_normalized",
  "matchRet_capm_t2_normalized",
  "CAPM Alpha",
  t_threshold,
  "Trailing 5-Year CAPM Alpha",
  filter_type = filter_type,
  return_threshold = return_threshold
)

# Create filtered plots for FF3
if (filter_type == "return") {
  cat("\n=== FF3 FILTERED (avg alpha >=", return_threshold, ") STATISTICS ===\n")
} else {
  cat("\n=== FF3 FILTERED (t >=", t_threshold, ") STATISTICS ===\n")
}

# Use helper function to normalize and aggregate
dm_ff3_aggregated <- normalize_and_aggregate_dm(
  dm_filtered_ff3, 
  "abnormal_ff3", 
  "ff3_t2_normalized"
)

# Create filtered plot data
ret_for_plot0_ff3_t2 <- create_filtered_plot_data(
  ret_for_plot0_adj,
  signals_ff3_t2,
  dm_ff3_aggregated,
  "abnormal_ff3_normalized",
  "matchRet_ff3_t2_normalized",
  "ff3_t2_normalized"
)

cat("Published signals with FF3 t >=", t_threshold, ":", length(signals_ff3_t2), "\n")
cat("Published signals with filtered DM matches:", length(unique(ret_for_plot0_ff3_t2$pubname)), "\n")

# Create and save plot
printme_ff3_t2 <- create_risk_adjusted_plot(
  ret_for_plot0_ff3_t2,
  "abnormal_ff3_normalized",
  "matchRet_ff3_t2_normalized",
  "FF3 Alpha",
  t_threshold,
  "Trailing 5-Year FF3 Alpha",
  filter_type = filter_type,
  return_threshold = return_threshold
)

# Also compute UNNORMALIZED CAPM/FF3 tables to print (raw units) -------------------------
# Moved into helper function print_unnormalized_tables(); call placed later before TV section

# Time-varying abnormal returns (IS beta in IS, post-sample beta in OOS) --------

# Load model categories EARLY (needed for time-varying and other analyses)
# Moved here from line 875 to be available for all analyses
mappings <- load_signal_mappings("DataInput/SignalsTheoryChecked.csv", inclSignals)
czcat_full <- mappings$czcat_full
theory_mapping <- mappings$theory_mapping
model_mapping <- mappings$model_mapping
discipline_mapping <- mappings$discipline_mapping
journal_mapping <- mappings$journal_mapping
discipline_mapping_filtered <- mappings$discipline_mapping_filtered
journal_mapping_filtered <- mappings$journal_mapping_filtered

# Define helper functions here (needed for analyses below)
# ----------------------------------------------

# Function to compute outperformance metrics with standard errors

# Function to create comprehensive summary tables for any analysis type

# Function to print formatted summary table

# Function to export tables to CSV

cat("\n\n=== TIME-VARYING ABNORMAL RETURNS (IS/OOS BETAS) ===\n")

# Check if time-varying columns exist in the data
if("abnormal_capm_tv" %in% names(candidateReturns_adj) && "abnormal_ff3_tv" %in% names(candidateReturns_adj)) {
  
  # Compute statistics for time-varying abnormal returns with proper NA handling
  dm_stats_tv <- candidateReturns_adj[
    samptype == "insamp" & !is.na(abnormal_capm_tv),
    .(
      abar_capm_tv_dm_t = {
        m <- mean(abnormal_capm_tv, na.rm = TRUE)
        s <- sd(abnormal_capm_tv, na.rm = TRUE)
        n <- sum(!is.na(abnormal_capm_tv))
        if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
      },
      abar_ff3_tv_dm_t = {
        m <- mean(abnormal_ff3_tv, na.rm = TRUE)
        s <- sd(abnormal_ff3_tv, na.rm = TRUE)
        n <- sum(!is.na(abnormal_ff3_tv))
        if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
      },
      abar_capm_tv_dm = mean(abnormal_capm_tv, na.rm = TRUE),
      abar_ff3_tv_dm = mean(abnormal_ff3_tv, na.rm = TRUE)
    ),
    by = .(actSignal, candSignalname)
  ]
  
  # NEW: Define published TV signal sets using TV alpha stats
  if (filter_type == "return") {
    signals_capm_tv_t2 <- unique(czret[abar_capm_tv >= return_threshold]$signalname)
    signals_ff3_tv_t2  <- unique(czret[abar_ff3_tv  >= return_threshold]$signalname)
  } else {
    signals_capm_tv_t2 <- unique(czret[abar_capm_tv_t >= t_threshold]$signalname)
    signals_ff3_tv_t2  <- unique(czret[abar_ff3_tv_t  >= t_threshold]$signalname)
  }
  
  # Additionally require published signals to pass the raw filter for comparability
  signals_capm_tv_t2 <- intersect(signals_capm_tv_t2, signals_raw_t2)
  signals_ff3_tv_t2  <- intersect(signals_ff3_tv_t2, signals_raw_t2)
  
  # CAPM time-varying filtering
  if (filter_type == "return") {
    cat("\n=== CAPM TIME-VARYING (avg alpha >=", return_threshold, ") STATISTICS ===\n")
    dm_filtered_capm_tv <- candidateReturns_adj %>%
      inner_join(
        dm_stats_tv %>% filter(abar_capm_tv_dm >= return_threshold),
        by = c("actSignal", "candSignalname")
      )
  } else {
    cat("\n=== CAPM TIME-VARYING (t >=", t_threshold, ") STATISTICS ===\n")
    dm_filtered_capm_tv <- candidateReturns_adj %>%
      inner_join(
        dm_stats_tv %>% filter(abar_capm_tv_dm_t >= t_threshold),
        by = c("actSignal", "candSignalname")
      )
  }
  
  # Normalize and aggregate
  dm_capm_tv_aggregated <- normalize_and_aggregate_dm(
    dm_filtered_capm_tv, 
    "abnormal_capm_tv", 
    "capm_tv_t2_normalized"
  )
  
  # Create filtered plot data
  ret_for_plot0_capm_tv_t2 <- create_filtered_plot_data(
    ret_for_plot0_adj,
    signals_capm_tv_t2,  # Use TV-filtered published signals
    dm_capm_tv_aggregated,
    "abnormal_capm_tv_normalized",  # Use published TV-normalized alpha
    "matchRet_capm_tv_t2_normalized",
    "capm_tv_t2_normalized"
  )
  
  cat("Published signals with CAPM-TV", ifelse(filter_type == "return", paste("avg alpha >=", return_threshold), paste("t >=", t_threshold)), ":", length(signals_capm_tv_t2), "\n")
  cat("Published signals with filtered DM matches (time-varying):", length(unique(ret_for_plot0_capm_tv_t2$pubname)), "\n")
  if (filter_type == "return") {
    cat("DM signals with time-varying CAPM avg alpha >=", return_threshold, ":", sum(dm_stats_tv$abar_capm_tv_dm >= return_threshold, na.rm = TRUE), "\n")
  } else {
    cat("DM signals with time-varying CAPM t >=", t_threshold, ":", sum(dm_stats_tv$abar_capm_tv_dm_t >= t_threshold, na.rm = TRUE), "\n")
  }
  
  # Create and save plot
  printme_capm_tv_t2 <- create_risk_adjusted_plot(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized",
    "matchRet_capm_tv_t2_normalized",
    "CAPM TV Alpha",
    t_threshold,
    "Trailing 5-Year CAPM Alpha",
    filter_type = filter_type,
    return_threshold = return_threshold
  )
  
  # FF3 time-varying filtering
  if (filter_type == "return") {
    cat("\n=== FF3 TIME-VARYING (avg alpha >=", return_threshold, ") STATISTICS ===\n")
    dm_filtered_ff3_tv <- candidateReturns_adj %>%
      inner_join(
        dm_stats_tv %>% filter(abar_ff3_tv_dm >= return_threshold),
        by = c("actSignal", "candSignalname")
      )
  } else {
    cat("\n=== FF3 TIME-VARYING (t >=", t_threshold, ") STATISTICS ===\n")
    dm_filtered_ff3_tv <- candidateReturns_adj %>%
      inner_join(
        dm_stats_tv %>% filter(abar_ff3_tv_dm_t >= t_threshold),
        by = c("actSignal", "candSignalname")
      )
  }
  
  # Normalize and aggregate
  dm_ff3_tv_aggregated <- normalize_and_aggregate_dm(
    dm_filtered_ff3_tv, 
    "abnormal_ff3_tv", 
    "ff3_tv_t2_normalized"
  )
  
  # Create filtered plot data
  ret_for_plot0_ff3_tv_t2 <- create_filtered_plot_data(
    ret_for_plot0_adj,
    signals_ff3_tv_t2,  # Use TV-filtered published signals
    dm_ff3_tv_aggregated,
    "abnormal_ff3_tv_normalized",  # Use published TV-normalized alpha
    "matchRet_ff3_tv_t2_normalized",
    "ff3_tv_t2_normalized"
  )
  
  cat("Published signals with FF3-TV", ifelse(filter_type == "return", paste("avg alpha >=", return_threshold), paste("t >=", t_threshold)), ":", length(signals_ff3_tv_t2), "\n")
  cat("Published signals with filtered DM matches (time-varying):", length(unique(ret_for_plot0_ff3_tv_t2$pubname)), "\n")
  if (filter_type == "return") {
    cat("DM signals with time-varying FF3 avg alpha >=", return_threshold, ":", sum(dm_stats_tv$abar_ff3_tv_dm >= return_threshold, na.rm = TRUE), "\n")
  } else {
    cat("DM signals with time-varying FF3 t >=", t_threshold, ":", sum(dm_stats_tv$abar_ff3_tv_dm_t >= t_threshold, na.rm = TRUE), "\n")
  }
  
  # Create and save plot
  printme_ff3_tv_t2 <- create_risk_adjusted_plot(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_tv_normalized",
    "matchRet_ff3_tv_t2_normalized",
    "FF3 TV Alpha",
    t_threshold,
    "Trailing 5-Year FF3 Alpha",
    filter_type = filter_type,
    return_threshold = return_threshold
  )
  
  # Create Time-Varying Alpha Summary Tables
  cat("\n\n=== TIME-VARYING ALPHA SUMMARY TABLES ===\n")
  
  # Prepare data for TV alpha summary tables
  tv_plot_data <- list()
  
  # Add raw data for comparison (filtered by the same signals)
  if (filter_type == "return") {
    tv_plot_data[["raw"]] <- ret_for_plot0 %>% 
      filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_avg) %>% distinct(), 
                by = c("pubname" = "signalname")) %>%
      filter(rbar_avg >= return_threshold)
  } else {
    tv_plot_data[["raw"]] <- ret_for_plot0 %>% 
      filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_t) %>% distinct(), 
                by = c("pubname" = "signalname")) %>%
      filter(rbar_t >= t_threshold)
  }
  
  tv_plot_data[["capm_tv"]] <- ret_for_plot0_capm_tv_t2
  tv_plot_data[["ff3_tv"]] <- ret_for_plot0_ff3_tv_t2
  
  # Create mappings (reuse existing ones from main analysis)
  tv_mappings <- list(
    theory = theory_mapping,
    model = model_mapping
  )
  
  # Create TV alpha summaries
  tv_summaries <- create_summary_tables(
    tv_plot_data,
    tv_mappings,
    table_name = "Time-Varying Alpha Analysis",
    filter_desc = ifelse(filter_type == "return", 
                        paste0("avg >= ", return_threshold),
                        paste0("t >= ", t_threshold))
  )
  
  # Print TV alpha summary by theory
  print_summary_table(
    tv_summaries[["theory"]],
    groups = c("Risk", "Mispricing", "Agnostic"),
    group_col = "theory_group",
    table_title = "TIME-VARYING ALPHA BY THEORETICAL FOUNDATION",
    analysis_types = c("raw", "capm_tv", "ff3_tv"),
    analysis_labels = c("Raw", "CAPM-TV", "FF3-TV")
  )
  
  # Print TV alpha summary by model
  print_summary_table(
    tv_summaries[["model"]],
    groups = c("No Model", "Stylized", "Dynamic or Quantitative"),
    group_col = "modeltype_grouped",
    table_title = "TIME-VARYING ALPHA BY MODELING FORMALISM",
    analysis_types = c("raw", "capm_tv", "ff3_tv"),
    analysis_labels = c("Raw", "CAPM-TV", "FF3-TV")
  )
  
  # Export TV alpha tables
  export_filename <- paste0(results_dir, "/tv_alpha_summary_",
                           ifelse(filter_type == "return", 
                                  paste0("r", gsub("\\.", "", as.character(return_threshold))),
                                  paste0("t", t_threshold)), ".csv")
  export_summary_tables(tv_summaries, export_filename, 
                        filter_desc = ifelse(filter_type == "return",
                                           paste0("Return >= ", return_threshold),
                                           paste0("T-stat >= ", t_threshold)))
  
} else {
  cat("\nTime-varying abnormal returns not available in the data.\n")
  cat("Please run 2d_RiskAdjustDataMinedSignals.R with the updated code to generate these columns.\n")
}

# Generic Summary Table Functions ----------------------------------------------

# Function to create comprehensive summary tables for any analysis type



# FULL-SAMPLE ALPHA SUMMARY TABLES (Using new functions) --------------------
cat("\n\n=== FULL-SAMPLE ALPHA SUMMARY TABLES (FILTERED) ===\n")

# Prepare data for full-sample alpha summary tables
fs_plot_data <- list()

# Add raw filtered data
if (filter_type == "return") {
  fs_plot_data[["raw"]] <- ret_for_plot0 %>% 
    filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_avg) %>% distinct(), 
              by = c("pubname" = "signalname")) %>%
    filter(rbar_avg >= return_threshold)
} else {
  fs_plot_data[["raw"]] <- ret_for_plot0 %>% 
    filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), 
              by = c("pubname" = "signalname")) %>%
    filter(rbar_t >= t_threshold)
}

fs_plot_data[["capm"]] <- ret_for_plot0_capm_t2
fs_plot_data[["ff3"]] <- ret_for_plot0_ff3_t2

# Create mappings
fs_mappings <- list(
  theory = theory_mapping,
  model = model_mapping
)

# Create full-sample summaries using new functions
fs_summaries <- create_summary_tables(
  fs_plot_data,
  fs_mappings,
  table_name = "Full-Sample Alpha Analysis",
  filter_desc = ifelse(filter_type == "return", 
                      paste0("avg >= ", return_threshold),
                      paste0("t >= ", t_threshold))
)

# Print full-sample summary by theory
print_summary_table(
  fs_summaries[["theory"]],
  groups = c("Risk", "Mispricing", "Agnostic"),
  group_col = "theory_group",
  table_title = "FULL-SAMPLE ALPHA BY THEORETICAL FOUNDATION",
  analysis_types = c("raw", "capm", "ff3"),
  analysis_labels = c("Raw", "CAPM", "FF3")
)

# Print full-sample summary by model
print_summary_table(
  fs_summaries[["model"]],
  groups = c("No Model", "Stylized", "Dynamic or Quantitative"),
  group_col = "modeltype_grouped",
  table_title = "FULL-SAMPLE ALPHA BY MODELING FORMALISM",
  analysis_types = c("raw", "capm", "ff3"),
  analysis_labels = c("Raw", "CAPM", "FF3")
)

# T-STAT FILTERED SUMMARY TABLE (t >= t_threshold) ----------------------------------------
if (filter_type == "return") {
  cat("\n\n=== SUMMARY TABLE WITH AVG RETURN/ALPHA >=", return_threshold, " FILTER ===\n")
} else {
  cat("\n\n=== SUMMARY TABLE WITH T >=", t_threshold, " FILTER ===\n")
}

# Raw returns filter (by theory)
if (filter_type == "return") {
  raw_t2_summary_theory <- compute_outperformance(
    ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_avg) %>% distinct(), by = c("pubname" = "signalname")) %>%
      filter(rbar_avg >= return_threshold), 
    "ret", "matchRet", theory_mapping, "theory_group"
  )
} else {
  raw_t2_summary_theory <- compute_outperformance(
    ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
      filter(rbar_t >= t_threshold), 
    "ret", "matchRet", theory_mapping, "theory_group"
  )
}

# CAPM t >= t_threshold filtered (by theory)
capm_t2_summary_theory <- compute_outperformance(
  ret_for_plot0_capm_t2,
  "abnormal_capm_normalized", "matchRet_capm_t2_normalized", theory_mapping, "theory_group"
)

# FF3 t >= t_threshold filtered (by theory) 
ff3_t2_summary_theory <- compute_outperformance(
  ret_for_plot0_ff3_t2,
  "abnormal_ff3_normalized", "matchRet_ff3_t2_normalized", theory_mapping, "theory_group"
)

# Raw returns filter (by model)
if (filter_type == "return") {
  raw_t2_summary_model <- compute_outperformance(
    ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_avg) %>% distinct(), by = c("pubname" = "signalname")) %>%
      filter(rbar_avg >= return_threshold), 
    "ret", "matchRet", model_mapping, "modeltype_grouped"
  )
} else {
  raw_t2_summary_model <- compute_outperformance(
    ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
      filter(rbar_t >= t_threshold), 
    "ret", "matchRet", model_mapping, "modeltype_grouped"
  )
}

# CAPM t >= t_threshold filtered (by model)
capm_t2_summary_model <- compute_outperformance(
  ret_for_plot0_capm_t2,
  "abnormal_capm_normalized", "matchRet_capm_t2_normalized", model_mapping, "modeltype_grouped"
)

# FF3 t >= t_threshold filtered (by model)
ff3_t2_summary_model <- compute_outperformance(
  ret_for_plot0_ff3_t2,
  "abnormal_ff3_normalized", "matchRet_ff3_t2_normalized", model_mapping, "modeltype_grouped"
)

# Overall filtered summaries
if (filter_type == "return") {
  filtered_signals_raw <- czret$signalname[czret$rbar_avg >= return_threshold]
} else {
  filtered_signals_raw <- czret$signalname[czret$rbar_t >= t_threshold]
}

overall_t2_summary_raw <- compute_overall_summary(
  plot_data = ret_for_plot0 %>% filter(!is.na(matchRet), pubname %in% filtered_signals_raw),
  ret_col = "ret",
  dm_col = "matchRet"
)

overall_t2_summary_capm <- compute_overall_summary(
  plot_data = ret_for_plot0_capm_t2,
  ret_col = "abnormal_capm_normalized",
  dm_col = "matchRet_capm_t2_normalized"
)

overall_t2_summary_ff3 <- compute_overall_summary(
  plot_data = ret_for_plot0_ff3_t2,
  ret_col = "abnormal_ff3_normalized",
  dm_col = "matchRet_ff3_t2_normalized"
)

# Time-varying summaries (if available)
if("abnormal_capm_tv" %in% names(candidateReturns_adj) && exists("ret_for_plot0_capm_tv_t2")) {
  # CAPM time-varying t >= t_threshold filtered (by theory)
  capm_tv_t2_summary_theory <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized", "matchRet_capm_tv_t2_normalized", theory_mapping, "theory_group"
  )
  
  # FF3 time-varying t >= t_threshold filtered (by theory)
  ff3_tv_t2_summary_theory <- compute_outperformance(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_tv_normalized", "matchRet_ff3_tv_t2_normalized", theory_mapping, "theory_group"
  )
  
  # CAPM time-varying t >= t_threshold filtered (by model)
  capm_tv_t2_summary_model <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized", "matchRet_capm_tv_t2_normalized", model_mapping, "modeltype_grouped"
  )
  
  # FF3 time-varying t >= t_threshold filtered (by model)
  ff3_tv_t2_summary_model <- compute_outperformance(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_tv_normalized", "matchRet_ff3_tv_t2_normalized", model_mapping, "modeltype_grouped"
  )
  
  # CAPM time-varying t >= t_threshold filtered (by discipline)
  capm_tv_t2_summary_discipline <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized", "matchRet_capm_tv_t2_normalized", discipline_mapping_filtered, "discipline"
  )
  
  # FF3 time-varying t >= t_threshold filtered (by discipline)
  ff3_tv_t2_summary_discipline <- compute_outperformance(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_tv_normalized", "matchRet_ff3_tv_t2_normalized", discipline_mapping_filtered, "discipline"
  )
  
  # CAPM time-varying t >= t_threshold filtered (by journal)
  capm_tv_t2_summary_journal <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized", "matchRet_capm_tv_t2_normalized", journal_mapping_filtered, "journal_rank"
  )
  
  # FF3 time-varying t >= t_threshold filtered (by journal)
  ff3_tv_t2_summary_journal <- compute_outperformance(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_tv_normalized", "matchRet_ff3_tv_t2_normalized", journal_mapping_filtered, "journal_rank"
  )
  
  # Overall time-varying summaries
  overall_t2_summary_capm_tv <- compute_overall_summary(
    plot_data = ret_for_plot0_capm_tv_t2,
    ret_col = "abnormal_capm_tv_normalized",
    dm_col = "matchRet_capm_tv_t2_normalized"
  )
  
  overall_t2_summary_ff3_tv <- compute_overall_summary(
    plot_data = ret_for_plot0_ff3_tv_t2,
    ret_col = "abnormal_ff3_tv_normalized",
    dm_col = "matchRet_ff3_tv_t2_normalized"
  )
}

# Helper functions moved to helpers/risk_adjusted_helpers.R

# ============================================================================
# IMPROVED HELPER FUNCTIONS FOR TABLE GENERATION
# ============================================================================

# build_table_row moved to helpers/risk_adjusted_helpers.R

# Unified function to format values with standard errors
# format_value_se moved to helpers/risk_adjusted_helpers.R

# Function to create LaTeX table from summary data
# create_latex_table moved to helpers/risk_adjusted_helpers.R

# Enhanced function to create LaTeX table with proper formatting
# create_formatted_latex_table moved to helpers/risk_adjusted_helpers.R

# Function to build time-varying summary tables
build_tv_summary_table <- function(categories, groups, summaries, digits = 0) {
  
  # Initialize result data frame
  result_df <- data.frame(
    Category = categories,
    Group = groups,
    stringsAsFactors = FALSE
  )
  
  # Add Raw columns if available in summaries
  if (all(sapply(summaries, function(x) all(c("raw_pub_oos", "raw_pub_oos_se", "raw_outperform", "raw_outperform_se") %in% names(x))))) {
    # Raw return
    result_df[["Raw_Return"]] <- mapply(function(grp, cat_data) {
      val <- cat_data[["raw_pub_oos"]]
      se <- cat_data[["raw_pub_oos_se"]]
      format_value_se(val, se, digits, FALSE)
    }, groups, summaries, SIMPLIFY = TRUE)
    # Raw outperformance
    result_df[["Raw_Outperformance"]] <- mapply(function(grp, cat_data) {
      val <- cat_data[["raw_outperform"]]
      se <- cat_data[["raw_outperform_se"]]
      format_value_se(val, se, digits, FALSE)
    }, groups, summaries, SIMPLIFY = TRUE)
  }
  
  # Build columns for CAPM-TV and FF3-TV
  for (analysis in c("capm_tv", "ff3_tv")) {
    analysis_label <- switch(analysis,
                           "capm_tv" = "CAPM_TV",
                           "ff3_tv" = "FF3_TV",
                           analysis)
    
    # Add return column
    col_name <- paste0(analysis_label, "_Return")
    result_df[[col_name]] <- mapply(function(grp, cat_data) {
      val <- cat_data[[paste0(analysis, "_pub_oos")]]
      se <- cat_data[[paste0(analysis, "_pub_oos_se")]]
      format_value_se(val, se, digits, FALSE)
    }, groups, summaries, SIMPLIFY = TRUE)
    
    # Add outperformance column
    col_name <- paste0(analysis_label, "_Outperformance")
    result_df[[col_name]] <- mapply(function(grp, cat_data) {
      val <- cat_data[[paste0(analysis, "_outperform")]]
      se <- cat_data[[paste0(analysis, "_outperform_se")]]
      format_value_se(val, se, digits, FALSE)
    }, groups, summaries, SIMPLIFY = TRUE)
  }
  
  return(result_df)
}

# Refactored function to build summary tables efficiently
build_summary_table <- function(categories, groups, summaries, 
                               analysis_types = c("raw", "capm", "ff3"),
                               metrics_config = list(
                                 return = c("pub_oos", "pub_oos_se"),
                                 outperform = c("outperform", "outperform_se")
                               ),
                               format_latex = FALSE, digits = 0) {
  
  # Initialize result data frame
  result_df <- data.frame(
    Category = categories,
    Group = groups,
    stringsAsFactors = FALSE
  )
  
  # Build columns in the correct order: Raw_Return, CAPM_Return, FF3_Return, Raw_Outperform, CAPM_Outperform, FF3_Outperformance
  # First add all return columns
  for (analysis in analysis_types) {
    analysis_label <- switch(analysis,
                           "raw" = "Raw",
                           "capm" = "CAPM", 
                           "ff3" = "FF3",
                           analysis)
    
    if ("return" %in% names(metrics_config)) {
      col_name <- paste0(analysis_label, "_Return")
      result_df[[col_name]] <- mapply(function(grp, cat_data) {
        val <- cat_data[[paste0(analysis, "_", metrics_config$return[1])]]
        se <- cat_data[[paste0(analysis, "_", metrics_config$return[2])]]
        format_value_se(val, se, digits, format_latex)
      }, groups, summaries, SIMPLIFY = TRUE)
    }
  }
  
  # Then add all outperformance columns
  for (analysis in analysis_types) {
    analysis_label <- switch(analysis,
                           "raw" = "Raw",
                           "capm" = "CAPM", 
                           "ff3" = "FF3",
                           analysis)
    
    if ("outperform" %in% names(metrics_config)) {
      col_name <- paste0(analysis_label, "_Outperformance")
      result_df[[col_name]] <- mapply(function(grp, cat_data) {
        val <- cat_data[[paste0(analysis, "_", metrics_config$outperform[1])]]
        se <- cat_data[[paste0(analysis, "_", metrics_config$outperform[2])]]
        format_value_se(val, se, digits, format_latex)
      }, groups, summaries, SIMPLIFY = TRUE)
    }
  }
  
  return(result_df)
}

# Function to export tables in multiple formats
# export_tables_multi_format moved to helpers/risk_adjusted_helpers.R

# Print filtered table
if (filter_type == "return") {
  cat("\nPost-Sample Return (avg>=", return_threshold, ")     Outperformance vs Data-Mining (avg>=", return_threshold, ")\n")
} else {
  cat("\nPost-Sample Return (t>=", t_threshold, ")     Outperformance vs Data-Mining (t>=", t_threshold, ")\n")
}
cat("                Raw    CAPM    FF3    Raw    CAPM    FF3\n")
cat("Theoretical Foundation\n")

groups_theory <- c("Risk", "Mispricing", "Agnostic") 
for(group in groups_theory) {
  # Post-sample returns
  raw_ret <- round_zero(get_values(raw_t2_summary_theory, "theory_group", group, "pub_oos"))
  capm_ret <- round_zero(get_values(capm_t2_summary_theory, "theory_group", group, "pub_oos"))  
  ff3_ret <- round_zero(get_values(ff3_t2_summary_theory, "theory_group", group, "pub_oos"))
  
  # Outperformance
  raw_out <- round_zero(get_values(raw_t2_summary_theory, "theory_group", group, "outperform"))
  capm_out <- round_zero(get_values(capm_t2_summary_theory, "theory_group", group, "outperform"))
  ff3_out <- round_zero(get_values(ff3_t2_summary_theory, "theory_group", group, "outperform"))
  
  # Standard errors  
  raw_se <- round_zero(get_values(raw_t2_summary_theory, "theory_group", group, "pub_oos_se"))
  capm_se <- round_zero(get_values(capm_t2_summary_theory, "theory_group", group, "pub_oos_se"))
  ff3_se <- round_zero(get_values(ff3_t2_summary_theory, "theory_group", group, "pub_oos_se"))
  
  raw_out_se <- round_zero(get_values(raw_t2_summary_theory, "theory_group", group, "outperform_se"))
  capm_out_se <- round_zero(get_values(capm_t2_summary_theory, "theory_group", group, "outperform_se"))
  ff3_out_se <- round_zero(get_values(ff3_t2_summary_theory, "theory_group", group, "outperform_se"))
  
  cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
              group, raw_ret, capm_ret, ff3_ret, raw_out, capm_out, ff3_out))
  cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
              "", raw_se, capm_se, ff3_se, raw_out_se, capm_out_se, ff3_out_se))
}

cat("Modeling Formalism\n")
groups_model <- c("No Model", "Stylized", "Dynamic or Quantitative")
for(group in groups_model) {
  # Post-sample returns
  raw_ret <- round_zero(get_values(raw_t2_summary_model, "modeltype_grouped", group, "pub_oos"))
  capm_ret <- round_zero(get_values(capm_t2_summary_model, "modeltype_grouped", group, "pub_oos"))  
  ff3_ret <- round_zero(get_values(ff3_t2_summary_model, "modeltype_grouped", group, "pub_oos"))
  
  # Outperformance
  raw_out <- round_zero(get_values(raw_t2_summary_model, "modeltype_grouped", group, "outperform"))
  capm_out <- round_zero(get_values(capm_t2_summary_model, "modeltype_grouped", group, "outperform"))
  ff3_out <- round_zero(get_values(ff3_t2_summary_model, "modeltype_grouped", group, "outperform"))
  
  # Standard errors  
  raw_se <- round_zero(get_values(raw_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  capm_se <- round_zero(get_values(capm_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  ff3_se <- round_zero(get_values(ff3_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  
  raw_out_se <- round_zero(get_values(raw_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
  capm_out_se <- round_zero(get_values(capm_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
  ff3_out_se <- round_zero(get_values(ff3_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
  
  cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
              group, raw_ret, capm_ret, ff3_ret, raw_out, capm_out, ff3_out))
  cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
              "", raw_se, capm_se, ff3_se, raw_out_se, capm_out_se, ff3_out_se))
}

cat("Overall\n")
cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
            "All", round_zero(overall_t2_summary_raw$pub_oos), round_zero(overall_t2_summary_capm$pub_oos), round_zero(overall_t2_summary_ff3$pub_oos), 
            round_zero(overall_t2_summary_raw$outperform), round_zero(overall_t2_summary_capm$outperform), round_zero(overall_t2_summary_ff3$outperform)))
cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
            "", round_zero(overall_t2_summary_raw$pub_oos_se), round_zero(overall_t2_summary_capm$pub_oos_se), round_zero(overall_t2_summary_ff3$pub_oos_se), 
            round_zero(overall_t2_summary_raw$outperform_se), round_zero(overall_t2_summary_capm$outperform_se), round_zero(overall_t2_summary_ff3$outperform_se)))

# ANY MODEL VS NO MODEL TABLE (t >= t_threshold) ----------------------------------------
if (filter_type == "return") {
  cat("\n\n=== ANY MODEL VS NO MODEL TABLE (avg >=", return_threshold, ") ===\n")
} else {
  cat("\n\n=== ANY MODEL VS NO MODEL TABLE (t >=", t_threshold, ") ===\n")
}

# Create Any Model vs No Model mapping
anymodel_mapping <- czcat_full %>%
  transmute(
    signalname,
    model_binary = case_when(
      NoModel == 1 ~ "No Model",
      TRUE ~ "Any Model"
    )
  )

# Raw returns filter (by model binary)
if (filter_type == "return") {
  raw_t2_summary_anymodel <- compute_outperformance(
    ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_avg) %>% distinct(), by = c("pubname" = "signalname")) %>%
      filter(rbar_avg >= return_threshold), 
    "ret", "matchRet", anymodel_mapping, "model_binary"
  )
} else {
  raw_t2_summary_anymodel <- compute_outperformance(
    ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
      filter(rbar_t >= t_threshold), 
    "ret", "matchRet", anymodel_mapping, "model_binary"
  )
}

# CAPM t >= t_threshold filtered (by model binary)
capm_t2_summary_anymodel <- compute_outperformance(
  ret_for_plot0_capm_t2,
  "abnormal_capm_normalized", "matchRet_capm_t2_normalized", anymodel_mapping, "model_binary"
)

# FF3 t >= t_threshold filtered (by model binary)
ff3_t2_summary_anymodel <- compute_outperformance(
  ret_for_plot0_ff3_t2,
  "abnormal_ff3_normalized", "matchRet_ff3_t2_normalized", anymodel_mapping, "model_binary"
)

# Print Any Model vs No Model table
if (filter_type == "return") {
  cat("\nPost-Sample Return (avg>=", return_threshold, ")     Outperformance vs Data-Mining (avg>=", return_threshold, ")\n")
} else {
  cat("\nPost-Sample Return (t>=", t_threshold, ")     Outperformance vs Data-Mining (t>=", t_threshold, ")\n")
}
cat("                Raw    CAPM    FF3    Raw    CAPM    FF3\n")

for(group in c("No Model", "Any Model")) {
  # Post-sample returns
  raw_ret <- round(get_values(raw_t2_summary_anymodel, "model_binary", group, "pub_oos"))
  capm_ret <- round(get_values(capm_t2_summary_anymodel, "model_binary", group, "pub_oos"))  
  ff3_ret <- round(get_values(ff3_t2_summary_anymodel, "model_binary", group, "pub_oos"))
  
  # Outperformance
  raw_out <- round(get_values(raw_t2_summary_anymodel, "model_binary", group, "outperform"))
  capm_out <- round(get_values(capm_t2_summary_anymodel, "model_binary", group, "outperform"))
  ff3_out <- round(get_values(ff3_t2_summary_anymodel, "model_binary", group, "outperform"))
  
  # Standard errors  
  raw_se <- round(get_values(raw_t2_summary_anymodel, "model_binary", group, "pub_oos_se"))
  capm_se <- round(get_values(capm_t2_summary_anymodel, "model_binary", group, "pub_oos_se"))
  ff3_se <- round(get_values(ff3_t2_summary_anymodel, "model_binary", group, "pub_oos_se"))
  
  raw_out_se <- round(get_values(raw_t2_summary_anymodel, "model_binary", group, "outperform_se"))
  capm_out_se <- round(get_values(capm_t2_summary_anymodel, "model_binary", group, "outperform_se"))
  ff3_out_se <- round(get_values(ff3_t2_summary_anymodel, "model_binary", group, "outperform_se"))
  
  cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
              group, raw_ret, capm_ret, ff3_ret, raw_out, capm_out, ff3_out))
  cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
              "", raw_se, capm_se, ff3_se, raw_out_se, capm_out_se, ff3_out_se))
}

# Store Any Model vs No Model data for later export
anymodel_table_data <- list()
anymodel_groups <- c("No Model", "Any Model")

for(i in 1:length(anymodel_groups)) {
  group <- anymodel_groups[i]
  anymodel_table_data[[i]] <- list(
    raw_pub_oos = get_values(raw_t2_summary_anymodel, "model_binary", group, "pub_oos"),
    raw_pub_oos_se = get_values(raw_t2_summary_anymodel, "model_binary", group, "pub_oos_se"),
    raw_outperform = get_values(raw_t2_summary_anymodel, "model_binary", group, "outperform"),
    raw_outperform_se = get_values(raw_t2_summary_anymodel, "model_binary", group, "outperform_se"),
    
    capm_pub_oos = get_values(capm_t2_summary_anymodel, "model_binary", group, "pub_oos"),
    capm_pub_oos_se = get_values(capm_t2_summary_anymodel, "model_binary", group, "pub_oos_se"),
    capm_outperform = get_values(capm_t2_summary_anymodel, "model_binary", group, "outperform"),
    capm_outperform_se = get_values(capm_t2_summary_anymodel, "model_binary", group, "outperform_se"),
    
    ff3_pub_oos = get_values(ff3_t2_summary_anymodel, "model_binary", group, "pub_oos"),
    ff3_pub_oos_se = get_values(ff3_t2_summary_anymodel, "model_binary", group, "pub_oos_se"),
    ff3_outperform = get_values(ff3_t2_summary_anymodel, "model_binary", group, "outperform"),
    ff3_outperform_se = get_values(ff3_t2_summary_anymodel, "model_binary", group, "outperform_se")
  )
}

# Time-varying Any Model vs No Model (if available)
if("abnormal_capm_tv" %in% names(candidateReturns_adj) && exists("ret_for_plot0_capm_tv_t2")) {
  # CAPM time-varying t >= t_threshold filtered (by model binary)
  capm_tv_t2_summary_anymodel <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized", "matchRet_capm_tv_t2_normalized", anymodel_mapping, "model_binary"
  )
  
  # FF3 time-varying t >= t_threshold filtered (by model binary)
  ff3_tv_t2_summary_anymodel <- compute_outperformance(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_tv_normalized", "matchRet_ff3_tv_t2_normalized", anymodel_mapping, "model_binary"
  )
  
  cat("\nTime-Varying Results:\n")
  cat("                       CAPM-TV  FF3-TV        CAPM-TV  FF3-TV\n")
  
  for(group in c("No Model", "Any Model")) {
    # Post-sample returns
    capm_tv_ret <- round(get_values(capm_tv_t2_summary_anymodel, "model_binary", group, "pub_oos"))  
    ff3_tv_ret <- round(get_values(ff3_tv_t2_summary_anymodel, "model_binary", group, "pub_oos"))
    
    # Outperformance
    capm_tv_out <- round(get_values(capm_tv_t2_summary_anymodel, "model_binary", group, "outperform"))
    ff3_tv_out <- round(get_values(ff3_tv_t2_summary_anymodel, "model_binary", group, "outperform"))
    
    # Standard errors  
    capm_tv_se <- round(get_values(capm_tv_t2_summary_anymodel, "model_binary", group, "pub_oos_se"))
    ff3_tv_se <- round(get_values(ff3_tv_t2_summary_anymodel, "model_binary", group, "pub_oos_se"))
    
    capm_tv_out_se <- round(get_values(capm_tv_t2_summary_anymodel, "model_binary", group, "outperform_se"))
    ff3_tv_out_se <- round(get_values(ff3_tv_t2_summary_anymodel, "model_binary", group, "outperform_se"))
    
    cat(sprintf("%-12s           %4s     %4s          %4s     %4s\n",
                group, capm_tv_ret, ff3_tv_ret, capm_tv_out, ff3_tv_out))
    cat(sprintf("%-12s           (%2s)    (%2s)          (%2s)    (%2s)\n",
                "", capm_tv_se, ff3_tv_se, capm_tv_out_se, ff3_tv_out_se))
  }
}

# Time-varying abnormal returns table (if available)
if("abnormal_capm_tv" %in% names(candidateReturns_adj) && exists("ret_for_plot0_capm_tv_t2")) {
  if (filter_type == "return") {
  cat("\n\n=== TIME-VARYING ABNORMAL RETURNS TABLE (avg >=", return_threshold, ") ===\n")
} else {
  cat("\n\n=== TIME-VARYING ABNORMAL RETURNS TABLE (t >=", t_threshold, ") ===\n")
}
  if (filter_type == "return") {
    cat("\nPost-Sample Return (avg>=", return_threshold, ")     Outperformance vs Data-Mining (avg>=", return_threshold, ")\n")
  } else {
    cat("\nPost-Sample Return (t>=", t_threshold, ")     Outperformance vs Data-Mining (t>=", t_threshold, ")\n")
  }
  cat("                       CAPM-TV  FF3-TV        CAPM-TV  FF3-TV\n")
  cat("Theoretical Foundation\n")
  
  for(group in groups_theory) {
    # Post-sample returns
    capm_tv_ret <- round(get_values(capm_tv_t2_summary_theory, "theory_group", group, "pub_oos"))  
    ff3_tv_ret <- round(get_values(ff3_tv_t2_summary_theory, "theory_group", group, "pub_oos"))
    
    # Outperformance
    capm_tv_out <- round(get_values(capm_tv_t2_summary_theory, "theory_group", group, "outperform"))
    ff3_tv_out <- round(get_values(ff3_tv_t2_summary_theory, "theory_group", group, "outperform"))
    
    # Standard errors  
    capm_tv_se <- round(get_values(capm_tv_t2_summary_theory, "theory_group", group, "pub_oos_se"))
    ff3_tv_se <- round(get_values(ff3_tv_t2_summary_theory, "theory_group", group, "pub_oos_se"))
    
    capm_tv_out_se <- round(get_values(capm_tv_t2_summary_theory, "theory_group", group, "outperform_se"))
    ff3_tv_out_se <- round(get_values(ff3_tv_t2_summary_theory, "theory_group", group, "outperform_se"))
    
    cat(sprintf("%-12s           %4s     %4s          %4s     %4s\n",
                group, capm_tv_ret, ff3_tv_ret, capm_tv_out, ff3_tv_out))
    cat(sprintf("%-12s           (%2s)    (%2s)          (%2s)    (%2s)\n",
                "", capm_tv_se, ff3_tv_se, capm_tv_out_se, ff3_tv_out_se))
  }
  
  cat("\nModeling Formalism\n")
  groups_model <- c("No Model", "Stylized", "Dynamic or Quantitative")
  for(group in groups_model) {
    # Post-sample returns
    capm_tv_ret <- round(get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos"))
    ff3_tv_ret <- round(get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos"))
    
    # Outperformance
    capm_tv_out <- round(get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "outperform"))
    ff3_tv_out <- round(get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "outperform"))
    
    # Standard errors
    capm_tv_se <- round(get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
    ff3_tv_se <- round(get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
    
    capm_tv_out_se <- round(get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
    ff3_tv_out_se <- round(get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
    
    cat(sprintf("%-23s    %4s     %4s          %4s     %4s\n",
                group, capm_tv_ret, ff3_tv_ret, capm_tv_out, ff3_tv_out))
    cat(sprintf("%-23s    (%2s)    (%2s)          (%2s)    (%2s)\n",
                "", capm_tv_se, ff3_tv_se, capm_tv_out_se, ff3_tv_out_se))
  }
  
  # Overall
  cat(sprintf("%-12s           %4s     %4s          %4s     %4s\n",
              "All", round(overall_t2_summary_capm_tv$pub_oos), round(overall_t2_summary_ff3_tv$pub_oos),
              round(overall_t2_summary_capm_tv$outperform), round(overall_t2_summary_ff3_tv$outperform)))
  cat(sprintf("%-12s           (%2s)    (%2s)          (%2s)    (%2s)\n",
              "", round(overall_t2_summary_capm_tv$pub_oos_se), round(overall_t2_summary_ff3_tv$pub_oos_se), 
              round(overall_t2_summary_capm_tv$outperform_se), round(overall_t2_summary_ff3_tv$outperform_se)))
  
  # Store time-varying data for LaTeX export
  tv_theory_data <- list()
  for(i in 1:length(groups_theory)) {
    group <- groups_theory[i]
    tv_theory_data[[i]] <- list(
      # Raw
      raw_pub_oos = get_values(raw_t2_summary_theory, "theory_group", group, "pub_oos"),
      raw_pub_oos_se = get_values(raw_t2_summary_theory, "theory_group", group, "pub_oos_se"),
      raw_outperform = get_values(raw_t2_summary_theory, "theory_group", group, "outperform"),
      raw_outperform_se = get_values(raw_t2_summary_theory, "theory_group", group, "outperform_se"),
      
      # CAPM-TV
      capm_tv_pub_oos = get_values(capm_tv_t2_summary_theory, "theory_group", group, "pub_oos"),
      capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_theory, "theory_group", group, "pub_oos_se"),
      capm_tv_outperform = get_values(capm_tv_t2_summary_theory, "theory_group", group, "outperform"),
      capm_tv_outperform_se = get_values(capm_tv_t2_summary_theory, "theory_group", group, "outperform_se"),
      
      # FF3-TV
      ff3_tv_pub_oos = get_values(ff3_tv_t2_summary_theory, "theory_group", group, "pub_oos"),
      ff3_tv_pub_oos_se = get_values(ff3_tv_t2_summary_theory, "theory_group", group, "pub_oos_se"),
      ff3_tv_outperform = get_values(ff3_tv_t2_summary_theory, "theory_group", group, "outperform"),
      ff3_tv_outperform_se = get_values(ff3_tv_t2_summary_theory, "theory_group", group, "outperform_se")
    )
  }
  
  tv_model_data <- list()
  for(i in 1:length(groups_model)) {
    group <- groups_model[i]
    tv_model_data[[i]] <- list(
      # Raw
      raw_pub_oos = get_values(raw_t2_summary_model, "modeltype_grouped", group, "pub_oos"),
      raw_pub_oos_se = get_values(raw_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"),
      raw_outperform = get_values(raw_t2_summary_model, "modeltype_grouped", group, "outperform"),
      raw_outperform_se = get_values(raw_t2_summary_model, "modeltype_grouped", group, "outperform_se"),
      
      # CAPM-TV
      capm_tv_pub_oos = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos"),
      capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"),
      capm_tv_outperform = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "outperform"),
      capm_tv_outperform_se = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "outperform_se"),
      
      # FF3-TV
      ff3_tv_pub_oos = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos"),
      ff3_tv_pub_oos_se = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"),
      ff3_tv_outperform = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "outperform"),
      ff3_tv_outperform_se = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "outperform_se")
    )
  }
  
  # Add overall TV results
  tv_overall_data <- list(
    # Raw
    raw_pub_oos = overall_t2_summary_raw$pub_oos,
    raw_pub_oos_se = overall_t2_summary_raw$pub_oos_se,
    raw_outperform = overall_t2_summary_raw$outperform,
    raw_outperform_se = overall_t2_summary_raw$outperform_se,
    
    # CAPM-TV
    capm_tv_pub_oos = overall_t2_summary_capm_tv$pub_oos,
    capm_tv_pub_oos_se = overall_t2_summary_capm_tv$pub_oos_se,
    capm_tv_outperform = overall_t2_summary_capm_tv$outperform,
    capm_tv_outperform_se = overall_t2_summary_capm_tv$outperform_se,
    
    # FF3-TV
    ff3_tv_pub_oos = overall_t2_summary_ff3_tv$pub_oos,
    ff3_tv_pub_oos_se = overall_t2_summary_ff3_tv$pub_oos_se,
    ff3_tv_outperform = overall_t2_summary_ff3_tv$outperform,
    ff3_tv_outperform_se = overall_t2_summary_ff3_tv$outperform_se
  )
  
  # PRE-COMPUTE RAW DISCIPLINE AND JOURNAL SUMMARIES FOR TV TABLES
  # Filter data to exclude Economics discipline
  discipline_mapping_filtered <- discipline_mapping %>% filter(discipline %in% c("Finance", "Accounting"))
  
  # Create data with discipline column
  if (filter_type == "return") {
    discipline_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_avg) %>% distinct(), by = c("pubname" = "signalname")) %>%
      filter(rbar_avg >= return_threshold) %>%
      inner_join(discipline_mapping_filtered, by = c("pubname" = "signalname"))
  } else {
    discipline_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
      filter(rbar_t >= t_threshold) %>%
      inner_join(discipline_mapping_filtered, by = c("pubname" = "signalname"))
  }
  
  # Raw returns with t >= t_threshold filter (by discipline) - excluding Economics
  raw_t2_summary_discipline <- discipline_data %>%
    group_by(discipline) %>%
    summarise(
      n_signals = n_distinct(pubname),
      pub_oos = mean(ret[eventDate > 0], na.rm = TRUE),
      pub_oos_se = {
        n <- sum(eventDate > 0 & !is.na(ret))
        if (n > 1) sd(ret[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
      },
      dm_oos = mean(matchRet[eventDate > 0], na.rm = TRUE),
      dm_oos_se = {
        n <- sum(eventDate > 0 & !is.na(matchRet))
        if (n > 1) sd(matchRet[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
      },
      outperform = pub_oos - dm_oos,
      outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
      .groups = 'drop'
    )
  
  # Filter data to exclude Economics journals
  journal_mapping_filtered <- journal_mapping %>% filter(journal_rank != "Economics")
  
  # Raw returns filter (by journal) - excluding Economics
  if (filter_type == "return") {
    journal_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_avg) %>% distinct(), by = c("pubname" = "signalname")) %>%
      filter(rbar_avg >= return_threshold) %>%
      inner_join(journal_mapping_filtered, by = c("pubname" = "signalname"))
  } else {
    journal_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
      left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
      filter(rbar_t >= t_threshold) %>%
      inner_join(journal_mapping_filtered, by = c("pubname" = "signalname"))
  }
  
  raw_t2_summary_journal <- journal_data %>%
    group_by(journal_rank) %>%
    summarise(
      n_signals = n_distinct(pubname),
      pub_oos = mean(ret[eventDate > 0], na.rm = TRUE),
      pub_oos_se = {
        n <- sum(eventDate > 0 & !is.na(ret))
        if (n > 1) sd(ret[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
      },
      dm_oos = mean(matchRet[eventDate > 0], na.rm = TRUE),
      dm_oos_se = {
        n <- sum(eventDate > 0 & !is.na(matchRet))
        if (n > 1) sd(matchRet[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
      },
      outperform = pub_oos - dm_oos,
      outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
      .groups = 'drop'
    )
  
  # Collect time-varying discipline data (if summaries exist)
  if (exists("capm_tv_t2_summary_discipline") && exists("ff3_tv_t2_summary_discipline")) {
    tv_discipline_data <- list()
    discipline_groups <- c("Finance", "Accounting")
    for(i in 1:length(discipline_groups)) {
      group <- discipline_groups[i]
      tv_discipline_data[[i]] <- list(
        # Raw
        raw_pub_oos = get_values(raw_t2_summary_discipline, "discipline", group, "pub_oos"),
        raw_pub_oos_se = get_values(raw_t2_summary_discipline, "discipline", group, "pub_oos_se"),
        raw_outperform = get_values(raw_t2_summary_discipline, "discipline", group, "outperform"),
        raw_outperform_se = get_values(raw_t2_summary_discipline, "discipline", group, "outperform_se"),
        
        # CAPM-TV
        capm_tv_pub_oos = get_values(capm_tv_t2_summary_discipline, "discipline", group, "pub_oos"),
        capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_discipline, "discipline", group, "pub_oos_se"),
        capm_tv_outperform = get_values(capm_tv_t2_summary_discipline, "discipline", group, "outperform"),
        capm_tv_outperform_se = get_values(capm_tv_t2_summary_discipline, "discipline", group, "outperform_se"),
        
        # FF3-TV
        ff3_tv_pub_oos = get_values(ff3_tv_t2_summary_discipline, "discipline", group, "pub_oos"),
        ff3_tv_pub_oos_se = get_values(ff3_tv_t2_summary_discipline, "discipline", group, "pub_oos_se"),
        ff3_tv_outperform = get_values(ff3_tv_t2_summary_discipline, "discipline", group, "outperform"),
        ff3_tv_outperform_se = get_values(ff3_tv_t2_summary_discipline, "discipline", group, "outperform_se")
      )
    }
  }
  
  # Collect time-varying journal data (if summaries exist)
  if (exists("capm_tv_t2_summary_journal") && exists("ff3_tv_t2_summary_journal")) {
    tv_journal_data <- list()
    journal_groups <- c("JF, JFE, RFS", "AR, JAR, JAE", "Other")
    for(i in 1:length(journal_groups)) {
      group <- journal_groups[i]
      tv_journal_data[[i]] <- list(
        # Raw
        raw_pub_oos = get_values(raw_t2_summary_journal, "journal_rank", group, "pub_oos"),
        raw_pub_oos_se = get_values(raw_t2_summary_journal, "journal_rank", group, "pub_oos_se"),
        raw_outperform = get_values(raw_t2_summary_journal, "journal_rank", group, "outperform"),
        raw_outperform_se = get_values(raw_t2_summary_journal, "journal_rank", group, "outperform_se"),
        
        # CAPM-TV
        capm_tv_pub_oos = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "pub_oos"),
        capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "pub_oos_se"),
        capm_tv_outperform = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "outperform"),
        capm_tv_outperform_se = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "outperform_se"),
        
        # FF3-TV
        ff3_tv_pub_oos = get_values(ff3_tv_t2_summary_journal, "journal_rank", group, "pub_oos"),
        ff3_tv_pub_oos_se = get_values(ff3_tv_t2_summary_journal, "journal_rank", group, "pub_oos_se"),
        ff3_tv_outperform = get_values(ff3_tv_t2_summary_journal, "journal_rank", group, "outperform"),
        ff3_tv_outperform_se = get_values(ff3_tv_t2_summary_journal, "journal_rank", group, "outperform_se")
      )
    }
  }
  
  # Collect time-varying Any Model vs No Model data (if model summaries exist)
  if (exists("capm_tv_t2_summary_model") && exists("ff3_tv_t2_summary_model")) {
    tv_anymodel_data <- list(
      # No Model row
      raw_pub_oos = get_values(raw_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos"),
      raw_pub_oos_se = get_values(raw_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos_se"),
      raw_outperform = get_values(raw_t2_summary_model, "modeltype_grouped", "No Model", "outperform"),
      raw_outperform_se = get_values(raw_t2_summary_model, "modeltype_grouped", "No Model", "outperform_se"),
      
      capm_tv_pub_oos = get_values(capm_tv_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos"),
      capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos_se"),
      capm_tv_outperform = get_values(capm_tv_t2_summary_model, "modeltype_grouped", "No Model", "outperform"),
      capm_tv_outperform_se = get_values(capm_tv_t2_summary_model, "modeltype_grouped", "No Model", "outperform_se"),
      
      ff3_tv_pub_oos = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos"),
      ff3_tv_pub_oos_se = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos_se"),
      ff3_tv_outperform = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "No Model", "outperform"),
      ff3_tv_outperform_se = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "No Model", "outperform_se")
    )
    
    # For Any Model, combine Stylized and Dynamic/Quantitative
    anymodel_capm_tv_pub_oos <- mean(c(
      get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"),
      get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos")
    ), na.rm = TRUE)
    
    anymodel_capm_tv_outperform <- mean(c(
      get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"),
      get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform")
    ), na.rm = TRUE)
    
    anymodel_ff3_tv_pub_oos <- mean(c(
      get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"),
      get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos")
    ), na.rm = TRUE)
    
    anymodel_ff3_tv_outperform <- mean(c(
      get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"),
      get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform")
    ), na.rm = TRUE)
    
    # Approximate standard errors for Any Model (using pooled variance)
    anymodel_capm_tv_pub_oos_se <- sqrt(mean(c(
      get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")^2,
      get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")^2
    ), na.rm = TRUE))
    
    anymodel_capm_tv_outperform_se <- sqrt(mean(c(
      get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")^2,
      get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")^2
    ), na.rm = TRUE))
    
    anymodel_ff3_tv_pub_oos_se <- sqrt(mean(c(
      get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")^2,
      get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")^2
    ), na.rm = TRUE))
    
    anymodel_ff3_tv_outperform_se <- sqrt(mean(c(
      get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")^2,
      get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")^2
    ), na.rm = TRUE))
    
    tv_anymodel_any <- list(
      # Raw
      raw_pub_oos = mean(c(
        get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"),
        get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos")
      ), na.rm = TRUE),
      raw_pub_oos_se = sqrt(mean(c(
        get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")^2,
        get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")^2
      ), na.rm = TRUE)),
      raw_outperform = mean(c(
        get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"),
        get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform")
      ), na.rm = TRUE),
      raw_outperform_se = sqrt(mean(c(
        get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")^2,
        get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")^2
      ), na.rm = TRUE)),
      
      # CAPM-TV
      capm_tv_pub_oos = anymodel_capm_tv_pub_oos,
      capm_tv_pub_oos_se = anymodel_capm_tv_pub_oos_se,
      capm_tv_outperform = anymodel_capm_tv_outperform,
      capm_tv_outperform_se = anymodel_capm_tv_outperform_se,
      
      # FF3-TV
      ff3_tv_pub_oos = anymodel_ff3_tv_pub_oos,
      ff3_tv_pub_oos_se = anymodel_ff3_tv_pub_oos_se,
      ff3_tv_outperform = anymodel_ff3_tv_outperform,
      ff3_tv_outperform_se = anymodel_ff3_tv_outperform_se
    )
  }
}

# DISCIPLINE AND JOURNAL RANKING TABLE (t >= t_threshold) ---------------------------------
if (filter_type == "return") {
  cat("\n\n=== SUMMARY TABLE BY DISCIPLINE AND JOURNAL RANKING (avg >=", return_threshold, ") ===\n")
} else {
  cat("\n\n=== SUMMARY TABLE BY DISCIPLINE AND JOURNAL RANKING (t >=", t_threshold, ") ===\n")
}

# Filter data to exclude Economics discipline
discipline_mapping_filtered <- discipline_mapping %>% filter(discipline %in% c("Finance", "Accounting"))

# Create data with discipline column
if (filter_type == "return") {
  discipline_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_avg) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_avg >= return_threshold) %>%
    inner_join(discipline_mapping_filtered, by = c("pubname" = "signalname"))
} else {
  discipline_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_t >= t_threshold) %>%
    inner_join(discipline_mapping_filtered, by = c("pubname" = "signalname"))
}

# Raw returns with t >= t_threshold filter (by discipline) - excluding Economics
raw_t2_summary_discipline <- discipline_data %>%
  group_by(discipline) %>%
  summarise(
    n_signals = n_distinct(pubname),
    pub_oos = mean(ret[eventDate > 0], na.rm = TRUE),
    pub_oos_se = {
      n <- sum(eventDate > 0 & !is.na(ret))
      if (n > 1) sd(ret[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    dm_oos = mean(matchRet[eventDate > 0], na.rm = TRUE),
    dm_oos_se = {
      n <- sum(eventDate > 0 & !is.na(matchRet))
      if (n > 1) sd(matchRet[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    outperform = pub_oos - dm_oos,
    outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
    .groups = 'drop'
  )

# CAPM t >= t_threshold filtered (by discipline) - excluding Economics
discipline_data_capm <- ret_for_plot0_capm_t2 %>%
  inner_join(discipline_mapping_filtered, by = c("pubname" = "signalname"))

capm_t2_summary_discipline <- discipline_data_capm %>%
  group_by(discipline) %>%
  summarise(
    n_signals = n_distinct(pubname),
    pub_oos = mean(abnormal_capm_normalized[eventDate > 0], na.rm = TRUE),
    pub_oos_se = {
      n <- sum(eventDate > 0 & !is.na(abnormal_capm_normalized))
      if (n > 1) sd(abnormal_capm_normalized[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    dm_oos = mean(matchRet_capm_t2_normalized[eventDate > 0], na.rm = TRUE),
    dm_oos_se = {
      n <- sum(eventDate > 0 & !is.na(matchRet_capm_t2_normalized))
      if (n > 1) sd(matchRet_capm_t2_normalized[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    outperform = pub_oos - dm_oos,
    outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
    .groups = 'drop'
  )

# FF3 t >= t_threshold filtered (by discipline) - excluding Economics
discipline_data_ff3 <- ret_for_plot0_ff3_t2 %>%
  inner_join(discipline_mapping_filtered, by = c("pubname" = "signalname"))

ff3_t2_summary_discipline <- discipline_data_ff3 %>%
  group_by(discipline) %>%
  summarise(
    n_signals = n_distinct(pubname),
    pub_oos = mean(abnormal_ff3_normalized[eventDate > 0], na.rm = TRUE),
    pub_oos_se = {
      n <- sum(eventDate > 0 & !is.na(abnormal_ff3_normalized))
      if (n > 1) sd(abnormal_ff3_normalized[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    dm_oos = mean(matchRet_ff3_t2_normalized[eventDate > 0], na.rm = TRUE),
    dm_oos_se = {
      n <- sum(eventDate > 0 & !is.na(matchRet_ff3_t2_normalized))
      if (n > 1) sd(matchRet_ff3_t2_normalized[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    outperform = pub_oos - dm_oos,
    outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
    .groups = 'drop'
  )

# Filter data to exclude Economics journals
journal_mapping_filtered <- journal_mapping %>% filter(journal_rank != "Economics")

# Raw returns filter (by journal) - excluding Economics
if (filter_type == "return") {
  journal_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_avg) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_avg >= return_threshold) %>%
    inner_join(journal_mapping_filtered, by = c("pubname" = "signalname"))
} else {
  journal_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_t >= t_threshold) %>%
    inner_join(journal_mapping_filtered, by = c("pubname" = "signalname"))
}

raw_t2_summary_journal <- journal_data %>%
  group_by(journal_rank) %>%
  summarise(
    n_signals = n_distinct(pubname),
    pub_oos = mean(ret[eventDate > 0], na.rm = TRUE),
    pub_oos_se = {
      n <- sum(eventDate > 0 & !is.na(ret))
      if (n > 1) sd(ret[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    dm_oos = mean(matchRet[eventDate > 0], na.rm = TRUE),
    dm_oos_se = {
      n <- sum(eventDate > 0 & !is.na(matchRet))
      if (n > 1) sd(matchRet[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    outperform = pub_oos - dm_oos,
    outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
    .groups = 'drop'
  )

# CAPM t >= t_threshold filtered (by journal) - excluding Economics
journal_data_capm <- ret_for_plot0_capm_t2 %>%
  inner_join(journal_mapping_filtered, by = c("pubname" = "signalname"))

capm_t2_summary_journal <- journal_data_capm %>%
  group_by(journal_rank) %>%
  summarise(
    n_signals = n_distinct(pubname),
    pub_oos = mean(abnormal_capm_normalized[eventDate > 0], na.rm = TRUE),
    pub_oos_se = {
      n <- sum(eventDate > 0 & !is.na(abnormal_capm_normalized))
      if (n > 1) sd(abnormal_capm_normalized[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    dm_oos = mean(matchRet_capm_t2_normalized[eventDate > 0], na.rm = TRUE),
    dm_oos_se = {
      n <- sum(eventDate > 0 & !is.na(matchRet_capm_t2_normalized))
      if (n > 1) sd(matchRet_capm_t2_normalized[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    outperform = pub_oos - dm_oos,
    outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
    .groups = 'drop'
  )

# FF3 t >= t_threshold filtered (by journal) - excluding Economics
journal_data_ff3 <- ret_for_plot0_ff3_t2 %>%
  inner_join(journal_mapping_filtered, by = c("pubname" = "signalname"))

ff3_t2_summary_journal <- journal_data_ff3 %>%
  group_by(journal_rank) %>%
  summarise(
    n_signals = n_distinct(pubname),
    pub_oos = mean(abnormal_ff3_normalized[eventDate > 0], na.rm = TRUE),
    pub_oos_se = {
      n <- sum(eventDate > 0 & !is.na(abnormal_ff3_normalized))
      if (n > 1) sd(abnormal_ff3_normalized[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    dm_oos = mean(matchRet_ff3_t2_normalized[eventDate > 0], na.rm = TRUE),
    dm_oos_se = {
      n <- sum(eventDate > 0 & !is.na(matchRet_ff3_t2_normalized))
      if (n > 1) sd(matchRet_ff3_t2_normalized[eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    outperform = pub_oos - dm_oos,
    outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
    .groups = 'drop'
  )

# Print discipline and journal table
if (filter_type == "return") {
  cat("\nPost-Sample Return (avg>=", return_threshold, ")     Outperformance vs Data-Mining (avg>=", return_threshold, ")\n")
} else {
  cat("\nPost-Sample Return (t>=", t_threshold, ")     Outperformance vs Data-Mining (t>=", t_threshold, ")\n")
}
cat("                Raw    CAPM    FF3    Raw    CAPM    FF3\n")
cat("Discipline\n")

groups_discipline <- c("Finance", "Accounting") 
for(group in groups_discipline) {
  # Post-sample returns
  raw_ret <- round(get_values(raw_t2_summary_discipline, "discipline", group, "pub_oos"))
  capm_ret <- round(get_values(capm_t2_summary_discipline, "discipline", group, "pub_oos"))  
  ff3_ret <- round(get_values(ff3_t2_summary_discipline, "discipline", group, "pub_oos"))
  
  # Outperformance
  raw_out <- round(get_values(raw_t2_summary_discipline, "discipline", group, "outperform"))
  capm_out <- round(get_values(capm_t2_summary_discipline, "discipline", group, "outperform"))
  ff3_out <- round(get_values(ff3_t2_summary_discipline, "discipline", group, "outperform"))
  
  # Standard errors  
  raw_se <- round(get_values(raw_t2_summary_discipline, "discipline", group, "pub_oos_se"))
  capm_se <- round(get_values(capm_t2_summary_discipline, "discipline", group, "pub_oos_se"))
  ff3_se <- round(get_values(ff3_t2_summary_discipline, "discipline", group, "pub_oos_se"))
  
  raw_out_se <- round(get_values(raw_t2_summary_discipline, "discipline", group, "outperform_se"))
  capm_out_se <- round(get_values(capm_t2_summary_discipline, "discipline", group, "outperform_se"))
  ff3_out_se <- round(get_values(ff3_t2_summary_discipline, "discipline", group, "outperform_se"))
  
  cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
              group, raw_ret, capm_ret, ff3_ret, raw_out, capm_out, ff3_out))
  cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
              "", raw_se, capm_se, ff3_se, raw_out_se, capm_out_se, ff3_out_se))
}

cat("Journal Ranking\n")
groups_journal <- c("JF, JFE, RFS", "AR, JAR, JAE", "Other")
for(group in groups_journal) {
  # Post-sample returns
  raw_ret <- round(get_values(raw_t2_summary_journal, "journal_rank", group, "pub_oos"))
  capm_ret <- round(get_values(capm_t2_summary_journal, "journal_rank", group, "pub_oos"))  
  ff3_ret <- round(get_values(ff3_t2_summary_journal, "journal_rank", group, "pub_oos"))
  
  # Outperformance
  raw_out <- round(get_values(raw_t2_summary_journal, "journal_rank", group, "outperform"))
  capm_out <- round(get_values(capm_t2_summary_journal, "journal_rank", group, "outperform"))
  ff3_out <- round(get_values(ff3_t2_summary_journal, "journal_rank", group, "outperform"))
  
  # Standard errors  
  raw_se <- round(get_values(raw_t2_summary_journal, "journal_rank", group, "pub_oos_se"))
  capm_se <- round(get_values(capm_t2_summary_journal, "journal_rank", group, "pub_oos_se"))
  ff3_se <- round(get_values(ff3_t2_summary_journal, "journal_rank", group, "pub_oos_se"))
  
  raw_out_se <- round(get_values(raw_t2_summary_journal, "journal_rank", group, "outperform_se"))
  capm_out_se <- round(get_values(capm_t2_summary_journal, "journal_rank", group, "outperform_se"))
  ff3_out_se <- round(get_values(ff3_t2_summary_journal, "journal_rank", group, "outperform_se"))
  
  cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
              group, raw_ret, capm_ret, ff3_ret, raw_out, capm_out, ff3_out))
  cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
              "", raw_se, capm_se, ff3_se, raw_out_se, capm_out_se, ff3_out_se))
}

# Export tables to CSV and LaTeX -------------------------------------------------------

# REFACTORED TABLE CREATION USING NEW HELPER FUNCTIONS

# Build main summary table using refactored approach
main_groups <- c("Risk", "Mispricing", "Agnostic", "No Model", "Stylized", "Dynamic or Quantitative")
main_categories <- c(rep("Theoretical Foundation", 3), rep("Modeling Formalism", 3))

# Collect theory summaries
theory_summaries <- list(
  raw = raw_t2_summary_theory,
  capm = capm_t2_summary_theory,
  ff3 = ff3_t2_summary_theory
)

# Collect model summaries
model_summaries <- list(
  raw = raw_t2_summary_model,
  capm = capm_t2_summary_model,
  ff3 = ff3_t2_summary_model
)

# Build data for each group
main_table_data <- list()
for (i in 1:length(main_groups)) {
  group <- main_groups[i]
  
  if (i <= 3) {
    # Theory groups
    main_table_data[[i]] <- build_table_row(theory_summaries, group, "theory_group")
  } else {
    # Model groups
    model_group <- main_groups[i]
    main_table_data[[i]] <- build_table_row(model_summaries, model_group, "modeltype_grouped")
  }
}

# Add overall row
overall_data <- list(
  raw_pub_oos = overall_t2_summary_raw$pub_oos,
  raw_pub_oos_se = overall_t2_summary_raw$pub_oos_se,
  raw_outperform = overall_t2_summary_raw$outperform,
  raw_outperform_se = overall_t2_summary_raw$outperform_se,
  capm_pub_oos = overall_t2_summary_capm$pub_oos,
  capm_pub_oos_se = overall_t2_summary_capm$pub_oos_se,
  capm_outperform = overall_t2_summary_capm$outperform,
  capm_outperform_se = overall_t2_summary_capm$outperform_se,
  ff3_pub_oos = overall_t2_summary_ff3$pub_oos,
  ff3_pub_oos_se = overall_t2_summary_ff3$pub_oos_se,
  ff3_outperform = overall_t2_summary_ff3$outperform,
  ff3_outperform_se = overall_t2_summary_ff3$outperform_se
)

main_categories <- c(main_categories, "Overall")
main_groups <- c(main_groups, "All")
main_table_data[[length(main_table_data) + 1]] <- overall_data

# Create the main table using the new build_summary_table function
export_table_main <- build_summary_table(
  categories = main_categories,
  groups = main_groups,
  summaries = main_table_data,
  analysis_types = c("raw", "capm", "ff3"),
  format_latex = FALSE,
  digits = 0
)

# Build discipline/journal table using refactored approach
discipline_groups <- c("Finance", "Accounting")
discipline_categories <- rep("Discipline", 2)

journal_groups <- c("JF, JFE, RFS", "AR, JAR, JAE", "Other")
journal_categories <- rep("Journal Ranking", 3)

# Collect discipline summaries
discipline_summaries <- list(
  raw = raw_t2_summary_discipline,
  capm = capm_t2_summary_discipline,
  ff3 = ff3_t2_summary_discipline
)

# Collect journal summaries  
journal_summaries <- list(
  raw = raw_t2_summary_journal,
  capm = capm_t2_summary_journal,
  ff3 = ff3_t2_summary_journal
)

# Build discipline data
discipline_table_data <- list()
for (i in 1:length(discipline_groups)) {
  discipline_table_data[[i]] <- build_table_row(discipline_summaries, discipline_groups[i], "discipline")
}

# Build journal data
for (i in 1:length(journal_groups)) {
  discipline_table_data[[length(discipline_table_data) + 1]] <- build_table_row(journal_summaries, journal_groups[i], "journal_rank")
}

# Create the discipline/journal table
export_table_discipline <- build_summary_table(
  categories = c(discipline_categories, journal_categories),
  groups = c(discipline_groups, journal_groups),
  summaries = discipline_table_data,
  analysis_types = c("raw", "capm", "ff3"),
  format_latex = FALSE,
  digits = 0
)

# [Old repetitive code with 66+ format_with_se(get_values(...)) calls removed]
# Now using the refactored build_summary_table() function above 
# Export to CSV and LaTeX using new multi-format function
# Format file suffix based on threshold value
file_suffix <- ifelse(filter_type == "return", 
                     paste0("_r", gsub("\\.", "", format(return_threshold, nsmall = 0))), 
                     paste0("_t", t_threshold))

# Export main theory/model table in multiple formats
export_tables_multi_format(
  export_table_main,
  base_filename = paste0(results_dir, "/Table_RiskAdjusted_TheoryModel", file_suffix),
  formats = c("csv", "latex"),
  latex_options = list(
    caption = "Risk-Adjusted Returns: Theoretical Foundation and Modeling Formalism",
    label = "tab:risk_adjusted_theory_model"
  )
)

# Export discipline/journal table in multiple formats  
export_tables_multi_format(
  export_table_discipline,
  base_filename = paste0(results_dir, "/Table_RiskAdjusted_DisciplineJournal", file_suffix),
  formats = c("csv", "latex"),
  latex_options = list(
    caption = "Risk-Adjusted Returns: Discipline and Journal Rankings",
    label = "tab:risk_adjusted_discipline_journal"
  )
)

# Create and export the Any Model vs No Model table
if (exists("anymodel_table_data")) {
  export_table_anymodel <- build_summary_table(
    categories = rep("", length(anymodel_groups)),  # No categories for this table
    groups = anymodel_groups,
    summaries = anymodel_table_data,
    analysis_types = c("raw", "capm", "ff3"),
    format_latex = FALSE,
    digits = 0
  )
  
  # Export Any Model vs No Model table in multiple formats
  export_tables_multi_format(
    export_table_anymodel %>% select(-Category),  # Remove empty Category column
    base_filename = paste0(results_dir, "/Table_RiskAdjusted_AnyModelVsNoModel", file_suffix),
    formats = c("csv", "latex"),
    latex_options = list(
      caption = "Risk-Adjusted Returns: Any Model vs No Model",
      label = "tab:risk_adjusted_anymodel"
    )
  )
}

# Export time-varying tables if they exist
if (exists("tv_theory_data") && exists("tv_model_data")) {
  # Combine theory and model TV data
  tv_categories <- c(rep("Theoretical Foundation", length(groups_theory)),
                     rep("Modeling Formalism", length(groups_model)),
                     "Overall")
  tv_groups <- c(groups_theory, groups_model, "All")
  tv_all_data <- c(tv_theory_data, tv_model_data, list(tv_overall_data))
  
  # Create time-varying table
  export_table_tv <- build_tv_summary_table(
    categories = tv_categories,
    groups = tv_groups,
    summaries = tv_all_data,
    digits = 0
  )
  
  # Export time-varying table
  export_tables_multi_format(
    export_table_tv,
    base_filename = paste0(results_dir, "/Table_RiskAdjusted_TimeVarying", file_suffix),
    formats = c("csv", "latex"),
    latex_options = list(
      caption = "Time-Varying Risk-Adjusted Returns: Theoretical Foundation and Modeling Formalism",
      label = "tab:risk_adjusted_tv",
      group_headers = list(
        list(title = "Raw", span = 2),
        list(title = "CAPM-TV", span = 2),
        list(title = "FF3-TV", span = 2)
      )
    )
  )
  
  # Export time-varying discipline/journal table if data exists
  if (exists("tv_discipline_data") && exists("tv_journal_data")) {
    # Combine discipline and journal TV data
    tv_dj_categories <- c(rep("Discipline", 2), rep("Journal Rank", 3))
    tv_dj_groups <- c("Finance", "Accounting", "JF, JFE, RFS", "AR, JAR, JAE", "Other")
    tv_dj_all_data <- c(tv_discipline_data, tv_journal_data)
    
    # Create time-varying discipline/journal table
    export_table_tv_dj <- build_tv_summary_table(
      categories = tv_dj_categories,
      groups = tv_dj_groups,
      summaries = tv_dj_all_data,
      digits = 0
    )
    
    # Export time-varying discipline/journal table
    export_tables_multi_format(
      export_table_tv_dj,
      base_filename = paste0(results_dir, "/Table_RiskAdjusted_TimeVarying_DisciplineJournal", file_suffix),
      formats = c("csv", "latex"),
      latex_options = list(
        caption = "Time-Varying Risk-Adjusted Returns: Discipline and Journal Rank",
        label = "tab:risk_adjusted_tv_dj",
        group_headers = list(
          list(title = "Raw", span = 2),
          list(title = "CAPM-TV", span = 2),
          list(title = "FF3-TV", span = 2)
        )
      )
    )
  }
  
  # Export time-varying Any Model vs No Model table if data exists
  if (exists("tv_anymodel_data")) {
    # Create time-varying Any Model vs No Model table
    tv_am_categories <- c("")
    tv_am_groups <- c("No Model")
    tv_am_all_data <- list(tv_anymodel_data)
    
    # Add Any Model data 
    # Calculate Any Model averages from Stylized and Dynamic or Quantitative
    tv_anymodel_any <- list(
      # Raw
      raw_pub_oos = mean(c(
        get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"),
        get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos")
      ), na.rm = TRUE),
      raw_pub_oos_se = sqrt(mean(c(
        get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")^2,
        get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")^2
      ), na.rm = TRUE)),
      raw_outperform = mean(c(
        get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"),
        get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform")
      ), na.rm = TRUE),
      raw_outperform_se = sqrt(mean(c(
        get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")^2,
        get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")^2
      ), na.rm = TRUE)),
      
      # CAPM-TV
      capm_tv_pub_oos = mean(c(
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"),
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos")
      ), na.rm = TRUE),
      capm_tv_pub_oos_se = sqrt(mean(c(
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")^2,
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")^2
      ), na.rm = TRUE)),
      capm_tv_outperform = mean(c(
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"),
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform")
      ), na.rm = TRUE),
      capm_tv_outperform_se = sqrt(mean(c(
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")^2,
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")^2
      ), na.rm = TRUE)),
      
      # FF3-TV
      ff3_tv_pub_oos = mean(c(
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"),
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos")
      ), na.rm = TRUE),
      ff3_tv_pub_oos_se = sqrt(mean(c(
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")^2,
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")^2
      ), na.rm = TRUE)),
      ff3_tv_outperform = mean(c(
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"),
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform")
      ), na.rm = TRUE),
      ff3_tv_outperform_se = sqrt(mean(c(
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")^2,
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")^2
      ), na.rm = TRUE))
    )
    
    tv_am_categories <- c("", "")
    tv_am_groups <- c("No Model", "Any Model")
    tv_am_all_data <- list(tv_anymodel_data, tv_anymodel_any)
    
    export_table_tv_am <- build_tv_summary_table(
      categories = tv_am_categories,
      groups = tv_am_groups,
      summaries = tv_am_all_data,
      digits = 0
    )
    
    # Export time-varying Any Model vs No Model table
    export_tables_multi_format(
      export_table_tv_am,
      base_filename = paste0(results_dir, "/Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel", file_suffix),
      formats = c("csv", "latex"),
      latex_options = list(
        caption = "Time-Varying Risk-Adjusted Returns: Any Model vs No Model",
        label = "tab:risk_adjusted_tv_anymodel",
        group_headers = list(
          list(title = "Raw", span = 2),
          list(title = "CAPM-TV", span = 2),
          list(title = "FF3-TV", span = 2)
        )
      )
    )
  }
}

# Also export the raw summary data for reference (CSV only for raw data)
# Using a loop to reduce repetition
summary_data_list <- list(
  list(data = raw_t2_summary_theory, name = "Raw_TheoryGroup"),
  list(data = capm_t2_summary_theory, name = "CAPM_TheoryGroup"),
  list(data = ff3_t2_summary_theory, name = "FF3_TheoryGroup"),
  list(data = raw_t2_summary_model, name = "Raw_ModelGroup"),
  list(data = capm_t2_summary_model, name = "CAPM_ModelGroup"),
  list(data = ff3_t2_summary_model, name = "FF3_ModelGroup"),
  list(data = raw_t2_summary_discipline, name = "Raw_Discipline"),
  list(data = capm_t2_summary_discipline, name = "CAPM_Discipline"),
  list(data = ff3_t2_summary_discipline, name = "FF3_Discipline"),
  list(data = raw_t2_summary_journal, name = "Raw_Journal"),
  list(data = capm_t2_summary_journal, name = "CAPM_Journal"),
  list(data = ff3_t2_summary_journal, name = "FF3_Journal")
)

# Export all summary data efficiently
for (item in summary_data_list) {
  write.csv(item$data, paste0(results_dir, "/", item$name, file_suffix, ".csv"), row.names = FALSE)
}

cat("\n=== EXPORTED TABLES ===\n")
cat(paste("All files saved in:", results_dir, "\n"))
cat("\nMain summary tables:\n")
cat(paste0("- Table_RiskAdjusted_TheoryModel", file_suffix, ".csv and .tex\n"))
cat(paste0("- Table_RiskAdjusted_DisciplineJournal", file_suffix, ".csv and .tex\n"))
cat(paste0("- Table_RiskAdjusted_AnyModelVsNoModel", file_suffix, ".csv and .tex\n"))
if (exists("tv_theory_data")) {
  cat(paste0("- Table_RiskAdjusted_TimeVarying", file_suffix, ".csv and .tex\n"))
}
cat("\nDetailed breakdowns:\n")
cat("- Raw/CAPM/FF3 by TheoryGroup/ModelGroup/Discipline/Journal (12 files)\n")
cat("\nPlots generated:\n")
plot_files <- list.files(results_dir, pattern = "^Fig_RiskAdj_.*\\.pdf$", full.names = FALSE)
if (length(plot_files) == 0) {
  cat("- None found\n")
} else {
  cat(paste0("- ", plot_files, "\n"), sep = "")
}

# Helper: print unnormalized tables for CAPM/FF3 (raw units)
print_unnormalized_tables <- function() {
  cat("\n\n=== UNNORMALIZED FULL-SAMPLE ALPHA TABLES (raw units) ===\n")
  dm_capm_aggregated_raw <- aggregate_dm_no_norm(
    dm_filtered_capm,
    "abnormal_capm",
    "capm_t2_raw"
  )
  dm_ff3_aggregated_raw <- aggregate_dm_no_norm(
    dm_filtered_ff3,
    "abnormal_ff3",
    "ff3_t2_raw"
  )
  ret_for_plot0_capm_t2_raw <- create_filtered_plot_data(
    ret_for_plot0_adj,
    signals_capm_t2,
    dm_capm_aggregated_raw,
    "abnormal_capm",
    "matchRet_capm_t2_raw",
    "capm_t2_raw"
  )
  ret_for_plot0_ff3_t2_raw <- create_filtered_plot_data(
    ret_for_plot0_adj,
    signals_ff3_t2,
    dm_ff3_aggregated_raw,
    "abnormal_ff3",
    "matchRet_ff3_t2_raw",
    "ff3_t2_raw"
  )
  capm_t2_summary_theory_rawunits <- compute_outperformance(
    ret_for_plot0_capm_t2_raw,
    "abnormal_capm", "matchRet_capm_t2_raw", theory_mapping, "theory_group"
  )
  ff3_t2_summary_theory_rawunits <- compute_outperformance(
    ret_for_plot0_ff3_t2_raw,
    "abnormal_ff3", "matchRet_ff3_t2_raw", theory_mapping, "theory_group"
  )
  capm_t2_summary_model_rawunits <- compute_outperformance(
    ret_for_plot0_capm_t2_raw,
    "abnormal_capm", "matchRet_capm_t2_raw", model_mapping, "modeltype_grouped"
  )
  ff3_t2_summary_model_rawunits <- compute_outperformance(
    ret_for_plot0_ff3_t2_raw,
    "abnormal_ff3", "matchRet_ff3_t2_raw", model_mapping, "modeltype_grouped"
  )
  print_summary_table(
    summaries = list(
      raw = raw_t2_summary_theory,
      capm_un = capm_t2_summary_theory_rawunits,
      ff3_un = ff3_t2_summary_theory_rawunits
    ),
    groups = c("Risk", "Mispricing", "Agnostic"),
    group_col = "theory_group",
    table_title = if (filter_type == "return") {
      paste0("FULL-SAMPLE ALPHA (UNNORMALIZED, avg>=", return_threshold, ") BY THEORETICAL FOUNDATION")
    } else {
      paste0("FULL-SAMPLE ALPHA (UNNORMALIZED, t>=", t_threshold, ") BY THEORETICAL FOUNDATION")
    },
    analysis_types = c("raw", "capm_un", "ff3_un"),
    analysis_labels = c("Raw", "CAPM(un)", "FF3(un)")
  )
  print_summary_table(
    summaries = list(
      raw = raw_t2_summary_model,
      capm_un = capm_t2_summary_model_rawunits,
      ff3_un = ff3_t2_summary_model_rawunits
    ),
    groups = c("No Model", "Stylized", "Dynamic or Quantitative"),
    group_col = "modeltype_grouped",
    table_title = if (filter_type == "return") {
      paste0("FULL-SAMPLE ALPHA (UNNORMALIZED, avg>=", return_threshold, ") BY MODELING FORMALISM")
    } else {
      paste0("FULL-SAMPLE ALPHA (UNNORMALIZED, t>=", t_threshold, ") BY MODELING FORMALISM")
    },
    analysis_types = c("raw", "capm_un", "ff3_un"),
    analysis_labels = c("Raw", "CAPM(un)", "FF3(un)")
  )
  
  # Overall (All) row for unnormalized full-sample
  overall_un_raw <- compute_overall_summary(
    plot_data = ret_for_plot0 %>% filter(!is.na(matchRet), pubname %in% filtered_signals_raw),
    ret_col = "ret",
    dm_col = "matchRet"
  ) %>% select(-group, -n_signals)
  
  overall_un_capm <- compute_overall_summary(
    plot_data = ret_for_plot0_capm_t2_raw,
    ret_col = "abnormal_capm",
    dm_col = "matchRet_capm_t2_raw"
  ) %>% select(-group, -n_signals)
  
  overall_un_ff3 <- compute_overall_summary(
    plot_data = ret_for_plot0_ff3_t2_raw,
    ret_col = "abnormal_ff3",
    dm_col = "matchRet_ff3_t2_raw"
  ) %>% select(-group, -n_signals)
  
  cat("\nOVERALL (UNNORMALIZED)\n")
  cat(sprintf("%-12s %4.0f   %6.2f   %6.2f   %4.0f   %6.2f   %6.2f\n",
              "All",
              round(overall_un_raw$pub_oos), overall_un_capm$pub_oos, overall_un_ff3$pub_oos,
              round(overall_un_raw$outperform), overall_un_capm$outperform, overall_un_ff3$outperform))
  cat(sprintf("%-12s (%2.0f)   (%6.2f)   (%6.2f)   (%2.0f)   (%6.2f)   (%6.2f)\n",
              "",
              round(overall_un_raw$pub_oos_se), overall_un_capm$pub_oos_se, overall_un_ff3$pub_oos_se,
              round(overall_un_raw$outperform_se), overall_un_capm$outperform_se, overall_un_ff3$outperform_se))
  
  # Time-varying (unnormalized) CAPM/FF3
  if (exists("dm_filtered_capm_tv") && exists("dm_filtered_ff3_tv")) {
    cat("\n\n=== UNNORMALIZED TIME-VARYING ALPHA TABLES (raw units) ===\n")
    dm_capm_tv_aggregated_raw <- aggregate_dm_no_norm(
      dm_filtered_capm_tv,
      "abnormal_capm_tv",
      "capm_tv_t2_raw"
    )
    dm_ff3_tv_aggregated_raw <- aggregate_dm_no_norm(
      dm_filtered_ff3_tv,
      "abnormal_ff3_tv",
      "ff3_tv_t2_raw"
    )
    ret_for_plot0_capm_tv_t2_raw <- create_filtered_plot_data(
      ret_for_plot0_adj,
      signals_capm_tv_t2,
      dm_capm_tv_aggregated_raw,
      "abnormal_capm_tv",
      "matchRet_capm_tv_t2_raw",
      "capm_tv_t2_raw"
    )
    ret_for_plot0_ff3_tv_t2_raw <- create_filtered_plot_data(
      ret_for_plot0_adj,
      signals_ff3_tv_t2,
      dm_ff3_tv_aggregated_raw,
      "abnormal_ff3_tv",
      "matchRet_ff3_tv_t2_raw",
      "ff3_tv_t2_raw"
    )
    capm_tv_t2_summary_theory_rawunits <- compute_outperformance(
      ret_for_plot0_capm_tv_t2_raw,
      "abnormal_capm_tv", "matchRet_capm_tv_t2_raw", theory_mapping, "theory_group"
    )
    ff3_tv_t2_summary_theory_rawunits <- compute_outperformance(
      ret_for_plot0_ff3_tv_t2_raw,
      "abnormal_ff3_tv", "matchRet_ff3_tv_t2_raw", theory_mapping, "theory_group"
    )
    capm_tv_t2_summary_model_rawunits <- compute_outperformance(
      ret_for_plot0_capm_tv_t2_raw,
      "abnormal_capm_tv", "matchRet_capm_tv_t2_raw", model_mapping, "modeltype_grouped"
    )
    ff3_tv_t2_summary_model_rawunits <- compute_outperformance(
      ret_for_plot0_ff3_tv_t2_raw,
      "abnormal_ff3_tv", "matchRet_ff3_tv_t2_raw", model_mapping, "modeltype_grouped"
    )
    print_summary_table(
      summaries = list(
        raw = raw_t2_summary_theory,
        capm_tv_un = capm_tv_t2_summary_theory_rawunits,
        ff3_tv_un = ff3_tv_t2_summary_theory_rawunits
      ),
      groups = c("Risk", "Mispricing", "Agnostic"),
      group_col = "theory_group",
      table_title = if (filter_type == "return") {
        paste0("TIME-VARYING ALPHA (UNNORMALIZED, avg>=", return_threshold, ") BY THEORETICAL FOUNDATION")
      } else {
        paste0("TIME-VARYING ALPHA (UNNORMALIZED, t>=", t_threshold, ") BY THEORETICAL FOUNDATION")
      },
      analysis_types = c("raw", "capm_tv_un", "ff3_tv_un"),
      analysis_labels = c("Raw", "CAPM-TV(un)", "FF3-TV(un)")
    )
    print_summary_table(
      summaries = list(
        raw = raw_t2_summary_model,
        capm_tv_un = capm_tv_t2_summary_model_rawunits,
        ff3_tv_un = ff3_tv_t2_summary_model_rawunits
      ),
      groups = c("No Model", "Stylized", "Dynamic or Quantitative"),
      group_col = "modeltype_grouped",
      table_title = if (filter_type == "return") {
        paste0("TIME-VARYING ALPHA (UNNORMALIZED, avg>=", return_threshold, ") BY MODELING FORMALISM")
      } else {
        paste0("TIME-VARYING ALPHA (UNNORMALIZED, t>=", t_threshold, ") BY MODELING FORMALISM")
      },
      analysis_types = c("raw", "capm_tv_un", "ff3_tv_un"),
      analysis_labels = c("Raw", "CAPM-TV(un)", "FF3-TV(un)")
    )
    
    # Overall (All) row for unnormalized time-varying
    overall_un_capm_tv <- compute_overall_summary(
      plot_data = ret_for_plot0_capm_tv_t2_raw,
      ret_col = "abnormal_capm_tv",
      dm_col = "matchRet_capm_tv_t2_raw"
    ) %>% select(-group, -n_signals)
    
    overall_un_ff3_tv <- compute_overall_summary(
      plot_data = ret_for_plot0_ff3_tv_t2_raw,
      ret_col = "abnormal_ff3_tv",
      dm_col = "matchRet_ff3_tv_t2_raw"
    ) %>% select(-group, -n_signals)
    
    cat("\nOVERALL TIME-VARYING (UNNORMALIZED)\n")
    cat(sprintf("%-12s %6.2f   %6.2f   %6.2f\n",
                "All (CAPM-TV)", overall_un_capm_tv$pub_oos, overall_un_capm_tv$outperform, overall_un_capm_tv$dm_oos))
    cat(sprintf("%-12s (%6.2f)   (%6.2f)   (%6.2f)\n",
                "", overall_un_capm_tv$pub_oos_se, overall_un_capm_tv$outperform_se, overall_un_capm_tv$dm_oos_se))
    cat(sprintf("%-12s %6.2f   %6.2f   %6.2f\n",
                "All (FF3-TV)", overall_un_ff3_tv$pub_oos, overall_un_ff3_tv$outperform, overall_un_ff3_tv$dm_oos))
    cat(sprintf("%-12s (%6.2f)   (%6.2f)   (%6.2f)\n",
                "", overall_un_ff3_tv$pub_oos_se, overall_un_ff3_tv$outperform_se, overall_un_ff3_tv$dm_oos_se))
  }
}

# Invoke after function definition
print_unnormalized_tables()
