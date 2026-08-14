# Render the sample-specific risk-adjusted by-group tables for Section 4 and
# Table IA.10.
#
# How to run: set the working directory to flex-mining/, then
#   Rscript S4b_RVsDM_ByGroup.R
# Inputs:  czsum_allpredictors.RDS, czret_keeponly.RDS, ret_for_plot0.RDS,
#          the versioned MatchPubRiskAdjusted.RData cache, Fama-French factors,
#          and DataInput/SignalsTheoryChecked.csv
# Outputs: Table_RiskAdjusted_TimeVarying_ff4_t2.{csv,tex},
#          Table_RiskAdjusted_TimeVarying_DisciplineJournal_ff4_t2.{csv,tex},
#          and Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel_ff4_t2.{csv,tex}
#          under ../Results/RiskAdjusted/TstatFilter; corrected full-sample
#          audit tables under ../Results/RiskAdjusted/FullSampleTstatFilter;
#          and six paper-facing table fragments at top-level ../Results
#
# This calculation path was moved from
# 4c4_RiskAdjustedResearchVsDMPlotsTVFF4.R and now uses the phase-two estimands:
# predictor-month means, month/predictor clustered inference, and paired
# published-minus-data-mined differences.
#
# FIXED: Time-varying beta/alpha consistency between published and DM signals
# - Published signals now use sampstart/sampend periods (not eventDate-based)
# - DM signals use sampstart/sampend periods (consistent with published)
# - Both use same IS/OOS period definitions for adjustments

# Preflight -------------------------------------------------------------
settings_env <- new.env(parent = globalenv())
sys.source("config.R", envir = settings_env)
version <- settings_env$globalSettings$dataVersion
rm(settings_env)

risk_adj_file <- paste0(
  "../Data/Processed/", version, " MatchPubRiskAdjusted.RData"
)
required_files <- c(
  "../Data/Processed/czsum_allpredictors.RDS",
  "../Data/Processed/czret_keeponly.RDS",
  "../Data/Processed/ret_for_plot0.RDS",
  risk_adj_file,
  "../Data/Raw/FamaFrenchFactors.RData",
  "DataInput/SignalsTheoryChecked.csv"
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing Section 4 risk-adjustment input(s): ",
    paste(missing_files, collapse = ", "),
    ". Run the required upstream chapter first."
  )
}

# Setup ----------------------------------------------------------------
source("0_Environment.R")
source("helpers/risk_adjusted_helpers_tv.R")

t_threshold = 2

# Filter type fixed to t-stat only
filter_type <- "tstat"

# Create results subfolder for risk-adjusted analysis
base_results_dir <- "../Results/RiskAdjusted"
results_dir <- file.path(base_results_dir, "TstatFilter")
full_sample_results_dir <- file.path(base_results_dir, "FullSampleTstatFilter")

if (!dir.exists(results_dir)) {
  dir.create(results_dir, recursive = TRUE)
  cat("Created directory:", results_dir, "\n")
} else {
  cat("Using existing directory:", results_dir, "\n")
}
if (!dir.exists(full_sample_results_dir)) {
  dir.create(full_sample_results_dir, recursive = TRUE)
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

# Ensure samptype exists on published data for consistent IS/OOS split
if (!"samptype" %in% names(czret)) {
  czret <- czret %>%
    mutate(samptype = ifelse(date >= sampstart & date <= sampend, 'insamp', 'oos'))
}

# Load pre-computed matched returns
ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")

# Load pre-computed risk-adjusted DM returns
# Load individual DM returns for t-stat computation
candidateReturns_adj <- readRDS(risk_adj_file)

# Ensure samptype exists on DM data for consistent IS/OOS split
if (!"samptype" %in% names(candidateReturns_adj)) {
  candidateReturns_adj <- candidateReturns_adj %>%
    mutate(samptype = ifelse(date >= sampstart & date <= sampend, 'insamp', 'oos'))
}

# Load FF factors and join ------------------------------------------------
FamaFrenchFactors <- readRDS('../Data/Raw/FamaFrenchFactors.RData') %>%
  rename(date = yearm)

czret <- czret %>%
  left_join(FamaFrenchFactors, by = c('date'))

ret_for_plot0 <- ret_for_plot0 %>%
  left_join(
    czret %>% select(signalname, eventDate, date, mktrf, smb, hml, umd, sampstart, sampend),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  )

## Time-varying CAPM/FF4 adjustments
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
  hml_max = max(hml, na.rm = TRUE),
  umd_min = min(umd, na.rm = TRUE),
  umd_p05 = quantile(umd, 0.05, na.rm = TRUE),
  umd_p25 = quantile(umd, 0.25, na.rm = TRUE),
  umd_p75 = quantile(umd, 0.75, na.rm = TRUE),
  umd_p95 = quantile(umd, 0.95, na.rm = TRUE),
  umd_max = max(umd, na.rm = TRUE)
)])

cat("\nMomentum factor (umd) loaded successfully!\n")
cat("UMD available:", sum(!is.na(czret$umd)), "observations\n")

# Compute raw return t-stats on actual returns (not scaled)
# FIXED: Use sampstart/sampend-based periods for consistency with DM signals
czret[date >= sampstart & date <= sampend, `:=`(
  rbar_t = {
    m <- mean(ret, na.rm = TRUE)
    s <- sd(ret, na.rm = TRUE)
    n <- sum(!is.na(ret))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, rbar_t := nafill(rbar_t, "locf"), by = .(signalname)]

# Compute published-side time-varying CAPM/FF4 betas and normalized alphas
# Use IS betas within IS period and OOS betas within OOS period
# FIXED: Use sampstart/sampend periods to match DM signal approach
betas_capm_is <- czret[date >= sampstart & date <= sampend & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_is = extract_beta(ret, mktrf)), by = signalname]
betas_capm_oos <- czret[date > sampend & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_oos = extract_beta(ret, mktrf)), by = signalname]
czret <- merge(czret, betas_capm_is, by = 'signalname', all.x = TRUE)
czret <- merge(czret, betas_capm_oos, by = 'signalname', all.x = TRUE)
# FIXED: Use sampstart/sampend-based samptype for consistency with DM signals
czret[, beta_capm_tv := ifelse(date >= sampstart & date <= sampend, beta_capm_is, beta_capm_oos)]
czret[, abnormal_capm_tv := ret - beta_capm_tv * mktrf]
# FIXED: Use sampstart/sampend periods for computing in-sample statistics
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
# FIXED: Use sampstart/sampend-based periods for consistency with DM signals
czret[, beta_ff4_tv := ifelse(date >= sampstart & date <= sampend, beta_ff4_is, beta_ff4_oos)]
czret[, s_ff4_tv := ifelse(date >= sampstart & date <= sampend, s_ff4_is, s_ff4_oos)]
czret[, h_ff4_tv := ifelse(date >= sampstart & date <= sampend, h_ff4_is, h_ff4_oos)]
czret[, u_ff4_tv := ifelse(date >= sampstart & date <= sampend, u_ff4_is, u_ff4_oos)]
czret[, abnormal_ff4_tv := ret - (beta_ff4_tv * mktrf + s_ff4_tv * smb + h_ff4_tv * hml + u_ff4_tv * umd)]
# FIXED: Use sampstart/sampend periods for computing in-sample statistics
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

# Compute published-side full-sample CAPM/FF3 betas and normalized alphas.
# Full-sample betas use all observations from the original sample start onward.
betas_capm_fs <- czret[date >= sampstart & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_fs = extract_beta(ret, mktrf)), by = signalname]
czret <- merge(czret, betas_capm_fs, by = "signalname", all.x = TRUE)
czret[, abnormal_capm_fs := ret - beta_capm_fs * mktrf]
czret[date >= sampstart & date <= sampend, `:=`(
  abar_capm_fs = mean(abnormal_capm_fs, na.rm = TRUE),
  abar_capm_fs_t = {
    m <- mean(abnormal_capm_fs, na.rm = TRUE)
    s <- sd(abnormal_capm_fs, na.rm = TRUE)
    n <- sum(!is.na(abnormal_capm_fs))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_capm_fs := nafill(abar_capm_fs, "locf"), by = signalname]
czret[, abar_capm_fs_t := nafill(abar_capm_fs_t, "locf"), by = signalname]
czret[, abnormal_capm_fs_normalized := ifelse(
  abs(abar_capm_fs) > 1e-10, 100 * abnormal_capm_fs / abar_capm_fs, NA_real_
)]

ff3_fs <- czret[
  date >= sampstart & !is.na(ret) & !is.na(mktrf) & !is.na(smb) & !is.na(hml),
  {
    coeffs <- extract_ff3_coeffs(ret, mktrf, smb, hml)
    .(beta_ff3_fs = coeffs[1], s_ff3_fs = coeffs[2], h_ff3_fs = coeffs[3])
  },
  by = signalname
]
czret <- merge(czret, ff3_fs, by = "signalname", all.x = TRUE)
czret[, abnormal_ff3_fs := ret - (
  beta_ff3_fs * mktrf + s_ff3_fs * smb + h_ff3_fs * hml
)]
czret[date >= sampstart & date <= sampend, `:=`(
  abar_ff3_fs = mean(abnormal_ff3_fs, na.rm = TRUE),
  abar_ff3_fs_t = {
    m <- mean(abnormal_ff3_fs, na.rm = TRUE)
    s <- sd(abnormal_ff3_fs, na.rm = TRUE)
    n <- sum(!is.na(abnormal_ff3_fs))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_ff3_fs := nafill(abar_ff3_fs, "locf"), by = signalname]
czret[, abar_ff3_fs_t := nafill(abar_ff3_fs_t, "locf"), by = signalname]
czret[, abnormal_ff3_fs_normalized := ifelse(
  abs(abar_ff3_fs) > 1e-10, 100 * abnormal_ff3_fs / abar_ff3_fs, NA_real_
)]

# Compute normalized DM abnormal returns ---------------------------------

# First, create the risk-adjusted data for plotting by joining with `czret`
ret_for_plot0_adj <- ret_for_plot0 %>%
  left_join(
    czret %>% select(
      signalname, eventDate,
      abnormal_capm_tv, abnormal_ff4_tv,
      abnormal_capm_tv_normalized, abnormal_ff4_tv_normalized,
      abnormal_capm_fs, abnormal_ff3_fs,
      abnormal_capm_fs_normalized, abnormal_ff3_fs_normalized
    ),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  )

# Default Plot Settings --------------------------------------------------
fontsizeall = 28
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5
global_xl = -360
global_xh = 300

print("Risk adjustments computed successfully!")
print(paste("Number of signals with time-varying CAPM adjustments:", sum(!is.na(czret$beta_capm_tv))))
print(paste("Number of signals with time-varying FF4 adjustments:", sum(!is.na(czret$beta_ff4_tv))))
print(paste("Number of signals with full-sample CAPM adjustments:", sum(!is.na(czret$beta_capm_fs))))
print(paste("Number of signals with full-sample FF3 adjustments:", sum(!is.na(czret$beta_ff3_fs))))

# Filtered versions (t-stat or return) -----------------------------------------------

cat("\n\n=== T-STAT FILTERED ANALYSIS (t > ", t_threshold, ") ===\n")

cat("\nComputing statistics for individual DM signals...\n")
filters <- prepare_dm_filters(
  candidateReturns_adj = candidateReturns_adj,
  czret = czret,
  filter_type = filter_type,
  t_threshold = t_threshold
)

dm_stats <- filters$dm_stats
signals_raw_t2 <- filters$signals_raw

cat("\nNumber of PUBLISHED signals with t > ", t_threshold, ":\n")
cat("Raw returns:", length(signals_raw_t2), "\n")





# Also compute UNNORMALIZED CAPM/FF4 tables to print (raw units) -------------------------
# Moved into helper function print_unnormalized_tables(); call placed later before  section

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

groups_theory <- c("Risk", "Mispricing", "Agnostic")
groups_model <- c("No Model", "Stylized", "Dynamic or Quantitative")

# Check if time-varying columns exist in the data
if("abnormal_capm_tv" %in% names(candidateReturns_adj) && "abnormal_ff4_tv" %in% names(candidateReturns_adj)) {

  # Compute statistics for time-varying abnormal returns with proper NA handling
  # FIXED: Use sampstart/sampend-based periods for consistency with published signals
  dm_stats_tv <- candidateReturns_adj[
    (date >= sampstart & date <= sampend) & !is.na(abnormal_capm_tv),
    .(
      abar_capm_tv_dm_t = {
        m <- mean(abnormal_capm_tv, na.rm = TRUE)
        s <- sd(abnormal_capm_tv, na.rm = TRUE)
        n <- sum(!is.na(abnormal_capm_tv))
        if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
      },
      abar_ff4_tv_dm_t = {
        m <- mean(abnormal_ff4_tv, na.rm = TRUE)
        s <- sd(abnormal_ff4_tv, na.rm = TRUE)
        n <- sum(!is.na(abnormal_ff4_tv))
        if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
      },
      abar_capm_tv_dm = mean(abnormal_capm_tv, na.rm = TRUE),
      abar_ff4_tv_dm = mean(abnormal_ff4_tv, na.rm = TRUE)
    ),
    by = .(actSignal, candSignalname)
  ]

  # NEW: Define published signal sets using alpha stats (t-stat only)
  signals_capm_tv_t2 <- unique(czret[abar_capm_tv_t > t_threshold]$signalname)
  signals_ff4_tv_t2  <- unique(czret[abar_ff4_tv_t  > t_threshold]$signalname)

  # Additionally require published signals to pass the raw filter for comparability
  signals_capm_tv_t2 <- intersect(signals_capm_tv_t2, signals_raw_t2)
  signals_ff4_tv_t2  <- intersect(signals_ff4_tv_t2, signals_raw_t2)

  # CAPM time-varying filtering (t-stat only)
  cat("\n=== CAPM TIME-VARYING (t > ", t_threshold, ") STATISTICS ===\n")
  dm_filtered_capm_tv <- candidateReturns_adj %>%
    inner_join(
      dm_stats_tv %>% filter(abar_capm_tv_dm_t > t_threshold),
      by = c("actSignal", "candSignalname")
    )
  assert_dm_screen(
    dm_filtered_capm_tv, dm_stats_tv, "abar_capm_tv_dm_t",
    t_threshold, "CAPM"
  )

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

  cat("Published signals with CAPM t > ", t_threshold, ":", length(signals_capm_tv_t2), "\n")
  cat("Published signals with filtered DM matches (time-varying):", length(unique(ret_for_plot0_capm_tv_t2$pubname)), "\n")
  cat("DM signals with time-varying CAPM t > ", t_threshold, ":", sum(dm_stats_tv$abar_capm_tv_dm_t > t_threshold, na.rm = TRUE), "\n")

  # FF4 time-varying filtering (t-stat only)
  cat("\n=== FF4 TIME-VARYING (t > ", t_threshold, ") STATISTICS ===\n")
  dm_filtered_ff4_tv <- candidateReturns_adj %>%
    inner_join(
      dm_stats_tv %>% filter(abar_ff4_tv_dm_t > t_threshold),
      by = c("actSignal", "candSignalname")
    )
  assert_dm_screen(
    dm_filtered_ff4_tv, dm_stats_tv, "abar_ff4_tv_dm_t",
    t_threshold, "FF4"
  )

  # Normalize and aggregate
  dm_ff4_tv_aggregated <- normalize_and_aggregate_dm(
    dm_filtered_ff4_tv,
    "abnormal_ff4_tv",
    "ff4_tv_t2_normalized"
  )

  # Create filtered plot data
  ret_for_plot0_ff4_tv_t2 <- create_filtered_plot_data(
    ret_for_plot0_adj,
    signals_ff4_tv_t2,  # Use TV-filtered published signals
    dm_ff4_tv_aggregated,
    "abnormal_ff4_tv_normalized",  # Use published TV-normalized alpha
    "matchRet_ff4_tv_t2_normalized",
    "ff4_tv_t2_normalized"
  )

  cat("Published signals with FF4 t > ", t_threshold, ":", length(signals_ff4_tv_t2), "\n")
  cat("Published signals with filtered DM matches (time-varying):", length(unique(ret_for_plot0_ff4_tv_t2$pubname)), "\n")
  cat("DM signals with time-varying FF4 t > ", t_threshold, ":", sum(dm_stats_tv$abar_ff4_tv_dm_t > t_threshold, na.rm = TRUE), "\n")

  # Create Time-Varying Alpha Summary Tables
  cat("\n\n=== TIME-VARYING ALPHA SUMMARY TABLES ===\n")

  # Prepare data for alpha summary tables
  tv_plot_data <- list()

  # Add raw data for comparison (filtered by the same signals) - t-stat only
  tv_plot_data[["raw"]] <- ret_for_plot0 %>%
    filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(),
              by = c("pubname" = "signalname")) %>%
    filter(rbar_t > t_threshold)

  tv_plot_data[["capm_tv"]] <- ret_for_plot0_capm_tv_t2
  tv_plot_data[["ff4_tv"]] <- ret_for_plot0_ff4_tv_t2

  expected_raw_signals <- unique(czret[rbar_t > t_threshold]$signalname)
  expected_capm_signals <- intersect(
    unique(czret[abar_capm_tv_t > t_threshold]$signalname), expected_raw_signals
  )
  expected_ff4_signals <- intersect(
    unique(czret[abar_ff4_tv_t > t_threshold]$signalname), expected_raw_signals
  )
  if (!setequal(signals_raw_t2, expected_raw_signals)) {
    stop("Raw published screen differs from rbar_t > ", t_threshold)
  }
  if (!setequal(signals_capm_tv_t2, expected_capm_signals)) {
    stop("CAPM published screen differs from alpha-t/raw intersection")
  }
  if (!setequal(signals_ff4_tv_t2, expected_ff4_signals)) {
    stop("FF4 published screen differs from alpha-t/raw intersection")
  }
  sample_diagnostics <- bind_rows(
    audit_analysis_sample(tv_plot_data[["raw"]], "ret", "matchRet", "Raw"),
    audit_analysis_sample(
      tv_plot_data[["capm_tv"]], "abnormal_capm_tv_normalized",
      "matchRet_capm_tv_t2_normalized", "CAPM"
    ),
    audit_analysis_sample(
      tv_plot_data[["ff4_tv"]], "abnormal_ff4_tv_normalized",
      "matchRet_ff4_tv_t2_normalized", "FF4"
    )
  )
  print(sample_diagnostics)
  cat(
    "Published screens: raw=", length(expected_raw_signals),
    "; CAPM alpha/raw=", length(unique(czret[abar_capm_tv_t > t_threshold]$signalname)),
    "/", length(expected_capm_signals),
    "; FF4 alpha/raw=", length(unique(czret[abar_ff4_tv_t > t_threshold]$signalname)),
    "/", length(expected_ff4_signals), "\n", sep = ""
  )
  cat(
    "Data-mined screens: CAPM=",
    sum(dm_stats_tv$abar_capm_tv_dm_t > t_threshold, na.rm = TRUE),
    "; FF4=", sum(dm_stats_tv$abar_ff4_tv_dm_t > t_threshold, na.rm = TRUE),
    " candidate signal pairs\n", sep = ""
  )

  # Create mappings (reuse existing ones from main analysis)
  tv_mappings <- list(
    theory = theory_mapping,
    model = model_mapping
  )

  # Create alpha summaries
  tv_summaries <- create_summary_tables(
    tv_plot_data,
    tv_mappings,
    table_name = "Alpha Analysis",
    filter_desc = paste0("t > ", t_threshold)
  )

  # Print alpha summary by theory
  print_summary_table(
    tv_summaries[["theory"]],
    groups = c("Risk", "Mispricing", "Agnostic"),
    group_col = "theory_group",
    table_title = "ALPHA BY THEORETICAL FOUNDATION",
    analysis_types = c("raw", "capm_tv", "ff4_tv"),
    analysis_labels = c("Raw", "CAPM", "FF4")
  )

  # Print alpha summary by model
  print_summary_table(
    tv_summaries[["model"]],
    groups = c("No Model", "Stylized", "Dynamic or Quantitative"),
    group_col = "modeltype_grouped",
    table_title = "ALPHA BY MODELING FORMALISM",
    analysis_types = c("raw", "capm_tv", "ff4_tv"),
    analysis_labels = c("Raw", "CAPM", "FF4")
  )

} else {
  cat("\nTime-varying abnormal returns not available in the data.\n")
  cat("Please run 2d_RiskAdjustDataMinedSignals.R with the updated code to generate these columns.\n")
}

# The filtered candidate panels are much larger than the predictor-month
# panels retained for inference. Release them before constructing the
# full-sample screens so the two adjustment families run sequentially.
rm(
  dm_filtered_capm_tv, dm_filtered_ff4_tv,
  dm_capm_tv_aggregated, dm_ff4_tv_aggregated,
  dm_stats_tv, tv_plot_data, tv_summaries
)
invisible(gc())

# Full-sample abnormal returns (one beta from sample start onward) --------
if (all(c("abnormal_capm", "abnormal_ff3") %in% names(candidateReturns_adj))) {
  cat("\n\n=== FULL-SAMPLE ABNORMAL RETURNS ===\n")
  dm_stats_fs <- candidateReturns_adj[
    date >= sampstart & date <= sampend & !is.na(abnormal_capm),
    .(
      abar_capm_fs_dm_t = {
        m <- mean(abnormal_capm, na.rm = TRUE)
        s <- sd(abnormal_capm, na.rm = TRUE)
        n <- sum(!is.na(abnormal_capm))
        if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
      },
      abar_ff3_fs_dm_t = {
        m <- mean(abnormal_ff3, na.rm = TRUE)
        s <- sd(abnormal_ff3, na.rm = TRUE)
        n <- sum(!is.na(abnormal_ff3))
        if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
      }
    ),
    by = .(actSignal, candSignalname)
  ]

  signals_capm_fs_t2 <- intersect(
    unique(czret[abar_capm_fs_t > t_threshold]$signalname), signals_raw_t2
  )
  signals_ff3_fs_t2 <- intersect(
    unique(czret[abar_ff3_fs_t > t_threshold]$signalname), signals_raw_t2
  )

  dm_filtered_capm_fs <- candidateReturns_adj %>%
    inner_join(
      dm_stats_fs %>% filter(abar_capm_fs_dm_t > t_threshold),
      by = c("actSignal", "candSignalname")
    )
  assert_dm_screen(
    dm_filtered_capm_fs, dm_stats_fs, "abar_capm_fs_dm_t", t_threshold, "Full-sample CAPM"
  )
  dm_capm_fs_aggregated <- normalize_and_aggregate_dm(
    dm_filtered_capm_fs, "abnormal_capm", "capm_fs_t2_normalized"
  )
  ret_for_plot0_capm_fs_t2 <- create_filtered_plot_data(
    ret_for_plot0_adj,
    signals_capm_fs_t2,
    dm_capm_fs_aggregated,
    "abnormal_capm_fs_normalized",
    "matchRet_capm_fs_t2_normalized",
    "capm_fs_t2_normalized"
  )
  rm(dm_filtered_capm_fs, dm_capm_fs_aggregated)
  invisible(gc())

  dm_filtered_ff3_fs <- candidateReturns_adj %>%
    inner_join(
      dm_stats_fs %>% filter(abar_ff3_fs_dm_t > t_threshold),
      by = c("actSignal", "candSignalname")
    )
  assert_dm_screen(
    dm_filtered_ff3_fs, dm_stats_fs, "abar_ff3_fs_dm_t", t_threshold, "Full-sample FF3"
  )
  dm_ff3_fs_aggregated <- normalize_and_aggregate_dm(
    dm_filtered_ff3_fs, "abnormal_ff3", "ff3_fs_t2_normalized"
  )
  ret_for_plot0_ff3_fs_t2 <- create_filtered_plot_data(
    ret_for_plot0_adj,
    signals_ff3_fs_t2,
    dm_ff3_fs_aggregated,
    "abnormal_ff3_fs_normalized",
    "matchRet_ff3_fs_t2_normalized",
    "ff3_fs_t2_normalized"
  )
  rm(dm_filtered_ff3_fs, dm_ff3_fs_aggregated)
  invisible(gc())

  fs_sample_diagnostics <- bind_rows(
    audit_analysis_sample(
      ret_for_plot0_capm_fs_t2, "abnormal_capm_fs_normalized",
      "matchRet_capm_fs_t2_normalized", "Full-sample CAPM"
    ),
    audit_analysis_sample(
      ret_for_plot0_ff3_fs_t2, "abnormal_ff3_fs_normalized",
      "matchRet_ff3_fs_t2_normalized", "Full-sample FF3"
    )
  )
  print(fs_sample_diagnostics)
} else {
  stop("Full-sample abnormal_capm/abnormal_ff3 columns are missing from the risk-adjustment cache")
}

# Generic Summary Table Functions ----------------------------------------------

# Function to create comprehensive summary tables for any analysis type



# FULL-SAMPLE SUMMARY TABLES FOR RAW RETURNS (Using new functions) --------------------
cat("\n\n=== FULL-SAMPLE SUMMARY TABLES FOR RAW RETURNS (FILTERED) ===\n")

# Prepare data for full-sample raw returns summary tables
fs_plot_data <- list()

# Add raw filtered data (t-stat only)
fs_plot_data[["raw"]] <- ret_for_plot0 %>%
  filter(!is.na(matchRet)) %>%
  left_join(czret %>% select(signalname, rbar_t) %>% distinct(),
            by = c("pubname" = "signalname")) %>%
  filter(rbar_t > t_threshold)



# Create mappings
fs_mappings <- list(
  theory = theory_mapping,
  model = model_mapping
)

# Create full-sample summaries using new functions
fs_summaries <- create_summary_tables(
  fs_plot_data,
  fs_mappings,
  table_name = "Full-Sample Raw Returns Analysis",
      filter_desc = paste0("t > ", t_threshold)
)

# Print full-sample summary by theory
print_summary_table(
  fs_summaries[["theory"]],
  groups = c("Risk", "Mispricing", "Agnostic"),
  group_col = "theory_group",
  table_title = "RAW RETURNS BY THEORETICAL FOUNDATION",
  analysis_types = c("raw"),
  analysis_labels = c("Raw")
)

# Print full-sample summary by model
print_summary_table(
  fs_summaries[["model"]],
  groups = c("No Model", "Stylized", "Dynamic or Quantitative"),
  group_col = "modeltype_grouped",
  table_title = "RAW RETURNS BY MODELING FORMALISM",
  analysis_types = c("raw"),
  analysis_labels = c("Raw")
)

# T-STAT FILTERED SUMMARY TABLE (t > t_threshold) ----------------------------------------
cat("\n\n=== SUMMARY TABLE WITH T > ", t_threshold, " FILTER ===\n")

# Raw returns filter (by theory) t-stat only
raw_t2_summary_theory <- compute_outperformance(
  ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_t > t_threshold),
  "ret", "matchRet", theory_mapping, "theory_group"
)



# Raw returns filter (by model) t-stat only
raw_t2_summary_model <- compute_outperformance(
  ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_t > t_threshold),
  "ret", "matchRet", model_mapping, "modeltype_grouped"
)



# Overall filtered summaries (t-stat only)
filtered_signals_raw <- czret$signalname[czret$rbar_t > t_threshold]

overall_t2_summary_raw <- compute_overall_summary(
  plot_data = ret_for_plot0 %>% filter(!is.na(matchRet), pubname %in% filtered_signals_raw),
  ret_col = "ret",
  dm_col = "matchRet"
)



# Time-varying summaries (if available)
if("abnormal_capm_tv" %in% names(candidateReturns_adj) && exists("ret_for_plot0_capm_tv_t2")) {
  # CAPM time-varying t > t_threshold filtered (by theory)
  capm_tv_t2_summary_theory <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized", "matchRet_capm_tv_t2_normalized", theory_mapping, "theory_group"
  )

  # FF4 time-varying t > t_threshold filtered (by theory)
  ff4_tv_t2_summary_theory <- compute_outperformance(
    ret_for_plot0_ff4_tv_t2,
    "abnormal_ff4_tv_normalized", "matchRet_ff4_tv_t2_normalized", theory_mapping, "theory_group"
  )

  # CAPM time-varying t > t_threshold filtered (by model)
  capm_tv_t2_summary_model <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized", "matchRet_capm_tv_t2_normalized", model_mapping, "modeltype_grouped"
  )

  # FF4 time-varying t > t_threshold filtered (by model)
  ff4_tv_t2_summary_model <- compute_outperformance(
    ret_for_plot0_ff4_tv_t2,
    "abnormal_ff4_tv_normalized", "matchRet_ff4_tv_t2_normalized", model_mapping, "modeltype_grouped"
  )

  # CAPM time-varying t > t_threshold filtered (by discipline)
  capm_tv_t2_summary_discipline <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized", "matchRet_capm_tv_t2_normalized", discipline_mapping_filtered, "discipline"
  )

  # FF4 time-varying t > t_threshold filtered (by discipline)
  ff4_tv_t2_summary_discipline <- compute_outperformance(
    ret_for_plot0_ff4_tv_t2,
    "abnormal_ff4_tv_normalized", "matchRet_ff4_tv_t2_normalized", discipline_mapping_filtered, "discipline"
  )

  # CAPM time-varying t > t_threshold filtered (by journal)
  capm_tv_t2_summary_journal <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized", "matchRet_capm_tv_t2_normalized", journal_mapping_filtered, "journal_rank"
  )

  # FF4 time-varying t > t_threshold filtered (by journal)
  ff4_tv_t2_summary_journal <- compute_outperformance(
    ret_for_plot0_ff4_tv_t2,
    "abnormal_ff4_tv_normalized", "matchRet_ff4_tv_t2_normalized", journal_mapping_filtered, "journal_rank"
  )

  # Overall time-varying summaries
  overall_t2_summary_capm_tv <- compute_overall_summary(
    plot_data = ret_for_plot0_capm_tv_t2,
    ret_col = "abnormal_capm_tv_normalized",
    dm_col = "matchRet_capm_tv_t2_normalized"
  )

  overall_t2_summary_ff4_tv <- compute_overall_summary(
    plot_data = ret_for_plot0_ff4_tv_t2,
    ret_col = "abnormal_ff4_tv_normalized",
    dm_col = "matchRet_ff4_tv_t2_normalized"
  )
}

# Full-sample CAPM/FF3 summaries use the same predictor-month and paired,
# two-way-clustered inference contract as the sample-specific tables.
capm_fs_t2_summary_theory <- compute_outperformance(
  ret_for_plot0_capm_fs_t2,
  "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized",
  theory_mapping, "theory_group"
)
ff3_fs_t2_summary_theory <- compute_outperformance(
  ret_for_plot0_ff3_fs_t2,
  "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized",
  theory_mapping, "theory_group"
)
capm_fs_t2_summary_model <- compute_outperformance(
  ret_for_plot0_capm_fs_t2,
  "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized",
  model_mapping, "modeltype_grouped"
)
ff3_fs_t2_summary_model <- compute_outperformance(
  ret_for_plot0_ff3_fs_t2,
  "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized",
  model_mapping, "modeltype_grouped"
)
capm_fs_t2_summary_discipline <- compute_outperformance(
  ret_for_plot0_capm_fs_t2,
  "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized",
  discipline_mapping_filtered, "discipline"
)
ff3_fs_t2_summary_discipline <- compute_outperformance(
  ret_for_plot0_ff3_fs_t2,
  "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized",
  discipline_mapping_filtered, "discipline"
)
capm_fs_t2_summary_journal <- compute_outperformance(
  ret_for_plot0_capm_fs_t2,
  "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized",
  journal_mapping_filtered, "journal_rank"
)
ff3_fs_t2_summary_journal <- compute_outperformance(
  ret_for_plot0_ff3_fs_t2,
  "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized",
  journal_mapping_filtered, "journal_rank"
)
overall_t2_summary_capm_fs <- compute_overall_summary(
  ret_for_plot0_capm_fs_t2,
  "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized"
)
overall_t2_summary_ff3_fs <- compute_overall_summary(
  ret_for_plot0_ff3_fs_t2,
  "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized"
)

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

# Function definitions moved to helpers/risk_adjusted_helpers_tv.R to avoid duplication

# Function to export tables in multiple formats
# export_tables_multi_format moved to helpers/risk_adjusted_helpers.R



# ANY MODEL VS NO MODEL TABLE (t > t_threshold) ----------------------------------------
cat("\n\n=== ANY MODEL VS NO MODEL TABLE (t > ", t_threshold, ") ===\n")

# Create Any Model vs No Model mapping
anymodel_mapping <- czcat_full %>%
  transmute(
    signalname,
    model_binary = case_when(
      NoModel == 1 ~ "No Model",
      TRUE ~ "Any Model"
    )
  )

# Raw returns filter (by model binary) - t-stat only
raw_t2_summary_anymodel <- compute_outperformance(
  ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_t > t_threshold),
  "ret", "matchRet", anymodel_mapping, "model_binary"
)

capm_fs_t2_summary_anymodel <- compute_outperformance(
  ret_for_plot0_capm_fs_t2,
  "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized",
  anymodel_mapping, "model_binary"
)
ff3_fs_t2_summary_anymodel <- compute_outperformance(
  ret_for_plot0_ff3_fs_t2,
  "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized",
  anymodel_mapping, "model_binary"
)



# Print Any Model vs No Model table
cat("\nPost-Sample Return (t>", t_threshold, ")     Outperformance vs Data-Mining (t>", t_threshold, ")\n")
cat("                Raw\n")

for(group in c("No Model", "Any Model")) {
  # Post-sample returns
  raw_ret <- round(get_values(raw_t2_summary_anymodel, "model_binary", group, "pub_oos"))

  # Outperformance
  raw_out <- round(get_values(raw_t2_summary_anymodel, "model_binary", group, "outperform"))

  # Standard errors
  raw_se <- round(get_values(raw_t2_summary_anymodel, "model_binary", group, "pub_oos_se"))

  raw_out_se <- round(get_values(raw_t2_summary_anymodel, "model_binary", group, "outperform_se"))

  cat(sprintf("%-12s %4s   %4s\n",
              group, raw_ret, raw_out))
  cat(sprintf("%-12s (%2s)   (%2s)\n",
              "", raw_se, raw_out_se))
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
    raw_outperform_se = get_values(raw_t2_summary_anymodel, "model_binary", group, "outperform_se")
  )
}

# Time-varying Any Model vs No Model (if available)
if("abnormal_capm_tv" %in% names(candidateReturns_adj) && exists("ret_for_plot0_capm_tv_t2")) {
  # CAPM time-varying t > t_threshold filtered (by model binary)
  capm_tv_t2_summary_anymodel <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_tv_normalized", "matchRet_capm_tv_t2_normalized", anymodel_mapping, "model_binary"
  )

  # FF4 time-varying t > t_threshold filtered (by model binary)
  ff4_tv_t2_summary_anymodel <- compute_outperformance(
    ret_for_plot0_ff4_tv_t2,
    "abnormal_ff4_tv_normalized", "matchRet_ff4_tv_t2_normalized", anymodel_mapping, "model_binary"
  )

  cat("\nTime-Varying Results:\n")
  cat("                       CAPM  FF4        CAPM  FF4\n")

  for(group in c("No Model", "Any Model")) {
    # Post-sample returns
    capm_tv_ret <- round(get_values(capm_tv_t2_summary_anymodel, "model_binary", group, "pub_oos"))
    ff4_tv_ret <- round(get_values(ff4_tv_t2_summary_anymodel, "model_binary", group, "pub_oos"))

    # Outperformance
    capm_tv_out <- round(get_values(capm_tv_t2_summary_anymodel, "model_binary", group, "outperform"))
    ff4_tv_out <- round(get_values(ff4_tv_t2_summary_anymodel, "model_binary", group, "outperform"))

    # Standard errors
    capm_tv_se <- round(get_values(capm_tv_t2_summary_anymodel, "model_binary", group, "pub_oos_se"))
    ff4_tv_se <- round(get_values(ff4_tv_t2_summary_anymodel, "model_binary", group, "pub_oos_se"))

    capm_tv_out_se <- round(get_values(capm_tv_t2_summary_anymodel, "model_binary", group, "outperform_se"))
    ff4_tv_out_se <- round(get_values(ff4_tv_t2_summary_anymodel, "model_binary", group, "outperform_se"))

    cat(sprintf("%-12s           %4s     %4s          %4s     %4s\n",
                group, capm_tv_ret, ff4_tv_ret, capm_tv_out, ff4_tv_out))
    cat(sprintf("%-12s           (%2s)    (%2s)          (%2s)    (%2s)\n",
                "", capm_tv_se, ff4_tv_se, capm_tv_out_se, ff4_tv_out_se))
  }
}

# Time-varying abnormal returns table (if available)
if("abnormal_capm_tv" %in% names(candidateReturns_adj) && exists("ret_for_plot0_capm_tv_t2")) {
  cat("\n\n=== TIME-VARYING ABNORMAL RETURNS TABLE (t > ", t_threshold, ") ===\n")
  cat("\nPost-Sample Return (t>", t_threshold, ")     Outperformance vs Data-Mining (t>", t_threshold, ")\n")
  cat("                       CAPM  FF4        CAPM  FF4\n")
  cat("Theoretical Foundation\n")

  for(group in groups_theory) {
    # Post-sample returns
    capm_tv_ret <- round(get_values(capm_tv_t2_summary_theory, "theory_group", group, "pub_oos"))
    ff4_tv_ret <- round(get_values(ff4_tv_t2_summary_theory, "theory_group", group, "pub_oos"))

    # Outperformance
    capm_tv_out <- round(get_values(capm_tv_t2_summary_theory, "theory_group", group, "outperform"))
    ff4_tv_out <- round(get_values(ff4_tv_t2_summary_theory, "theory_group", group, "outperform"))

    # Standard errors
    capm_tv_se <- round(get_values(capm_tv_t2_summary_theory, "theory_group", group, "pub_oos_se"))
    ff4_tv_se <- round(get_values(ff4_tv_t2_summary_theory, "theory_group", group, "pub_oos_se"))

    capm_tv_out_se <- round(get_values(capm_tv_t2_summary_theory, "theory_group", group, "outperform_se"))
    ff4_tv_out_se <- round(get_values(ff4_tv_t2_summary_theory, "theory_group", group, "outperform_se"))

    cat(sprintf("%-12s           %4s     %4s          %4s     %4s\n",
                group, capm_tv_ret, ff4_tv_ret, capm_tv_out, ff4_tv_out))
    cat(sprintf("%-12s           (%2s)    (%2s)          (%2s)    (%2s)\n",
                "", capm_tv_se, ff4_tv_se, capm_tv_out_se, ff4_tv_out_se))
  }

  cat("\nModeling Formalism\n")
  groups_model <- c("No Model", "Stylized", "Dynamic or Quantitative")
  for(group in groups_model) {
    # Post-sample returns
    capm_tv_ret <- round(get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos"))
    ff4_tv_ret <- round(get_values(ff4_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos"))

    # Outperformance
    capm_tv_out <- round(get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "outperform"))
    ff4_tv_out <- round(get_values(ff4_tv_t2_summary_model, "modeltype_grouped", group, "outperform"))

    # Standard errors
    capm_tv_se <- round(get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
    ff4_tv_se <- round(get_values(ff4_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))

    capm_tv_out_se <- round(get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
    ff4_tv_out_se <- round(get_values(ff4_tv_t2_summary_model, "modeltype_grouped", group, "outperform_se"))

    cat(sprintf("%-23s    %4s     %4s          %4s     %4s\n",
                group, capm_tv_ret, ff4_tv_ret, capm_tv_out, ff4_tv_out))
    cat(sprintf("%-23s    (%2s)    (%2s)          (%2s)    (%2s)\n",
                "", capm_tv_se, ff4_tv_se, capm_tv_out_se, ff4_tv_out_se))
  }

  # Overall
  cat(sprintf("%-12s           %4s     %4s          %4s     %4s\n",
              "All", round(overall_t2_summary_capm_tv$pub_oos), round(overall_t2_summary_ff4_tv$pub_oos),
              round(overall_t2_summary_capm_tv$outperform), round(overall_t2_summary_ff4_tv$outperform)))
  cat(sprintf("%-12s           (%2s)    (%2s)          (%2s)    (%2s)\n",
              "", round(overall_t2_summary_capm_tv$pub_oos_se), round(overall_t2_summary_ff4_tv$pub_oos_se),
              round(overall_t2_summary_capm_tv$outperform_se), round(overall_t2_summary_ff4_tv$outperform_se)))

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

      # CAPM
      capm_tv_pub_oos = get_values(capm_tv_t2_summary_theory, "theory_group", group, "pub_oos"),
      capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_theory, "theory_group", group, "pub_oos_se"),
      capm_tv_outperform = get_values(capm_tv_t2_summary_theory, "theory_group", group, "outperform"),
      capm_tv_outperform_se = get_values(capm_tv_t2_summary_theory, "theory_group", group, "outperform_se"),

      # FF4
      ff4_tv_pub_oos = get_values(ff4_tv_t2_summary_theory, "theory_group", group, "pub_oos"),
      ff4_tv_pub_oos_se = get_values(ff4_tv_t2_summary_theory, "theory_group", group, "pub_oos_se"),
      ff4_tv_outperform = get_values(ff4_tv_t2_summary_theory, "theory_group", group, "outperform"),
      ff4_tv_outperform_se = get_values(ff4_tv_t2_summary_theory, "theory_group", group, "outperform_se")
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

      # CAPM
      capm_tv_pub_oos = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos"),
      capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"),
      capm_tv_outperform = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "outperform"),
      capm_tv_outperform_se = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "outperform_se"),

      # FF4
      ff4_tv_pub_oos = get_values(ff4_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos"),
      ff4_tv_pub_oos_se = get_values(ff4_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"),
      ff4_tv_outperform = get_values(ff4_tv_t2_summary_model, "modeltype_grouped", group, "outperform"),
      ff4_tv_outperform_se = get_values(ff4_tv_t2_summary_model, "modeltype_grouped", group, "outperform_se")
    )
  }

  # Add overall results
  tv_overall_data <- list(
    # Raw
    raw_pub_oos = overall_t2_summary_raw$pub_oos,
    raw_pub_oos_se = overall_t2_summary_raw$pub_oos_se,
    raw_outperform = overall_t2_summary_raw$outperform,
    raw_outperform_se = overall_t2_summary_raw$outperform_se,

    # CAPM
    capm_tv_pub_oos = overall_t2_summary_capm_tv$pub_oos,
    capm_tv_pub_oos_se = overall_t2_summary_capm_tv$pub_oos_se,
    capm_tv_outperform = overall_t2_summary_capm_tv$outperform,
    capm_tv_outperform_se = overall_t2_summary_capm_tv$outperform_se,

    # FF4
    ff4_tv_pub_oos = overall_t2_summary_ff4_tv$pub_oos,
    ff4_tv_pub_oos_se = overall_t2_summary_ff4_tv$pub_oos_se,
    ff4_tv_outperform = overall_t2_summary_ff4_tv$outperform,
    ff4_tv_outperform_se = overall_t2_summary_ff4_tv$outperform_se
  )

  # PRE-COMPUTE RAW DISCIPLINE AND JOURNAL SUMMARIES FOR TABLES
  # Filter data to exclude Economics discipline
  discipline_mapping_filtered <- discipline_mapping %>% filter(discipline %in% c("Finance", "Accounting"))

  # Create data with discipline column (t-stat only)
  discipline_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_t > t_threshold) %>%
    inner_join(discipline_mapping_filtered, by = c("pubname" = "signalname"))

  # Raw returns with t > t_threshold filter (by discipline) - excluding Economics
  raw_t2_summary_discipline <- compute_outperformance(
    discipline_data %>% select(-discipline),
    "ret", "matchRet", discipline_mapping_filtered, "discipline"
  )

  # Filter data to exclude Economics journals
  journal_mapping_filtered <- journal_mapping %>% filter(journal_rank != "Economics")

  # Raw returns filter (by journal) - excluding Economics (t-stat only)
  journal_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_t > t_threshold) %>%
    inner_join(journal_mapping_filtered, by = c("pubname" = "signalname"))

  raw_t2_summary_journal <- compute_outperformance(
    journal_data %>% select(-journal_rank),
    "ret", "matchRet", journal_mapping_filtered, "journal_rank"
  )

  # Collect time-varying discipline data (if summaries exist)
  if (exists("capm_tv_t2_summary_discipline") && exists("ff4_tv_t2_summary_discipline")) {
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

        # CAPM
        capm_tv_pub_oos = get_values(capm_tv_t2_summary_discipline, "discipline", group, "pub_oos"),
        capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_discipline, "discipline", group, "pub_oos_se"),
        capm_tv_outperform = get_values(capm_tv_t2_summary_discipline, "discipline", group, "outperform"),
        capm_tv_outperform_se = get_values(capm_tv_t2_summary_discipline, "discipline", group, "outperform_se"),

        # FF4
        ff4_tv_pub_oos = get_values(ff4_tv_t2_summary_discipline, "discipline", group, "pub_oos"),
        ff4_tv_pub_oos_se = get_values(ff4_tv_t2_summary_discipline, "discipline", group, "pub_oos_se"),
        ff4_tv_outperform = get_values(ff4_tv_t2_summary_discipline, "discipline", group, "outperform"),
        ff4_tv_outperform_se = get_values(ff4_tv_t2_summary_discipline, "discipline", group, "outperform_se")
      )
    }
  }

  # Collect time-varying journal data (if summaries exist)
  if (exists("capm_tv_t2_summary_journal") && exists("ff4_tv_t2_summary_journal")) {
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

        # CAPM
        capm_tv_pub_oos = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "pub_oos"),
        capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "pub_oos_se"),
        capm_tv_outperform = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "outperform"),
        capm_tv_outperform_se = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "outperform_se"),

        # FF4
        ff4_tv_pub_oos = get_values(ff4_tv_t2_summary_journal, "journal_rank", group, "pub_oos"),
        ff4_tv_pub_oos_se = get_values(ff4_tv_t2_summary_journal, "journal_rank", group, "pub_oos_se"),
        ff4_tv_outperform = get_values(ff4_tv_t2_summary_journal, "journal_rank", group, "outperform"),
        ff4_tv_outperform_se = get_values(ff4_tv_t2_summary_journal, "journal_rank", group, "outperform_se")
      )
    }
  }

  # Collect time-varying Any Model vs No Model data (if model summaries exist)
  if (exists("capm_tv_t2_summary_model") && exists("ff4_tv_t2_summary_model")) {
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

      ff4_tv_pub_oos = get_values(ff4_tv_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos"),
      ff4_tv_pub_oos_se = get_values(ff4_tv_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos_se"),
      ff4_tv_outperform = get_values(ff4_tv_t2_summary_model, "modeltype_grouped", "No Model", "outperform"),
      ff4_tv_outperform_se = get_values(ff4_tv_t2_summary_model, "modeltype_grouped", "No Model", "outperform_se")
    )

    # Any Model is estimated on the pooled signal-month panel. It is not an
    # equal-weighted average of the two displayed model subgroups.
    tv_anymodel_any <- list(
      raw_pub_oos = get_values(raw_t2_summary_anymodel, "model_binary", "Any Model", "pub_oos"),
      raw_pub_oos_se = get_values(raw_t2_summary_anymodel, "model_binary", "Any Model", "pub_oos_se"),
      raw_outperform = get_values(raw_t2_summary_anymodel, "model_binary", "Any Model", "outperform"),
      raw_outperform_se = get_values(raw_t2_summary_anymodel, "model_binary", "Any Model", "outperform_se"),
      capm_tv_pub_oos = get_values(capm_tv_t2_summary_anymodel, "model_binary", "Any Model", "pub_oos"),
      capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_anymodel, "model_binary", "Any Model", "pub_oos_se"),
      capm_tv_outperform = get_values(capm_tv_t2_summary_anymodel, "model_binary", "Any Model", "outperform"),
      capm_tv_outperform_se = get_values(capm_tv_t2_summary_anymodel, "model_binary", "Any Model", "outperform_se"),
      ff4_tv_pub_oos = get_values(ff4_tv_t2_summary_anymodel, "model_binary", "Any Model", "pub_oos"),
      ff4_tv_pub_oos_se = get_values(ff4_tv_t2_summary_anymodel, "model_binary", "Any Model", "pub_oos_se"),
      ff4_tv_outperform = get_values(ff4_tv_t2_summary_anymodel, "model_binary", "Any Model", "outperform"),
      ff4_tv_outperform_se = get_values(ff4_tv_t2_summary_anymodel, "model_binary", "Any Model", "outperform_se")
    )
  }
}

# DISCIPLINE AND JOURNAL RANKING TABLE (t > t_threshold) ---------------------------------
cat("\n\n=== SUMMARY TABLE BY DISCIPLINE AND JOURNAL RANKING (t > ", t_threshold, ") ===\n")

# Filter data to exclude Economics discipline
discipline_mapping_filtered <- discipline_mapping %>% filter(discipline %in% c("Finance", "Accounting"))

# Create data with discipline column (t-stat only)
discipline_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
  left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
  filter(rbar_t > t_threshold) %>%
  inner_join(discipline_mapping_filtered, by = c("pubname" = "signalname"))

# Raw returns with t > t_threshold filter (by discipline) - excluding Economics
raw_t2_summary_discipline <- compute_outperformance(
  discipline_data %>% select(-discipline),
  "ret", "matchRet", discipline_mapping_filtered, "discipline"
)





# Filter data to exclude Economics journals
journal_mapping_filtered <- journal_mapping %>% filter(journal_rank != "Economics")

# Raw returns filter (by journal) - excluding Economics (t-stat only)
journal_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
  left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
  filter(rbar_t > t_threshold) %>%
  inner_join(journal_mapping_filtered, by = c("pubname" = "signalname"))

raw_t2_summary_journal <- compute_outperformance(
  journal_data %>% select(-journal_rank),
  "ret", "matchRet", journal_mapping_filtered, "journal_rank"
)





# Print discipline and journal table
cat("\nPost-Sample Return (t>", t_threshold, ")     Outperformance vs Data-Mining (t>", t_threshold, ")\n")
cat("                Raw\n")
cat("Discipline\n")

groups_discipline <- c("Finance", "Accounting")
for(group in groups_discipline) {
  # Post-sample returns
  raw_ret <- round(get_values(raw_t2_summary_discipline, "discipline", group, "pub_oos"))

  # Outperformance
  raw_out <- round(get_values(raw_t2_summary_discipline, "discipline", group, "outperform"))

  # Standard errors
  raw_se <- round(get_values(raw_t2_summary_discipline, "discipline", group, "pub_oos_se"))

  raw_out_se <- round(get_values(raw_t2_summary_discipline, "discipline", group, "outperform_se"))

  cat(sprintf("%-12s %4s   %4s\n",
              group, raw_ret, raw_out))
  cat(sprintf("%-12s (%2s)   (%2s)\n",
              "", raw_se, raw_out_se))
}

cat("Journal Ranking\n")
groups_journal <- c("JF, JFE, RFS", "AR, JAR, JAE", "Other")
for(group in groups_journal) {
  # Post-sample returns
  raw_ret <- round(get_values(raw_t2_summary_journal, "journal_rank", group, "pub_oos"))

  # Outperformance
  raw_out <- round(get_values(raw_t2_summary_journal, "journal_rank", group, "outperform"))

  # Standard errors
  raw_se <- round(get_values(raw_t2_summary_journal, "journal_rank", group, "pub_oos_se"))

  raw_out_se <- round(get_values(raw_t2_summary_journal, "journal_rank", group, "outperform_se"))

  cat(sprintf("%-12s %4s   %4s\n",
              group, raw_ret, raw_out))
  cat(sprintf("%-12s (%2s)   (%2s)\n",
              "", raw_se, raw_out_se))
}

# Assemble unrounded full-sample summaries for audit and paper renderers.
bundle_group_summaries <- function(raw, capm, ff3, group_col, group) {
  list(
    raw_pub_oos = get_values(raw, group_col, group, "pub_oos"),
    raw_pub_oos_se = get_values(raw, group_col, group, "pub_oos_se"),
    raw_outperform = get_values(raw, group_col, group, "outperform"),
    raw_outperform_se = get_values(raw, group_col, group, "outperform_se"),
    capm_fs_pub_oos = get_values(capm, group_col, group, "pub_oos"),
    capm_fs_pub_oos_se = get_values(capm, group_col, group, "pub_oos_se"),
    capm_fs_outperform = get_values(capm, group_col, group, "outperform"),
    capm_fs_outperform_se = get_values(capm, group_col, group, "outperform_se"),
    ff3_fs_pub_oos = get_values(ff3, group_col, group, "pub_oos"),
    ff3_fs_pub_oos_se = get_values(ff3, group_col, group, "pub_oos_se"),
    ff3_fs_outperform = get_values(ff3, group_col, group, "outperform"),
    ff3_fs_outperform_se = get_values(ff3, group_col, group, "outperform_se")
  )
}

fs_theory_data <- setNames(lapply(groups_theory, function(group) {
  bundle_group_summaries(
    raw_t2_summary_theory, capm_fs_t2_summary_theory,
    ff3_fs_t2_summary_theory, "theory_group", group
  )
}), groups_theory)
fs_model_data <- setNames(lapply(groups_model, function(group) {
  bundle_group_summaries(
    raw_t2_summary_model, capm_fs_t2_summary_model,
    ff3_fs_t2_summary_model, "modeltype_grouped", group
  )
}), groups_model)
fs_discipline_data <- setNames(lapply(groups_discipline, function(group) {
  bundle_group_summaries(
    raw_t2_summary_discipline, capm_fs_t2_summary_discipline,
    ff3_fs_t2_summary_discipline, "discipline", group
  )
}), groups_discipline)
fs_journal_data <- setNames(lapply(groups_journal, function(group) {
  bundle_group_summaries(
    raw_t2_summary_journal, capm_fs_t2_summary_journal,
    ff3_fs_t2_summary_journal, "journal_rank", group
  )
}), groups_journal)
fs_anymodel_data <- setNames(lapply(anymodel_groups, function(group) {
  bundle_group_summaries(
    raw_t2_summary_anymodel, capm_fs_t2_summary_anymodel,
    ff3_fs_t2_summary_anymodel, "model_binary", group
  )
}), anymodel_groups)
fs_overall_data <- list(
  raw_pub_oos = overall_t2_summary_raw$pub_oos,
  raw_pub_oos_se = overall_t2_summary_raw$pub_oos_se,
  raw_outperform = overall_t2_summary_raw$outperform,
  raw_outperform_se = overall_t2_summary_raw$outperform_se,
  capm_fs_pub_oos = overall_t2_summary_capm_fs$pub_oos,
  capm_fs_pub_oos_se = overall_t2_summary_capm_fs$pub_oos_se,
  capm_fs_outperform = overall_t2_summary_capm_fs$outperform,
  capm_fs_outperform_se = overall_t2_summary_capm_fs$outperform_se,
  ff3_fs_pub_oos = overall_t2_summary_ff3_fs$pub_oos,
  ff3_fs_pub_oos_se = overall_t2_summary_ff3_fs$pub_oos_se,
  ff3_fs_outperform = overall_t2_summary_ff3_fs$outperform,
  ff3_fs_outperform_se = overall_t2_summary_ff3_fs$outperform_se
)

fs_theory_model_table <- build_full_sample_summary_table(
  c(rep("Theoretical Explanation", 3), rep("Modeling Formalism", 3), "Overall"),
  c(groups_theory, groups_model, "All"),
  c(fs_theory_data, fs_model_data, list(fs_overall_data))
)
fs_discipline_journal_table <- build_full_sample_summary_table(
  c(rep("Discipline", 2), rep("Journal Rank", 3)),
  c(groups_discipline, groups_journal),
  c(fs_discipline_data, fs_journal_data)
)
fs_anymodel_table <- build_full_sample_summary_table(
  c("", ""), anymodel_groups, fs_anymodel_data
)

# Build the three retained time-varying tables in memory, then render below.
if (exists("tv_theory_data") && exists("tv_model_data")) {
  # Combine theory and model data
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

  # Export time-varying table (remove FF3 columns for FF4 analysis)
  export_table_tv_ff4 <- export_table_tv %>% select(-matches("^FF3_"))
  # Export time-varying discipline/journal table if data exists
  if (exists("tv_discipline_data") && exists("tv_journal_data")) {
    # Combine discipline and journal data
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

    # Export time-varying discipline/journal table (remove FF3 columns for FF4 analysis)
    export_table_tv_dj_ff4 <- export_table_tv_dj %>% select(-matches("^FF3_"))
  }

  # Export time-varying Any Model vs No Model table if data exists
  if (exists("tv_anymodel_data")) {
    tv_am_categories <- c("", "")
    tv_am_groups <- c("No Model", "Any Model")
    tv_am_all_data <- list(tv_anymodel_data, tv_anymodel_any)

    export_table_tv_am <- build_tv_summary_table(
      categories = tv_am_categories,
      groups = tv_am_groups,
      summaries = tv_am_all_data,
      digits = 0
    )

    # Export time-varying Any Model vs No Model table (remove FF3 columns for FF4 analysis)
    export_table_tv_am_ff4 <- export_table_tv_am %>% select(-matches("^FF3_"))
  }
}

# Render the retained output contract ----------------------------------
file_suffix <- paste0("_ff4_t", t_threshold)
headers <- list(
  list(title = "Raw", span = 2),
  list(title = "CAPM", span = 2),
  list(title = "FF4", span = 2)
)

export_audit_tabular(
  export_table_tv_ff4,
  file.path(results_dir, paste0("Table_RiskAdjusted_TimeVarying", file_suffix)),
  headers
)

# Paper-facing fragments preserve the manuscript's headings and row order.
# Name the unrounded summaries here so the renderer never parses audit files.
names(tv_theory_data) <- groups_theory
names(tv_model_data) <- groups_model
names(tv_discipline_data) <- c("Finance", "Accounting")
names(tv_journal_data) <- c("JF, JFE, RFS", "AR, JAR, JAE", "Other")
tv_anymodel_named <- list(
  "No Model" = tv_anymodel_data,
  "Any Model" = tv_anymodel_any
)

write_paper_theory_model_tabular(
  tv_theory_data,
  tv_model_data,
  tv_overall_data,
  file.path("../Results", paste0("Table_RiskAdjusted_TimeVarying", file_suffix, ".tex"))
)
write_paper_discipline_journal_tabular(
  tv_discipline_data,
  tv_journal_data,
  file.path(
    "../Results",
    paste0("Table_RiskAdjusted_TimeVarying_DisciplineJournal", file_suffix, ".tex")
  )
)
write_paper_anymodel_tabular(
  tv_anymodel_named,
  file.path(
    "../Results",
    paste0("Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel", file_suffix, ".tex")
  )
)
export_audit_tabular(
  export_table_tv_dj_ff4,
  file.path(
    results_dir,
    paste0("Table_RiskAdjusted_TimeVarying_DisciplineJournal", file_suffix)
  ),
  headers
)
export_audit_tabular(
  export_table_tv_am_ff4,
  file.path(
    results_dir,
    paste0("Table_RiskAdjusted_TimeVarying_AnyModelVsNoModel", file_suffix)
  ),
  headers
)

full_sample_suffix <- paste0("_t", t_threshold)
full_sample_headers <- list(
  list(title = "Raw", span = 2),
  list(title = "CAPM", span = 2),
  list(title = "FF3", span = 2)
)
export_audit_tabular(
  fs_theory_model_table,
  file.path(
    full_sample_results_dir,
    paste0("Table_RiskAdjusted_FullSample", full_sample_suffix)
  ),
  full_sample_headers
)
export_audit_tabular(
  fs_discipline_journal_table,
  file.path(
    full_sample_results_dir,
    paste0("Table_RiskAdjusted_FullSample_DisciplineJournal", full_sample_suffix)
  ),
  full_sample_headers
)
export_audit_tabular(
  fs_anymodel_table,
  file.path(
    full_sample_results_dir,
    paste0("Table_RiskAdjusted_FullSample_AnyModelVsNoModel", full_sample_suffix)
  ),
  full_sample_headers
)

write_paper_fullsample_theory_model_tabular(
  fs_theory_data,
  fs_model_data,
  fs_overall_data,
  "../Results/Table_RiskAdjusted_FullSample_Appendix.tex"
)
write_paper_fullsample_discipline_journal_tabular(
  fs_discipline_data,
  fs_journal_data,
  "../Results/Table_RiskAdjusted_FullSample_DisciplineJournal_Appendix.tex"
)
write_paper_fullsample_anymodel_tabular(
  fs_anymodel_data,
  "../Results/Table_RiskAdjusted_FullSample_AnyModelVsNoModel_Appendix.tex"
)

message("Wrote source artifacts for Tables 6, 7, IA.6, IA.7, IA.10, and IA.11.")
