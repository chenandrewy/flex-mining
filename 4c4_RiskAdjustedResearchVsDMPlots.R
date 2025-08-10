# Risk-Adjusted Data-mining comparisons 
# Based on 4c2_ResearchVsDMPlots.R but with CAPM and FF3 adjustments
# This file compares raw vs risk-adjusted returns for published vs data-mined signals

# Setup ----------------------------------------------------------------
rm(list = ls())
source("0_Environment.R")

t_threshold_a = 1
t_threshold_b = 2
# For raw returns: threshold is in basis points (e.g., 15 = 0.15% per month)
# For CAPM/FF3 alphas: values are already normalized to ~100, so use 15 for consistency
return_threshold = 0.3  # basis points for raw returns/alphas. Returns and FF5 factors are in percentages so 0.3 = 30 bps per month

# Filter type: "tstat" for t-stat filtering, "return" for return filtering
# Check for environment variable first, otherwise use default
filter_type <- Sys.getenv("FILTER_TYPE", "return")  # Default to return filtering

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
extract_beta <- function(ret, mktrf) {
  model <- lm(ret ~ mktrf)
  bet <- coef(model)[2]
  return(bet)
}

extract_ff3_coeffs <- function(ret, mktrf, smb, hml) {
  model <- lm(ret ~ mktrf + smb + hml)
  coeffs <- coef(model)
  return(coeffs[2:4])  # Return beta, s, and h coefficients
}

# Helper function to normalize and aggregate DM returns with t-stat filtering
normalize_and_aggregate_dm <- function(dm_data, abnormal_col, suffix_name) {
  # Normalize each DM return series individually
  dm_normalized <- dm_data %>%
    group_by(actSignal, candSignalname) %>%
    mutate(
      insample_mean = mean(.data[[abnormal_col]][samptype == "insamp"], na.rm = TRUE),
      normalized_ind = ifelse(abs(insample_mean) > 1e-10, 
                             100 * .data[[abnormal_col]] / insample_mean, 
                             NA)
    ) %>%
    ungroup()
  
  # Aggregate across DM signals for each (actSignal, eventDate)
  dm_aggregated <- dm_normalized %>%
    group_by(actSignal, eventDate) %>%
    summarise(
      !!sym(paste0("matchRet_", suffix_name)) := mean(normalized_ind, na.rm = TRUE),
      !!sym(paste0("n_matches_", suffix_name)) := n_distinct(candSignalname),
      .groups = 'drop'
    )
  
  return(dm_aggregated)
}

# Helper function to create filtered plot data
create_filtered_plot_data <- function(ret_for_plot0_adj, signals_list, dm_aggregated, 
                                     pub_col, dm_col, suffix_name) {
  # Join filtered DM to plotting data
  plot_data <- ret_for_plot0_adj %>%
    filter(pubname %in% signals_list) %>%
    left_join(
      dm_aggregated,
      by = c("pubname" = "actSignal", "eventDate" = "eventDate")
    ) %>%
    filter(!is.na(!!sym(dm_col)))
  
  return(plot_data)
}

# Helper function to create and save risk-adjusted plots
create_risk_adjusted_plot <- function(plot_data, pub_col, dm_col, 
                                     adjustment_type, t_threshold, 
                                     y_axis_label, y_high = 125,
                                     filter_type = "tstat", return_threshold = 0.15) {
  
  if(nrow(plot_data) > 0) {
    # Print summary statistics
    plot_data %>% 
      summarise(
        pub_mean_insamp = mean(.data[[pub_col]][eventDate <= 0], na.rm = TRUE),
        pub_mean_oos = mean(.data[[pub_col]][eventDate > 0], na.rm = TRUE),
        dm_mean_insamp = mean(.data[[dm_col]][eventDate <= 0], na.rm = TRUE),
        dm_mean_oos = mean(.data[[dm_col]][eventDate > 0], na.rm = TRUE)
      ) %>% print()
    
    # Create suffix for file naming
    if (filter_type == "return") {
      suffix <- paste0(tolower(adjustment_type), "_r", gsub("\\.", "", format(return_threshold, nsmall = 0)))
    } else {
      suffix <- paste0(tolower(adjustment_type), "_t", t_threshold)
    }
    
    # Create plot
    plot_obj <- ReturnPlotsWithDM_std_errors_indicators(
      dt = plot_data %>% 
        transmute(eventDate, pubname, theory, 
                 ret = !!sym(pub_col), 
                 matchRet = !!sym(dm_col)) %>%
        left_join(czret %>% select(signalname, eventDate, date) %>% distinct(), 
                 by = c("pubname" = "signalname", "eventDate" = "eventDate")) %>%
        rename(calendarDate = date),
      basepath = "../Results/temp_",
      suffix = suffix,
      rollmonths = 60,
      colors = colors,
      labelmatch = FALSE,
      yl = 0,
      yh = y_high,
      xl = global_xl,
      xh = global_xh,
      legendlabels = c(
        paste0("Published (", adjustment_type, ", ",
               ifelse(filter_type == "return", 
                      paste0("avg>=", return_threshold), 
                      paste0("t>=", t_threshold)), ")"),
        paste0("Data-Mined (", adjustment_type, ", ",
               ifelse(filter_type == "return", 
                      paste0("avg>=", return_threshold), 
                      paste0("t>=", t_threshold)), ")"),
        'N/A'
      ),
      legendpos = c(35,20)/100,
      fontsize = fontsizeall,
      yaxislab = y_axis_label,
      linesize = linesizeall
    )
    
    # Save plot
    ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_", suffix, ".pdf"), 
           plot_obj, width = 10, height = 8)
    
    # Remove temp file
    temp_file <- paste0("../Results/temp_", suffix, ".pdf")
    if(file.exists(temp_file)) file.remove(temp_file)
    
    return(plot_obj)
  } else {
    cat("No data available for plotting.\n")
    return(NULL)
  }
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

# Compute normalized DM abnormal returns ---------------------------------

# First, create the risk-adjusted data for plotting by joining with `czret`
ret_for_plot0_adj <- ret_for_plot0 %>%
  left_join(
    czret %>% select(signalname, eventDate, abnormal_capm_normalized, abnormal_ff3_normalized),
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
tempsuffix = "raw_returns"

printme_raw = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot0 %>% filter(!is.na(matchRet)) %>% 
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


# Filtered versions (t-stat or return) -----------------------------------------------

if (filter_type == "return") {
  cat("\n\n=== RETURN FILTERED ANALYSIS (avg return/alpha >=", return_threshold, ") ===\n")
} else {
  cat("\n\n=== T-STAT FILTERED ANALYSIS (t >=", t_threshold_b, ") ===\n")
}

# First compute statistics for individual DM signals
cat("\nComputing statistics for individual DM signals...\n")
setDT(candidateReturns_adj)

# Add samptype if not present
if(!"samptype" %in% names(candidateReturns_adj)) {
  candidateReturns_adj <- candidateReturns_adj %>%
    left_join(
      czret %>% select(signalname, eventDate, samptype) %>% distinct(),
      by = c("actSignal" = "signalname", "eventDate" = "eventDate")
    )
}

# Compute statistics for each DM signal with proper NA handling
dm_stats <- candidateReturns_adj[
  samptype == "insamp" & !is.na(abnormal_capm),
  .(
    # T-stats with proper NA handling and zero SD protection
    abar_capm_dm_t = {
      m <- mean(abnormal_capm, na.rm = TRUE)
      s <- sd(abnormal_capm, na.rm = TRUE)
      n <- sum(!is.na(abnormal_capm))
      if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
    },
    abar_ff3_dm_t = {
      m <- mean(abnormal_ff3, na.rm = TRUE)
      s <- sd(abnormal_ff3, na.rm = TRUE)
      n <- sum(!is.na(abnormal_ff3))
      if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
    },
    # Average returns/alphas
    abar_capm_dm = mean(abnormal_capm, na.rm = TRUE),
    abar_ff3_dm = mean(abnormal_ff3, na.rm = TRUE)
  ),
  by = .(actSignal, candSignalname)
]

# Filter DM signals based on chosen filter type
if (filter_type == "return") {
  dm_filtered_capm <- candidateReturns_adj %>%
    inner_join(
      dm_stats %>% filter(abar_capm_dm >= return_threshold),
      by = c("actSignal", "candSignalname")
    )
  
  dm_filtered_ff3 <- candidateReturns_adj %>%
    inner_join(
      dm_stats %>% filter(abar_ff3_dm >= return_threshold),
      by = c("actSignal", "candSignalname")
    )
  
  # Get published signals that meet return threshold
  # Note: abar_capm, abar_ff3 are already in percentage terms (multiplied by 100)
  # For raw returns, use rbar_avg which is in basis points (return_threshold is in basis points)
  signals_raw_t2 <- unique(czret[rbar_avg >= return_threshold]$signalname)
  signals_capm_t2 <- unique(czret[abar_capm >= return_threshold]$signalname)
  signals_ff3_t2 <- unique(czret[abar_ff3 >= return_threshold]$signalname)
  
} else {
  dm_filtered_capm <- candidateReturns_adj %>%
    inner_join(
      dm_stats %>% filter(abar_capm_dm_t >= t_threshold_b),
      by = c("actSignal", "candSignalname")
    )
  
  dm_filtered_ff3 <- candidateReturns_adj %>%
    inner_join(
      dm_stats %>% filter(abar_ff3_dm_t >= t_threshold_b),
      by = c("actSignal", "candSignalname")
    )
  
  # Get signals with t >= t_threshold_b for different measures
  signals_raw_t2 <- unique(czret[rbar_t >= t_threshold_b]$signalname)
  signals_capm_t2 <- unique(czret[abar_capm_t >= t_threshold_b]$signalname)
  signals_ff3_t2 <- unique(czret[abar_ff3_t >= t_threshold_b]$signalname)
}

if (filter_type == "return") {
  cat("\nNumber of PUBLISHED signals with avg return/alpha >=", return_threshold, ":\n")
  cat("Raw returns:", length(signals_raw_t2), "\n")
  cat("CAPM alpha:", length(signals_capm_t2), "\n")
  cat("FF3 alpha:", length(signals_ff3_t2), "\n")
  
  cat("\nNumber of DM signals with avg return/alpha >=", return_threshold, ":\n")
  cat("CAPM alpha:", sum(dm_stats$abar_capm_dm >= return_threshold, na.rm = TRUE), "\n")
  cat("FF3 alpha:", sum(dm_stats$abar_ff3_dm >= return_threshold, na.rm = TRUE), "\n")
} else {
  cat("\nNumber of PUBLISHED signals with t >=", t_threshold_b, ":\n")
  cat("Raw returns:", length(signals_raw_t2), "\n")
  cat("CAPM alpha:", length(signals_capm_t2), "\n")
  cat("FF3 alpha:", length(signals_ff3_t2), "\n")
  
  cat("\nNumber of DM signals with t >=", t_threshold_b, ":\n")
  cat("CAPM alpha:", sum(dm_stats$abar_capm_dm_t >= t_threshold_b, na.rm = TRUE), "\n")
  cat("FF3 alpha:", sum(dm_stats$abar_ff3_dm_t >= t_threshold_b, na.rm = TRUE), "\n")
}

# Create filtered plots for CAPM
if (filter_type == "return") {
  cat("\n=== CAPM FILTERED (avg alpha >=", return_threshold, ") STATISTICS ===\n")
} else {
  cat("\n=== CAPM FILTERED (t >=", t_threshold_b, ") STATISTICS ===\n")
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

cat("Published signals with CAPM t >=", t_threshold_b, ":", length(signals_capm_t2), "\n")
cat("Published signals with filtered DM matches:", length(unique(ret_for_plot0_capm_t2$pubname)), "\n")

# Create and save plot
printme_capm_t2 <- create_risk_adjusted_plot(
  ret_for_plot0_capm_t2,
  "abnormal_capm_normalized",
  "matchRet_capm_t2_normalized",
  "CAPM Alpha",
  t_threshold_b,
  "Trailing 5-Year CAPM Alpha",
  filter_type = filter_type,
  return_threshold = return_threshold
)

# Create filtered plots for FF3
if (filter_type == "return") {
  cat("\n=== FF3 FILTERED (avg alpha >=", return_threshold, ") STATISTICS ===\n")
} else {
  cat("\n=== FF3 FILTERED (t >=", t_threshold_b, ") STATISTICS ===\n")
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

cat("Published signals with FF3 t >=", t_threshold_b, ":", length(signals_ff3_t2), "\n")
cat("Published signals with filtered DM matches:", length(unique(ret_for_plot0_ff3_t2$pubname)), "\n")

# Create and save plot
printme_ff3_t2 <- create_risk_adjusted_plot(
  ret_for_plot0_ff3_t2,
  "abnormal_ff3_normalized",
  "matchRet_ff3_t2_normalized",
  "FF3 Alpha",
  t_threshold_b,
  "Trailing 5-Year FF3 Alpha",
  filter_type = filter_type,
  return_threshold = return_threshold
)

# Time-varying abnormal returns (IS beta in IS, post-sample beta in OOS) --------

# Load model categories EARLY (needed for time-varying and other analyses)
# Moved here from line 875 to be available for all analyses
czcat_full <- fread("DataInput/SignalsTheoryChecked.csv") %>%
  select(signalname, Year, theory, Journal, NoModel, Stylized, Dynamic, Quantitative) %>%
  filter(signalname %in% inclSignals) %>%
  mutate(
    modeltype = case_when(
      NoModel == 1 ~ "No Model",
      Stylized == 1 ~ "Stylized", 
      Dynamic == 1 ~ "Dynamic",
      Quantitative == 1 ~ "Quantitative"
    ),
    # Combine Dynamic and Quantitative as in your target format
    modeltype_grouped = case_when(
      modeltype == "No Model" ~ "No Model",
      modeltype == "Stylized" ~ "Stylized",
      modeltype %in% c("Dynamic", "Quantitative") ~ "Dynamic or Quantitative"
    ),
    theory_group = case_when(
      theory %in% c("risk", "Risk") ~ "Risk",
      theory %in% c("mispricing", "Mispricing") ~ "Mispricing", 
      TRUE ~ "Agnostic"
    ),
    # Add discipline (Finance vs Accounting vs Economics)
    discipline = case_when(
      Journal %in% c("JAR", "JAE", "AR") ~ "Accounting",
      Journal %in% c("QJE", "JPE") ~ "Economics",
      TRUE ~ "Finance"
    ),
    # Add journal ranking from 3g (excluding Economics journals)
    journal_rank = case_when(
      Journal %in% c("JF", "JFE", "RFS") ~ "JF, JFE, RFS",
      Journal %in% c("JAR", "JAE", "AR") ~ "AR, JAR, JAE", 
      Journal %in% c("QJE", "JPE") ~ "Economics", # Mark but will exclude
      TRUE ~ "Other"
    )
  )

# Get mappings
theory_mapping <- czcat_full %>% select(signalname, theory_group) %>% distinct()
model_mapping <- czcat_full %>% select(signalname, modeltype_grouped) %>% distinct()
discipline_mapping <- czcat_full %>% select(signalname, discipline) %>% distinct()
journal_mapping <- czcat_full %>% select(signalname, journal_rank) %>% distinct()

# Define filtered mappings early so they are available to time-varying section
discipline_mapping_filtered <- discipline_mapping %>% filter(discipline %in% c("Finance", "Accounting"))
journal_mapping_filtered <- journal_mapping %>% filter(journal_rank != "Economics")

# Define helper functions here (needed for analyses below)
# ----------------------------------------------

# Function to compute outperformance metrics with standard errors
compute_outperformance <- function(plot_data, ret_col, dm_ret_col, group_map, group_col = "theory_group") {
  plot_data %>%
    left_join(group_map, by = c("pubname" = "signalname")) %>%
    filter(!is.na(.data[[group_col]])) %>%
    group_by(.data[[group_col]]) %>%
    summarise(
      n_signals = n_distinct(pubname),
      pub_oos = mean(.data[[ret_col]][eventDate > 0], na.rm = TRUE),
      pub_oos_se = {
        n <- sum(eventDate > 0 & !is.na(.data[[ret_col]]))
        if (n > 1) sd(.data[[ret_col]][eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
      },
      dm_oos = mean(.data[[dm_ret_col]][eventDate > 0], na.rm = TRUE),
      dm_oos_se = {
        n <- sum(eventDate > 0 & !is.na(.data[[dm_ret_col]]))
        if (n > 1) sd(.data[[dm_ret_col]][eventDate > 0], na.rm = TRUE) / sqrt(n) else NA_real_
      },
      outperform = pub_oos - dm_oos,
      outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
      .groups = 'drop'
    )
}

# Function to create comprehensive summary tables for any analysis type
create_summary_tables <- function(plot_data_list, group_mappings, table_name = "Analysis",
                                  filter_desc = "", overall_summaries = NULL) {
  
  # plot_data_list should be a named list: list(raw = data1, capm = data2, ff3 = data3)
  # group_mappings should be a named list: list(theory = mapping1, model = mapping2, ...)
  # overall_summaries is optional: list(raw = summary1, capm = summary2, ff3 = summary3)
  
  results <- list()
  
  for (group_type in names(group_mappings)) {
    group_map <- group_mappings[[group_type]]
    group_col <- names(group_map)[2]  # Get the group column name
    
    results[[group_type]] <- list()
    
    for (analysis_type in names(plot_data_list)) {
      plot_data <- plot_data_list[[analysis_type]]
      
      # Determine column names based on analysis type
      ret_col <- switch(analysis_type,
                       "raw" = "ret",
                       "capm" = "abnormal_capm_normalized",
                       "ff3" = "abnormal_ff3_normalized",
                       "capm_tv" = "abnormal_capm_normalized",
                       "ff3_tv" = "abnormal_ff3_normalized",
                       "ret")  # default
      
      dm_col <- switch(analysis_type,
                      "raw" = "matchRet",
                      "capm" = "matchRet_capm_t2_normalized",
                      "ff3" = "matchRet_ff3_t2_normalized",
                      "capm_tv" = "matchRet_capm_tv_t2_normalized",
                      "ff3_tv" = "matchRet_ff3_tv_t2_normalized",
                      "matchRet")  # default
      
      # Compute outperformance for this group
      if (!is.null(plot_data) && nrow(plot_data) > 0) {
        results[[group_type]][[analysis_type]] <- compute_outperformance(
          plot_data, ret_col, dm_col, group_map, group_col
        )
      }
    }
  }
  
  # Add overall summaries if provided
  if (!is.null(overall_summaries)) {
    results[["overall"]] <- overall_summaries
  }
  
  return(results)
}

# Function to print formatted summary table
print_summary_table <- function(summaries, groups, group_col, table_title, 
                               analysis_types = c("raw", "capm", "ff3"),
                               analysis_labels = c("Raw", "CAPM", "FF3")) {
  
  cat("\n", table_title, "\n", sep="")
  cat(strrep("-", nchar(table_title)), "\n")
  
  # Header
  cat(sprintf("%-25s", ""))
  for (label in analysis_labels) {
    cat(sprintf("   %s          ", label))
  }
  cat("\n")
  
  cat(sprintf("%-25s", "Group"))
  for (i in 1:length(analysis_types)) {
    cat("  Post-Samp  Outperf")
  }
  cat("\n")
  
  # Helper to safely get values
  get_value <- function(summary, group_col, group, metric) {
    if (is.null(summary) || nrow(summary) == 0) return(NA)
    val <- summary[[metric]][summary[[group_col]] == group]
    if (length(val) == 0) return(NA)
    return(val[1])
  }
  
  # Print each group
  for (group in groups) {
    # Values row
    cat(sprintf("%-25s", group))
    for (analysis in analysis_types) {
      summary <- summaries[[analysis]]
      pub_oos <- get_value(summary, group_col, group, "pub_oos")
      outperform <- get_value(summary, group_col, group, "outperform")
      cat(sprintf("  %8.2f  %8.2f", 
                 ifelse(is.na(pub_oos), NA, pub_oos),
                 ifelse(is.na(outperform), NA, outperform)))
    }
    cat("\n")
    
    # Standard errors row
    cat(sprintf("%-25s", ""))
    for (analysis in analysis_types) {
      summary <- summaries[[analysis]]
      pub_oos_se <- get_value(summary, group_col, group, "pub_oos_se")
      outperform_se <- get_value(summary, group_col, group, "outperform_se")
      cat(sprintf("  (%6.2f)  (%6.2f)", 
                 ifelse(is.na(pub_oos_se), NA, pub_oos_se),
                 ifelse(is.na(outperform_se), NA, outperform_se)))
    }
    cat("\n")
  }
}

# Function to export tables to CSV
export_summary_tables <- function(summaries, filename, filter_desc = "") {
  
  # Create a comprehensive list for export
  all_results <- list()
  
  for (category in names(summaries)) {
    if (category == "overall") next  # Handle overall separately
    
    for (analysis_type in names(summaries[[category]])) {
      summary <- summaries[[category]][[analysis_type]]
      if (!is.null(summary) && nrow(summary) > 0) {
        summary$category <- category
        summary$analysis_type <- analysis_type
        
        # Add filter description as metadata
        if (filter_desc != "") {
          summary$filter <- filter_desc
        }
        
        # Use bind_rows instead of rbind to handle different column sets
        if (length(all_results) == 0) {
          all_results <- summary
        } else {
          all_results <- bind_rows(all_results, summary)
        }
      }
    }
  }
  
  # Save to CSV
  if (length(all_results) > 0) {
    write.csv(all_results, filename, row.names = FALSE)
    cat("\nSummary tables exported to:", filename, "\n")
  } else {
    cat("\nNo summary data to export.\n")
  }
}

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
  
  # CAPM time-varying filtering
  if (filter_type == "return") {
    cat("\n=== CAPM TIME-VARYING (avg alpha >=", return_threshold, ") STATISTICS ===\n")
    dm_filtered_capm_tv <- candidateReturns_adj %>%
      inner_join(
        dm_stats_tv %>% filter(abar_capm_tv_dm >= return_threshold),
        by = c("actSignal", "candSignalname")
      )
  } else {
    cat("\n=== CAPM TIME-VARYING (t >=", t_threshold_b, ") STATISTICS ===\n")
    dm_filtered_capm_tv <- candidateReturns_adj %>%
      inner_join(
        dm_stats_tv %>% filter(abar_capm_tv_dm_t >= t_threshold_b),
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
    signals_capm_t2,  # Use same published signals as before
    dm_capm_tv_aggregated,
    "abnormal_capm_normalized",  # Published signals still use full-sample normalization
    "matchRet_capm_tv_t2_normalized",
    "capm_tv_t2_normalized"
  )
  
  cat("Published signals with CAPM", ifelse(filter_type == "return", paste("avg alpha >=", return_threshold), paste("t >=", t_threshold_b)), ":", length(signals_capm_t2), "\n")
  cat("Published signals with filtered DM matches (time-varying):", length(unique(ret_for_plot0_capm_tv_t2$pubname)), "\n")
  if (filter_type == "return") {
    cat("DM signals with time-varying CAPM avg alpha >=", return_threshold, ":", sum(dm_stats_tv$abar_capm_tv_dm >= return_threshold, na.rm = TRUE), "\n")
  } else {
    cat("DM signals with time-varying CAPM t >=", t_threshold_b, ":", sum(dm_stats_tv$abar_capm_tv_dm_t >= t_threshold_b, na.rm = TRUE), "\n")
  }
  
  # Create and save plot
  printme_capm_tv_t2 <- create_risk_adjusted_plot(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_normalized",
    "matchRet_capm_tv_t2_normalized",
    "CAPM TV Alpha",
    t_threshold_b,
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
    cat("\n=== FF3 TIME-VARYING (t >=", t_threshold_b, ") STATISTICS ===\n")
    dm_filtered_ff3_tv <- candidateReturns_adj %>%
      inner_join(
        dm_stats_tv %>% filter(abar_ff3_tv_dm_t >= t_threshold_b),
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
    signals_ff3_t2,  # Use same published signals as before
    dm_ff3_tv_aggregated,
    "abnormal_ff3_normalized",  # Published signals still use full-sample normalization
    "matchRet_ff3_tv_t2_normalized",
    "ff3_tv_t2_normalized"
  )
  
  cat("Published signals with FF3", ifelse(filter_type == "return", paste("avg alpha >=", return_threshold), paste("t >=", t_threshold_b)), ":", length(signals_ff3_t2), "\n")
  cat("Published signals with filtered DM matches (time-varying):", length(unique(ret_for_plot0_ff3_tv_t2$pubname)), "\n")
  if (filter_type == "return") {
    cat("DM signals with time-varying FF3 avg alpha >=", return_threshold, ":", sum(dm_stats_tv$abar_ff3_tv_dm >= return_threshold, na.rm = TRUE), "\n")
  } else {
    cat("DM signals with time-varying FF3 t >=", t_threshold_b, ":", sum(dm_stats_tv$abar_ff3_tv_dm_t >= t_threshold_b, na.rm = TRUE), "\n")
  }
  
  # Create and save plot
  printme_ff3_tv_t2 <- create_risk_adjusted_plot(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_normalized",
    "matchRet_ff3_tv_t2_normalized",
    "FF3 TV Alpha",
    t_threshold_b,
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
      filter(rbar_t >= t_threshold_b)
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
                        paste0("t >= ", t_threshold_b))
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
                                  paste0("t", t_threshold_b)), ".csv")
  export_summary_tables(tv_summaries, export_filename, 
                        filter_desc = ifelse(filter_type == "return",
                                           paste0("Return >= ", return_threshold),
                                           paste0("T-stat >= ", t_threshold_b)))
  
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
    filter(rbar_t >= t_threshold_b)
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
                      paste0("t >= ", t_threshold_b))
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

# T-STAT FILTERED SUMMARY TABLE (t >= t_threshold_b) ----------------------------------------
if (filter_type == "return") {
  cat("\n\n=== SUMMARY TABLE WITH AVG RETURN/ALPHA >=", return_threshold, " FILTER ===\n")
} else {
  cat("\n\n=== SUMMARY TABLE WITH T >=", t_threshold_b, " FILTER ===\n")
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
      filter(rbar_t >= t_threshold_b), 
    "ret", "matchRet", theory_mapping, "theory_group"
  )
}

# CAPM t >= t_threshold_b filtered (by theory)
capm_t2_summary_theory <- compute_outperformance(
  ret_for_plot0_capm_t2,
  "abnormal_capm_normalized", "matchRet_capm_t2_normalized", theory_mapping, "theory_group"
)

# FF3 t >= t_threshold_b filtered (by theory) 
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
      filter(rbar_t >= t_threshold_b), 
    "ret", "matchRet", model_mapping, "modeltype_grouped"
  )
}

# CAPM t >= t_threshold_b filtered (by model)
capm_t2_summary_model <- compute_outperformance(
  ret_for_plot0_capm_t2,
  "abnormal_capm_normalized", "matchRet_capm_t2_normalized", model_mapping, "modeltype_grouped"
)

# FF3 t >= t_threshold_b filtered (by model)
ff3_t2_summary_model <- compute_outperformance(
  ret_for_plot0_ff3_t2,
  "abnormal_ff3_normalized", "matchRet_ff3_t2_normalized", model_mapping, "modeltype_grouped"
)

# Overall filtered summaries
if (filter_type == "return") {
  filtered_signals_raw <- czret$signalname[czret$rbar_avg >= return_threshold]
} else {
  filtered_signals_raw <- czret$signalname[czret$rbar_t >= t_threshold_b]
}

overall_t2_summary_raw <- data.frame(
  group = "Overall",
  n_signals = length(unique(filtered_signals_raw)),
  pub_oos = mean(ret_for_plot0$ret[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                                   ret_for_plot0$pubname %in% filtered_signals_raw], na.rm = TRUE),
  pub_oos_se = sd(ret_for_plot0$ret[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                                    ret_for_plot0$pubname %in% filtered_signals_raw], na.rm = TRUE) / 
               sqrt(sum(ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                        ret_for_plot0$pubname %in% filtered_signals_raw)),
  dm_oos = mean(ret_for_plot0$matchRet[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                                       ret_for_plot0$pubname %in% filtered_signals_raw], na.rm = TRUE),
  dm_oos_se = sd(ret_for_plot0$matchRet[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                                        ret_for_plot0$pubname %in% filtered_signals_raw], na.rm = TRUE) / 
              sqrt(sum(ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                       ret_for_plot0$pubname %in% filtered_signals_raw)),
  outperform = NA,
  outperform_se = NA
)
overall_t2_summary_raw$outperform <- overall_t2_summary_raw$pub_oos - overall_t2_summary_raw$dm_oos
overall_t2_summary_raw$outperform_se <- sqrt(overall_t2_summary_raw$pub_oos_se^2 + overall_t2_summary_raw$dm_oos_se^2)

overall_t2_summary_capm <- data.frame(
  group = "Overall",
  n_signals = length(unique(ret_for_plot0_capm_t2$pubname)),
  pub_oos = mean(ret_for_plot0_capm_t2$abnormal_capm_normalized[ret_for_plot0_capm_t2$eventDate > 0], na.rm = TRUE),
  pub_oos_se = sd(ret_for_plot0_capm_t2$abnormal_capm_normalized[ret_for_plot0_capm_t2$eventDate > 0], na.rm = TRUE) / 
               sqrt(sum(ret_for_plot0_capm_t2$eventDate > 0 & !is.na(ret_for_plot0_capm_t2$abnormal_capm_normalized))),
  dm_oos = mean(ret_for_plot0_capm_t2$matchRet_capm_t2_normalized[ret_for_plot0_capm_t2$eventDate > 0], na.rm = TRUE),
  dm_oos_se = sd(ret_for_plot0_capm_t2$matchRet_capm_t2_normalized[ret_for_plot0_capm_t2$eventDate > 0], na.rm = TRUE) / 
              sqrt(sum(ret_for_plot0_capm_t2$eventDate > 0 & !is.na(ret_for_plot0_capm_t2$matchRet_capm_t2_normalized))),
  outperform = NA,
  outperform_se = NA
)
overall_t2_summary_capm$outperform <- overall_t2_summary_capm$pub_oos - overall_t2_summary_capm$dm_oos
overall_t2_summary_capm$outperform_se <- sqrt(overall_t2_summary_capm$pub_oos_se^2 + overall_t2_summary_capm$dm_oos_se^2)

overall_t2_summary_ff3 <- data.frame(
  group = "Overall",
  n_signals = length(unique(ret_for_plot0_ff3_t2$pubname)),
  pub_oos = mean(ret_for_plot0_ff3_t2$abnormal_ff3_normalized[ret_for_plot0_ff3_t2$eventDate > 0], na.rm = TRUE),
  pub_oos_se = sd(ret_for_plot0_ff3_t2$abnormal_ff3_normalized[ret_for_plot0_ff3_t2$eventDate > 0], na.rm = TRUE) / 
               sqrt(sum(ret_for_plot0_ff3_t2$eventDate > 0 & !is.na(ret_for_plot0_ff3_t2$abnormal_ff3_normalized))),
  dm_oos = mean(ret_for_plot0_ff3_t2$matchRet_ff3_t2_normalized[ret_for_plot0_ff3_t2$eventDate > 0], na.rm = TRUE),
  dm_oos_se = sd(ret_for_plot0_ff3_t2$matchRet_ff3_t2_normalized[ret_for_plot0_ff3_t2$eventDate > 0], na.rm = TRUE) / 
              sqrt(sum(ret_for_plot0_ff3_t2$eventDate > 0 & !is.na(ret_for_plot0_ff3_t2$matchRet_ff3_t2_normalized))),
  outperform = NA,
  outperform_se = NA
)
overall_t2_summary_ff3$outperform <- overall_t2_summary_ff3$pub_oos - overall_t2_summary_ff3$dm_oos
overall_t2_summary_ff3$outperform_se <- sqrt(overall_t2_summary_ff3$pub_oos_se^2 + overall_t2_summary_ff3$dm_oos_se^2)

# Time-varying summaries (if available)
if("abnormal_capm_tv" %in% names(candidateReturns_adj) && exists("ret_for_plot0_capm_tv_t2")) {
  # CAPM time-varying t >= t_threshold_b filtered (by theory)
  capm_tv_t2_summary_theory <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_normalized", "matchRet_capm_tv_t2_normalized", theory_mapping, "theory_group"
  )
  
  # FF3 time-varying t >= t_threshold_b filtered (by theory)
  ff3_tv_t2_summary_theory <- compute_outperformance(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_normalized", "matchRet_ff3_tv_t2_normalized", theory_mapping, "theory_group"
  )
  
  # CAPM time-varying t >= t_threshold_b filtered (by model)
  capm_tv_t2_summary_model <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_normalized", "matchRet_capm_tv_t2_normalized", model_mapping, "modeltype_grouped"
  )
  
  # FF3 time-varying t >= t_threshold_b filtered (by model)
  ff3_tv_t2_summary_model <- compute_outperformance(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_normalized", "matchRet_ff3_tv_t2_normalized", model_mapping, "modeltype_grouped"
  )
  
  # CAPM time-varying t >= t_threshold_b filtered (by discipline)
  capm_tv_t2_summary_discipline <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_normalized", "matchRet_capm_tv_t2_normalized", discipline_mapping_filtered, "discipline"
  )
  
  # FF3 time-varying t >= t_threshold_b filtered (by discipline)
  ff3_tv_t2_summary_discipline <- compute_outperformance(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_normalized", "matchRet_ff3_tv_t2_normalized", discipline_mapping_filtered, "discipline"
  )
  
  # CAPM time-varying t >= t_threshold_b filtered (by journal)
  capm_tv_t2_summary_journal <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_normalized", "matchRet_capm_tv_t2_normalized", journal_mapping_filtered, "journal_rank"
  )
  
  # FF3 time-varying t >= t_threshold_b filtered (by journal)
  ff3_tv_t2_summary_journal <- compute_outperformance(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_normalized", "matchRet_ff3_tv_t2_normalized", journal_mapping_filtered, "journal_rank"
  )
  
  # Overall time-varying summaries
  overall_t2_summary_capm_tv <- data.frame(
    group = "Overall",
    n_signals = length(unique(ret_for_plot0_capm_tv_t2$pubname)),
    pub_oos = mean(ret_for_plot0_capm_tv_t2$abnormal_capm_normalized[ret_for_plot0_capm_tv_t2$eventDate > 0], na.rm = TRUE),
    pub_oos_se = sd(ret_for_plot0_capm_tv_t2$abnormal_capm_normalized[ret_for_plot0_capm_tv_t2$eventDate > 0], na.rm = TRUE) / 
                 sqrt(sum(ret_for_plot0_capm_tv_t2$eventDate > 0 & !is.na(ret_for_plot0_capm_tv_t2$abnormal_capm_normalized))),
    dm_oos = mean(ret_for_plot0_capm_tv_t2$matchRet_capm_tv_t2_normalized[ret_for_plot0_capm_tv_t2$eventDate > 0], na.rm = TRUE),
    dm_oos_se = sd(ret_for_plot0_capm_tv_t2$matchRet_capm_tv_t2_normalized[ret_for_plot0_capm_tv_t2$eventDate > 0], na.rm = TRUE) / 
                sqrt(sum(ret_for_plot0_capm_tv_t2$eventDate > 0 & !is.na(ret_for_plot0_capm_tv_t2$matchRet_capm_tv_t2_normalized))),
    outperform = NA,
    outperform_se = NA
  )
  overall_t2_summary_capm_tv$outperform <- overall_t2_summary_capm_tv$pub_oos - overall_t2_summary_capm_tv$dm_oos
  overall_t2_summary_capm_tv$outperform_se <- sqrt(overall_t2_summary_capm_tv$pub_oos_se^2 + overall_t2_summary_capm_tv$dm_oos_se^2)
  
  overall_t2_summary_ff3_tv <- data.frame(
    group = "Overall",
    n_signals = length(unique(ret_for_plot0_ff3_tv_t2$pubname)),
    pub_oos = mean(ret_for_plot0_ff3_tv_t2$abnormal_ff3_normalized[ret_for_plot0_ff3_tv_t2$eventDate > 0], na.rm = TRUE),
    pub_oos_se = sd(ret_for_plot0_ff3_tv_t2$abnormal_ff3_normalized[ret_for_plot0_ff3_tv_t2$eventDate > 0], na.rm = TRUE) / 
                 sqrt(sum(ret_for_plot0_ff3_tv_t2$eventDate > 0 & !is.na(ret_for_plot0_ff3_tv_t2$abnormal_ff3_normalized))),
    dm_oos = mean(ret_for_plot0_ff3_tv_t2$matchRet_ff3_tv_t2_normalized[ret_for_plot0_ff3_tv_t2$eventDate > 0], na.rm = TRUE),
    dm_oos_se = sd(ret_for_plot0_ff3_tv_t2$matchRet_ff3_tv_t2_normalized[ret_for_plot0_ff3_tv_t2$eventDate > 0], na.rm = TRUE) / 
                sqrt(sum(ret_for_plot0_ff3_tv_t2$eventDate > 0 & !is.na(ret_for_plot0_ff3_tv_t2$matchRet_ff3_tv_t2_normalized))),
    outperform = NA,
    outperform_se = NA
  )
  overall_t2_summary_ff3_tv$outperform <- overall_t2_summary_ff3_tv$pub_oos - overall_t2_summary_ff3_tv$dm_oos
  overall_t2_summary_ff3_tv$outperform_se <- sqrt(overall_t2_summary_ff3_tv$pub_oos_se^2 + overall_t2_summary_ff3_tv$dm_oos_se^2)
}

# Helper function to get values by group
get_values <- function(summary_df, group_col, group_val, value_col) {
  idx <- which(summary_df[[group_col]] == group_val)
  if(length(idx) > 0) return(summary_df[[value_col]][idx]) else return(NA)
}

# ============================================================================
# IMPROVED HELPER FUNCTIONS FOR TABLE GENERATION
# ============================================================================

# Helper function to build table data more efficiently
build_table_row <- function(summaries, group_val, group_col, metrics = c("pub_oos", "pub_oos_se", "outperform", "outperform_se")) {
  row_data <- list()
  
  for (analysis in names(summaries)) {
    for (metric in metrics) {
      col_name <- paste0(analysis, "_", metric)
      row_data[[col_name]] <- get_values(summaries[[analysis]], group_col, group_val, metric)
    }
  }
  
  return(row_data)
}

# Unified function to format values with standard errors
format_value_se <- function(value, se, digits = 0, latex = FALSE) {
  if (is.na(value) || is.na(se)) return(NA)
  
  if (latex) {
    return(sprintf("$%.*f$ ($%.*f$)", digits, value, digits, se))
  } else {
    return(sprintf("%.*f (%.*f)", digits, value, digits, se))
  }
}

# Function to create LaTeX table from summary data
create_latex_table <- function(table_data, caption = "", label = "", 
                              column_spec = NULL, booktabs = TRUE,
                              size = "\\small", placement = "htbp") {
  
  # Load required package (should be managed by renv)
  if (!requireNamespace("xtable", quietly = TRUE)) {
    stop("xtable package is required but not installed. Please install it using renv::install('xtable')")
  }
  library(xtable)
  
  # Create xtable object
  xt <- xtable(table_data, caption = caption, label = label)
  
  # Set column alignment if not specified
  if (is.null(column_spec)) {
    column_spec <- paste0("l", paste(rep("r", ncol(table_data)), collapse = ""))
  }
  xtable::align(xt) <- column_spec
  
  # Generate LaTeX code with options
  latex_code <- print(xt, 
                     booktabs = booktabs,
                     include.rownames = FALSE,
                     floating.environment = "table",
                     table.placement = placement,
                     size = size,
                     sanitize.text.function = identity,
                     print.results = FALSE)
  
  return(latex_code)
}

# Enhanced function to create LaTeX table with proper formatting
create_formatted_latex_table <- function(table_data, caption = "", label = "",
                                        group_headers = NULL, placement = "htbp",
                                        separate_se_rows = TRUE) {
  
  # Build the LaTeX table manually for better control
  latex_lines <- character()
  
  # Table environment
  latex_lines <- c(latex_lines, 
                  paste0("\\begin{table}[", placement, "]"),
                  "\\centering")
  
  # Caption
  if (caption != "") {
    latex_lines <- c(latex_lines, paste0("\\caption{", caption, "}"))
  }
  
  # Determine column specification based on the table structure
  # Look for columns to identify structure
  col_names <- names(table_data)
  has_category <- "Category" %in% col_names
  has_group <- "Group" %in% col_names
  
  # Count data columns (excluding Category and Group)
  data_cols <- col_names[!col_names %in% c("Category", "Group")]
  n_data_cols <- length(data_cols)
  
  # Build column spec: left-aligned for labels, centered for data
  if (has_group && !has_category) {
    col_spec <- paste0("l", paste(rep("c", n_data_cols), collapse = ""))
  } else if (has_category && has_group) {
    col_spec <- paste0("l", paste(rep("c", n_data_cols), collapse = ""))  # Only one 'l' for Group
  } else {
    col_spec <- paste(rep("c", ncol(table_data)), collapse = "")
  }
  
  latex_lines <- c(latex_lines, paste0("\\begin{tabular}{", col_spec, "}"))
  latex_lines <- c(latex_lines, "\\toprule")
  
  # Create column headers with spanning headers
  if (!is.null(group_headers)) {
    # First row with spanning headers
    header_line <- "& "  # Empty cell for row labels
    
    for (i in seq_along(group_headers)) {
      span_info <- group_headers[[i]]
      header_line <- paste0(header_line, 
                           "\\multicolumn{", span_info$span, "}{c}{", span_info$title, "}")
      if (i < length(group_headers)) {
        header_line <- paste0(header_line, " & ")
      }
    }
    header_line <- paste0(header_line, " \\\\")
    latex_lines <- c(latex_lines, header_line)
    
    # Add cmidrule for each spanning header
    rule_line <- ""
    current_col <- 2  # Start from column 2 (after row label column)
    for (span_info in group_headers) {
      end_col <- current_col + span_info$span - 1
      if (rule_line != "") rule_line <- paste0(rule_line, " ")
      rule_line <- paste0(rule_line, "\\cmidrule(lr){", current_col, "-", end_col, "}")
      current_col <- end_col + 1
    }
    latex_lines <- c(latex_lines, rule_line)
  }
  
  # Second row with individual column headers
  # Clean column names for display - extract just the type (Raw/CAPM/FF3/CAPM-TV/FF3-TV)
  display_cols <- character()
  
  # Check if this is a time-varying table (has CAPM_TV and FF3_TV columns)
  is_time_varying <- any(grepl("CAPM_TV|FF3_TV", data_cols))
  
  if (is_time_varying) {
    # For time-varying tables, columns are ordered as:
    # CAPM_TV_Return, CAPM_TV_Outperformance, FF3_TV_Return, FF3_TV_Outperformance
    # The group headers should be CAPM-TV and FF3-TV (span 2 each)
    # and the second header row should be Return, Outperformance under each group
    display_cols <- c("Return", "Outperformance", "Return", "Outperformance")
  } else {
    # For regular tables, extract the analysis type
    for (col in data_cols) {
      if (grepl("Raw", col)) {
        display_cols <- c(display_cols, "Raw")
      } else if (grepl("CAPM", col) && !grepl("_TV", col)) {
        display_cols <- c(display_cols, "CAPM")
      } else if (grepl("FF3", col) && !grepl("_TV", col)) {
        display_cols <- c(display_cols, "FF3")
      } else {
        # Fallback - clean the column name
        clean_col <- gsub("_Return$|_Outperformance$", "", col)
        display_cols <- c(display_cols, clean_col)
      }
    }
  }
  
  header_line <- paste(c("", display_cols), collapse = " & ")
  latex_lines <- c(latex_lines, paste0(header_line, " \\\\"))
  latex_lines <- c(latex_lines, "\\midrule")
  
  # Process data rows with separated values and SEs
  current_category <- ""
  
  for (i in 1:nrow(table_data)) {
    row_data <- table_data[i,]
    
    # Handle category headers (bold) if present
    if (has_category) {
      if (row_data$Category != current_category) {
        current_category <- as.character(row_data$Category)
        # Create empty cells for all data columns
        empty_cells <- rep("", length(data_cols))
        category_line <- paste(c(paste0("\\textbf{", current_category, "}"), empty_cells), collapse = " & ")
        latex_lines <- c(latex_lines, paste0(category_line, " \\\\"))
      }
    }
    
    # Get group label
    group_label <- if(has_group) as.character(row_data$Group) else ""
    
    # Extract values and SEs from the formatted strings
    values <- character()
    ses <- character()
    
    for (col in data_cols) {
      val_str <- as.character(row_data[[col]])
      # Parse the "value (se)" format
      if (grepl("\\(", val_str)) {
        parts <- strsplit(val_str, " \\(")[[1]]
        values <- c(values, trimws(parts[1]))
        se_part <- gsub("\\)", "", parts[2])
        ses <- c(ses, paste0("(", trimws(se_part), ")"))
      } else {
        values <- c(values, val_str)
        ses <- c(ses, "")
      }
    }
    
    # Write value row
    value_line <- paste(c(group_label, values), collapse = " & ")
    latex_lines <- c(latex_lines, paste0(value_line, " \\\\"))
    
    # Write SE row (with empty first cell for alignment)
    se_line <- paste(c("", ses), collapse = " & ")
    latex_lines <- c(latex_lines, paste0(se_line, " \\\\"))
  }
  
  # Close table
  latex_lines <- c(latex_lines, "\\bottomrule")
  latex_lines <- c(latex_lines, "\\end{tabular}")
  
  # Label
  if (label != "") {
    latex_lines <- c(latex_lines, paste0("\\label{", label, "}"))
  }
  
  latex_lines <- c(latex_lines, "\\end{table}")
  
  return(paste(latex_lines, collapse = "\n"))
}

# Function to build time-varying summary tables
build_tv_summary_table <- function(categories, groups, summaries, digits = 0) {
  
  # Initialize result data frame
  result_df <- data.frame(
    Category = categories,
    Group = groups,
    stringsAsFactors = FALSE
  )
  
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
  
  # Build columns in the correct order: Raw_Return, CAPM_Return, FF3_Return, Raw_Outperform, CAPM_Outperform, FF3_Outperform
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
export_tables_multi_format <- function(table_data, base_filename, 
                                      formats = c("csv", "latex", "txt"),
                                      latex_options = list()) {
  
  results_files <- list()
  
  # Export CSV
  if ("csv" %in% formats) {
    csv_file <- paste0(base_filename, ".csv")
    write.csv(table_data, csv_file, row.names = FALSE)
    results_files$csv <- csv_file
    cat("Exported CSV:", csv_file, "\n")
  }
  
  # Export LaTeX with enhanced formatting
  if ("latex" %in% formats) {
    latex_file <- paste0(base_filename, ".tex")
    
    # Apply default LaTeX options with group headers
    default_opts <- list(
      caption = "", 
      label = "",
      group_headers = list(
        list(title = "Post-Sample Return", span = 3),
        list(title = "Outperformance vs Data-Mining", span = 3)
      ),
      placement = "htbp"
    )
    # Use modifyList but handle group_headers specially to ensure proper override
    latex_opts <- modifyList(default_opts, latex_options)
    # If group_headers is provided in latex_options, use it completely
    if (!is.null(latex_options$group_headers)) {
      latex_opts$group_headers <- latex_options$group_headers
    }
    
    # Use the enhanced formatted table function
    latex_code <- do.call(create_formatted_latex_table, c(list(table_data = table_data), latex_opts))
    writeLines(latex_code, latex_file)
    results_files$latex <- latex_file
    cat("Exported LaTeX:", latex_file, "\n")
  }
  
  # Export formatted text
  if ("txt" %in% formats) {
    txt_file <- paste0(base_filename, ".txt")
    sink(txt_file)
    print(table_data, row.names = FALSE)
    sink()
    results_files$txt <- txt_file
    cat("Exported TXT:", txt_file, "\n")
  }
  
  return(results_files)
}

# Print filtered table
if (filter_type == "return") {
  cat("\nPost-Sample Return (avg>=", return_threshold, ")     Outperformance vs Data-Mining (avg>=", return_threshold, ")\n")
} else {
  cat("\nPost-Sample Return (t>=", t_threshold_b, ")     Outperformance vs Data-Mining (t>=", t_threshold_b, ")\n")
}
cat("                Raw    CAPM    FF3    Raw    CAPM    FF3\n")
cat("Theoretical Foundation\n")

groups_theory <- c("Risk", "Mispricing", "Agnostic") 
for(group in groups_theory) {
  # Post-sample returns
  raw_ret <- round(get_values(raw_t2_summary_theory, "theory_group", group, "pub_oos"))
  capm_ret <- round(get_values(capm_t2_summary_theory, "theory_group", group, "pub_oos"))  
  ff3_ret <- round(get_values(ff3_t2_summary_theory, "theory_group", group, "pub_oos"))
  
  # Outperformance
  raw_out <- round(get_values(raw_t2_summary_theory, "theory_group", group, "outperform"))
  capm_out <- round(get_values(capm_t2_summary_theory, "theory_group", group, "outperform"))
  ff3_out <- round(get_values(ff3_t2_summary_theory, "theory_group", group, "outperform"))
  
  # Standard errors  
  raw_se <- round(get_values(raw_t2_summary_theory, "theory_group", group, "pub_oos_se"))
  capm_se <- round(get_values(capm_t2_summary_theory, "theory_group", group, "pub_oos_se"))
  ff3_se <- round(get_values(ff3_t2_summary_theory, "theory_group", group, "pub_oos_se"))
  
  raw_out_se <- round(get_values(raw_t2_summary_theory, "theory_group", group, "outperform_se"))
  capm_out_se <- round(get_values(capm_t2_summary_theory, "theory_group", group, "outperform_se"))
  ff3_out_se <- round(get_values(ff3_t2_summary_theory, "theory_group", group, "outperform_se"))
  
  cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
              group, raw_ret, capm_ret, ff3_ret, raw_out, capm_out, ff3_out))
  cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
              "", raw_se, capm_se, ff3_se, raw_out_se, capm_out_se, ff3_out_se))
}

cat("Modeling Formalism\n")
groups_model <- c("No Model", "Stylized", "Dynamic or Quantitative")
for(group in groups_model) {
  # Post-sample returns
  raw_ret <- round(get_values(raw_t2_summary_model, "modeltype_grouped", group, "pub_oos"))
  capm_ret <- round(get_values(capm_t2_summary_model, "modeltype_grouped", group, "pub_oos"))  
  ff3_ret <- round(get_values(ff3_t2_summary_model, "modeltype_grouped", group, "pub_oos"))
  
  # Outperformance
  raw_out <- round(get_values(raw_t2_summary_model, "modeltype_grouped", group, "outperform"))
  capm_out <- round(get_values(capm_t2_summary_model, "modeltype_grouped", group, "outperform"))
  ff3_out <- round(get_values(ff3_t2_summary_model, "modeltype_grouped", group, "outperform"))
  
  # Standard errors  
  raw_se <- round(get_values(raw_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  capm_se <- round(get_values(capm_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  ff3_se <- round(get_values(ff3_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  
  raw_out_se <- round(get_values(raw_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
  capm_out_se <- round(get_values(capm_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
  ff3_out_se <- round(get_values(ff3_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
  
  cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
              group, raw_ret, capm_ret, ff3_ret, raw_out, capm_out, ff3_out))
  cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
              "", raw_se, capm_se, ff3_se, raw_out_se, capm_out_se, ff3_out_se))
}

cat("Overall\n")
cat(sprintf("%-12s %4s   %4s   %4s   %4s   %4s   %4s\n",
            "All", round(overall_t2_summary_raw$pub_oos), round(overall_t2_summary_capm$pub_oos), round(overall_t2_summary_ff3$pub_oos), 
            round(overall_t2_summary_raw$outperform), round(overall_t2_summary_capm$outperform), round(overall_t2_summary_ff3$outperform)))
cat(sprintf("%-12s (%2s)   (%2s)   (%2s)   (%2s)   (%2s)   (%2s)\n",
            "", round(overall_t2_summary_raw$pub_oos_se), round(overall_t2_summary_capm$pub_oos_se), round(overall_t2_summary_ff3$pub_oos_se), 
            round(overall_t2_summary_raw$outperform_se), round(overall_t2_summary_capm$outperform_se), round(overall_t2_summary_ff3$outperform_se)))

# ANY MODEL VS NO MODEL TABLE (t >= t_threshold_b) ----------------------------------------
if (filter_type == "return") {
  cat("\n\n=== ANY MODEL VS NO MODEL TABLE (avg >=", return_threshold, ") ===\n")
} else {
  cat("\n\n=== ANY MODEL VS NO MODEL TABLE (t >=", t_threshold_b, ") ===\n")
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
      filter(rbar_t >= t_threshold_b), 
    "ret", "matchRet", anymodel_mapping, "model_binary"
  )
}

# CAPM t >= t_threshold_b filtered (by model binary)
capm_t2_summary_anymodel <- compute_outperformance(
  ret_for_plot0_capm_t2,
  "abnormal_capm_normalized", "matchRet_capm_t2_normalized", anymodel_mapping, "model_binary"
)

# FF3 t >= t_threshold_b filtered (by model binary)
ff3_t2_summary_anymodel <- compute_outperformance(
  ret_for_plot0_ff3_t2,
  "abnormal_ff3_normalized", "matchRet_ff3_t2_normalized", anymodel_mapping, "model_binary"
)

# Print Any Model vs No Model table
if (filter_type == "return") {
  cat("\nPost-Sample Return (avg>=", return_threshold, ")     Outperformance vs Data-Mining (avg>=", return_threshold, ")\n")
} else {
  cat("\nPost-Sample Return (t>=", t_threshold_b, ")     Outperformance vs Data-Mining (t>=", t_threshold_b, ")\n")
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
  # CAPM time-varying t >= t_threshold_b filtered (by model binary)
  capm_tv_t2_summary_anymodel <- compute_outperformance(
    ret_for_plot0_capm_tv_t2,
    "abnormal_capm_normalized", "matchRet_capm_tv_t2_normalized", anymodel_mapping, "model_binary"
  )
  
  # FF3 time-varying t >= t_threshold_b filtered (by model binary)
  ff3_tv_t2_summary_anymodel <- compute_outperformance(
    ret_for_plot0_ff3_tv_t2,
    "abnormal_ff3_normalized", "matchRet_ff3_tv_t2_normalized", anymodel_mapping, "model_binary"
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
  cat("\n\n=== TIME-VARYING ABNORMAL RETURNS TABLE (t >=", t_threshold_b, ") ===\n")
}
  if (filter_type == "return") {
    cat("\nPost-Sample Return (avg>=", return_threshold, ")     Outperformance vs Data-Mining (avg>=", return_threshold, ")\n")
  } else {
    cat("\nPost-Sample Return (t>=", t_threshold_b, ")     Outperformance vs Data-Mining (t>=", t_threshold_b, ")\n")
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
      capm_tv_pub_oos = get_values(capm_tv_t2_summary_theory, "theory_group", group, "pub_oos"),
      capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_theory, "theory_group", group, "pub_oos_se"),
      capm_tv_outperform = get_values(capm_tv_t2_summary_theory, "theory_group", group, "outperform"),
      capm_tv_outperform_se = get_values(capm_tv_t2_summary_theory, "theory_group", group, "outperform_se"),
      
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
      capm_tv_pub_oos = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos"),
      capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"),
      capm_tv_outperform = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "outperform"),
      capm_tv_outperform_se = get_values(capm_tv_t2_summary_model, "modeltype_grouped", group, "outperform_se"),
      
      ff3_tv_pub_oos = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos"),
      ff3_tv_pub_oos_se = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"),
      ff3_tv_outperform = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "outperform"),
      ff3_tv_outperform_se = get_values(ff3_tv_t2_summary_model, "modeltype_grouped", group, "outperform_se")
    )
  }
  
  # Add overall TV results
  tv_overall_data <- list(
    capm_tv_pub_oos = overall_t2_summary_capm_tv$pub_oos,
    capm_tv_pub_oos_se = overall_t2_summary_capm_tv$pub_oos_se,
    capm_tv_outperform = overall_t2_summary_capm_tv$outperform,
    capm_tv_outperform_se = overall_t2_summary_capm_tv$outperform_se,
    
    ff3_tv_pub_oos = overall_t2_summary_ff3_tv$pub_oos,
    ff3_tv_pub_oos_se = overall_t2_summary_ff3_tv$pub_oos_se,
    ff3_tv_outperform = overall_t2_summary_ff3_tv$outperform,
    ff3_tv_outperform_se = overall_t2_summary_ff3_tv$outperform_se
  )
  
  # Collect time-varying discipline data (if summaries exist)
  if (exists("capm_tv_t2_summary_discipline") && exists("ff3_tv_t2_summary_discipline")) {
    tv_discipline_data <- list()
    discipline_groups <- c("Finance", "Accounting")
    for(i in 1:length(discipline_groups)) {
      group <- discipline_groups[i]
      tv_discipline_data[[i]] <- list(
        capm_tv_pub_oos = get_values(capm_tv_t2_summary_discipline, "discipline", group, "pub_oos"),
        capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_discipline, "discipline", group, "pub_oos_se"),
        capm_tv_outperform = get_values(capm_tv_t2_summary_discipline, "discipline", group, "outperform"),
        capm_tv_outperform_se = get_values(capm_tv_t2_summary_discipline, "discipline", group, "outperform_se"),
        
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
        capm_tv_pub_oos = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "pub_oos"),
        capm_tv_pub_oos_se = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "pub_oos_se"),
        capm_tv_outperform = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "outperform"),
        capm_tv_outperform_se = get_values(capm_tv_t2_summary_journal, "journal_rank", group, "outperform_se"),
        
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
      capm_tv_pub_oos = anymodel_capm_tv_pub_oos,
      capm_tv_pub_oos_se = anymodel_capm_tv_pub_oos_se,
      capm_tv_outperform = anymodel_capm_tv_outperform,
      capm_tv_outperform_se = anymodel_capm_tv_outperform_se,
      
      ff3_tv_pub_oos = anymodel_ff3_tv_pub_oos,
      ff3_tv_pub_oos_se = anymodel_ff3_tv_pub_oos_se,
      ff3_tv_outperform = anymodel_ff3_tv_outperform,
      ff3_tv_outperform_se = anymodel_ff3_tv_outperform_se
    )
  }
}

# DISCIPLINE AND JOURNAL RANKING TABLE (t >= t_threshold_b) ---------------------------------
if (filter_type == "return") {
  cat("\n\n=== SUMMARY TABLE BY DISCIPLINE AND JOURNAL RANKING (avg >=", return_threshold, ") ===\n")
} else {
  cat("\n\n=== SUMMARY TABLE BY DISCIPLINE AND JOURNAL RANKING (t >=", t_threshold_b, ") ===\n")
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
    filter(rbar_t >= t_threshold_b) %>%
    inner_join(discipline_mapping_filtered, by = c("pubname" = "signalname"))
}

# Raw returns with t >= t_threshold_b filter (by discipline) - excluding Economics
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

# CAPM t >= t_threshold_b filtered (by discipline) - excluding Economics
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

# FF3 t >= t_threshold_b filtered (by discipline) - excluding Economics
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
    filter(rbar_t >= t_threshold_b) %>%
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

# CAPM t >= t_threshold_b filtered (by journal) - excluding Economics
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

# FF3 t >= t_threshold_b filtered (by journal) - excluding Economics
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
  cat("\nPost-Sample Return (t>=", t_threshold_b, ")     Outperformance vs Data-Mining (t>=", t_threshold_b, ")\n")
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
                     paste0("_t", t_threshold_b))

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
      capm_tv_pub_oos = mean(c(
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"),
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos")
      )),
      capm_tv_pub_oos_se = sqrt(mean(c(
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")^2,
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")^2
      ))),
      capm_tv_outperform = mean(c(
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"),
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform")
      )),
      capm_tv_outperform_se = sqrt(mean(c(
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")^2,
        get_values(capm_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")^2
      ))),
      
      ff3_tv_pub_oos = mean(c(
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"),
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos")
      )),
      ff3_tv_pub_oos_se = sqrt(mean(c(
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")^2,
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")^2
      ))),
      ff3_tv_outperform = mean(c(
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"),
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform")
      )),
      ff3_tv_outperform_se = sqrt(mean(c(
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")^2,
        get_values(ff3_tv_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")^2
      )))
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
# Only run t >= 1 analysis for t-stat filtering
if (filter_type == "tstat") {
  cat("\n=== T >=", t_threshold_a, " FILTERED ANALYSIS ===\n")
  
  # Identify published signals with t >= t_threshold_a
  signals_capm_ta <- unique(czret[abar_capm_t >= t_threshold_a]$signalname)
  signals_ff3_ta <- unique(czret[abar_ff3_t >= t_threshold_a]$signalname)

# Filter DM signals by t >= t_threshold_a
dm_filtered_capm_ta <- candidateReturns_adj %>%
  inner_join(
    dm_stats %>% filter(abar_capm_dm_t >= t_threshold_a),
    by = c("actSignal", "candSignalname")
  )

dm_filtered_ff3_ta <- candidateReturns_adj %>%
  inner_join(
    dm_stats %>% filter(abar_ff3_dm_t >= t_threshold_a),
    by = c("actSignal", "candSignalname")
  )

# CAPM t >= t_threshold_a
cat("\n=== CAPM FILTERED (t >=", t_threshold_a, ") STATISTICS ===\n")

# Use helper function to normalize and aggregate
dm_capm_aggregated_ta <- normalize_and_aggregate_dm(
  dm_filtered_capm_ta, 
  "abnormal_capm", 
  "capm_ta_normalized"
)

# Create filtered plot data
ret_for_plot0_capm_ta <- create_filtered_plot_data(
  ret_for_plot0_adj,
  signals_capm_ta,
  dm_capm_aggregated_ta,
  "abnormal_capm_normalized",
  "matchRet_capm_ta_normalized",
  "capm_ta_normalized"
)

cat("Published signals with CAPM t >=", t_threshold_a, ":", length(signals_capm_ta), "\n")
cat("Published signals with filtered DM matches:", length(unique(ret_for_plot0_capm_ta$pubname)), "\n")

# Create and save plot
printme_capm_ta <- create_risk_adjusted_plot(
  ret_for_plot0_capm_ta,
  "abnormal_capm_normalized",
  "matchRet_capm_ta_normalized",
  "CAPM Alpha",
  t_threshold_a,
  "Trailing 5-Year CAPM Alpha",
  y_high = 150,
  filter_type = filter_type,
  return_threshold = return_threshold
)

# FF3 t >= t_threshold_a
cat("\n=== FF3 FILTERED (t >=", t_threshold_a, ") STATISTICS ===\n")

# Use helper function to normalize and aggregate
dm_ff3_aggregated_ta <- normalize_and_aggregate_dm(
  dm_filtered_ff3_ta, 
  "abnormal_ff3", 
  "ff3_ta_normalized"
)

# Create filtered plot data
ret_for_plot0_ff3_ta <- create_filtered_plot_data(
  ret_for_plot0_adj,
  signals_ff3_ta,
  dm_ff3_aggregated_ta,
  "abnormal_ff3_normalized",
  "matchRet_ff3_ta_normalized",
  "ff3_ta_normalized"
)

cat("Published signals with FF3 t >=", t_threshold_a, ":", length(signals_ff3_ta), "\n")
cat("Published signals with filtered DM matches:", length(unique(ret_for_plot0_ff3_ta$pubname)), "\n")

# Create and save plot
printme_ff3_ta <- create_risk_adjusted_plot(
  ret_for_plot0_ff3_ta,
  "abnormal_ff3_normalized",
  "matchRet_ff3_ta_normalized",
  "FF3 Alpha",
  t_threshold_a,
  "Trailing 5-Year FF3 Alpha",
  filter_type = filter_type,
  return_threshold = return_threshold
)

} # End of t >= 1 analysis block

print("\nRisk-adjusted analysis completed successfully!")