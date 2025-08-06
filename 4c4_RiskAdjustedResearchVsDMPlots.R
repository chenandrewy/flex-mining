# Risk-Adjusted Data-mining comparisons 
# Based on 4c2_ResearchVsDMPlots.R but with CAPM and FF3 adjustments
# This file compares raw vs risk-adjusted returns for published vs data-mined signals

# Setup ----------------------------------------------------------------
rm(list = ls())
source("0_Environment.R")
library(doParallel)

t_threshold_a = 1
t_threshold_b = 2
return_threshold = 0.15  # 15 bps threshold for average in-sample return/alpha

# Filter type: "tstat" for t-stat filtering, "return" for return filtering
# Check for environment variable first, otherwise use default
filter_type <- Sys.getenv("FILTER_TYPE", "tstat")  # Default to t-stat filtering

# Create results subfolder for risk-adjusted analysis
base_results_dir <- "../Results/RiskAdjusted"
if (filter_type == "return") {
  results_dir <- file.path(base_results_dir, paste0("ReturnFilter_", return_threshold))
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
if (!file.exists("../Data/Processed/ret_for_plot1.RDS")) {
  cat("Ret_for_plot1 file not found. Running 4c1_ResearchVsDMprep.R...\n")
  source("4c1_ResearchVsDMprep.R")
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
      suffix <- paste0(tolower(adjustment_type), "_r", gsub("\\.", "", as.character(return_threshold)))
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
ret_for_plot1 <- readRDS("../Data/Processed/ret_for_plot1.RDS")

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

ret_for_plot1 <- ret_for_plot1 %>%
  left_join(
    czret %>% select(signalname, eventDate, date, mktrf, smb, hml),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  )

## CAPM adjustments - full sample betas
czret %>% setDT()

print(czret[, .(min(ret_scaled), max(ret_scaled), min(ret), max(ret), min(mktrf, na.rm = TRUE), max(mktrf, na.rm = TRUE), min(smb, na.rm = TRUE), max(smb, na.rm = TRUE), min(hml, na.rm = TRUE), max(hml, na.rm = TRUE))])

# Full-sample betas for published signals
czret[, beta_capm := extract_beta(ret = ret, mktrf = mktrf), by = signalname]
czret[, abnormal_capm := ret - beta_capm*mktrf]

# Normalize abnormal returns by in-sample mean and compute t-stats
czret[samptype == 'insamp', `:=`(
  abar_capm = mean(abnormal_capm, na.rm = TRUE),
  abar_capm_t = mean(abnormal_capm, na.rm = TRUE) / sd(abnormal_capm, na.rm = TRUE) * sqrt(.N)
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
  abar_ff3_t = mean(abnormal_ff3, na.rm = TRUE) / sd(abnormal_ff3, na.rm = TRUE) * sqrt(.N)
), by = signalname]
czret[, abar_ff3 := nafill(abar_ff3, "locf"), by = .(signalname)]
czret[, abar_ff3_t := nafill(abar_ff3_t, "locf"), by = .(signalname)]

# Fix: Add protection against division by zero
czret[, abnormal_ff3_normalized := ifelse(abs(abar_ff3) > 1e-10, 100*abnormal_ff3/abar_ff3, NA)]

# Also compute raw return t-stats for comparison
czret[samptype == 'insamp', `:=`(
  rbar_scaled = mean(ret_scaled, na.rm = TRUE),
  rbar_scaled_t = mean(ret_scaled, na.rm = TRUE) / sd(ret_scaled, na.rm = TRUE) * sqrt(.N)
), by = signalname]
czret[, rbar_scaled_t := nafill(rbar_scaled_t, "locf"), by = .(signalname)]

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

# Compute statistics for each DM signal
dm_stats <- candidateReturns_adj[
  samptype == "insamp" & !is.na(abnormal_capm),
  .(
    # T-stats
    abar_capm_dm_t = mean(abnormal_capm, na.rm = TRUE) / sd(abnormal_capm, na.rm = TRUE) * sqrt(.N),
    abar_ff3_dm_t = mean(abnormal_ff3, na.rm = TRUE) / sd(abnormal_ff3, na.rm = TRUE) * sqrt(.N),
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
  # Note: rbar_scaled, abar_capm, abar_ff3 are already in percentage terms (multiplied by 100)
  signals_raw_t2 <- unique(czret[rbar_scaled >= return_threshold]$signalname)
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
  signals_raw_t2 <- unique(czret[rbar_scaled_t >= t_threshold_b]$signalname)
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
  "Trailing 5-Year CAPM Alpha (% of In-Sample Alpha)",
  filter_type = filter_type,
  return_threshold = return_threshold
)

# Create filtered plots for FF3 t >= t_threshold_b
cat("\n=== FF3 FILTERED (t >=", t_threshold_b, ") STATISTICS ===\n")

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
  "Trailing 5-Year FF3 Alpha (% of In-Sample Alpha)",
  filter_type = filter_type,
  return_threshold = return_threshold
)

# Time-varying abnormal returns (IS beta in IS, post-sample beta in OOS) --------

cat("\n\n=== TIME-VARYING ABNORMAL RETURNS (IS/OOS BETAS) ===\n")

# Check if time-varying columns exist in the data
if("abnormal_capm_tv" %in% names(candidateReturns_adj) && "abnormal_ff3_tv" %in% names(candidateReturns_adj)) {
  
  # Compute statistics for time-varying abnormal returns
  dm_stats_tv <- candidateReturns_adj[
    samptype == "insamp" & !is.na(abnormal_capm_tv),
    .(
      abar_capm_tv_dm_t = mean(abnormal_capm_tv, na.rm = TRUE) / sd(abnormal_capm_tv, na.rm = TRUE) * sqrt(.N),
      abar_ff3_tv_dm_t = mean(abnormal_ff3_tv, na.rm = TRUE) / sd(abnormal_ff3_tv, na.rm = TRUE) * sqrt(.N),
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
    "Trailing 5-Year CAPM Alpha (% of In-Sample Alpha, Time-Varying Betas)",
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
    "Trailing 5-Year FF3 Alpha (% of In-Sample Alpha, Time-Varying Betas)",
    filter_type = filter_type,
    return_threshold = return_threshold
  )
  
} else {
  cat("\nTime-varying abnormal returns not available in the data.\n")
  cat("Please run 2d_RiskAdjustDataMinedSignals.R with the updated code to generate these columns.\n")
}

# Create summary table by theoretical foundation -------------------------------

cat("\n\n=== SUMMARY TABLE BY THEORETICAL FOUNDATION ===\n")

# Load model categories from 3f and journal categories from 3g
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

# Function to compute outperformance metrics with standard errors
compute_outperformance <- function(plot_data, ret_col, dm_ret_col, group_map, group_col = "theory_group") {
  plot_data %>%
    left_join(group_map, by = c("pubname" = "signalname")) %>%
    filter(!is.na(.data[[group_col]])) %>%
    group_by(.data[[group_col]]) %>%
    summarise(
      n_signals = n_distinct(pubname),
      pub_oos = mean(.data[[ret_col]][eventDate > 0], na.rm = TRUE),
      pub_oos_se = sd(.data[[ret_col]][eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(.data[[ret_col]]))),
      dm_oos = mean(.data[[dm_ret_col]][eventDate > 0], na.rm = TRUE),
      dm_oos_se = sd(.data[[dm_ret_col]][eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(.data[[dm_ret_col]]))),
      outperform = pub_oos - dm_oos,
      outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
      .groups = 'drop'
    )
}



# T-STAT FILTERED SUMMARY TABLE (t >= t_threshold_b) ----------------------------------------
if (filter_type == "return") {
  cat("\n\n=== SUMMARY TABLE WITH AVG RETURN/ALPHA >=", return_threshold, " FILTER ===\n")
} else {
  cat("\n\n=== SUMMARY TABLE WITH T >=", t_threshold_b, " FILTER ===\n")
}

# Raw returns with t >= t_threshold_b filter (by theory)
raw_t2_summary_theory <- compute_outperformance(
  ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_scaled_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_scaled_t >= t_threshold_b), 
  "ret", "matchRet", theory_mapping, "theory_group"
)

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

# Raw returns with t >= t_threshold_b filter (by model)
raw_t2_summary_model <- compute_outperformance(
  ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_scaled_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_scaled_t >= t_threshold_b), 
  "ret", "matchRet", model_mapping, "modeltype_grouped"
)

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

# Overall t >= t_threshold_b summaries
overall_t2_summary_raw <- data.frame(
  group = "Overall",
  n_signals = length(unique(czret$signalname[czret$rbar_scaled_t >= t_threshold_b])),
  pub_oos = mean(ret_for_plot0$ret[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                                   ret_for_plot0$pubname %in% czret$signalname[czret$rbar_scaled_t >= t_threshold_b]], na.rm = TRUE),
  pub_oos_se = sd(ret_for_plot0$ret[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                                    ret_for_plot0$pubname %in% czret$signalname[czret$rbar_scaled_t >= t_threshold_b]], na.rm = TRUE) / 
               sqrt(sum(ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                        ret_for_plot0$pubname %in% czret$signalname[czret$rbar_scaled_t >= t_threshold_b])),
  dm_oos = mean(ret_for_plot0$matchRet[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                                       ret_for_plot0$pubname %in% czret$signalname[czret$rbar_scaled_t >= t_threshold_b]], na.rm = TRUE),
  dm_oos_se = sd(ret_for_plot0$matchRet[ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                                        ret_for_plot0$pubname %in% czret$signalname[czret$rbar_scaled_t >= t_threshold_b]], na.rm = TRUE) / 
              sqrt(sum(ret_for_plot0$eventDate > 0 & !is.na(ret_for_plot0$matchRet) & 
                       ret_for_plot0$pubname %in% czret$signalname[czret$rbar_scaled_t >= t_threshold_b])),
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

# Raw returns with t >= t_threshold_b filter (by model binary)
raw_t2_summary_anymodel <- compute_outperformance(
  ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_scaled_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_scaled_t >= t_threshold_b), 
  "ret", "matchRet", anymodel_mapping, "model_binary"
)

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
discipline_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
  left_join(czret %>% select(signalname, rbar_scaled_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
  filter(rbar_scaled_t >= t_threshold_b) %>%
  inner_join(discipline_mapping_filtered, by = c("pubname" = "signalname"))

# Raw returns with t >= t_threshold_b filter (by discipline) - excluding Economics
raw_t2_summary_discipline <- discipline_data %>%
  group_by(discipline) %>%
  summarise(
    n_signals = n_distinct(pubname),
    pub_oos = mean(ret[eventDate > 0], na.rm = TRUE),
    pub_oos_se = sd(ret[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(ret))),
    dm_oos = mean(matchRet[eventDate > 0], na.rm = TRUE),
    dm_oos_se = sd(matchRet[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(matchRet))),
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
    pub_oos_se = sd(abnormal_capm_normalized[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(abnormal_capm_normalized))),
    dm_oos = mean(matchRet_capm_t2_normalized[eventDate > 0], na.rm = TRUE),
    dm_oos_se = sd(matchRet_capm_t2_normalized[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(matchRet_capm_t2_normalized))),
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
    pub_oos_se = sd(abnormal_ff3_normalized[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(abnormal_ff3_normalized))),
    dm_oos = mean(matchRet_ff3_t2_normalized[eventDate > 0], na.rm = TRUE),
    dm_oos_se = sd(matchRet_ff3_t2_normalized[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(matchRet_ff3_t2_normalized))),
    outperform = pub_oos - dm_oos,
    outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
    .groups = 'drop'
  )

# Filter data to exclude Economics journals
journal_mapping_filtered <- journal_mapping %>% filter(journal_rank != "Economics")

# Raw returns with t >= t_threshold_b filter (by journal) - excluding Economics
journal_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
  left_join(czret %>% select(signalname, rbar_scaled_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
  filter(rbar_scaled_t >= t_threshold_b) %>%
  inner_join(journal_mapping_filtered, by = c("pubname" = "signalname"))

raw_t2_summary_journal <- journal_data %>%
  group_by(journal_rank) %>%
  summarise(
    n_signals = n_distinct(pubname),
    pub_oos = mean(ret[eventDate > 0], na.rm = TRUE),
    pub_oos_se = sd(ret[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(ret))),
    dm_oos = mean(matchRet[eventDate > 0], na.rm = TRUE),
    dm_oos_se = sd(matchRet[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(matchRet))),
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
    pub_oos_se = sd(abnormal_capm_normalized[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(abnormal_capm_normalized))),
    dm_oos = mean(matchRet_capm_t2_normalized[eventDate > 0], na.rm = TRUE),
    dm_oos_se = sd(matchRet_capm_t2_normalized[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(matchRet_capm_t2_normalized))),
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
    pub_oos_se = sd(abnormal_ff3_normalized[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(abnormal_ff3_normalized))),
    dm_oos = mean(matchRet_ff3_t2_normalized[eventDate > 0], na.rm = TRUE),
    dm_oos_se = sd(matchRet_ff3_t2_normalized[eventDate > 0], na.rm = TRUE) / sqrt(sum(eventDate > 0 & !is.na(matchRet_ff3_t2_normalized))),
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

# Export tables to CSV -------------------------------------------------------

# Function to format values with standard errors in parentheses
format_with_se <- function(value, se) {
  if(is.na(value) || is.na(se)) return(NA)
  return(paste0(round(value), " (", round(se), ")"))
}

# Create comprehensive summary table for export (formatted like printed table)
export_table_main <- data.frame(
  Category = c(rep("Theoretical Foundation", 3), rep("Modeling Formalism", 3), "Overall"),
  Group = c("Risk", "Mispricing", "Agnostic", "No Model", "Stylized", "Dynamic or Quantitative", "All"),
  # Post-sample returns with standard errors
  Raw_Return = c(
    format_with_se(get_values(raw_t2_summary_theory, "theory_group", "Risk", "pub_oos"), 
                   get_values(raw_t2_summary_theory, "theory_group", "Risk", "pub_oos_se")),
    format_with_se(get_values(raw_t2_summary_theory, "theory_group", "Mispricing", "pub_oos"), 
                   get_values(raw_t2_summary_theory, "theory_group", "Mispricing", "pub_oos_se")),
    format_with_se(get_values(raw_t2_summary_theory, "theory_group", "Agnostic", "pub_oos"), 
                   get_values(raw_t2_summary_theory, "theory_group", "Agnostic", "pub_oos_se")),
    format_with_se(get_values(raw_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos"), 
                   get_values(raw_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos_se")),
    format_with_se(get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"), 
                   get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")),
    format_with_se(get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos"), 
                   get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")),
    format_with_se(overall_t2_summary_raw$pub_oos, overall_t2_summary_raw$pub_oos_se)
  ),
  CAPM_Return = c(
    format_with_se(get_values(capm_t2_summary_theory, "theory_group", "Risk", "pub_oos"), 
                   get_values(capm_t2_summary_theory, "theory_group", "Risk", "pub_oos_se")),
    format_with_se(get_values(capm_t2_summary_theory, "theory_group", "Mispricing", "pub_oos"), 
                   get_values(capm_t2_summary_theory, "theory_group", "Mispricing", "pub_oos_se")),
    format_with_se(get_values(capm_t2_summary_theory, "theory_group", "Agnostic", "pub_oos"), 
                   get_values(capm_t2_summary_theory, "theory_group", "Agnostic", "pub_oos_se")),
    format_with_se(get_values(capm_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos"), 
                   get_values(capm_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos_se")),
    format_with_se(get_values(capm_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"), 
                   get_values(capm_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")),
    format_with_se(get_values(capm_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos"), 
                   get_values(capm_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")),
    format_with_se(overall_t2_summary_capm$pub_oos, overall_t2_summary_capm$pub_oos_se)
  ),
  FF3_Return = c(
    format_with_se(get_values(ff3_t2_summary_theory, "theory_group", "Risk", "pub_oos"), 
                   get_values(ff3_t2_summary_theory, "theory_group", "Risk", "pub_oos_se")),
    format_with_se(get_values(ff3_t2_summary_theory, "theory_group", "Mispricing", "pub_oos"), 
                   get_values(ff3_t2_summary_theory, "theory_group", "Mispricing", "pub_oos_se")),
    format_with_se(get_values(ff3_t2_summary_theory, "theory_group", "Agnostic", "pub_oos"), 
                   get_values(ff3_t2_summary_theory, "theory_group", "Agnostic", "pub_oos_se")),
    format_with_se(get_values(ff3_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos"), 
                   get_values(ff3_t2_summary_model, "modeltype_grouped", "No Model", "pub_oos_se")),
    format_with_se(get_values(ff3_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"), 
                   get_values(ff3_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")),
    format_with_se(get_values(ff3_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos"), 
                   get_values(ff3_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")),
    format_with_se(overall_t2_summary_ff3$pub_oos, overall_t2_summary_ff3$pub_oos_se)
  ),
  # Outperformance vs data-mining with standard errors
  Raw_Outperformance = c(
    format_with_se(get_values(raw_t2_summary_theory, "theory_group", "Risk", "outperform"), 
                   get_values(raw_t2_summary_theory, "theory_group", "Risk", "outperform_se")),
    format_with_se(get_values(raw_t2_summary_theory, "theory_group", "Mispricing", "outperform"), 
                   get_values(raw_t2_summary_theory, "theory_group", "Mispricing", "outperform_se")),
    format_with_se(get_values(raw_t2_summary_theory, "theory_group", "Agnostic", "outperform"), 
                   get_values(raw_t2_summary_theory, "theory_group", "Agnostic", "outperform_se")),
    format_with_se(get_values(raw_t2_summary_model, "modeltype_grouped", "No Model", "outperform"), 
                   get_values(raw_t2_summary_model, "modeltype_grouped", "No Model", "outperform_se")),
    format_with_se(get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"), 
                   get_values(raw_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")),
    format_with_se(get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform"), 
                   get_values(raw_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")),
    format_with_se(overall_t2_summary_raw$outperform, overall_t2_summary_raw$outperform_se)
  ),
  CAPM_Outperformance = c(
    format_with_se(get_values(capm_t2_summary_theory, "theory_group", "Risk", "outperform"), 
                   get_values(capm_t2_summary_theory, "theory_group", "Risk", "outperform_se")),
    format_with_se(get_values(capm_t2_summary_theory, "theory_group", "Mispricing", "outperform"), 
                   get_values(capm_t2_summary_theory, "theory_group", "Mispricing", "outperform_se")),
    format_with_se(get_values(capm_t2_summary_theory, "theory_group", "Agnostic", "outperform"), 
                   get_values(capm_t2_summary_theory, "theory_group", "Agnostic", "outperform_se")),
    format_with_se(get_values(capm_t2_summary_model, "modeltype_grouped", "No Model", "outperform"), 
                   get_values(capm_t2_summary_model, "modeltype_grouped", "No Model", "outperform_se")),
    format_with_se(get_values(capm_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"), 
                   get_values(capm_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")),
    format_with_se(get_values(capm_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform"), 
                   get_values(capm_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")),
    format_with_se(overall_t2_summary_capm$outperform, overall_t2_summary_capm$outperform_se)
  ),
  FF3_Outperformance = c(
    format_with_se(get_values(ff3_t2_summary_theory, "theory_group", "Risk", "outperform"), 
                   get_values(ff3_t2_summary_theory, "theory_group", "Risk", "outperform_se")),
    format_with_se(get_values(ff3_t2_summary_theory, "theory_group", "Mispricing", "outperform"), 
                   get_values(ff3_t2_summary_theory, "theory_group", "Mispricing", "outperform_se")),
    format_with_se(get_values(ff3_t2_summary_theory, "theory_group", "Agnostic", "outperform"), 
                   get_values(ff3_t2_summary_theory, "theory_group", "Agnostic", "outperform_se")),
    format_with_se(get_values(ff3_t2_summary_model, "modeltype_grouped", "No Model", "outperform"), 
                   get_values(ff3_t2_summary_model, "modeltype_grouped", "No Model", "outperform_se")),
    format_with_se(get_values(ff3_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"), 
                   get_values(ff3_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")),
    format_with_se(get_values(ff3_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform"), 
                   get_values(ff3_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")),
    format_with_se(overall_t2_summary_ff3$outperform, overall_t2_summary_ff3$outperform_se)
  )
)

# Create discipline/journal table for export
export_table_discipline <- data.frame(
  Category = c(rep("Discipline", 2), rep("Journal Ranking", 3)),
  Group = c("Finance", "Accounting", "JF, JFE, RFS", "AR, JAR, JAE", "Other"),
  Raw_Return = c(
    format_with_se(get_values(raw_t2_summary_discipline, "discipline", "Finance", "pub_oos"), 
                   get_values(raw_t2_summary_discipline, "discipline", "Finance", "pub_oos_se")),
    format_with_se(get_values(raw_t2_summary_discipline, "discipline", "Accounting", "pub_oos"), 
                   get_values(raw_t2_summary_discipline, "discipline", "Accounting", "pub_oos_se")),
    format_with_se(get_values(raw_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "pub_oos"), 
                   get_values(raw_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "pub_oos_se")),
    format_with_se(get_values(raw_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "pub_oos"), 
                   get_values(raw_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "pub_oos_se")),
    format_with_se(get_values(raw_t2_summary_journal, "journal_rank", "Other", "pub_oos"), 
                   get_values(raw_t2_summary_journal, "journal_rank", "Other", "pub_oos_se"))
  ),
  CAPM_Return = c(
    format_with_se(get_values(capm_t2_summary_discipline, "discipline", "Finance", "pub_oos"), 
                   get_values(capm_t2_summary_discipline, "discipline", "Finance", "pub_oos_se")),
    format_with_se(get_values(capm_t2_summary_discipline, "discipline", "Accounting", "pub_oos"), 
                   get_values(capm_t2_summary_discipline, "discipline", "Accounting", "pub_oos_se")),
    format_with_se(get_values(capm_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "pub_oos"), 
                   get_values(capm_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "pub_oos_se")),
    format_with_se(get_values(capm_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "pub_oos"), 
                   get_values(capm_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "pub_oos_se")),
    format_with_se(get_values(capm_t2_summary_journal, "journal_rank", "Other", "pub_oos"), 
                   get_values(capm_t2_summary_journal, "journal_rank", "Other", "pub_oos_se"))
  ),
  FF3_Return = c(
    format_with_se(get_values(ff3_t2_summary_discipline, "discipline", "Finance", "pub_oos"), 
                   get_values(ff3_t2_summary_discipline, "discipline", "Finance", "pub_oos_se")),
    format_with_se(get_values(ff3_t2_summary_discipline, "discipline", "Accounting", "pub_oos"), 
                   get_values(ff3_t2_summary_discipline, "discipline", "Accounting", "pub_oos_se")),
    format_with_se(get_values(ff3_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "pub_oos"), 
                   get_values(ff3_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "pub_oos_se")),
    format_with_se(get_values(ff3_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "pub_oos"), 
                   get_values(ff3_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "pub_oos_se")),
    format_with_se(get_values(ff3_t2_summary_journal, "journal_rank", "Other", "pub_oos"), 
                   get_values(ff3_t2_summary_journal, "journal_rank", "Other", "pub_oos_se"))
  ),
  Raw_Outperformance = c(
    format_with_se(get_values(raw_t2_summary_discipline, "discipline", "Finance", "outperform"), 
                   get_values(raw_t2_summary_discipline, "discipline", "Finance", "outperform_se")),
    format_with_se(get_values(raw_t2_summary_discipline, "discipline", "Accounting", "outperform"), 
                   get_values(raw_t2_summary_discipline, "discipline", "Accounting", "outperform_se")),
    format_with_se(get_values(raw_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "outperform"), 
                   get_values(raw_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "outperform_se")),
    format_with_se(get_values(raw_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "outperform"), 
                   get_values(raw_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "outperform_se")),
    format_with_se(get_values(raw_t2_summary_journal, "journal_rank", "Other", "outperform"), 
                   get_values(raw_t2_summary_journal, "journal_rank", "Other", "outperform_se"))
  ),
  CAPM_Outperformance = c(
    format_with_se(get_values(capm_t2_summary_discipline, "discipline", "Finance", "outperform"), 
                   get_values(capm_t2_summary_discipline, "discipline", "Finance", "outperform_se")),
    format_with_se(get_values(capm_t2_summary_discipline, "discipline", "Accounting", "outperform"), 
                   get_values(capm_t2_summary_discipline, "discipline", "Accounting", "outperform_se")),
    format_with_se(get_values(capm_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "outperform"), 
                   get_values(capm_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "outperform_se")),
    format_with_se(get_values(capm_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "outperform"), 
                   get_values(capm_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "outperform_se")),
    format_with_se(get_values(capm_t2_summary_journal, "journal_rank", "Other", "outperform"), 
                   get_values(capm_t2_summary_journal, "journal_rank", "Other", "outperform_se"))
  ),
  FF3_Outperformance = c(
    format_with_se(get_values(ff3_t2_summary_discipline, "discipline", "Finance", "outperform"), 
                   get_values(ff3_t2_summary_discipline, "discipline", "Finance", "outperform_se")),
    format_with_se(get_values(ff3_t2_summary_discipline, "discipline", "Accounting", "outperform"), 
                   get_values(ff3_t2_summary_discipline, "discipline", "Accounting", "outperform_se")),
    format_with_se(get_values(ff3_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "outperform"), 
                   get_values(ff3_t2_summary_journal, "journal_rank", "JF, JFE, RFS", "outperform_se")),
    format_with_se(get_values(ff3_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "outperform"), 
                   get_values(ff3_t2_summary_journal, "journal_rank", "AR, JAR, JAE", "outperform_se")),
    format_with_se(get_values(ff3_t2_summary_journal, "journal_rank", "Other", "outperform"), 
                   get_values(ff3_t2_summary_journal, "journal_rank", "Other", "outperform_se"))
  )
)

# Export to CSV
file_suffix <- ifelse(filter_type == "return", paste0("_r", gsub("\\.", "", as.character(return_threshold))), paste0("_t", t_threshold_b))
write.csv(export_table_main, paste0(results_dir, "/Table_RiskAdjusted_TheoryModel", file_suffix, ".csv"), row.names = FALSE)
write.csv(export_table_discipline, paste0(results_dir, "/Table_RiskAdjusted_DisciplineJournal", file_suffix, ".csv"), row.names = FALSE)

# Also export the raw summary data for reference
write.csv(raw_t2_summary_theory, paste0(results_dir, "/Raw_TheoryGroup", file_suffix, ".csv"), row.names = FALSE)
write.csv(capm_t2_summary_theory, paste0(results_dir, "/CAPM_TheoryGroup", file_suffix, ".csv"), row.names = FALSE)
write.csv(ff3_t2_summary_theory, paste0(results_dir, "/FF3_TheoryGroup", file_suffix, ".csv"), row.names = FALSE)
write.csv(raw_t2_summary_model, paste0(results_dir, "/Raw_ModelGroup", file_suffix, ".csv"), row.names = FALSE)
write.csv(capm_t2_summary_model, paste0(results_dir, "/CAPM_ModelGroup", file_suffix, ".csv"), row.names = FALSE)
write.csv(ff3_t2_summary_model, paste0(results_dir, "/FF3_ModelGroup", file_suffix, ".csv"), row.names = FALSE)
write.csv(raw_t2_summary_discipline, paste0(results_dir, "/Raw_Discipline", file_suffix, ".csv"), row.names = FALSE)
write.csv(capm_t2_summary_discipline, paste0(results_dir, "/CAPM_Discipline", file_suffix, ".csv"), row.names = FALSE)
write.csv(ff3_t2_summary_discipline, paste0(results_dir, "/FF3_Discipline", file_suffix, ".csv"), row.names = FALSE)
write.csv(raw_t2_summary_journal, paste0(results_dir, "/Raw_Journal", file_suffix, ".csv"), row.names = FALSE)
write.csv(capm_t2_summary_journal, paste0(results_dir, "/CAPM_Journal", file_suffix, ".csv"), row.names = FALSE)
write.csv(ff3_t2_summary_journal, paste0(results_dir, "/FF3_Journal", file_suffix, ".csv"), row.names = FALSE)

cat("\n=== EXPORTED TABLES ===\n")
cat(paste("All files saved in:", results_dir, "\n"))
cat("\nMain summary tables:\n")
cat(paste0("- Table_RiskAdjusted_TheoryModel", file_suffix, ".csv\n"))
cat(paste0("- Table_RiskAdjusted_DisciplineJournal", file_suffix, ".csv\n"))
cat("\nDetailed breakdowns:\n")
cat("- Raw/CAPM/FF3 by TheoryGroup/ModelGroup/Discipline/Journal (12 files)\n")
cat("\nPlots generated:\n")
cat("- Fig_RiskAdj_raw_returns.pdf\n")
cat("- Fig_RiskAdj_capm_adjusted.pdf\n") 
cat("- Fig_RiskAdj_ff3_adjusted.pdf\n")
if (filter_type == "tstat") {
  cat("- Fig_RiskAdj_capm_t", t_threshold_a, ".pdf (t>=", t_threshold_a, " filtered)\n", sep="")
  cat("- Fig_RiskAdj_ff3_t", t_threshold_a, ".pdf (t>=", t_threshold_a, " filtered)\n", sep="")
}
cat("- Fig_RiskAdj_capm_t", t_threshold_b, ".pdf (t>=", t_threshold_b, " filtered)\n", sep="")
cat("- Fig_RiskAdj_ff3_t", t_threshold_b, ".pdf (t>=", t_threshold_b, " filtered)\n", sep="")
if("abnormal_capm_tv" %in% names(candidateReturns_adj)) {
  cat("- Fig_RiskAdj_capm_tv_t", t_threshold_b, ".pdf (time-varying, t>=", t_threshold_b, " filtered)\n", sep="")
  cat("- Fig_RiskAdj_ff3_tv_t", t_threshold_b, ".pdf (time-varying, t>=", t_threshold_b, " filtered)\n", sep="")
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
  "Trailing 5-Year CAPM Alpha (% of In-Sample Alpha)",
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
  "Trailing 5-Year FF3 Alpha (% of In-Sample Alpha)",
  filter_type = filter_type,
  return_threshold = return_threshold
)

} # End of t >= 1 analysis block

print("\nRisk-adjusted analysis completed successfully!")