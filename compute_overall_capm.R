# Compute exact overall CAPM values by re-running the relevant parts of both scripts

rm(list = ls())
source('0_Environment.R')

# Load helper functions
source('helpers/risk_adjusted_helpers_tv.R')
source('helpers/risk_adjusted_helpers_fs.R')

# Setup
DMname = paste0('../Data/Processed/', globalSettings$dataVersion, ' LongShort.RData')
DMshortname = DMname %>% str_remove('../Data/Processed/') %>% str_remove(' LongShort.RData')

# Load data
czret <- readRDS('../Data/Processed/czret_allpredictors.RDS')
risk_adj_file <- paste0('../Data/Processed/', DMshortname, ' MatchPubRiskAdjusted.RData')
candidateReturns_adj <- readRDS(risk_adj_file)

# Load signal mappings
signals_checked_csv <- "../Data/Raw/SignalsTheoryChecked.csv"
inclSignals <- restrictInclSignals(restrictType = globalSettings$restrictType, topT = globalSettings$topT)
mappings <- load_signal_mappings(signals_checked_csv, inclSignals)

# Get IS returns for published signals
czsum <- readRDS('../Data/Processed/czsum_allpredictors.RDS') %>%
  filter(signalname %in% inclSignals, Keep)

czret_is <- czret %>%
  filter(signalname %in% inclSignals) %>%
  inner_join(czsum %>% select(signalname, sampstart, sampend), by = "signalname") %>%
  mutate(ret_scaled = 100 * ret / insampMean) %>%
  rename(pubname = signalname)

# Compute published signal adjustments (same for both TV and FS)
ret_for_plot0 <- czret_is %>%
  group_by(pubname) %>%
  arrange(pubname, eventDate) %>%
  mutate(
    # Time-varying CAPM
    abnormal_capm_tv = ifelse(date >= sampstart & date <= sampend,
                               ret_scaled - beta_is * mktrf,
                               ret_scaled - beta_oos * mktrf),
    abnormal_capm_tv_normalized = 100 * abnormal_capm_tv / mean(abnormal_capm_tv[date >= sampstart & date <= sampend], na.rm = TRUE),
    
    # Full sample CAPM  
    abnormal_capm_fs = ret_scaled - beta_full * mktrf,
    abnormal_capm_fs_normalized = 100 * abnormal_capm_fs / mean(abnormal_capm_fs[date >= sampstart & date <= sampend], na.rm = TRUE)
  ) %>%
  ungroup()

# Apply t-stat filter
t_threshold <- 2
filtered_signals_raw <- unique(czret$signalname[czret$rbar_t > t_threshold])

# TIME-VARYING ANALYSIS
cat("\n=== TIME-VARYING CAPM ANALYSIS ===\n")

# Get TV filtered signals
filters_tv <- prepare_dm_filters(candidateReturns_adj, czret, "tstat", t_threshold)
dm_stats_tv <- filters_tv$dm_stats
setDT(dm_stats_tv)
filtered_dm_capm_tv <- dm_stats_tv[abar_capm_tv_dm_t > t_threshold]
pub_signals_capm_tv <- unique(czret[abnormal_capm_is_t > t_threshold]$signalname)
dm_signals_capm_tv <- filtered_dm_capm_tv[, .(actSignal, candSignalname)]
candidateReturns_capm_tv <- candidateReturns_adj[dm_signals_capm_tv, on = .(actSignal, candSignalname)]

# Aggregate TV DM returns
dm_capm_tv_t2 <- normalize_and_aggregate_dm(
  candidateReturns_capm_tv,
  "abnormal_capm_tv",
  "capm_tv_t2_normalized"
)

# Create TV plot data
ret_for_plot0_capm_tv_t2 <- create_filtered_plot_data(
  ret_for_plot0,
  intersect(pub_signals_capm_tv, filtered_signals_raw),
  dm_capm_tv_t2,
  "abnormal_capm_tv_normalized",
  "matchRet_capm_tv_t2_normalized",
  "capm_tv_t2"
)

# Compute TV overall summary
overall_tv <- compute_overall_summary(
  plot_data = ret_for_plot0_capm_tv_t2,
  ret_col = "abnormal_capm_tv_normalized",
  dm_col = "matchRet_capm_tv_t2_normalized"
)

cat("Number of signals:", length(unique(ret_for_plot0_capm_tv_t2$pubname)), "\n")
cat("Published (Post-Sample):", sprintf("%.3f", overall_tv$pub_oos), "\n")
cat("Data-mined (Post-Sample):", sprintf("%.3f", overall_tv$dm_oos), "\n")
cat("Outperformance:", sprintf("%.3f", overall_tv$outperform), "\n")
cat("Standard errors: Published", sprintf("%.3f", overall_tv$pub_oos_se), 
    "Outperformance", sprintf("%.3f", overall_tv$outperform_se), "\n")

# FULL SAMPLE ANALYSIS
cat("\n=== FULL SAMPLE CAPM ANALYSIS ===\n")

# Get FS filtered signals
filters_fs <- prepare_dm_filters_fs(candidateReturns_adj, czret, "tstat", t_threshold)
dm_stats_fs <- filters_fs$dm_stats
setDT(dm_stats_fs)
filtered_dm_capm_fs <- dm_stats_fs[abar_capm_fs_dm_t > t_threshold]
pub_signals_capm_fs <- unique(czret[abnormal_capm_full_t > t_threshold]$signalname)
dm_signals_capm_fs <- filtered_dm_capm_fs[, .(actSignal, candSignalname)]
candidateReturns_capm_fs <- candidateReturns_adj[dm_signals_capm_fs, on = .(actSignal, candSignalname)]

# Aggregate FS DM returns
dm_capm_fs_t2 <- normalize_and_aggregate_dm(
  candidateReturns_capm_fs,
  "abnormal_capm",
  "capm_fs_t2_normalized"
)

# Create FS plot data
ret_for_plot0_capm_fs_t2 <- create_filtered_plot_data(
  ret_for_plot0,
  intersect(pub_signals_capm_fs, filtered_signals_raw),
  dm_capm_fs_t2,
  "abnormal_capm_fs_normalized",
  "matchRet_capm_fs_t2_normalized",
  "capm_fs_t2"
)

# Compute FS overall summary
overall_fs <- compute_overall_summary_fs(
  plot_data = ret_for_plot0_capm_fs_t2,
  ret_col = "abnormal_capm_fs_normalized",
  dm_col = "matchRet_capm_fs_t2_normalized"
)

cat("Number of signals:", length(unique(ret_for_plot0_capm_fs_t2$pubname)), "\n")
cat("Published (Post-Sample):", sprintf("%.3f", overall_fs$pub_oos), "\n")
cat("Data-mined (Post-Sample):", sprintf("%.3f", overall_fs$dm_oos), "\n")
cat("Outperformance:", sprintf("%.3f", overall_fs$outperform), "\n")
cat("Standard errors: Published", sprintf("%.3f", overall_fs$pub_oos_se),
    "Outperformance", sprintf("%.3f", overall_fs$outperform_se), "\n")

# COMPARISON
cat("\n=== COMPARISON (TV vs FS) ===\n")
cat("Published Return - TV:", sprintf("%.3f", overall_tv$pub_oos), "vs FS:", sprintf("%.3f", overall_fs$pub_oos), 
    "Diff:", sprintf("%.3f", overall_tv$pub_oos - overall_fs$pub_oos), "\n")
cat("DM Return - TV:", sprintf("%.3f", overall_tv$dm_oos), "vs FS:", sprintf("%.3f", overall_fs$dm_oos),
    "Diff:", sprintf("%.3f", overall_tv$dm_oos - overall_fs$dm_oos), "\n")
cat("Outperformance - TV:", sprintf("%.3f", overall_tv$outperform), "vs FS:", sprintf("%.3f", overall_fs$outperform),
    "Diff:", sprintf("%.3f", overall_tv$outperform - overall_fs$outperform), "\n")

# Check signal composition
tv_signals <- unique(ret_for_plot0_capm_tv_t2$pubname)
fs_signals <- unique(ret_for_plot0_capm_fs_t2$pubname)
cat("\n=== SIGNAL COMPOSITION ===\n")
cat("Signals in TV only:", length(setdiff(tv_signals, fs_signals)), "\n")
cat("Signals in FS only:", length(setdiff(fs_signals, tv_signals)), "\n")
cat("Signals in both:", length(intersect(tv_signals, fs_signals)), "\n")