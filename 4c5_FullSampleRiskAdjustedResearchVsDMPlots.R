# Full Sample Risk-Adjusted Data-mining comparisons 
# Based on 4c4_RiskAdjustedResearchVsDMPlotsTV.R but with full sample CAPM and FF3 adjustments
# This file compares raw vs full sample risk-adjusted returns for published vs data-mined signals
#
# FIXED: Full sample beta/alpha consistency between published and DM signals
# - Published signals use full sample betas (from sampstart onwards)
# - DM signals use full sample betas (consistent with published)
# - Both use same IS/OOS period definitions for adjustments

# Setup ----------------------------------------------------------------
rm(list = ls())
source("0_Environment.R")
source("helpers/risk_adjusted_helpers_fs.R")

t_threshold = 2

# Filter type fixed to t-stat only
filter_type <- "tstat"

# Create results subfolder for full sample risk-adjusted analysis
base_results_dir <- "../Results/RiskAdjusted"
results_dir <- file.path(base_results_dir, "FullSampleTstatFilter")

if (!dir.exists(results_dir)) {
  dir.create(results_dir, recursive = TRUE)
  cat("Created directory:", results_dir, "\n")
} else {
  cat("Using existing directory:", results_dir, "\n")
}

# Check if full sample risk-adjusted DM files exist
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
    czret %>% select(signalname, eventDate, date, mktrf, smb, hml, sampstart, sampend),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  )

## Full sample CAPM/FF3 adjustments
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

# Compute published-side full sample CAPM/FF3 betas and normalized alphas
# Use full sample betas (from sampstart onwards) for all periods
# FIXED: Use sampstart onwards for full sample consistency with DM signals
betas_capm_fs <- czret[date >= sampstart & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_fs = extract_beta(ret, mktrf)), by = signalname]
czret <- merge(czret, betas_capm_fs, by = 'signalname', all.x = TRUE)
czret[, abnormal_capm := ret - beta_capm_fs * mktrf]
# FIXED: Use sampstart/sampend periods for computing in-sample statistics
czret[date >= sampstart & date <= sampend, `:=`(
  abar_capm_fs = mean(abnormal_capm, na.rm = TRUE),
  abar_capm_fs_t = {
    m <- mean(abnormal_capm, na.rm = TRUE)
    s <- sd(abnormal_capm, na.rm = TRUE)
    n <- sum(!is.na(abnormal_capm))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_capm_fs := nafill(abar_capm_fs, 'locf'), by = .(signalname)]
czret[, abar_capm_fs_t := nafill(abar_capm_fs_t, 'locf'), by = .(signalname)]
czret[, abnormal_capm_fs_normalized := ifelse(abs(abar_capm_fs) > 1e-10, 100 * abnormal_capm / abar_capm_fs, NA_real_)]

ff3_fs <- czret[date >= sampstart & !is.na(ret) & !is.na(mktrf) & !is.na(smb) & !is.na(hml), {
  coeffs <- extract_ff3_coeffs(ret, mktrf, smb, hml)
  .(beta_ff3_fs = coeffs[1], s_ff3_fs = coeffs[2], h_ff3_fs = coeffs[3])
}, by = signalname]
czret <- merge(czret, ff3_fs, by = 'signalname', all.x = TRUE)
czret[, abnormal_ff3 := ret - (beta_ff3_fs * mktrf + s_ff3_fs * smb + h_ff3_fs * hml)]
# FIXED: Use sampstart/sampend periods for computing in-sample statistics
czret[date >= sampstart & date <= sampend, `:=`(
  abar_ff3_fs = mean(abnormal_ff3, na.rm = TRUE),
  abar_ff3_fs_t = {
    m <- mean(abnormal_ff3, na.rm = TRUE)
    s <- sd(abnormal_ff3, na.rm = TRUE)
    n <- sum(!is.na(abnormal_ff3))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_ff3_fs := nafill(abar_ff3_fs, 'locf'), by = .(signalname)]
czret[, abar_ff3_fs_t := nafill(abar_ff3_fs_t, 'locf'), by = .(signalname)]
czret[, abnormal_ff3_fs_normalized := ifelse(abs(abar_ff3_fs) > 1e-10, 100 * abnormal_ff3 / abar_ff3_fs, NA_real_)]

# Compute normalized DM abnormal returns ---------------------------------

# First, create the risk-adjusted data for plotting by joining with `czret`
ret_for_plot0_adj <- ret_for_plot0 %>%
  left_join(
    czret %>% select(signalname, eventDate,
                      abnormal_capm, abnormal_ff3,
                      abnormal_capm_fs_normalized, abnormal_ff3_fs_normalized),
    by = c("pubname" = "signalname", "eventDate" = "eventDate")
  )

# Default Plot Settings --------------------------------------------------
fontsizeall = 28
ylaball = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5
global_xl = -360  
global_xh = 300   

print("Full sample risk adjustments computed successfully!")
print(paste("Number of signals with full sample CAPM adjustments:", sum(!is.na(czret$beta_capm_fs))))
print(paste("Number of signals with full sample FF3 adjustments:", sum(!is.na(czret$beta_ff3_fs))))

# Full Sample Risk-Adjusted Plots ----------------------------------------------------

## 1. Raw Returns (baseline from 4c2) ------------------------------------
# Filter published signals by raw t-stat threshold for raw plot
signals_raw_t2_plot <- unique(czret[rbar_t > t_threshold]$signalname)
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
      paste0("Published (Raw, t>", t_threshold, ")"),
      paste0("Data-Mined (Raw, t>", t_threshold, ")"),
      'N/A'
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

ggsave(filename = paste0(results_dir, "/Fig_FullSampleRiskAdj_", tempsuffix, '.pdf'), 
       printme_raw, width = 10, height = 8)

# Print summary statistics (filtered by raw t)
cat("\n=== RAW RETURNS PLOT STATISTICS (t > ", t_threshold, ") ===\n", sep = "")
ret_for_plot0 %>% 
  filter(!is.na(matchRet), pubname %in% signals_raw_t2_plot) %>%
  summarise(
    # FIXED: Use sampstart/sampend-based periods for consistency
    pub_mean_insamp = mean(ret[date >= sampstart & date <= sampend], na.rm = TRUE),
    pub_mean_oos = mean(ret[date > sampend], na.rm = TRUE),
    dm_mean_insamp = mean(matchRet[date >= sampstart & date <= sampend], na.rm = TRUE),
    dm_mean_oos = mean(matchRet[date > sampend], na.rm = TRUE)
  ) %>% print()

cat("\nNumber of signals:", length(unique(ret_for_plot0$pubname[!is.na(ret_for_plot0$matchRet) & ret_for_plot0$pubname %in% signals_raw_t2_plot])), "\n")

# Filtered versions (t-stat or return) -----------------------------------------------

cat("\n\n=== T-STAT FILTERED ANALYSIS (t > ", t_threshold, ") ===\n")

cat("\nComputing statistics for individual DM signals...\n")
filters <- prepare_dm_filters_fs(
  candidateReturns_adj = candidateReturns_adj,
  czret = czret,
  filter_type = filter_type,
  t_threshold = t_threshold
)

dm_stats <- filters$dm_stats
signals_raw_t2 <- filters$signals_raw

cat("\nNumber of PUBLISHED signals with t > ", t_threshold, ":\n")
cat("Raw returns:", length(signals_raw_t2), "\n")

# Load model categories EARLY (needed for full sample and other analyses)
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

cat("\n\n=== FULL SAMPLE ABNORMAL RETURNS (FULL SAMPLE BETAS) ===\n")

groups_theory <- c("Risk", "Mispricing", "Agnostic")
groups_model <- c("No Model", "Stylized", "Dynamic or Quantitative")

# Check if full sample columns exist in the data
if("abnormal_capm" %in% names(candidateReturns_adj) && "abnormal_ff3" %in% names(candidateReturns_adj)) {
  
  # Compute statistics for full sample abnormal returns with proper NA handling
  # FIXED: Use sampstart/sampend-based periods for consistency with published signals
  dm_stats_fs <- candidateReturns_adj[
    (date >= sampstart & date <= sampend) & !is.na(abnormal_capm),
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
      },
      abar_capm_fs_dm = mean(abnormal_capm, na.rm = TRUE),
      abar_ff3_fs_dm = mean(abnormal_ff3, na.rm = TRUE)
    ),
    by = .(actSignal, candSignalname)
  ]
  
  # NEW: Define published signal sets using alpha stats (t-stat only)
  signals_capm_fs_t2 <- unique(czret[abar_capm_fs_t > t_threshold]$signalname)
  signals_ff3_fs_t2  <- unique(czret[abar_ff3_fs_t  > t_threshold]$signalname)
  
  # Additionally require published signals to pass the raw filter for comparability
  signals_capm_fs_t2 <- intersect(signals_capm_fs_t2, signals_raw_t2)
  signals_ff3_fs_t2  <- intersect(signals_ff3_fs_t2, signals_raw_t2)
  
  # CAPM full sample filtering (t-stat only)
  cat("\n=== CAPM FULL SAMPLE (t > ", t_threshold, ") STATISTICS ===\n")
  dm_filtered_capm_fs <- candidateReturns_adj %>%
    inner_join(
      dm_stats_fs %>% filter(abar_capm_fs_dm_t > t_threshold),
      by = c("actSignal", "candSignalname")
    )
  
  # Normalize and aggregate
  dm_capm_fs_aggregated <- normalize_and_aggregate_dm_fs(
    dm_filtered_capm_fs, 
    "abnormal_capm", 
    "capm_fs_t2_normalized"
  )
  
  # Create filtered plot data
  ret_for_plot0_capm_fs_t2 <- create_filtered_plot_data_fs(
    ret_for_plot0_adj,
    signals_capm_fs_t2,  # Use FS-filtered published signals
    dm_capm_fs_aggregated,
    "abnormal_capm_fs_normalized",  # Use published FS-normalized alpha
    "matchRet_capm_fs_t2_normalized",
    "capm_fs_t2_normalized"
  )
  
  cat("Published signals with CAPM t > ", t_threshold, ":", length(signals_capm_fs_t2), "\n")
  cat("Published signals with filtered DM matches (full sample):", length(unique(ret_for_plot0_capm_fs_t2$pubname)), "\n")
  cat("DM signals with full sample CAPM t > ", t_threshold, ":", sum(dm_stats_fs$abar_capm_fs_dm_t > t_threshold, na.rm = TRUE), "\n")
  
  # Create and save plot
  printme_capm_fs_t2 <- create_full_sample_risk_adjusted_plot(
    ret_for_plot0_capm_fs_t2,
    "abnormal_capm_fs_normalized",
    "matchRet_capm_fs_t2_normalized",
    "CAPM Alpha",
    t_threshold,
    "Trailing 5-Year CAPM Alpha",
    filter_type = filter_type
  )
  
  # FF3 full sample filtering (t-stat only)
  cat("\n=== FF3 FULL SAMPLE (t > ", t_threshold, ") STATISTICS ===\n")
  dm_filtered_ff3_fs <- candidateReturns_adj %>%
    inner_join(
      dm_stats_fs %>% filter(abar_ff3_fs_dm_t > t_threshold),
      by = c("actSignal", "candSignalname")
    )
  
  # Normalize and aggregate
  dm_ff3_fs_aggregated <- normalize_and_aggregate_dm_fs(
    dm_filtered_ff3_fs, 
    "abnormal_ff3", 
    "ff3_fs_t2_normalized"
  )
  
  # Create filtered plot data
  ret_for_plot0_ff3_fs_t2 <- create_filtered_plot_data_fs(
    ret_for_plot0_adj,
    signals_ff3_fs_t2,  # Use FS-filtered published signals
    dm_ff3_fs_aggregated,
    "abnormal_ff3_fs_normalized",  # Use published FS-normalized alpha
    "matchRet_ff3_fs_t2_normalized",
    "ff3_fs_t2_normalized"
  )
  
  cat("Published signals with FF3 t > ", t_threshold, ":", length(signals_ff3_fs_t2), "\n")
  cat("Published signals with filtered DM matches (full sample):", length(unique(ret_for_plot0_ff3_fs_t2$pubname)), "\n")
  cat("DM signals with full sample FF3 t > ", t_threshold, ":", sum(dm_stats_fs$abar_ff3_fs_dm_t > t_threshold, na.rm = TRUE), "\n")
  
  # Create and save plot
  printme_ff3_fs_t2 <- create_full_sample_risk_adjusted_plot(
    ret_for_plot0_ff3_fs_t2,
    "abnormal_ff3_fs_normalized",
    "matchRet_ff3_fs_t2_normalized",
    "FF3 Alpha",
    t_threshold,
    "Trailing 5-Year FF3 Alpha",
    filter_type = filter_type
  )
  
  # Create Full Sample Alpha Summary Tables
  cat("\n\n=== FULL SAMPLE ALPHA SUMMARY TABLES ===\n")
  
  # Prepare data for alpha summary tables
  fs_plot_data <- list()
  
  # Add raw data for comparison (filtered by the same signals) - t-stat only
  fs_plot_data[["raw"]] <- ret_for_plot0 %>% 
    filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), 
              by = c("pubname" = "signalname")) %>%
    filter(rbar_t > t_threshold)
  
  fs_plot_data[["capm_fs"]] <- ret_for_plot0_capm_fs_t2
  fs_plot_data[["ff3_fs"]] <- ret_for_plot0_ff3_fs_t2
  
  # Create mappings (reuse existing ones from main analysis)
  fs_mappings <- list(
    theory = theory_mapping,
    model = model_mapping
  )
  
  # Create alpha summaries
  fs_summaries <- create_summary_tables_fs(
    fs_plot_data,
    fs_mappings,
    table_name = "Full Sample Alpha Analysis",
    filter_desc = paste0("t > ", t_threshold)
  )
  
  # Print alpha summary by theory
  print_summary_table_fs(
    fs_summaries[["theory"]],
    groups = c("Risk", "Mispricing", "Agnostic"),
    group_col = "theory_group",
    table_title = "FULL SAMPLE ALPHA BY THEORETICAL FOUNDATION",
    analysis_types = c("raw", "capm_fs", "ff3_fs"),
    analysis_labels = c("Raw", "CAPM", "FF3")
  )
  
  # Print alpha summary by model
  print_summary_table_fs(
    fs_summaries[["model"]],
    groups = c("No Model", "Stylized", "Dynamic or Quantitative"),
    group_col = "modeltype_grouped",
    table_title = "FULL SAMPLE ALPHA BY MODELING FORMALISM",
    analysis_types = c("raw", "capm_fs", "ff3_fs"),
    analysis_labels = c("Raw", "CAPM", "FF3")
  )
  
  # Export alpha tables
  export_filename <- paste0(results_dir, "/fs_alpha_summary_", paste0("t", t_threshold), ".csv")
  export_summary_tables_fs(fs_summaries, export_filename, 
                          filter_desc = paste0("T-stat > ", t_threshold))
  
} else {
  cat("\nFull sample abnormal returns not available in the data.\n")
  cat("Please run 2d_RiskAdjustDataMinedSignals.R with the updated code to generate these columns.\n")
}

# T-STAT FILTERED SUMMARY TABLE (t > t_threshold) ----------------------------------------
cat("\n\n=== SUMMARY TABLE WITH T > ", t_threshold, " FILTER ===\n")

# Raw returns filter (by theory) t-stat only
raw_t2_summary_theory <- compute_outperformance_fs(
  ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_t > t_threshold), 
  "ret", "matchRet", theory_mapping, "theory_group"
)

# Raw returns filter (by model) t-stat only
raw_t2_summary_model <- compute_outperformance_fs(
  ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_t > t_threshold), 
  "ret", "matchRet", model_mapping, "modeltype_grouped"
)

# Overall filtered summaries (t-stat only)
filtered_signals_raw <- czret$signalname[czret$rbar_t > t_threshold]

overall_t2_summary_raw <- compute_overall_summary_fs(
  plot_data = ret_for_plot0 %>% filter(!is.na(matchRet), pubname %in% filtered_signals_raw),
  ret_col = "ret",
  dm_col = "matchRet"
)

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
raw_t2_summary_discipline <- discipline_data %>%
  group_by(discipline) %>%
  summarise(
    n_signals = n_distinct(pubname),
    # FIXED: Use sampstart/sampend-based periods for consistency
    pub_oos = mean(ret[date > sampend], na.rm = TRUE),
    pub_oos_se = {
      n <- sum(date > sampend & !is.na(ret))
      if (n > 1) sd(ret[date > sampend], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    dm_oos = mean(matchRet[date > sampend], na.rm = TRUE),
    dm_oos_se = {
      n <- sum(date > sampend & !is.na(matchRet))
      if (n > 1) sd(matchRet[date > sampend], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    outperform = pub_oos - dm_oos,
    outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
    .groups = 'drop'
  )

# Filter data to exclude Economics journals
journal_mapping_filtered <- journal_mapping %>% filter(journal_rank != "Economics")

# Raw returns filter (by journal) - excluding Economics (t-stat only)
journal_data <- ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
  left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
  filter(rbar_t > t_threshold) %>%
  inner_join(journal_mapping_filtered, by = c("pubname" = "signalname"))

raw_t2_summary_journal <- journal_data %>%
  group_by(journal_rank) %>%
  summarise(
    n_signals = n_distinct(pubname),
    # FIXED: Use sampstart/sampend-based periods for consistency
    pub_oos = mean(ret[date > sampend], na.rm = TRUE),
    pub_oos_se = {
      n <- sum(date > sampend & !is.na(ret))
      if (n > 1) sd(ret[date > sampend], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    dm_oos = mean(matchRet[date > sampend], na.rm = TRUE),
    dm_oos_se = {
      n <- sum(date > sampend & !is.na(matchRet))
      if (n > 1) sd(matchRet[date > sampend], na.rm = TRUE) / sqrt(n) else NA_real_
    },
    outperform = pub_oos - dm_oos,
    outperform_se = sqrt(pub_oos_se^2 + dm_oos_se^2),
    .groups = 'drop'
  )

# Full sample summaries (if available)
if("abnormal_capm" %in% names(candidateReturns_adj) && exists("ret_for_plot0_capm_fs_t2")) {
  # CAPM full sample t > t_threshold filtered (by theory)
  capm_fs_t2_summary_theory <- compute_outperformance_fs(
    ret_for_plot0_capm_fs_t2,
    "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized", theory_mapping, "theory_group"
  )
  
  # FF3 full sample t > t_threshold filtered (by theory)
  ff3_fs_t2_summary_theory <- compute_outperformance_fs(
    ret_for_plot0_ff3_fs_t2,
    "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized", theory_mapping, "theory_group"
  )
  
  # CAPM full sample t > t_threshold filtered (by model)
  capm_fs_t2_summary_model <- compute_outperformance_fs(
    ret_for_plot0_capm_fs_t2,
    "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized", model_mapping, "modeltype_grouped"
  )
  
  # FF3 full sample t > t_threshold filtered (by model)
  ff3_fs_t2_summary_model <- compute_outperformance_fs(
    ret_for_plot0_ff3_fs_t2,
    "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized", model_mapping, "modeltype_grouped"
  )
  
  # Overall full sample summaries
overall_t2_summary_capm_fs <- compute_overall_summary_fs(
  plot_data = ret_for_plot0_capm_fs_t2,
  ret_col = "abnormal_capm_fs_normalized",
  dm_col = "matchRet_capm_fs_t2_normalized"
)

overall_t2_summary_ff3_fs <- compute_overall_summary_fs(
  plot_data = ret_for_plot0_ff3_fs_t2,
  ret_col = "abnormal_ff3_fs_normalized",
  dm_col = "matchRet_ff3_fs_t2_normalized"
)

# CAPM full sample t > t_threshold filtered (by discipline)
capm_fs_t2_summary_discipline <- compute_outperformance_fs(
  ret_for_plot0_capm_fs_t2,
  "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized", discipline_mapping_filtered, "discipline"
)

# FF3 full sample t > t_threshold filtered (by discipline)
ff3_fs_t2_summary_discipline <- compute_outperformance_fs(
  ret_for_plot0_ff3_fs_t2,
  "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized", discipline_mapping_filtered, "discipline"
)

# CAPM full sample t > t_threshold filtered (by journal)
capm_fs_t2_summary_journal <- compute_outperformance_fs(
  ret_for_plot0_capm_fs_t2,
  "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized", journal_mapping_filtered, "journal_rank"
)

# FF3 full sample t > t_threshold filtered (by journal)
ff3_fs_t2_summary_journal <- compute_outperformance_fs(
  ret_for_plot0_ff3_fs_t2,
  "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized", journal_mapping_filtered, "journal_rank"
)

# Create Any Model vs No Model mapping
anymodel_mapping <- czcat_full %>%
  transmute(
    signalname,
    model_binary = case_when(
      NoModel == 1 ~ "No Model",
      TRUE ~ "Any Model"
    )
  )

# CAPM full sample t > t_threshold filtered (by model binary)
capm_fs_t2_summary_anymodel <- compute_outperformance_fs(
  ret_for_plot0_capm_fs_t2,
  "abnormal_capm_fs_normalized", "matchRet_capm_fs_t2_normalized", anymodel_mapping, "model_binary"
)

# FF3 full sample t > t_threshold filtered (by model binary)
ff3_fs_t2_summary_anymodel <- compute_outperformance_fs(
  ret_for_plot0_ff3_fs_t2,
  "abnormal_ff3_fs_normalized", "matchRet_ff3_fs_t2_normalized", anymodel_mapping, "model_binary"
)
}

# Print summary tables
cat("\n\n=== FULL SAMPLE ABNORMAL RETURNS TABLE (t > ", t_threshold, ") ===\n")
cat("\nPost-Sample Return (t>", t_threshold, ")     Outperformance vs Data-Mining (t>", t_threshold, ")\n")
cat("                       CAPM  FF3        CAPM  FF3\n")
cat("Theoretical Foundation\n")

for(group in groups_theory) {
  # Post-sample returns
  capm_fs_ret <- round(get_values(capm_fs_t2_summary_theory, "theory_group", group, "pub_oos"))  
  ff3_fs_ret <- round(get_values(ff3_fs_t2_summary_theory, "theory_group", group, "pub_oos"))
  
  # Outperformance
  capm_fs_out <- round(get_values(capm_fs_t2_summary_theory, "theory_group", group, "outperform"))
  ff3_fs_out <- round(get_values(ff3_fs_t2_summary_theory, "theory_group", group, "outperform"))
  
  # Standard errors  
  capm_fs_se <- round(get_values(capm_fs_t2_summary_theory, "theory_group", group, "pub_oos_se"))
  ff3_fs_se <- round(get_values(ff3_fs_t2_summary_theory, "theory_group", group, "pub_oos_se"))
  
  capm_fs_out_se <- round(get_values(capm_fs_t2_summary_theory, "theory_group", group, "outperform_se"))
  ff3_fs_out_se <- round(get_values(ff3_fs_t2_summary_theory, "theory_group", group, "outperform_se"))
  
  cat(sprintf("%-12s           %4s     %4s          %4s     %4s\n",
              group, capm_fs_ret, ff3_fs_ret, capm_fs_out, ff3_fs_out))
  cat(sprintf("%-12s           (%2s)    (%2s)          (%2s)    (%2s)\n",
              "", capm_fs_se, ff3_fs_se, capm_fs_out_se, ff3_fs_out_se))
}

cat("\nModeling Formalism\n")
groups_model <- c("No Model", "Stylized", "Dynamic or Quantitative")
for(group in groups_model) {
  # Post-sample returns
  capm_fs_ret <- round(get_values(capm_fs_t2_summary_model, "modeltype_grouped", group, "pub_oos"))
  ff3_fs_ret <- round(get_values(ff3_fs_t2_summary_model, "modeltype_grouped", group, "pub_oos"))
  
  # Outperformance
  capm_fs_out <- round(get_values(capm_fs_t2_summary_model, "modeltype_grouped", group, "outperform"))
  ff3_fs_out <- round(get_values(ff3_fs_t2_summary_model, "modeltype_grouped", group, "outperform"))
  
  # Standard errors
  capm_fs_se <- round(get_values(capm_fs_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  ff3_fs_se <- round(get_values(capm_fs_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"))
  
  capm_fs_out_se <- round(get_values(capm_fs_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
  ff3_fs_out_se <- round(get_values(capm_fs_t2_summary_model, "modeltype_grouped", group, "outperform_se"))
  
  cat(sprintf("%-23s    %4s     %4s          %4s     %4s\n",
              group, capm_fs_ret, ff3_fs_ret, capm_fs_out, ff3_fs_out))
  cat(sprintf("%-23s    (%2s)    (%2s)          (%2s)    (%2s)\n",
              "", capm_fs_se, ff3_fs_se, capm_fs_out_se, ff3_fs_out_se))
}

# Overall
cat(sprintf("%-12s           %4s     %4s          %4s     %4s\n",
            "All", round(overall_t2_summary_capm_fs$pub_oos), round(overall_t2_summary_ff3_fs$pub_oos),
            round(overall_t2_summary_capm_fs$outperform), round(overall_t2_summary_ff3_fs$outperform)))
cat(sprintf("%-12s           (%2s)    (%2s)          (%2s)    (%2s)\n",
            "", round(overall_t2_summary_capm_fs$pub_oos_se), round(overall_t2_summary_ff3_fs$pub_oos_se), 
            round(overall_t2_summary_capm_fs$outperform_se), round(overall_t2_summary_ff3_fs$outperform_se)))

# Store full sample data for LaTeX export
fs_theory_data <- list()
for(i in 1:length(groups_theory)) {
  group <- groups_theory[i]
  fs_theory_data[[i]] <- list(
    # Raw
    raw_pub_oos = get_values(raw_t2_summary_theory, "theory_group", group, "pub_oos"),
    raw_pub_oos_se = get_values(raw_t2_summary_theory, "theory_group", group, "pub_oos_se"),
    raw_outperform = get_values(raw_t2_summary_theory, "theory_group", group, "outperform"),
    raw_outperform_se = get_values(raw_t2_summary_theory, "theory_group", group, "outperform_se"),
    
    # CAPM
    capm_fs_pub_oos = get_values(capm_fs_t2_summary_theory, "theory_group", group, "pub_oos"),
    capm_fs_pub_oos_se = get_values(capm_fs_t2_summary_theory, "theory_group", group, "pub_oos_se"),
    capm_fs_outperform = get_values(capm_fs_t2_summary_theory, "theory_group", group, "outperform"),
    capm_fs_outperform_se = get_values(capm_fs_t2_summary_theory, "theory_group", group, "outperform_se"),
    
    # FF3
    ff3_fs_pub_oos = get_values(ff3_fs_t2_summary_theory, "theory_group", group, "pub_oos"),
    ff3_fs_pub_oos_se = get_values(ff3_fs_t2_summary_theory, "theory_group", group, "pub_oos_se"),
    ff3_fs_outperform = get_values(ff3_fs_t2_summary_theory, "theory_group", group, "outperform"),
    ff3_fs_outperform_se = get_values(ff3_fs_t2_summary_theory, "theory_group", group, "outperform_se")
  )
}

fs_model_data <- list()
for(i in 1:length(groups_model)) {
  group <- groups_model[i]
  fs_model_data[[i]] <- list(
    # Raw
    raw_pub_oos = get_values(raw_t2_summary_model, "modeltype_grouped", group, "pub_oos"),
    raw_pub_oos_se = get_values(raw_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"),
    raw_outperform = get_values(raw_t2_summary_model, "modeltype_grouped", group, "outperform"),
    raw_outperform_se = get_values(raw_t2_summary_model, "modeltype_grouped", group, "outperform_se"),
    
    # CAPM
    capm_fs_pub_oos = get_values(capm_fs_t2_summary_model, "modeltype_grouped", group, "pub_oos"),
    capm_fs_pub_oos_se = get_values(capm_fs_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"),
    capm_fs_outperform = get_values(capm_fs_t2_summary_model, "modeltype_grouped", group, "outperform"),
    capm_fs_outperform_se = get_values(capm_fs_t2_summary_model, "modeltype_grouped", group, "outperform_se"),
    
    # FF3
    ff3_fs_pub_oos = get_values(ff3_fs_t2_summary_model, "modeltype_grouped", group, "pub_oos"),
    ff3_fs_pub_oos_se = get_values(ff3_fs_t2_summary_model, "modeltype_grouped", group, "pub_oos_se"),
    ff3_fs_outperform = get_values(ff3_fs_t2_summary_model, "modeltype_grouped", group, "outperform"),
    ff3_fs_outperform_se = get_values(ff3_fs_t2_summary_model, "modeltype_grouped", group, "outperform_se")
  )
}

# Add overall results
fs_overall_data <- list(
  # Raw
  raw_pub_oos = overall_t2_summary_raw$pub_oos,
  raw_pub_oos_se = overall_t2_summary_raw$pub_oos_se,
  raw_outperform = overall_t2_summary_raw$outperform,
  raw_outperform_se = overall_t2_summary_raw$outperform_se,
  
  # CAPM
  capm_fs_pub_oos = overall_t2_summary_capm_fs$pub_oos,
  capm_fs_pub_oos_se = overall_t2_summary_capm_fs$pub_oos_se,
  capm_fs_outperform = overall_t2_summary_capm_fs$outperform,
  capm_fs_outperform_se = overall_t2_summary_capm_fs$outperform_se,
  
  # FF3
  ff3_fs_pub_oos = overall_t2_summary_ff3_fs$pub_oos,
  ff3_fs_pub_oos_se = overall_t2_summary_ff3_fs$pub_oos_se,
  ff3_fs_outperform = overall_t2_summary_ff3_fs$outperform,
  ff3_fs_outperform_se = overall_t2_summary_ff3_fs$outperform_se
)

# Collect full sample discipline data
fs_discipline_data <- list()
discipline_groups <- c("Finance", "Accounting")
for(i in 1:length(discipline_groups)) {
  group <- discipline_groups[i]
  fs_discipline_data[[i]] <- list(
    # Raw
    raw_pub_oos = get_values(raw_t2_summary_discipline, "discipline", group, "pub_oos"),
    raw_pub_oos_se = get_values(raw_t2_summary_discipline, "discipline", group, "pub_oos_se"),
    raw_outperform = get_values(raw_t2_summary_discipline, "discipline", group, "outperform"),
    raw_outperform_se = get_values(raw_t2_summary_discipline, "discipline", group, "outperform_se"),
    
    # CAPM
    capm_fs_pub_oos = get_values(capm_fs_t2_summary_discipline, "discipline", group, "pub_oos"),
    capm_fs_pub_oos_se = get_values(capm_fs_t2_summary_discipline, "discipline", group, "pub_oos_se"),
    capm_fs_outperform = get_values(capm_fs_t2_summary_discipline, "discipline", group, "outperform"),
    capm_fs_outperform_se = get_values(capm_fs_t2_summary_discipline, "discipline", group, "outperform_se"),
    
    # FF3
    ff3_fs_pub_oos = get_values(ff3_fs_t2_summary_discipline, "discipline", group, "pub_oos"),
    ff3_fs_pub_oos_se = get_values(ff3_fs_t2_summary_discipline, "discipline", group, "pub_oos_se"),
    ff3_fs_outperform = get_values(ff3_fs_t2_summary_discipline, "discipline", group, "outperform"),
    ff3_fs_outperform_se = get_values(ff3_fs_t2_summary_discipline, "discipline", group, "outperform_se")
  )
}

# Collect full sample journal data
fs_journal_data <- list()
journal_groups <- c("JF, JFE, RFS", "AR, JAR, JAE", "Other")
for(i in 1:length(journal_groups)) {
  group <- journal_groups[i]
  fs_journal_data[[i]] <- list(
    # Raw
    raw_pub_oos = get_values(raw_t2_summary_journal, "journal_rank", group, "pub_oos"),
    raw_pub_oos_se = get_values(raw_t2_summary_journal, "journal_rank", group, "pub_oos_se"),
    raw_outperform = get_values(raw_t2_summary_journal, "journal_rank", group, "outperform"),
    raw_outperform_se = get_values(raw_t2_summary_journal, "journal_rank", group, "outperform_se"),
    
    # CAPM
    capm_fs_pub_oos = get_values(capm_fs_t2_summary_journal, "journal_rank", group, "pub_oos"),
    capm_fs_pub_oos_se = get_values(capm_fs_t2_summary_journal, "journal_rank", group, "pub_oos_se"),
    capm_fs_outperform = get_values(capm_fs_t2_summary_journal, "journal_rank", group, "outperform"),
    capm_fs_outperform_se = get_values(capm_fs_t2_summary_journal, "journal_rank", group, "outperform_se"),
    
    # FF3
    ff3_fs_pub_oos = get_values(ff3_fs_t2_summary_journal, "journal_rank", group, "pub_oos"),
    ff3_fs_pub_oos_se = get_values(ff3_fs_t2_summary_journal, "journal_rank", group, "pub_oos_se"),
    ff3_fs_outperform = get_values(ff3_fs_t2_summary_journal, "journal_rank", group, "outperform"),
    ff3_fs_outperform_se = get_values(ff3_fs_t2_summary_journal, "journal_rank", group, "outperform_se")
  )
}

# Raw returns filter (by model binary) - t-stat only
raw_t2_summary_anymodel <- compute_outperformance_fs(
  ret_for_plot0 %>% filter(!is.na(matchRet)) %>%
    left_join(czret %>% select(signalname, rbar_t) %>% distinct(), by = c("pubname" = "signalname")) %>%
    filter(rbar_t > t_threshold), 
  "ret", "matchRet", anymodel_mapping, "model_binary"
)

# Collect full sample Any Model vs No Model data
fs_anymodel_data <- list(
  # Raw
  raw_pub_oos = get_values(raw_t2_summary_anymodel, "model_binary", "No Model", "pub_oos"),
  raw_pub_oos_se = get_values(raw_t2_summary_anymodel, "model_binary", "No Model", "pub_oos_se"),
  raw_outperform = get_values(raw_t2_summary_anymodel, "model_binary", "No Model", "outperform"),
  raw_outperform_se = get_values(raw_t2_summary_anymodel, "model_binary", "No Model", "outperform_se"),
  
  # CAPM
  capm_fs_pub_oos = get_values(capm_fs_t2_summary_anymodel, "model_binary", "No Model", "pub_oos"),
  capm_fs_pub_oos_se = get_values(capm_fs_t2_summary_anymodel, "model_binary", "No Model", "pub_oos_se"),
  capm_fs_outperform = get_values(capm_fs_t2_summary_anymodel, "model_binary", "No Model", "outperform"),
  capm_fs_outperform_se = get_values(capm_fs_t2_summary_anymodel, "model_binary", "No Model", "outperform_se"),
  
  # FF3
  ff3_fs_pub_oos = get_values(ff3_fs_t2_summary_anymodel, "model_binary", "No Model", "pub_oos"),
  ff3_fs_pub_oos_se = get_values(ff3_fs_t2_summary_anymodel, "model_binary", "No Model", "pub_oos_se"),
  ff3_fs_outperform = get_values(ff3_fs_t2_summary_anymodel, "model_binary", "No Model", "outperform"),
  ff3_fs_outperform_se = get_values(ff3_fs_t2_summary_anymodel, "model_binary", "No Model", "outperform_se")
)

# Create table data for export -------------------------------------------------------

# Build main summary table using refactored approach
main_groups <- c("Risk", "Mispricing", "Agnostic", "No Model", "Stylized", "Dynamic or Quantitative")
main_categories <- c(rep("Theoretical Foundation", 3), rep("Modeling Formalism", 3))

# Collect theory summaries
theory_summaries <- list(
  raw = raw_t2_summary_theory
)

# Collect model summaries
model_summaries <- list(
  raw = raw_t2_summary_model
)

# Build data for each group
main_table_data <- list()
for (i in 1:length(main_groups)) {
  group <- main_groups[i]
  
  if (i <= 3) {
    # Theory groups
    main_table_data[[i]] <- build_table_row_fs(theory_summaries, group, "theory_group")
  } else {
    # Model groups
    model_group <- main_groups[i]
    main_table_data[[i]] <- build_table_row_fs(model_summaries, model_group, "modeltype_grouped")
  }
}

# Add overall row
overall_data <- list(
  raw_pub_oos = overall_t2_summary_raw$pub_oos,
  raw_pub_oos_se = overall_t2_summary_raw$pub_oos_se,
  raw_outperform = overall_t2_summary_raw$outperform,
  raw_outperform_se = overall_t2_summary_raw$outperform_se
)

main_categories <- c(main_categories, "Overall")
main_groups <- c(main_groups, "All")
main_table_data[[length(main_table_data) + 1]] <- overall_data

# Create the main table using the new build_summary_table function
export_table_main <- build_fs_summary_table(
  categories = main_categories,
  groups = main_groups,
  summaries = main_table_data,
  analysis_types = c("raw"),
  format_latex = TRUE,
  digits = 0
)

# Build discipline/journal table using refactored approach
discipline_groups <- c("Finance", "Accounting")
discipline_categories <- rep("Discipline", 2)

journal_groups <- c("JF, JFE, RFS", "AR, JAR, JAE", "Other")
journal_categories <- rep("Journal Ranking", 3)

# Collect discipline summaries
discipline_summaries <- list(
  raw = raw_t2_summary_discipline
)

# Collect journal summaries  
journal_summaries <- list(
  raw = raw_t2_summary_journal
)

# Build discipline data
discipline_table_data <- list()
for (i in 1:length(discipline_groups)) {
  discipline_table_data[[i]] <- build_table_row_fs(discipline_summaries, discipline_groups[i], "discipline")
}

# Build journal data
for (i in 1:length(journal_groups)) {
  discipline_table_data[[length(discipline_table_data) + 1]] <- build_table_row_fs(journal_summaries, journal_groups[i], "journal_rank")
}

# Create the discipline/journal table
export_table_discipline <- build_fs_summary_table(
  categories = c(discipline_categories, journal_categories),
  groups = c(discipline_groups, journal_groups),
  summaries = discipline_table_data,
  analysis_types = c("raw"),
  format_latex = TRUE,
  digits = 0
)

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

# Export tables to CSV and LaTeX -------------------------------------------------------

# Format file suffix based on t-stat threshold
file_suffix <- paste0("_t", t_threshold)

# Export main theory/model table in multiple formats (raw only)
export_tables_multi_format_fs(
  export_table_main,
  base_filename = paste0(results_dir, "/Table_RiskAdjusted_TheoryModel", file_suffix),
  formats = c("csv", "latex"),
  latex_options = list(
    caption = "Risk-Adjusted Returns: Theoretical Foundation and Modeling Formalism",
    label = "tab:risk_adjusted_theory_model",
    group_headers = list(
      list(title = "Post-Sample Return", span = 1),
      list(title = "Outperformance vs Data-Mining", span = 1)
    )
  )
)

# Export discipline/journal table in multiple formats (raw only)
export_tables_multi_format_fs(
  export_table_discipline,
  base_filename = paste0(results_dir, "/Table_RiskAdjusted_DisciplineJournal", file_suffix),
  formats = c("csv", "latex"),
  latex_options = list(
    caption = "Risk-Adjusted Returns: Discipline and Journal Rankings",
    label = "tab:risk_adjusted_discipline_journal",
    group_headers = list(
      list(title = "Post-Sample Return", span = 1),
      list(title = "Outperformance vs Data-Mining", span = 1)
    )
  )
)

# Create and export the Any Model vs No Model table (raw only)
if (exists("anymodel_table_data")) {
  export_table_anymodel <- build_fs_summary_table(
    categories = rep("", length(anymodel_groups)),  # No categories for this table
    groups = anymodel_groups,
    summaries = anymodel_table_data,
    analysis_types = c("raw"),
    format_latex = TRUE,
    digits = 0
  )
  
  # Export Any Model vs No Model table in multiple formats
  export_tables_multi_format_fs(
    export_table_anymodel %>% select(-Category),  # Remove empty Category column
    base_filename = paste0(results_dir, "/Table_RiskAdjusted_AnyModelVsNoModel", file_suffix),
    formats = c("csv", "latex"),
    latex_options = list(
      caption = "Risk-Adjusted Returns: Any Model vs No Model",
      label = "tab:risk_adjusted_anymodel",
      group_headers = list(
        list(title = "Post-Sample Return", span = 1),
        list(title = "Outperformance vs Data-Mining", span = 1)
      )
    )
  )
}

# Export full sample tables if they exist (Raw + CAPM + FF3)
if (exists("fs_theory_data") && exists("fs_model_data")) {
  # Combine theory and model data
  fs_categories <- c(rep("Theoretical Foundation", length(groups_theory)),
                     rep("Modeling Formalism", length(groups_model)),
                     "Overall")
  fs_groups <- c(groups_theory, groups_model, "All")
  fs_all_data <- c(fs_theory_data, fs_model_data, list(fs_overall_data))
  
  # Create full sample table
  export_table_fs <- build_fs_summary_table(
    categories = fs_categories,
    groups = fs_groups,
    summaries = fs_all_data,
    analysis_types = c("raw", "capm_fs", "ff3_fs"),
    digits = 0
  )
  
  # Export full sample table
  export_tables_multi_format_fs(
    export_table_fs,
    base_filename = paste0(results_dir, "/Table_RiskAdjusted_FullSample", file_suffix),
    formats = c("csv", "latex"),
    latex_options = list(
      caption = "Full Sample Risk-Adjusted Returns: Theoretical Foundation and Modeling Formalism",
      label = "tab:risk_adjusted_fs",
      group_headers = list(
        list(title = "Raw", span = 2),
        list(title = "CAPM", span = 2),
        list(title = "FF3", span = 2)
      )
    )
  )
  
  # Export full sample discipline/journal table if data exists
  if (exists("fs_discipline_data") && exists("fs_journal_data")) {
    # Combine discipline and journal data
    fs_dj_categories <- c(rep("Discipline", 2), rep("Journal Rank", 3))
    fs_dj_groups <- c("Finance", "Accounting", "JF, JFE, RFS", "AR, JAR, JAE", "Other")
    fs_dj_all_data <- c(fs_discipline_data, fs_journal_data)
    
    # Create full sample discipline/journal table
    export_table_fs_dj <- build_fs_summary_table(
      categories = fs_dj_categories,
      groups = fs_dj_groups,
      summaries = fs_dj_all_data,
      analysis_types = c("raw", "capm_fs", "ff3_fs"),
      digits = 0
    )
    
    # Export full sample discipline/journal table
    export_tables_multi_format_fs(
      export_table_fs_dj,
      base_filename = paste0(results_dir, "/Table_RiskAdjusted_FullSample_DisciplineJournal", file_suffix),
      formats = c("csv", "latex"),
      latex_options = list(
        caption = "Full Sample Risk-Adjusted Returns: Discipline and Journal Rank",
        label = "tab:risk_adjusted_fs_dj",
        group_headers = list(
          list(title = "Raw", span = 2),
          list(title = "CAPM", span = 2),
          list(title = "FF3", span = 2)
        )
      )
    )
  }
  
  # Export full sample Any Model vs No Model table if data exists
  if (exists("fs_anymodel_data")) {
    # Create full sample Any Model vs No Model table
    fs_am_categories <- c("")
    fs_am_groups <- c("No Model")
    fs_am_all_data <- list(fs_anymodel_data)
    
    # Add Any Model data 
    # Calculate Any Model averages from Stylized and Dynamic or Quantitative
    fs_anymodel_any <- list(
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
      
      # CAPM
      capm_fs_pub_oos = mean(c(
        get_values(capm_fs_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"),
        get_values(capm_fs_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos")
      ), na.rm = TRUE),
      capm_fs_pub_oos_se = sqrt(mean(c(
        get_values(capm_fs_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")^2,
        get_values(capm_fs_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")^2
      ), na.rm = TRUE)),
      capm_fs_outperform = mean(c(
        get_values(capm_fs_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"),
        get_values(capm_fs_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform")
      ), na.rm = TRUE),
      capm_fs_outperform_se = sqrt(mean(c(
        get_values(capm_fs_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")^2,
        get_values(capm_fs_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")^2
      ), na.rm = TRUE)),
      
      # FF3
      ff3_fs_pub_oos = mean(c(
        get_values(ff3_fs_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos"),
        get_values(ff3_fs_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos")
      ), na.rm = TRUE),
      ff3_fs_pub_oos_se = sqrt(mean(c(
        get_values(ff3_fs_t2_summary_model, "modeltype_grouped", "Stylized", "pub_oos_se")^2,
        get_values(ff3_fs_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "pub_oos_se")^2
      ), na.rm = TRUE)),
      ff3_fs_outperform = mean(c(
        get_values(ff3_fs_t2_summary_model, "modeltype_grouped", "Stylized", "outperform"),
        get_values(ff3_fs_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform")
      ), na.rm = TRUE),
      ff3_fs_outperform_se = sqrt(mean(c(
        get_values(ff3_fs_t2_summary_model, "modeltype_grouped", "Stylized", "outperform_se")^2,
        get_values(ff3_fs_t2_summary_model, "modeltype_grouped", "Dynamic or Quantitative", "outperform_se")^2
      ), na.rm = TRUE))
    )
    
    fs_am_categories <- c("", "")
    fs_am_groups <- c("No Model", "Any Model")
    fs_am_all_data <- list(fs_anymodel_data, fs_anymodel_any)
    
    export_table_fs_am <- build_fs_summary_table(
      categories = fs_am_categories,
      groups = fs_am_groups,
      summaries = fs_am_all_data,
      analysis_types = c("raw", "capm_fs", "ff3_fs"),
      digits = 0
    )
    
    # Export full sample Any Model vs No Model table
    export_tables_multi_format_fs(
      export_table_fs_am,
      base_filename = paste0(results_dir, "/Table_RiskAdjusted_FullSample_AnyModelVsNoModel", file_suffix),
      formats = c("csv", "latex"),
      latex_options = list(
        caption = "Full Sample Risk-Adjusted Returns: Any Model vs No Model",
        label = "tab:risk_adjusted_fs_anymodel",
        group_headers = list(
          list(title = "Raw", span = 2),
          list(title = "CAPM", span = 2),
          list(title = "FF3", span = 2)
        )
      )
    )
  }
}

# Also export the raw summary data for reference (CSV only for raw data)
# Using a loop to reduce repetition
summary_data_list <- list(
  list(data = raw_t2_summary_theory, name = "Raw_TheoryGroup"),
  list(data = raw_t2_summary_model, name = "Raw_ModelGroup"),
  list(data = raw_t2_summary_discipline, name = "Raw_Discipline"),
  list(data = raw_t2_summary_journal, name = "Raw_Journal")
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
if (exists("fs_theory_data")) {
  cat(paste0("- Table_RiskAdjusted_FullSample", file_suffix, ".csv and .tex\n"))
}
cat("\nDetailed breakdowns:\n")
cat("- Raw/CAPM/FF3 by TheoryGroup/ModelGroup/Discipline/Journal (12 files)\n")
cat("\nPlots generated:\n")
plot_files <- list.files(results_dir, pattern = "^Fig_FullSampleRiskAdj_.*\\.pdf$", full.names = FALSE)
if (length(plot_files) == 0) {
  cat("- None found\n")
} else {
  cat(paste0("- ", plot_files, "\n"), sep = "")
} 