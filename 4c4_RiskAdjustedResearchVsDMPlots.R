# Risk-Adjusted Data-mining comparisons 
# Based on 4c2_ResearchVsDMPlots.R but with CAPM and FF3 adjustments
# This file compares raw vs risk-adjusted returns for published vs data-mined signals

# Setup ----------------------------------------------------------------
rm(list = ls())
source("0_Environment.R")
library(doParallel)

t_threshold_a = 1
t_threshold_b = 2

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


# T-stat filtered versions -----------------------------------------------

cat("\n\n=== T-STAT FILTERED ANALYSIS (t >=", t_threshold_b, ") ===\n")

# First compute t-stats for individual DM signals
cat("\nComputing t-stats for individual DM signals...\n")
setDT(candidateReturns_adj)

# Add samptype if not present
if(!"samptype" %in% names(candidateReturns_adj)) {
  candidateReturns_adj <- candidateReturns_adj %>%
    left_join(
      czret %>% select(signalname, eventDate, samptype) %>% distinct(),
      by = c("actSignal" = "signalname", "eventDate" = "eventDate")
    )
}

# Compute t-stats for each DM signal
dm_tstats <- candidateReturns_adj[
  samptype == "insamp" & !is.na(abnormal_capm),
  .(
    abar_capm_dm_t = mean(abnormal_capm, na.rm = TRUE) / sd(abnormal_capm, na.rm = TRUE) * sqrt(.N),
    abar_ff3_dm_t = mean(abnormal_ff3, na.rm = TRUE) / sd(abnormal_ff3, na.rm = TRUE) * sqrt(.N)
  ),
  by = .(actSignal, candSignalname)
]

# Filter DM signals by t >= t_threshold_b and recompute aggregates
dm_filtered_capm <- candidateReturns_adj %>%
  inner_join(
    dm_tstats %>% filter(abar_capm_dm_t >= t_threshold_b),
    by = c("actSignal", "candSignalname")
  )

dm_filtered_ff3 <- candidateReturns_adj %>%
  inner_join(
    dm_tstats %>% filter(abar_ff3_dm_t >= t_threshold_b),
    by = c("actSignal", "candSignalname")
  )

# Get signals with t >= t_threshold_b for different measures
signals_raw_t2 <- unique(czret[rbar_scaled_t >= t_threshold_b]$signalname)
signals_capm_t2 <- unique(czret[abar_capm_t >= t_threshold_b]$signalname)
signals_ff3_t2 <- unique(czret[abar_ff3_t >= t_threshold_b]$signalname)

cat("\nNumber of PUBLISHED signals with t >=", t_threshold_b, ":\n")
cat("Raw returns:", length(signals_raw_t2), "\n")
cat("CAPM alpha:", length(signals_capm_t2), "\n")
cat("FF3 alpha:", length(signals_ff3_t2), "\n")

cat("\nNumber of DM signals with t >=", t_threshold_b, ":\n")
cat("CAPM alpha:", sum(dm_tstats$abar_capm_dm_t >= t_threshold_b, na.rm = TRUE), "\n")
cat("FF3 alpha:", sum(dm_tstats$abar_ff3_dm_t >= t_threshold_b, na.rm = TRUE), "\n")

# Create filtered plots for CAPM t >= t_threshold_b
cat("\n=== CAPM FILTERED (t >=", t_threshold_b, ") STATISTICS ===\n")

# Normalize each DM return series individually and aggregate
dm_capm_aggregated <- dm_filtered_capm %>%
  group_by(actSignal, candSignalname) %>%
  mutate(
    insample_mean = mean(abnormal_capm[samptype == "insamp"], na.rm = TRUE),
    abnormal_capm_normalized_ind = ifelse(abs(insample_mean) > 1e-10, 100 * abnormal_capm / insample_mean, NA)
  ) %>%
  ungroup() %>%
  group_by(actSignal, eventDate) %>%
  summarise(
    matchRet_capm_t2_normalized = mean(abnormal_capm_normalized_ind, na.rm = TRUE),
    n_matches_capm_t2 = n_distinct(candSignalname),
    .groups = 'drop'
  )

# Join filtered DM to plotting data
ret_for_plot0_capm_t2 <- ret_for_plot0_adj %>%
  filter(pubname %in% signals_capm_t2) %>%
  left_join(
    dm_capm_aggregated %>% select(actSignal, eventDate, matchRet_capm_t2_normalized, n_matches_capm_t2),
    by = c("pubname" = "actSignal", "eventDate" = "eventDate")
  ) %>%
  filter(!is.na(matchRet_capm_t2_normalized))

cat("Published signals with CAPM t >=", t_threshold_b, ":", length(signals_capm_t2), "\n")
cat("Published signals with filtered DM matches:", length(unique(ret_for_plot0_capm_t2$pubname)), "\n")

if(nrow(ret_for_plot0_capm_t2) > 0) {
  ret_for_plot0_capm_t2 %>% 
    summarise(
      pub_mean_insamp = mean(abnormal_capm_normalized[eventDate <= 0], na.rm = TRUE),
      pub_mean_oos = mean(abnormal_capm_normalized[eventDate > 0], na.rm = TRUE),
      dm_mean_insamp = mean(matchRet_capm_t2_normalized[eventDate <= 0], na.rm = TRUE),
      dm_mean_oos = mean(matchRet_capm_t2_normalized[eventDate > 0], na.rm = TRUE)
    ) %>% print()
  
  printme_capm_t2 = ReturnPlotsWithDM_std_errors_indicators(
    dt = ret_for_plot0_capm_t2 %>% 
      transmute(eventDate, pubname, theory, ret = abnormal_capm_normalized, matchRet = matchRet_capm_t2_normalized) %>%
      left_join(czret %>% select(signalname, eventDate, date) %>% distinct(), by = c("pubname" = "signalname", "eventDate" = "eventDate")) %>%
      rename(calendarDate = date),
    basepath = "../Results/temp_",
    suffix = paste0("capm_t", t_threshold_b),
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl = -0,
    yh = 125,
    xl = global_xl,
    xh = global_xh,
          legendlabels =
        c(
          paste0("Published (CAPM Alpha, t>=", t_threshold_b, ")"),
          paste0("Data-Mined (CAPM Alpha, t>=", t_threshold_b, ")"),
          'N/A'
        ),
    legendpos = c(35,20)/100,
    fontsize = fontsizeall,
    yaxislab = "Trailing 5-Year CAPM Alpha (% of In-Sample Alpha)",
    linesize = linesizeall
  )
  
  ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_capm_t", t_threshold_b, ".pdf"), 
         printme_capm_t2, width = 10, height = 8)
  # Fix: Remove temp file with correct naming
  temp_file <- paste0("../Results/temp_capm_t", t_threshold_b, ".pdf")
  if(file.exists(temp_file)) file.remove(temp_file)
}

# Create filtered plots for FF3 t >= t_threshold_b
cat("\n=== FF3 FILTERED (t >=", t_threshold_b, ") STATISTICS ===\n")

# Normalize each DM return series individually and aggregate
dm_ff3_aggregated <- dm_filtered_ff3 %>%
  group_by(actSignal, candSignalname) %>%
  mutate(
    insample_mean = mean(abnormal_ff3[samptype == "insamp"], na.rm = TRUE),
    abnormal_ff3_normalized_ind = ifelse(abs(insample_mean) > 1e-10, 100 * abnormal_ff3 / insample_mean, NA)
  ) %>%
  ungroup() %>%
  group_by(actSignal, eventDate) %>%
  summarise(
    matchRet_ff3_t2_normalized = mean(abnormal_ff3_normalized_ind, na.rm = TRUE),
    n_matches_ff3_t2 = n_distinct(candSignalname),
    .groups = 'drop'
  )

# Join filtered DM to plotting data
ret_for_plot0_ff3_t2 <- ret_for_plot0_adj %>%
  filter(pubname %in% signals_ff3_t2) %>%
  left_join(
    dm_ff3_aggregated %>% select(actSignal, eventDate, matchRet_ff3_t2_normalized, n_matches_ff3_t2),
    by = c("pubname" = "actSignal", "eventDate" = "eventDate")
  ) %>%
  filter(!is.na(matchRet_ff3_t2_normalized))

cat("Published signals with FF3 t >=", t_threshold_b, ":", length(signals_ff3_t2), "\n")
cat("Published signals with filtered DM matches:", length(unique(ret_for_plot0_ff3_t2$pubname)), "\n")

if(nrow(ret_for_plot0_ff3_t2) > 0) {
  ret_for_plot0_ff3_t2 %>% 
    summarise(
      pub_mean_insamp = mean(abnormal_ff3_normalized[eventDate <= 0], na.rm = TRUE),
      pub_mean_oos = mean(abnormal_ff3_normalized[eventDate > 0], na.rm = TRUE),
      dm_mean_insamp = mean(matchRet_ff3_t2_normalized[eventDate <= 0], na.rm = TRUE),
      dm_mean_oos = mean(matchRet_ff3_t2_normalized[eventDate > 0], na.rm = TRUE)
    ) %>% print()
  
  printme_ff3_t2 = ReturnPlotsWithDM_std_errors_indicators(
    dt = ret_for_plot0_ff3_t2 %>% 
      transmute(eventDate, pubname, theory, ret = abnormal_ff3_normalized, matchRet = matchRet_ff3_t2_normalized) %>%
      left_join(czret %>% select(signalname, eventDate, date) %>% distinct(), by = c("pubname" = "signalname", "eventDate" = "eventDate")) %>%
      rename(calendarDate = date),
    basepath = "../Results/temp_",
    suffix = paste0("ff3_t", t_threshold_b),
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl = -0,
    yh = 125,
    xl = global_xl,
    xh = global_xh,
          legendlabels =
        c(
          paste0("Published (FF3 Alpha, t>=", t_threshold_b, ")"),
          paste0("Data-Mined (FF3 Alpha, t>=", t_threshold_b, ")"),
          'N/A'
        ),
    legendpos = c(35,20)/100,
    fontsize = fontsizeall,
    yaxislab = "Trailing 5-Year FF3 Alpha (% of In-Sample Alpha)",
    linesize = linesizeall
  )
  
  ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_ff3_t", t_threshold_b, ".pdf"), 
         printme_ff3_t2, width = 10, height = 8)
  # Fix: Remove temp file with correct naming
  temp_file <- paste0("../Results/temp_ff3_t", t_threshold_b, ".pdf")
  if(file.exists(temp_file)) file.remove(temp_file)
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
cat("\n\n=== SUMMARY TABLE WITH T >=", t_threshold_b, " FILTER ===\n")

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

# Helper function to get values by group
get_values <- function(summary_df, group_col, group_val, value_col) {
  idx <- which(summary_df[[group_col]] == group_val)
  if(length(idx) > 0) return(summary_df[[value_col]][idx]) else return(NA)
}

# Print t >= t_threshold_b filtered table
cat("\nPost-Sample Return (t>=", t_threshold_b, ")     Outperformance vs Data-Mining (t>=", t_threshold_b, ")\n")
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

# DISCIPLINE AND JOURNAL RANKING TABLE (t >= t_threshold_b) ---------------------------------
cat("\n\n=== SUMMARY TABLE BY DISCIPLINE AND JOURNAL RANKING (t >=", t_threshold_b, ") ===\n")

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
cat("\nPost-Sample Return (t>=", t_threshold_b, ")     Outperformance vs Data-Mining (t>=", t_threshold_b, ")\n")
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
write.csv(export_table_main, paste0(results_dir, "/Table_RiskAdjusted_TheoryModel_t2.csv"), row.names = FALSE)
write.csv(export_table_discipline, paste0(results_dir, "/Table_RiskAdjusted_DisciplineJournal_t2.csv"), row.names = FALSE)

# Also export the raw summary data for reference
write.csv(raw_t2_summary_theory, paste0(results_dir, "/Raw_TheoryGroup_t2.csv"), row.names = FALSE)
write.csv(capm_t2_summary_theory, paste0(results_dir, "/CAPM_TheoryGroup_t2.csv"), row.names = FALSE)
write.csv(ff3_t2_summary_theory, paste0(results_dir, "/FF3_TheoryGroup_t2.csv"), row.names = FALSE)
write.csv(raw_t2_summary_model, paste0(results_dir, "/Raw_ModelGroup_t2.csv"), row.names = FALSE)
write.csv(capm_t2_summary_model, paste0(results_dir, "/CAPM_ModelGroup_t2.csv"), row.names = FALSE)
write.csv(ff3_t2_summary_model, paste0(results_dir, "/FF3_ModelGroup_t2.csv"), row.names = FALSE)
write.csv(raw_t2_summary_discipline, paste0(results_dir, "/Raw_Discipline_t2.csv"), row.names = FALSE)
write.csv(capm_t2_summary_discipline, paste0(results_dir, "/CAPM_Discipline_t2.csv"), row.names = FALSE)
write.csv(ff3_t2_summary_discipline, paste0(results_dir, "/FF3_Discipline_t2.csv"), row.names = FALSE)
write.csv(raw_t2_summary_journal, paste0(results_dir, "/Raw_Journal_t2.csv"), row.names = FALSE)
write.csv(capm_t2_summary_journal, paste0(results_dir, "/CAPM_Journal_t2.csv"), row.names = FALSE)
write.csv(ff3_t2_summary_journal, paste0(results_dir, "/FF3_Journal_t2.csv"), row.names = FALSE)

cat("\n=== EXPORTED TABLES ===\n")
cat(paste("All files saved in:", results_dir, "\n"))
cat("\nMain summary tables:\n")
cat("- Table_RiskAdjusted_TheoryModel_t2.csv\n")
cat("- Table_RiskAdjusted_DisciplineJournal_t2.csv\n")
cat("\nDetailed breakdowns:\n")
cat("- Raw/CAPM/FF3 by TheoryGroup/ModelGroup/Discipline/Journal (12 files)\n")
cat("\nPlots:\n")
cat("- Fig_RiskAdj_raw_returns.pdf\n")
cat("- Fig_RiskAdj_capm_adjusted.pdf\n") 
cat("- Fig_RiskAdj_ff3_adjusted.pdf\n")
cat("- Fig_RiskAdj_capm_t2.pdf (t>=", t_threshold_b, " filtered)\n")
cat("- Fig_RiskAdj_ff3_t2.pdf (t>=", t_threshold_b, " filtered)\n")
print("\nRisk-adjusted analysis completed successfully!")
cat("\n=== T >=", t_threshold_a, " FILTERED ANALYSIS ===\n")

# Identify published signals with t >= t_threshold_a (not t_threshold_b to avoid double filtering)
signals_capm_ta <- czret$signalname[czret$abar_capm_t >= t_threshold_a & !is.na(czret$abar_capm_t)]
signals_ff3_ta <- czret$signalname[czret$abar_ff3_t >= t_threshold_a & !is.na(czret$abar_ff3_t)]

# Filter and aggregate DM candidates for CAPM t >= t_threshold_a
dm_filtered_capm_ta <- candidateReturns_adj %>%
  left_join(dm_tstats, by = c("actSignal", "candSignalname")) %>%
  filter(
    actSignal %in% signals_capm_ta,
    abar_capm_dm_t >= t_threshold_a,  # Use t_threshold_a for consistency
    !is.na(abnormal_capm)
  ) %>%
  group_by(actSignal, eventDate) %>%
  summarise(
    matchRet_capm_ta = mean(abnormal_capm, na.rm = TRUE),
    n_matches_capm_ta = n(),
    samptype = first(samptype),
    .groups = 'drop'
  ) %>%
  left_join(
    czret %>% select(signalname, eventDate, date, mktrf, smb, hml),
    by = c("actSignal" = "signalname", "eventDate" = "eventDate")
  ) %>%
  group_by(actSignal) %>%
  mutate(
    abar_capm_dm_filtered = mean(matchRet_capm_ta[samptype == "insamp"], na.rm = TRUE),
    matchRet_capm_ta_normalized = ifelse(abs(abar_capm_dm_filtered) > 1e-10, 100 * matchRet_capm_ta / abar_capm_dm_filtered, NA)
  ) %>%
  ungroup()

# Filter and aggregate DM candidates for FF3 t >= t_threshold_a  
dm_filtered_ff3_ta <- candidateReturns_adj %>%
  left_join(dm_tstats, by = c("actSignal", "candSignalname")) %>%
  filter(
    actSignal %in% signals_ff3_ta,
    abar_ff3_dm_t >= t_threshold_a,  # Use t_threshold_a for consistency
    !is.na(abnormal_ff3)
  ) %>%
  group_by(actSignal, eventDate) %>%
  summarise(
    matchRet_ff3_ta = mean(abnormal_ff3, na.rm = TRUE),
    n_matches_ff3_ta = n(),
    samptype = first(samptype),
    .groups = 'drop'
  ) %>%
  left_join(
    czret %>% select(signalname, eventDate, date, mktrf, smb, hml),
    by = c("actSignal" = "signalname", "eventDate" = "eventDate")
  ) %>%
  group_by(actSignal) %>%
  mutate(
    abar_ff3_dm_filtered = mean(matchRet_ff3_ta[samptype == "insamp"], na.rm = TRUE),
    matchRet_ff3_ta_normalized = ifelse(abs(abar_ff3_dm_filtered) > 1e-10, 100 * matchRet_ff3_ta / abar_ff3_dm_filtered, NA)
  ) %>%
  ungroup()

# Join filtered DM to plotting data
ret_for_plot0_capm_ta <- ret_for_plot0_adj %>%
  filter(pubname %in% signals_capm_ta) %>%
  left_join(
    dm_filtered_capm_ta %>% select(actSignal, eventDate, matchRet_capm_ta_normalized, n_matches_capm_ta),
    by = c("pubname" = "actSignal", "eventDate" = "eventDate")
  ) %>%
  filter(!is.na(matchRet_capm_ta_normalized))

ret_for_plot0_ff3_ta <- ret_for_plot0_adj %>%
  filter(pubname %in% signals_ff3_ta) %>%
  left_join(
    dm_filtered_ff3_ta %>% select(actSignal, eventDate, matchRet_ff3_ta_normalized, n_matches_ff3_ta),
    by = c("pubname" = "actSignal", "eventDate" = "eventDate")
  ) %>%
  filter(!is.na(matchRet_ff3_ta_normalized))

# CAPM adjusted plot (t >= t_threshold_a)
tempsuffix = paste0("capm_t", t_threshold_a)
printme_capm_ta = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot0_capm_ta %>% 
    transmute(eventDate, pubname, theory, ret = abnormal_capm_normalized, matchRet = matchRet_capm_ta_normalized) %>%
    left_join(czret %>% select(signalname, eventDate, date) %>% distinct(), by = c("pubname" = "signalname", "eventDate" = "eventDate")) %>%
    rename(calendarDate = date),
  basepath = "../Results/temp_",
  suffix = tempsuffix,
  rollmonths = 60,
  colors = colors,
  labelmatch = FALSE,
  yl = -50,
  yh = 150,
  xl = global_xl,
  xh = global_xh,
  legendlabels = c(
    paste0("Published (CAPM Alpha / |Insample Alpha|, t>=", t_threshold_a, ")"),
    paste0("Data-Mined (CAPM Alpha / |Insample Alpha|, t>=", t_threshold_a, ")"),
    'N/A'
  ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_capm_t", t_threshold_a, '.pdf'), 
       printme_capm_ta, width = 10, height = 8)

cat("CAPM t>=", t_threshold_a, " signals:", length(unique(ret_for_plot0_capm_ta$pubname)), "\n")

# FF3 adjusted plot (t >= t_threshold_a)  
tempsuffix = paste0("ff3_t", t_threshold_a)
printme_ff3_ta = ReturnPlotsWithDM_std_errors_indicators(
  dt = ret_for_plot0_ff3_ta %>% 
    transmute(eventDate, pubname, theory, ret = abnormal_ff3_normalized, matchRet = matchRet_ff3_ta_normalized) %>%
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
  legendlabels = c(
    paste0("Published (FF3 Alpha / |Insample Alpha|, t>=", t_threshold_a, ")"),
    paste0("Data-Mined (FF3 Alpha / |Insample Alpha|, t>=", t_threshold_a, ")"),
    'N/A'
  ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

ggsave(filename = paste0(results_dir, "/Fig_RiskAdj_ff3_t", t_threshold_a, '.pdf'), 
       printme_ff3_ta, width = 10, height = 8)

cat("FF3 t>=", t_threshold_a, " signals:", length(unique(ret_for_plot0_ff3_ta$pubname)), "\n")

print("\nRisk-adjusted analysis completed successfully!")