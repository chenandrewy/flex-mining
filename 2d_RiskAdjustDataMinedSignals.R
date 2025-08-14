# Risk-adjust data-mined signals using CAPM and FF3
# Uses matched DM signals from 2b_MatchDataMinedToPub.R
# Computes full-sample betas (from sampstart onwards) for each DM signal
#
# FIXED: Time-varying beta/alpha consistency with published signals
# - DM signals use sampstart/sampend periods for IS/OOS classification
# - Published signals use same sampstart/sampend periods (in 4c4 file)
# - Both use same period definitions for TV adjustments

rm(list = ls())
source('0_Environment.R')
source('helpers/risk_adjusted_helpers_tv.R')
library(doParallel)

# Setup -------------------------------------------------------------------
DMname = paste0('../Data/Processed/',
                globalSettings$dataVersion, 
                ' LongShort.RData')

DMshortname = DMname %>% 
  str_remove('../Data/Processed/') %>% 
  str_remove(' LongShort.RData')

ncores = globalSettings$num_cores

# Helper functions --------------------------------------------------------
# extract_beta and extract_ff3_coeffs are sourced from helpers/risk_adjusted_helpers_tv.R

# Load data ---------------------------------------------------------------

# Published signal info for sample periods
inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, 
                                  topT = globalSettings$topT)

czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>% 
  filter(signalname %in% inclSignals, Keep) %>%
  select(signalname, sampstart, sampend)

# Load matched DM returns
matchdat = readRDS(paste0('../Data/Processed/', DMshortname, ' MatchPub.RData'))
candidateReturns = matchdat$candidateReturns

# Load Fama-French factors
FamaFrenchFactors <- readRDS('../Data/Raw/FamaFrenchFactors.RData') %>%
  rename(date = yearm)

# Add date variable and merge FF factors
candidateReturns <- candidateReturns %>%
  left_join(czsum, by = c('actSignal' = 'signalname')) %>%
  mutate(
    # Convert eventDate back to actual date
    date = sampend + eventDate/12,
    # FIXED: Use sampstart/sampend-based samptype for consistency with published signals
    samptype = ifelse(date >= sampstart & date <= sampend, 'insamp', 'oos')
  ) %>%
  left_join(FamaFrenchFactors, by = 'date')

# Compute risk adjustments ------------------------------------------------

print("Computing CAPM and FF3 adjustments for data-mined signals...")
n_unique_dm <- length(unique(candidateReturns$candSignalname))
n_unique_pairs <- nrow(unique(candidateReturns[, .(actSignal, candSignalname)]))
print(paste("Number of unique DM signals:", n_unique_dm))
print(paste("Number of unique (published, DM) pairs:", n_unique_pairs))

# Convert to data.table for speed
setDT(candidateReturns)

# Get unique pairs for progress tracking
unique_pairs <- unique(candidateReturns[, .(actSignal, candSignalname)])

# For each matched pair (actSignal, candSignalname), compute betas using different sample periods
print("Computing risk adjustments...")
tic <- Sys.time()

# 1. Full sample betas (from sampstart onwards)
print("Computing full sample betas...")
dm_risk_adj_full <- candidateReturns[
  date >= sampstart,  # Use full sample from sampstart onwards
  {
    # Progress tracking
    if (.GRP %% 100 == 0) {
      cat(sprintf("\rProcessing pair %d of %d (%.1f%%)...", 
                  .GRP, n_unique_pairs, .GRP/n_unique_pairs*100))
    }
    
    .(
      # CAPM
      beta_capm = extract_beta(ret, mktrf),
      # FF3
      ff3_coeffs = list(extract_ff3_coeffs(ret, mktrf, smb, hml))
    )
  },
  by = .(actSignal, candSignalname)
]

# 2. In-sample betas (between sampstart and sampend)
cat("\n")
print("Computing in-sample betas...")
dm_risk_adj_is <- candidateReturns[
  date >= sampstart & date <= sampend,  # In-sample period only
  {
    # Progress tracking
    if (.GRP %% 100 == 0) {
      cat(sprintf("\rProcessing pair %d of %d (%.1f%%)...", 
                  .GRP, n_unique_pairs, .GRP/n_unique_pairs*100))
    }
    
    .(
      # CAPM
      beta_capm_is = extract_beta(ret, mktrf),
      # FF3
      ff3_coeffs_is = list(extract_ff3_coeffs(ret, mktrf, smb, hml))
    )
  },
  by = .(actSignal, candSignalname)
]

# 3. Post-sample betas (after sampend)
cat("\n")
print("Computing post-sample betas...")
dm_risk_adj_post <- candidateReturns[
  date > sampend,  # Post-sample period only
  {
    # Progress tracking
    if (.GRP %% 100 == 0) {
      cat(sprintf("\rProcessing pair %d of %d (%.1f%%)...", 
                  .GRP, n_unique_pairs, .GRP/n_unique_pairs*100))
    }
    
    .(
      # CAPM
      beta_capm_post = extract_beta(ret, mktrf),
      # FF3
      ff3_coeffs_post = list(extract_ff3_coeffs(ret, mktrf, smb, hml))
    )
  },
  by = .(actSignal, candSignalname)
]

toc <- Sys.time()
cat("\n")
print(paste("Risk adjustment computation time:", round(difftime(toc, tic, units = "mins"), 2), "minutes"))

# Extract FF3 coefficients for all three sets
# Full sample
dm_risk_adj_full[, c("beta_ff3", "s_ff3", "h_ff3") := {
  coeffs <- unlist(ff3_coeffs)
  list(coeffs[1], coeffs[2], coeffs[3])
}]
dm_risk_adj_full[, ff3_coeffs := NULL]

# In-sample
dm_risk_adj_is[, c("beta_ff3_is", "s_ff3_is", "h_ff3_is") := {
  coeffs <- unlist(ff3_coeffs_is)
  list(coeffs[1], coeffs[2], coeffs[3])
}]
dm_risk_adj_is[, ff3_coeffs_is := NULL]

# Post-sample
dm_risk_adj_post[, c("beta_ff3_post", "s_ff3_post", "h_ff3_post") := {
  coeffs <- unlist(ff3_coeffs_post)
  list(coeffs[1], coeffs[2], coeffs[3])
}]
dm_risk_adj_post[, ff3_coeffs_post := NULL]

# Merge all betas back to full data
candidateReturns <- candidateReturns %>%
  left_join(dm_risk_adj_full, by = c('actSignal', 'candSignalname')) %>%
  left_join(dm_risk_adj_is, by = c('actSignal', 'candSignalname')) %>%
  left_join(dm_risk_adj_post, by = c('actSignal', 'candSignalname'))

# Compute abnormal returns
candidateReturns <- candidateReturns %>%
  mutate(
    # Full sample abnormal returns
    abnormal_capm = ret - beta_capm * mktrf,
    abnormal_ff3 = ret - (beta_ff3 * mktrf + s_ff3 * smb + h_ff3 * hml),
    
    # Time-varying abnormal returns: IS betas for IS period, post-sample betas for OOS period
    # FIXED: Use sampstart/sampend-based samptype for consistency with published signals
    abnormal_capm_tv = case_when(
      date >= sampstart & date <= sampend ~ ret - beta_capm_is * mktrf,  # IS period: use IS beta
      date > sampend ~ ret - beta_capm_post * mktrf,  # OOS period: use post-sample beta
      TRUE ~ NA_real_
    ),
    abnormal_ff3_tv = case_when(
      date >= sampstart & date <= sampend ~ ret - (beta_ff3_is * mktrf + s_ff3_is * smb + h_ff3_is * hml),  # IS period: use IS betas
      date > sampend ~ ret - (beta_ff3_post * mktrf + s_ff3_post * smb + h_ff3_post * hml),  # OOS period: use post-sample betas
      TRUE ~ NA_real_
    )
  )

# Summary statistics
print(paste("Full sample CAPM adjustments computed:", sum(!is.na(candidateReturns$beta_capm))))
print(paste("Full sample FF3 adjustments computed:", sum(!is.na(candidateReturns$beta_ff3))))
print(paste("In-sample CAPM adjustments computed:", sum(!is.na(candidateReturns$beta_capm_is))))
print(paste("Post-sample CAPM adjustments computed:", sum(!is.na(candidateReturns$beta_capm_post))))

# Check a few examples
print("\nExample risk adjustments (first 5 unique DM signals):")
candidateReturns %>%
  filter(!is.na(beta_capm)) %>%
  group_by(candSignalname) %>%
  slice(1) %>%
  ungroup() %>%
  slice(1:5) %>%
  select(actSignal, candSignalname, beta_capm, beta_capm_is, beta_capm_post, beta_ff3, beta_ff3_is, beta_ff3_post) %>%
  print()

# Save risk-adjusted DM returns -------------------------------------------

# Save full data with returns and coefficients
saveRDS(candidateReturns,
        file = paste0('../Data/Processed/', DMshortname, ' MatchPubRiskAdjusted.RData'))

print(paste("Risk-adjusted DM returns saved to:", 
            paste0('../Data/Processed/', DMshortname, ' MatchPubRiskAdjusted.RData')))

# Also save just the coefficients for easy access
dm_coefficients <- candidateReturns %>%
  group_by(actSignal, candSignalname) %>%
  slice(1) %>%
  select(actSignal, candSignalname, 
         beta_capm, beta_ff3, s_ff3, h_ff3,  # Full sample
         beta_capm_is, beta_ff3_is, s_ff3_is, h_ff3_is,  # In-sample
         beta_capm_post, beta_ff3_post, s_ff3_post, h_ff3_post) %>%  # Post-sample
  ungroup()

saveRDS(dm_coefficients,
        file = paste0('../Data/Processed/', DMshortname, ' MatchPubCoefficients.RData'))

print(paste("Risk coefficients saved to:", 
            paste0('../Data/Processed/', DMshortname, ' MatchPubCoefficients.RData')))

# Also save summary statistics for matched returns ------------------------

# Aggregate to (actSignal, eventDate) level for plotting
# Taking mean across all matched DM signals for each published signal
matched_risk_adj <- candidateReturns %>%
  group_by(actSignal, eventDate) %>%
  summarise(
    n_matches = n(),
    matchRet_raw = mean(ret, na.rm = TRUE),
    matchRet_capm = mean(abnormal_capm, na.rm = TRUE),
    matchRet_ff3 = mean(abnormal_ff3, na.rm = TRUE),
    matchRet_capm_tv = mean(abnormal_capm_tv, na.rm = TRUE),  # Time-varying CAPM
    matchRet_ff3_tv = mean(abnormal_ff3_tv, na.rm = TRUE),    # Time-varying FF3
    .groups = 'drop'
  )

saveRDS(matched_risk_adj,
        file = paste0('../Data/Processed/', DMshortname, ' MatchedRiskAdjSummary.RData'))

print("\nRisk adjustment complete!")
print(paste("Total observations processed:", nrow(candidateReturns)))
print(paste("Unique published signals:", length(unique(candidateReturns$actSignal))))
print(paste("Unique DM signals:", length(unique(candidateReturns$candSignalname))))