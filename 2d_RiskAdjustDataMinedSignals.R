# Risk-adjust data-mined signals using CAPM and FF3
# Uses matched DM signals from 2b_MatchDataMinedToPub.R
# Computes full-sample betas (from sampstart onwards) for each DM signal

rm(list = ls())
source('0_Environment.R')
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
extract_beta <- function(x, y) {
  if(sum(!is.na(x) & !is.na(y)) < 12) return(NA_real_)  # Need at least 12 months
  model <- lm(y ~ x)
  bet <- coef(model)[2]
  return(bet)
}

extract_ff3_coeffs <- function(ret, mktrf, smb, hml) {
  if(sum(!is.na(ret) & !is.na(mktrf)) < 12) return(c(NA_real_, NA_real_, NA_real_))
  model <- lm(ret ~ mktrf + smb + hml)
  coeffs <- coef(model)
  return(coeffs[2:4])  # Return beta, s, and h coefficients
}

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
    date = sampend + eventDate/12
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

# For each matched pair (actSignal, candSignalname), compute betas using data from sampstart onwards
print("Computing risk adjustments...")
tic <- Sys.time()

dm_risk_adj <- candidateReturns[
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

toc <- Sys.time()
cat("\n")
print(paste("Risk adjustment computation time:", round(difftime(toc, tic, units = "mins"), 2), "minutes"))

# Extract FF3 coefficients
dm_risk_adj[, c("beta_ff3", "s_ff3", "h_ff3") := {
  coeffs <- unlist(ff3_coeffs)
  list(coeffs[1], coeffs[2], coeffs[3])
}]
dm_risk_adj[, ff3_coeffs := NULL]

# Merge betas back to full data
candidateReturns <- candidateReturns %>%
  left_join(dm_risk_adj, by = c('actSignal', 'candSignalname'))

# Compute abnormal returns
candidateReturns <- candidateReturns %>%
  mutate(
    # CAPM abnormal
    abnormal_capm = ret - beta_capm * mktrf,
    # FF3 abnormal
    abnormal_ff3 = ret - (beta_ff3 * mktrf + s_ff3 * smb + h_ff3 * hml)
  )

# Summary statistics
print(paste("CAPM adjustments computed:", sum(!is.na(candidateReturns$beta_capm))))
print(paste("FF3 adjustments computed:", sum(!is.na(candidateReturns$beta_ff3))))

# Check a few examples
print("\nExample risk adjustments (first 5 unique DM signals):")
candidateReturns %>%
  filter(!is.na(beta_capm)) %>%
  group_by(candSignalname) %>%
  slice(1) %>%
  ungroup() %>%
  slice(1:5) %>%
  select(actSignal, candSignalname, beta_capm, beta_ff3, s_ff3, h_ff3) %>%
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
  select(actSignal, candSignalname, beta_capm, beta_ff3, s_ff3, h_ff3) %>%
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
    .groups = 'drop'
  )

saveRDS(matched_risk_adj,
        file = paste0('../Data/Processed/', DMshortname, ' MatchedRiskAdjSummary.RData'))

print("\nRisk adjustment complete!")
print(paste("Total observations processed:", nrow(candidateReturns)))
print(paste("Unique published signals:", length(unique(candidateReturns$actSignal))))
print(paste("Unique DM signals:", length(unique(candidateReturns$candSignalname))))