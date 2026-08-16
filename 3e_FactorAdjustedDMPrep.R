# Build sample-specific factor-adjusted published and data-mined benchmarks.
#
# How to run: normally run through 3_Precompute.R from flex-mining/.
# Inputs: cleaned published returns/factors and the chapter-2 versioned
#         MatchPubRiskAdjusted.RData pair cache
# Outputs: ../Data/Processed/risk_adjusted_dm_benchmarks.RDS

rm(list = ls())
source("0_Environment.R")
source("helpers/risk_adjusted_helpers_tv.R")

t_threshold <- 2
cache_path <- "../Data/Processed/risk_adjusted_dm_benchmarks.RDS"
risk_adj_path <- paste0(
  "../Data/Processed/", globalSettings$dataVersion,
  " MatchPubRiskAdjusted.RData"
)

inclSignals <- restrictInclSignals(
  restrictType = globalSettings$restrictType,
  topT = globalSettings$topT
)
factors <- readRDS("../Data/Raw/FamaFrenchFactors.RData") %>%
  rename(date = yearm)
czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  filter(signalname %in% inclSignals) %>%
  left_join(factors, by = "date") %>%
  setDT()
setorder(czret, signalname, eventDate)

# Baseline raw-return eligibility statistic.
czret[date >= sampstart & date <= sampend, rbar_t := {
  m <- mean(ret, na.rm = TRUE)
  s <- sd(ret, na.rm = TRUE)
  n <- sum(!is.na(ret))
  if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
}, by = signalname]
czret[, rbar_t := nafill(rbar_t, "locf"), by = signalname]

# CAPM coefficients are estimated separately in the original and post-sample
# regimes.  "tv" retains the legacy name; these are not rolling betas.
betas_capm_is <- czret[
  date >= sampstart & date <= sampend & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_is = extract_beta(ret, mktrf)), by = signalname
]
betas_capm_oos <- czret[
  date > sampend & !is.na(ret) & !is.na(mktrf),
  .(beta_capm_oos = extract_beta(ret, mktrf)), by = signalname
]
czret <- merge(czret, betas_capm_is, by = "signalname", all.x = TRUE)
czret <- merge(czret, betas_capm_oos, by = "signalname", all.x = TRUE)
czret[, beta_capm_tv := ifelse(
  date >= sampstart & date <= sampend, beta_capm_is, beta_capm_oos
)]
czret[, abnormal_capm_tv := ret - beta_capm_tv * mktrf]
czret[date >= sampstart & date <= sampend, `:=`(
  abar_capm_tv = mean(abnormal_capm_tv, na.rm = TRUE),
  abar_capm_tv_t = {
    m <- mean(abnormal_capm_tv, na.rm = TRUE)
    s <- sd(abnormal_capm_tv, na.rm = TRUE)
    n <- sum(!is.na(abnormal_capm_tv))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_capm_tv := nafill(abar_capm_tv, "locf"), by = signalname]
czret[, abar_capm_tv_t := nafill(abar_capm_tv_t, "locf"), by = signalname]
czret[, abnormal_capm_tv_normalized := ifelse(
  abs(abar_capm_tv) > 1e-10, 100 * abnormal_capm_tv / abar_capm_tv, NA_real_
)]

# FF3 plus momentum (Carhart).  Keep coefficient unpacking explicit so the
# FF3/FF4 recycling bug cannot recur.
ff4_is <- czret[
  date >= sampstart & date <= sampend & !is.na(ret) & !is.na(mktrf) &
    !is.na(smb) & !is.na(hml) & !is.na(umd), {
      coeffs <- extract_ff4_coeffs(ret, mktrf, smb, hml, umd)
      .(beta_ff4_is = coeffs[1], s_ff4_is = coeffs[2],
        h_ff4_is = coeffs[3], u_ff4_is = coeffs[4])
    }, by = signalname
]
ff4_oos <- czret[
  date > sampend & !is.na(ret) & !is.na(mktrf) &
    !is.na(smb) & !is.na(hml) & !is.na(umd), {
      coeffs <- extract_ff4_coeffs(ret, mktrf, smb, hml, umd)
      .(beta_ff4_oos = coeffs[1], s_ff4_oos = coeffs[2],
        h_ff4_oos = coeffs[3], u_ff4_oos = coeffs[4])
    }, by = signalname
]
czret <- merge(czret, ff4_is, by = "signalname", all.x = TRUE)
czret <- merge(czret, ff4_oos, by = "signalname", all.x = TRUE)
for (coefficient in c("beta", "s", "h", "u")) {
  target <- paste0(coefficient, "_ff4_tv")
  is_col <- paste0(coefficient, "_ff4_is")
  oos_col <- paste0(coefficient, "_ff4_oos")
  czret[, (target) := ifelse(
    date >= sampstart & date <= sampend,
    get(is_col), get(oos_col)
  )]
}
czret[, abnormal_ff4_tv := ret - (
  beta_ff4_tv * mktrf + s_ff4_tv * smb + h_ff4_tv * hml + u_ff4_tv * umd
)]
czret[date >= sampstart & date <= sampend, `:=`(
  abar_ff4_tv = mean(abnormal_ff4_tv, na.rm = TRUE),
  abar_ff4_tv_t = {
    m <- mean(abnormal_ff4_tv, na.rm = TRUE)
    s <- sd(abnormal_ff4_tv, na.rm = TRUE)
    n <- sum(!is.na(abnormal_ff4_tv))
    if (n > 1 && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret[, abar_ff4_tv := nafill(abar_ff4_tv, "locf"), by = signalname]
czret[, abar_ff4_tv_t := nafill(abar_ff4_tv_t, "locf"), by = signalname]
czret[, abnormal_ff4_tv_normalized := ifelse(
  abs(abar_ff4_tv) > 1e-10, 100 * abnormal_ff4_tv / abar_ff4_tv, NA_real_
)]

message("Loading risk-adjusted data-mined returns (large file)...")
candidateReturns_adj <- readRDS(risk_adj_path)
setDT(candidateReturns_adj)

# Preserve the legacy complete-row condition and compute model-specific pair
# alpha moments on the original published-signal sample.
dm_stats_tv <- candidateReturns_adj[
  date >= sampstart & date <= sampend & !is.na(abnormal_capm_tv),
  .(
    abar_capm_tv_dm = mean(abnormal_capm_tv, na.rm = TRUE),
    n_capm_tv_dm = sum(!is.na(abnormal_capm_tv)),
    sd_capm_tv_dm = sd(abnormal_capm_tv, na.rm = TRUE),
    abar_ff4_tv_dm = mean(abnormal_ff4_tv, na.rm = TRUE),
    n_ff4_tv_dm = sum(!is.na(abnormal_ff4_tv)),
    sd_ff4_tv_dm = sd(abnormal_ff4_tv, na.rm = TRUE)
  ), by = .(actSignal, candSignalname)
]
dm_stats_tv[, abar_capm_tv_dm_t := ifelse(
  n_capm_tv_dm > 1 & sd_capm_tv_dm > 0,
  abar_capm_tv_dm / sd_capm_tv_dm * sqrt(n_capm_tv_dm), NA_real_
)]
dm_stats_tv[, abar_ff4_tv_dm_t := ifelse(
  n_ff4_tv_dm > 1 & sd_ff4_tv_dm > 0,
  abar_ff4_tv_dm / sd_ff4_tv_dm * sqrt(n_ff4_tv_dm), NA_real_
)]

signals_raw_t2 <- unique(czret[rbar_t > t_threshold]$signalname)

build_model_panel <- function(model_key) {
  abnormal_col <- paste0("abnormal_", model_key, "_tv")
  published_mean_col <- paste0("abar_", model_key, "_tv")
  published_t_col <- paste0(published_mean_col, "_t")
  dm_t_col <- paste0("abar_", model_key, "_tv_dm_t")
  normalized_col <- paste0(abnormal_col, "_normalized")

  published_signals <- intersect(
    unique(czret[get(published_t_col) > t_threshold]$signalname),
    signals_raw_t2
  )
  eligible_pairs <- dm_stats_tv[
    !is.na(get(dm_t_col)) & get(dm_t_col) > t_threshold,
    .(actSignal, candSignalname)
  ]
  dm_filtered <- candidateReturns_adj[
    eligible_pairs, on = c("actSignal", "candSignalname"), nomatch = 0
  ]
  dm_agg <- normalize_and_aggregate_dm(
    dm_filtered, abnormal_col, model_key
  )
  setnames(
    dm_agg,
    c(paste0("matchRet_", model_key), paste0("n_matches_", model_key)),
    c("dm_return", "n_eligible_pairs")
  )
  dm_available <- dm_filtered[, .(
    n_pairs_available = uniqueN(candSignalname[!is.na(get(abnormal_col))])
  ), by = .(actSignal, eventDate)]

  panel <- czret[signalname %in% published_signals, .(
    pubname = signalname, eventDate, calendarDate = date,
    published_return = get(normalized_col)
  )] %>%
    inner_join(
      as_tibble(dm_agg),
      by = c("pubname" = "actSignal", "eventDate")
    ) %>%
    left_join(
      as_tibble(dm_available),
      by = c("pubname" = "actSignal", "eventDate")
    ) %>%
    filter(!is.na(dm_return))

  list(
    panel = panel,
    eligible_published_signals = sort(published_signals),
    eligible_pairs = as_tibble(eligible_pairs)
  )
}

capm <- build_model_panel("capm")
ff4 <- build_model_panel("ff4")

published_stats <- unique(czret[, .(
  signalname, rbar_t,
  beta_capm_is, beta_capm_oos, abar_capm_tv, abar_capm_tv_t,
  beta_ff4_is, s_ff4_is, h_ff4_is, u_ff4_is,
  beta_ff4_oos, s_ff4_oos, h_ff4_oos, u_ff4_oos,
  abar_ff4_tv, abar_ff4_tv_t
)])
published_stats[, `:=`(
  eligible_raw_t2 = !is.na(rbar_t) & rbar_t > t_threshold,
  eligible_capm_t2 = !is.na(rbar_t) & rbar_t > t_threshold &
    !is.na(abar_capm_tv_t) & abar_capm_tv_t > t_threshold,
  eligible_ff4_t2 = !is.na(rbar_t) & rbar_t > t_threshold &
    !is.na(abar_ff4_tv_t) & abar_ff4_tv_t > t_threshold
)]
dm_stats_tv[, `:=`(
  eligible_capm_t2 = !is.na(abar_capm_tv_dm_t) &
    abar_capm_tv_dm_t > t_threshold,
  eligible_ff4_t2 = !is.na(abar_ff4_tv_dm_t) &
    abar_ff4_tv_dm_t > t_threshold
)]

result <- list(
  capm = capm,
  ff4 = ff4,
  published_stats = as_tibble(published_stats),
  pair_stats = as_tibble(dm_stats_tv),
  metadata = list(
    schema_version = 1L,
    coefficient_regimes = c("original sample", "post-sample"),
    minimum_factor_observations = 60L,
    raw_t_threshold = t_threshold,
    alpha_t_threshold = t_threshold,
    normalization = "each series by its own original-sample alpha mean",
    factor_models = list(capm = "Mkt-RF", ff4 = c("Mkt-RF", "SMB", "HML", "UMD")),
    input_files = c(
      risk_adj_path,
      "../Data/Processed/czret_keeponly.RDS",
      "../Data/Raw/FamaFrenchFactors.RData"
    ),
    model_counts = list(
      capm = c(predictors = n_distinct(capm$panel$pubname),
               eligible_pairs = nrow(capm$eligible_pairs)),
      ff4 = c(predictors = n_distinct(ff4$panel$pubname),
              eligible_pairs = nrow(ff4$eligible_pairs))
    )
  )
)

stopifnot(
  !anyDuplicated(as.data.frame(result$capm$panel)[c("pubname", "eventDate")]),
  !anyDuplicated(as.data.frame(result$ff4$panel)[c("pubname", "eventDate")])
)
saveRDS(result, cache_path)
message("Wrote ", cache_path)
