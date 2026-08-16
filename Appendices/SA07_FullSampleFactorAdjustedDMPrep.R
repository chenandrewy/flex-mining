# Prepare appendix-only full-sample CAPM and FF3 DM benchmarks.
#
# How to run: normally run through SA_Appendices.R before the SA07 renderer.
# Inputs: canonical Chapter 3 raw benchmark contract, compact DM summaries,
#   mined long-short returns, published returns, and FF factors
# Outputs: ../Data/Processed/appendix_full_sample_dm_benchmarks.RDS

rm(list = ls())
source("0_Environment.R")
source("helpers/risk_adjusted_helpers_tv.R")
source("helpers/factor_adjusted_dm.R")

output_path <- "../Data/Processed/appendix_full_sample_dm_benchmarks.RDS"
dm_path <- paste0(
  "../Data/Processed/", globalSettings$dataVersion, " LongShort.RData"
)
raw <- readRDS("../Data/Processed/raw_dm_benchmarks.RDS")
summary <- readRDS("../Data/Processed/dmcomp_sumstats.RDS")$insampsum
base_pairs <- select_accounting_t2_pairs(
  summary, pubnames = raw$published$pubname
)
fingerprint <- accounting_t2_pair_fingerprint(base_pairs)
stopifnot(identical(
  fingerprint, raw$metadata$accounting_t2$pair_fingerprint_sha256
))

factors <- readRDS("../Data/Raw/FamaFrenchFactors.RData") %>%
  transmute(date = yearm, mktrf, smb, hml, umd) %>%
  setDT()
incl_signals <- restrictInclSignals(
  globalSettings$restrictType, globalSettings$topT
)
czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  filter(signalname %in% incl_signals) %>%
  left_join(as_tibble(factors), by = "date") %>%
  setDT()
setorder(czret, signalname, eventDate)

raw_stats <- czret[date >= sampstart & date <= sampend, .(
  rbar_t = {
    n <- sum(!is.na(ret)); m <- mean(ret, na.rm = TRUE); s <- sd(ret, na.rm = TRUE)
    if (n > 1L && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]

capm_fs <- czret[date >= sampstart, .(
  beta_capm_fs = extract_beta(ret, mktrf)
), by = signalname]
czret <- merge(czret, capm_fs, by = "signalname", all.x = TRUE)
czret[, abnormal_capm_fs := fifelse(
  date >= sampstart, ret - beta_capm_fs * mktrf, NA_real_
)]
capm_alpha <- czret[date >= sampstart & date <= sampend, .(
  abar_capm_fs = mean(abnormal_capm_fs, na.rm = TRUE),
  abar_capm_fs_t = {
    n <- sum(!is.na(abnormal_capm_fs)); m <- mean(abnormal_capm_fs, na.rm = TRUE)
    s <- sd(abnormal_capm_fs, na.rm = TRUE)
    if (n > 1L && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret <- merge(czret, capm_alpha, by = "signalname", all.x = TRUE)
czret[, abnormal_capm_fs_normalized := fifelse(
  abs(abar_capm_fs) > 1e-10, 100 * abnormal_capm_fs / abar_capm_fs, NA_real_
)]

ff3_fs <- czret[date >= sampstart, {
  z <- extract_ff3_coeffs(ret, mktrf, smb, hml)
  .(beta_ff3_fs = z[1], s_ff3_fs = z[2], h_ff3_fs = z[3])
}, by = signalname]
czret <- merge(czret, ff3_fs, by = "signalname", all.x = TRUE)
czret[, abnormal_ff3_fs := fifelse(
  date >= sampstart,
  ret - (beta_ff3_fs * mktrf + s_ff3_fs * smb + h_ff3_fs * hml),
  NA_real_
)]
ff3_alpha <- czret[date >= sampstart & date <= sampend, .(
  abar_ff3_fs = mean(abnormal_ff3_fs, na.rm = TRUE),
  abar_ff3_fs_t = {
    n <- sum(!is.na(abnormal_ff3_fs)); m <- mean(abnormal_ff3_fs, na.rm = TRUE)
    s <- sd(abnormal_ff3_fs, na.rm = TRUE)
    if (n > 1L && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret <- merge(czret, ff3_alpha, by = "signalname", all.x = TRUE)
czret[, abnormal_ff3_fs_normalized := fifelse(
  abs(abar_ff3_fs) > 1e-10, 100 * abnormal_ff3_fs / abar_ff3_fs, NA_real_
)]

published_stats <- Reduce(
  function(x, y) merge(x, y, by = "signalname", all = TRUE),
  list(raw_stats, capm_fs, capm_alpha, ff3_fs, ff3_alpha)
)
published_stats[, `:=`(
  eligible_raw_t2 = !is.na(rbar_t) & rbar_t > 2,
  eligible_capm_t2 = !is.na(rbar_t) & rbar_t > 2 &
    !is.na(abar_capm_fs_t) & abar_capm_fs_t > 2,
  eligible_ff3_t2 = !is.na(rbar_t) & rbar_t > 2 &
    !is.na(abar_ff3_fs_t) & abar_ff3_fs_t > 2
)]

dm <- build_broad_full_sample_factor_adjusted_dm(
  base_pairs, dm_path, factors, minimum_observations = 60L,
  alpha_threshold = 2L
)

make_contract <- function(model) {
  eligible_col <- paste0("eligible_", model, "_t2")
  published_col <- paste0("abnormal_", model, "_fs_normalized")
  eligible <- published_stats[get(eligible_col), signalname]
  published_panel <- czret[
    signalname %in% eligible & date >= sampstart,
    .(pubname = signalname, eventDate, calendarDate = date,
      published_return = get(published_col))
  ]
  dm_panel <- dm$panels[[model]][!is.na(dm_return)]
  dm_panel[, calendarDate := NULL]
  paired <- merge(
    published_panel, dm_panel,
    by = c("pubname", "eventDate"), all = FALSE
  )
  list(
    panel = as_tibble(paired),
    published_panel = as_tibble(published_panel),
    eligible_published_signals = sort(eligible)
  )
}

window_diagnostics <- dm$window_stats[, .(
  base_candidates = .N,
  capm_eligible_candidates = sum(capm_eligible, na.rm = TRUE),
  ff3_eligible_candidates = sum(ff3_eligible, na.rm = TRUE)
), by = .(sampstart, sampend)]
result <- list(
  capm = make_contract("capm"),
  ff3 = make_contract("ff3"),
  published_stats = as_tibble(published_stats),
  window_diagnostics = as_tibble(window_diagnostics),
  metadata = list(
    schema_version = 1L,
    appendix_only = TRUE,
    base_universe = "accounting_t2",
    base_pair_count = nrow(base_pairs),
    base_pair_fingerprint_sha256 = fingerprint,
    coefficient_regime = "all observations from published sample start onward",
    factor_models = list(capm = "Mkt-RF", ff3 = c("Mkt-RF", "SMB", "HML")),
    minimum_factor_observations = 60L,
    raw_t_threshold = 2,
    alpha_t_threshold = 2
  )
)
saveRDS(result, output_path)
message("Wrote ", output_path)
