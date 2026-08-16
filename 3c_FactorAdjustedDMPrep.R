# Prepare the main-text sample-specific factor-adjusted DM benchmarks.
#
# How to run: normally run through 3_Precompute.R after
#   3a_PrepDMBenchmarks.R. For validation, set FACTOR_DM_OUT_DIR to a temporary
#   directory.
# Inputs:  dmcomp_sumstats.RDS, raw_dm_benchmarks.RDS, the versioned mined
#          long-short universe, cleaned published returns, and FF factors
# Outputs: risk_adjusted_dm_benchmarks.RDS
#
# CAPM and FF4 use separate original- and post-sample coefficient regimes.
# Both begin from the exact broad accounting |t| > 2 pair universe used by the
# raw benchmark; no published mean-return or t-stat matching is applied.

rm(list = ls())
source("0_Environment.R")
source("helpers/risk_adjusted_helpers_tv.R")
source("helpers/factor_adjusted_dm.R")

out_dir <- Sys.getenv("FACTOR_DM_OUT_DIR", unset = "../Data/Processed")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
cache_path <- file.path(out_dir, "risk_adjusted_dm_benchmarks.RDS")
dm_path <- paste0(
  "../Data/Processed/", globalSettings$dataVersion, " LongShort.RData"
)

minimum_observations <- 60L
raw_t_threshold <- 2
alpha_t_threshold <- 2

incl_signals <- restrictInclSignals(
  restrictType = globalSettings$restrictType,
  topT = globalSettings$topT
)
raw_contract <- readRDS("../Data/Processed/raw_dm_benchmarks.RDS")
if (is.null(raw_contract$metadata$accounting_t2$pair_fingerprint_sha256)) {
  stop(
    "raw_dm_benchmarks.RDS lacks the canonical accounting-t2 fingerprint. ",
    "Rerun 3a_PrepDMBenchmarks.R before factor adjustment."
  )
}
dm_summary <- readRDS("../Data/Processed/dmcomp_sumstats.RDS")$insampsum
base_pairs <- select_accounting_t2_pairs(
  dm_summary,
  min_num_stocks = globalSettings$minNumStocks,
  t_threshold = raw_t_threshold,
  minimum_months = 60L,
  required_final_year_months = 12L,
  pubnames = raw_contract$published$pubname
)
base_fingerprint <- accounting_t2_pair_fingerprint(base_pairs)
stopifnot(
  identical(
    base_fingerprint,
    raw_contract$metadata$accounting_t2$pair_fingerprint_sha256
  ),
  nrow(base_pairs) == raw_contract$metadata$accounting_t2$pair_count
)

factors <- readRDS("../Data/Raw/FamaFrenchFactors.RData") %>%
  transmute(
    date = yearm, mktrf = mktrf, smb = smb, hml = hml, umd = umd
  ) %>%
  setDT()

# Published side -----------------------------------------------------------

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
  filter(signalname %in% incl_signals) %>%
  left_join(as_tibble(factors), by = "date") %>%
  setDT()
setorder(czret, signalname, eventDate)

published_raw <- czret[
  date >= sampstart & date <= sampend,
  .(
    rbar_t = {
      n <- sum(!is.na(ret)); m <- mean(ret, na.rm = TRUE); s <- sd(ret, na.rm = TRUE)
      if (n > 1L && s > 0) m / s * sqrt(n) else NA_real_
    }
  ),
  by = signalname
]

capm_is <- czret[
  date >= sampstart & date <= sampend,
  .(beta_capm_is = extract_beta(ret, mktrf)), by = signalname
]
capm_oos <- czret[
  date > sampend,
  .(beta_capm_oos = extract_beta(ret, mktrf)), by = signalname
]
czret <- merge(czret, capm_is, by = "signalname", all.x = TRUE)
czret <- merge(czret, capm_oos, by = "signalname", all.x = TRUE)
czret[, beta_capm_tv := fcase(
  date >= sampstart & date <= sampend, beta_capm_is,
  date > sampend, beta_capm_oos,
  default = NA_real_
)]
czret[, abnormal_capm_tv := ret - beta_capm_tv * mktrf]
capm_alpha <- czret[
  date >= sampstart & date <= sampend,
  .(
    abar_capm_tv = mean(abnormal_capm_tv, na.rm = TRUE),
    abar_capm_tv_t = {
      n <- sum(!is.na(abnormal_capm_tv))
      m <- mean(abnormal_capm_tv, na.rm = TRUE)
      s <- sd(abnormal_capm_tv, na.rm = TRUE)
      if (n > 1L && s > 0) m / s * sqrt(n) else NA_real_
    }
  ), by = signalname
]
czret <- merge(czret, capm_alpha, by = "signalname", all.x = TRUE)
czret[, abnormal_capm_tv_normalized := fifelse(
  abs(abar_capm_tv) > 1e-10,
  100 * abnormal_capm_tv / abar_capm_tv,
  NA_real_
)]

ff4_is <- czret[
  date >= sampstart & date <= sampend,
  {
    z <- extract_ff4_coeffs(ret, mktrf, smb, hml, umd)
    .(beta_ff4_is = z[1], s_ff4_is = z[2], h_ff4_is = z[3], u_ff4_is = z[4])
  }, by = signalname
]
ff4_oos <- czret[
  date > sampend,
  {
    z <- extract_ff4_coeffs(ret, mktrf, smb, hml, umd)
    .(beta_ff4_oos = z[1], s_ff4_oos = z[2], h_ff4_oos = z[3], u_ff4_oos = z[4])
  }, by = signalname
]
czret <- merge(czret, ff4_is, by = "signalname", all.x = TRUE)
czret <- merge(czret, ff4_oos, by = "signalname", all.x = TRUE)
for (coefficient in c("beta", "s", "h", "u")) {
  target <- paste0(coefficient, "_ff4_tv")
  czret[, (target) := fcase(
    date >= sampstart & date <= sampend,
    get(paste0(coefficient, "_ff4_is")),
    date > sampend,
    get(paste0(coefficient, "_ff4_oos")),
    default = NA_real_
  )]
}
czret[, abnormal_ff4_tv := ret - (
  beta_ff4_tv * mktrf + s_ff4_tv * smb +
    h_ff4_tv * hml + u_ff4_tv * umd
)]
ff4_alpha <- czret[
  date >= sampstart & date <= sampend,
  .(
    abar_ff4_tv = mean(abnormal_ff4_tv, na.rm = TRUE),
    abar_ff4_tv_t = {
      n <- sum(!is.na(abnormal_ff4_tv))
      m <- mean(abnormal_ff4_tv, na.rm = TRUE)
      s <- sd(abnormal_ff4_tv, na.rm = TRUE)
      if (n > 1L && s > 0) m / s * sqrt(n) else NA_real_
    }
  ), by = signalname
]
czret <- merge(czret, ff4_alpha, by = "signalname", all.x = TRUE)
czret[, abnormal_ff4_tv_normalized := fifelse(
  abs(abar_ff4_tv) > 1e-10,
  100 * abnormal_ff4_tv / abar_ff4_tv,
  NA_real_
)]

published_stats <- Reduce(
  function(x, y) merge(x, y, by = "signalname", all = TRUE),
  list(
    published_raw, capm_is, capm_oos, capm_alpha,
    ff4_is, ff4_oos, ff4_alpha
  )
)
published_stats[, `:=`(
  eligible_raw_t2 = !is.na(rbar_t) & rbar_t > raw_t_threshold,
  eligible_capm_t2 = !is.na(rbar_t) & rbar_t > raw_t_threshold &
    !is.na(abar_capm_tv_t) & abar_capm_tv_t > alpha_t_threshold,
  eligible_ff4_t2 = !is.na(rbar_t) & rbar_t > raw_t_threshold &
    !is.na(abar_ff4_tv_t) & abar_ff4_tv_t > alpha_t_threshold
)]

# Data-mined side ----------------------------------------------------------

dm_result <- build_broad_factor_adjusted_dm(
  base_pairs, dm_path, factors,
  minimum_observations = minimum_observations,
  alpha_threshold = alpha_t_threshold
)

build_model_contract <- function(model) {
  eligible_col <- paste0("eligible_", model, "_t2")
  published_col <- paste0("abnormal_", model, "_tv_normalized")
  eligible_signals <- published_stats[get(eligible_col), signalname]
  published_panel <- czret[
    signalname %in% eligible_signals & date >= sampstart,
    .(
      pubname = signalname, eventDate, calendarDate = date,
      published_return = get(published_col)
    )
  ]
  dm_panel <- dm_result$panels[[model]][!is.na(dm_return)]
  dm_panel[, calendarDate := NULL]
  paired <- merge(
    published_panel, dm_panel,
    by = c("pubname", "eventDate"), all = FALSE
  )
  setorder(paired, pubname, eventDate)
  list(
    panel = as_tibble(paired),
    published_panel = as_tibble(published_panel),
    eligible_published_signals = sort(eligible_signals)
  )
}

capm <- build_model_contract("capm")
ff4 <- build_model_contract("ff4")

window_diagnostics <- dm_result$window_stats[, .(
  base_candidates = .N,
  capm_eligible_candidates = sum(capm_eligible, na.rm = TRUE),
  ff4_eligible_candidates = sum(ff4_eligible, na.rm = TRUE)
), by = .(sampstart, sampend)]
publication_windows <- unique(base_pairs[, .(pubname, sampstart, sampend)])
model_pair_counts <- publication_windows[
  window_diagnostics,
  on = c("sampstart", "sampend"), allow.cartesian = TRUE
][, c(
  capm = sum(capm_eligible_candidates),
  ff4 = sum(ff4_eligible_candidates)
)]

result <- list(
  capm = capm,
  ff4 = ff4,
  published_stats = as_tibble(published_stats),
  window_diagnostics = as_tibble(window_diagnostics),
  metadata = list(
    schema_version = 2L,
    base_universe = "accounting_t2",
    base_pair_count = nrow(base_pairs),
    base_predictor_count = uniqueN(base_pairs$pubname),
    base_sample_window_count = uniqueN(base_pairs[, .(sampstart, sampend)]),
    base_pair_fingerprint_sha256 = base_fingerprint,
    coefficient_regimes = c("original sample", "post-sample"),
    minimum_factor_observations = minimum_observations,
    raw_t_threshold = raw_t_threshold,
    alpha_t_threshold = alpha_t_threshold,
    normalization = "each series by its own original-sample alpha mean",
    factor_models = list(
      capm = "Mkt-RF",
      ff4 = c("Mkt-RF", "SMB", "HML", "UMD")
    ),
    model_counts = list(
      capm = c(
        predictors = n_distinct(capm$panel$pubname),
        eligible_pairs = model_pair_counts[["capm"]]
      ),
      ff4 = c(
        predictors = n_distinct(ff4$panel$pubname),
        eligible_pairs = model_pair_counts[["ff4"]]
      )
    ),
    input_files = c(
      "../Data/Processed/dmcomp_sumstats.RDS",
      "../Data/Processed/raw_dm_benchmarks.RDS",
      dm_path,
      "../Data/Processed/czret_keeponly.RDS",
      "../Data/Raw/FamaFrenchFactors.RData"
    )
  )
)

stopifnot(
  !anyDuplicated(as.data.frame(result$capm$panel)[c("pubname", "eventDate")]),
  !anyDuplicated(as.data.frame(result$ff4$panel)[c("pubname", "eventDate")]),
  identical(
    result$metadata$base_pair_fingerprint_sha256,
    raw_contract$metadata$accounting_t2$pair_fingerprint_sha256
  )
)
saveRDS(result, cache_path)
message("Wrote ", cache_path)
