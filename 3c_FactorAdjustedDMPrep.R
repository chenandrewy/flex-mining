# Prepare the main and appendix factor-adjusted DM benchmarks.
#
# How to run: normally run through 3_Precompute.R after
#   3a_PrepDMBenchmarks.R. For validation, set FACTOR_DM_OUT_DIR to a temporary
#   directory.
# Inputs:  dmcomp_sumstats.RDS, raw_dm_benchmarks.RDS, the versioned mined
#          long-short universe, cleaned published returns, and FF factors
# Outputs: factor_adjusted_dm_benchmarks.RDS
#          appendix_full_sample_dm_benchmarks.RDS
#
# Sample-specific CAPM/FF4 and full-sample CAPM/FF3 begin from the exact broad
# accounting |t| > 2 pair universe used by the raw benchmark; no published
# mean-return or t-stat matching is applied.

rm(list = ls())
source("0_Environment.R")
# Factor-adjustment implementation ---------------------------------------

extract_beta <- function(ret, mktrf, minimum_observations = 60L) {
  observed <- stats::complete.cases(ret, mktrf)
  if (sum(observed) < minimum_observations) return(NA_real_)
  unname(stats::coef(stats::lm(ret[observed] ~ mktrf[observed]))[2L])
}

extract_ff3_coeffs <- function(
    ret, mktrf, smb, hml, minimum_observations = 60L) {
  observed <- stats::complete.cases(ret, mktrf, smb, hml)
  if (sum(observed) < minimum_observations) return(rep(NA_real_, 3L))
  unname(stats::coef(stats::lm(
    ret[observed] ~ mktrf[observed] + smb[observed] + hml[observed]
  ))[2:4])
}

extract_ff4_coeffs <- function(
    ret, mktrf, smb, hml, umd, minimum_observations = 60L) {
  observed <- stats::complete.cases(ret, mktrf, smb, hml, umd)
  if (sum(observed) < minimum_observations) return(rep(NA_real_, 4L))
  unname(stats::coef(stats::lm(
    ret[observed] ~ mktrf[observed] + smb[observed] +
      hml[observed] + umd[observed]
  ))[2:5])
}

factor_model_slopes <- function(y, factors, minimum_observations = 60L) {
  y <- as.matrix(y)
  factors <- as.matrix(factors)
  if (nrow(y) != nrow(factors)) {
    stop("Return and factor matrices must have the same number of rows.")
  }
  factor_complete <- stats::complete.cases(factors)
  y <- y[factor_complete, , drop = FALSE]
  factors <- factors[factor_complete, , drop = FALSE]

  n_series <- ncol(y)
  n_factor <- ncol(factors)
  slopes <- matrix(NA_real_, nrow = n_series, ncol = n_factor)
  colnames(slopes) <- colnames(factors)
  if (nrow(y) == 0L || n_series == 0L) {
    return(list(slopes = slopes, nobs = integer(n_series)))
  }

  observed <- !is.na(y)
  y_zero <- y
  y_zero[!observed] <- 0
  design <- cbind(`(Intercept)` = 1, factors)
  p <- ncol(design)
  nobs <- colSums(observed)
  xty <- crossprod(design, y_zero)

  # Each return series may have a different history.  Compute its small X'X
  # from vectorized sufficient statistics, then solve only the p-by-p system.
  xtx_terms <- vector("list", p * (p + 1L) / 2L)
  term <- 0L
  for (a in seq_len(p)) {
    for (b in a:p) {
      term <- term + 1L
      xtx_terms[[term]] <- as.numeric(crossprod(
        design[, a] * design[, b], observed
      ))
    }
  }

  eligible <- which(nobs >= minimum_observations)
  for (j in eligible) {
    xtx <- matrix(0, p, p)
    term <- 0L
    for (a in seq_len(p)) {
      for (b in a:p) {
        term <- term + 1L
        xtx[a, b] <- xtx_terms[[term]][j]
        xtx[b, a] <- xtx[a, b]
      }
    }
    coef <- tryCatch(
      solve(xtx, xty[, j]),
      error = function(e) rep(NA_real_, p)
    )
    slopes[j, ] <- coef[-1L]
  }
  list(slopes = slopes, nobs = as.integer(nobs))
}

factor_abnormal_returns <- function(y, factors, slopes) {
  y <- as.matrix(y)
  factors <- as.matrix(factors)
  slopes <- as.matrix(slopes)
  if (nrow(y) != nrow(factors) || ncol(y) != nrow(slopes) ||
      ncol(factors) != ncol(slopes)) {
    stop("Nonconformable return, factor, and slope matrices.")
  }
  abnormal <- y - factors %*% t(slopes)
  abnormal[is.na(y)] <- NA_real_
  abnormal
}

factor_alpha_stats <- function(abnormal) {
  abnormal <- as.matrix(abnormal)
  observed <- !is.na(abnormal)
  x <- abnormal
  x[!observed] <- 0
  n <- colSums(observed)
  total <- colSums(x)
  total_sq <- colSums(x * x)
  mean <- ifelse(n > 0L, total / n, NA_real_)
  variance <- ifelse(
    n > 1L,
    pmax((total_sq - total * total / n) / (n - 1L), 0),
    NA_real_
  )
  sd <- sqrt(variance)
  tstat <- ifelse(n > 1L & sd > 0, mean / sd * sqrt(n), NA_real_)
  data.table::data.table(
    alpha_mean = mean, alpha_sd = sd, alpha_n = as.integer(n),
    alpha_t = tstat
  )
}

cross_join_publications <- function(publications, panel) {
  publications <- data.table::copy(data.table::as.data.table(publications))
  panel <- data.table::copy(data.table::as.data.table(panel))
  publications[, `.factor_cross_key` := 1L]
  panel[, `.factor_cross_key` := 1L]
  result <- merge(
    publications, panel, by = ".factor_cross_key",
    allow.cartesian = TRUE
  )
  result[, `.factor_cross_key` := NULL]
  result
}

load_selected_dm_return_matrix <- function(dm_path, selected_pairs) {
  selected_pairs <- data.table::as.data.table(selected_pairs)
  selected_keys <- unique(selected_pairs[, .(
    sweight = tolower(sweight), dmname
  )])
  data.table::setorder(selected_keys, sweight, dmname)
  selected_keys[, column := .I]

  message("Loading mined returns and constructing the selected-strategy matrix...")
  mined <- readRDS(dm_path)
  returns <- data.table::as.data.table(mined$ret)
  weights <- data.table::as.data.table(mined$port_list)[, .(
    portid, sweight = tolower(sweight)
  )]
  rm(mined)
  returns <- weights[returns, on = "portid", nomatch = 0L]
  returns <- selected_keys[
    returns,
    on = c("sweight", "dmname" = "signalid"),
    nomatch = 0L
  ]

  dates <- sort(unique(as.numeric(returns$yearm)))
  returns[, row := match(as.numeric(yearm), dates)]
  matrix_returns <- matrix(
    NA_real_, nrow = length(dates), ncol = nrow(selected_keys)
  )
  matrix_returns[cbind(returns$row, returns$column)] <- returns$ret
  colnames(matrix_returns) <- paste(
    selected_keys$sweight, selected_keys$dmname, sep = "::"
  )
  rm(returns)
  gc()
  list(dates = dates, returns = matrix_returns, keys = selected_keys)
}

aggregate_normalized_abnormal <- function(
    y, factor_matrix, slopes_is, slopes_oos, is_rows, oos_rows,
    alpha_stats, alpha_threshold = 2, denominator_tolerance = 1e-10) {
  eligible <- !is.na(alpha_stats$alpha_t) &
    alpha_stats$alpha_t > alpha_threshold &
    !is.na(alpha_stats$alpha_mean) &
    abs(alpha_stats$alpha_mean) > denominator_tolerance
  result <- data.table::data.table(
    row = seq_len(nrow(y)), dm_return = NA_real_,
    n_pairs_available = 0L,
    n_eligible_pairs = sum(eligible)
  )
  if (!any(eligible)) return(result)

  abnormal <- matrix(NA_real_, nrow = nrow(y), ncol = sum(eligible))
  if (any(is_rows)) {
    abnormal[is_rows, ] <- factor_abnormal_returns(
      y[is_rows, eligible, drop = FALSE],
      factor_matrix[is_rows, , drop = FALSE],
      slopes_is[eligible, , drop = FALSE]
    )
  }
  if (any(oos_rows)) {
    abnormal[oos_rows, ] <- factor_abnormal_returns(
      y[oos_rows, eligible, drop = FALSE],
      factor_matrix[oos_rows, , drop = FALSE],
      slopes_oos[eligible, , drop = FALSE]
    )
  }
  normalized <- sweep(
    abnormal, 2L, alpha_stats$alpha_mean[eligible], "/"
  ) * 100
  available <- rowSums(!is.na(normalized))
  averaged <- rowMeans(normalized, na.rm = TRUE)
  averaged[available == 0L] <- NA_real_
  result[, `:=`(
    dm_return = averaged,
    n_pairs_available = as.integer(available)
  )]
  result
}

fit_dm_window_models <- function(
    return_store, factor_data, window_pairs,
    minimum_observations = 60L, alpha_threshold = 2) {
  window_pairs <- unique(data.table::as.data.table(window_pairs), by = c(
    "sweight", "dmname"
  ))
  data.table::setorder(window_pairs, sweight, dmname)
  key_lookup <- return_store$keys[window_pairs, on = c("sweight", "dmname")]
  if (anyNA(key_lookup$column)) {
    stop("A selected mined strategy is absent from the return matrix.")
  }
  y <- return_store$returns[, key_lookup$column, drop = FALSE]
  y <- sweep(y, 2L, window_pairs$orientation, "*")
  dates <- return_store$dates
  start <- as.numeric(window_pairs$sampstart[1L])
  end <- as.numeric(window_pairs$sampend[1L])
  is_rows <- dates >= start & dates <= end
  oos_rows <- dates > end

  factor_rows <- match(dates, as.numeric(factor_data$date))
  factor_matrix <- as.matrix(factor_data[factor_rows, .(mktrf, smb, hml, umd)])
  model_factors <- list(capm = "mktrf", ff4 = c("mktrf", "smb", "hml", "umd"))
  panels <- list()
  stats <- data.table::copy(window_pairs[, .(
    sampstart, sampend, sweight, dmname, raw_mean = rbar,
    raw_t = tstat, orientation
  )])

  for (model in names(model_factors)) {
    factor_names <- model_factors[[model]]
    f <- factor_matrix[, factor_names, drop = FALSE]
    fit_is <- factor_model_slopes(
      y[is_rows, , drop = FALSE], f[is_rows, , drop = FALSE],
      minimum_observations
    )
    fit_oos <- factor_model_slopes(
      y[oos_rows, , drop = FALSE], f[oos_rows, , drop = FALSE],
      minimum_observations
    )
    abnormal_is <- factor_abnormal_returns(
      y[is_rows, , drop = FALSE], f[is_rows, , drop = FALSE], fit_is$slopes
    )
    alpha <- factor_alpha_stats(abnormal_is)
    stats[, paste0(model, c("_alpha_mean", "_alpha_sd", "_alpha_n", "_alpha_t")) :=
      alpha]
    stats[, paste0(model, "_eligible") :=
      !is.na(alpha$alpha_t) & alpha$alpha_t > alpha_threshold]

    panel <- aggregate_normalized_abnormal(
      y, f, fit_is$slopes, fit_oos$slopes, is_rows, oos_rows,
      alpha, alpha_threshold
    )
    panel[, `:=`(
      calendarDate = dates,
      eventDate = as.integer(round(12 * (dates - end)))
    )]
    panels[[model]] <- panel[, .(
      eventDate, calendarDate, dm_return,
      n_eligible_pairs, n_pairs_available
    )]
  }
  list(panels = panels, stats = stats)
}

build_broad_factor_adjusted_dm <- function(
    selected_pairs, return_store, factors, minimum_observations = 60L,
    alpha_threshold = 2L, progress = TRUE) {
  pairs <- data.table::copy(data.table::as.data.table(selected_pairs))
  pairs[, sweight := tolower(sweight)]
  window_candidates <- unique(pairs[, .(
    sampstart, sampend, sweight, dmname, rbar, tstat, orientation
  )])
  windows <- unique(pairs[, .(sampstart, sampend)])
  data.table::setorder(windows, sampend, sampstart)
  factors <- data.table::as.data.table(factors)

  panels <- list(capm = vector("list", nrow(windows)),
                 ff4 = vector("list", nrow(windows)))
  window_stats <- vector("list", nrow(windows))
  for (i in seq_len(nrow(windows))) {
    start <- windows$sampstart[i]
    end <- windows$sampend[i]
    if (progress) {
      message("Factor-adjusting window ", i, "/", nrow(windows),
              " (", start, " to ", end, ")")
    }
    candidates <- window_candidates[sampstart == start & sampend == end]
    fitted <- fit_dm_window_models(
      return_store, factors, candidates,
      minimum_observations, alpha_threshold
    )
    publications <- unique(pairs[
      sampstart == start & sampend == end, .(pubname)
    ])
    for (model in names(panels)) {
      panels[[model]][[i]] <- cross_join_publications(
        publications, fitted$panels[[model]]
      )
    }
    window_stats[[i]] <- fitted$stats
    rm(fitted)
    gc(FALSE)
  }
  list(
    panels = lapply(panels, data.table::rbindlist, use.names = TRUE),
    window_stats = data.table::rbindlist(window_stats, use.names = TRUE)
  )
}

fit_dm_window_full_sample_models <- function(
    return_store, factor_data, window_pairs,
    minimum_observations = 60L, alpha_threshold = 2) {
  window_pairs <- unique(data.table::as.data.table(window_pairs), by = c(
    "sweight", "dmname"
  ))
  data.table::setorder(window_pairs, sweight, dmname)
  key_lookup <- return_store$keys[window_pairs, on = c("sweight", "dmname")]
  y <- return_store$returns[, key_lookup$column, drop = FALSE]
  y <- sweep(y, 2L, window_pairs$orientation, "*")
  dates <- return_store$dates
  start <- as.numeric(window_pairs$sampstart[1L])
  end <- as.numeric(window_pairs$sampend[1L])
  original_rows <- dates >= start & dates <= end
  full_rows <- dates >= start
  factor_rows <- match(dates, as.numeric(factor_data$date))
  factor_matrix <- as.matrix(factor_data[factor_rows, .(mktrf, smb, hml, umd)])
  model_factors <- list(capm = "mktrf", ff3 = c("mktrf", "smb", "hml"))
  panels <- list()
  stats <- data.table::copy(window_pairs[, .(
    sampstart, sampend, sweight, dmname, raw_mean = rbar,
    raw_t = tstat, orientation
  )])
  for (model in names(model_factors)) {
    f <- factor_matrix[, model_factors[[model]], drop = FALSE]
    fit <- factor_model_slopes(
      y[full_rows, , drop = FALSE], f[full_rows, , drop = FALSE],
      minimum_observations
    )
    abnormal_original <- factor_abnormal_returns(
      y[original_rows, , drop = FALSE],
      f[original_rows, , drop = FALSE], fit$slopes
    )
    alpha <- factor_alpha_stats(abnormal_original)
    stats[, paste0(model, c("_alpha_mean", "_alpha_sd", "_alpha_n", "_alpha_t")) :=
      alpha]
    stats[, paste0(model, "_eligible") :=
      !is.na(alpha$alpha_t) & alpha$alpha_t > alpha_threshold]
    panel <- aggregate_normalized_abnormal(
      y, f, fit$slopes, fit$slopes,
      full_rows, rep(FALSE, length(full_rows)),
      alpha, alpha_threshold
    )
    panel[, `:=`(
      calendarDate = dates,
      eventDate = as.integer(round(12 * (dates - end)))
    )]
    panels[[model]] <- panel[, .(
      eventDate, calendarDate, dm_return,
      n_eligible_pairs, n_pairs_available
    )]
  }
  list(panels = panels, stats = stats)
}

build_broad_full_sample_factor_adjusted_dm <- function(
    selected_pairs, return_store, factors, minimum_observations = 60L,
    alpha_threshold = 2L, progress = TRUE) {
  pairs <- data.table::copy(data.table::as.data.table(selected_pairs))
  pairs[, sweight := tolower(sweight)]
  candidates <- unique(pairs[, .(
    sampstart, sampend, sweight, dmname, rbar, tstat, orientation
  )])
  windows <- unique(pairs[, .(sampstart, sampend)])
  data.table::setorder(windows, sampend, sampstart)
  factors <- data.table::as.data.table(factors)
  panels <- list(capm = vector("list", nrow(windows)),
                 ff3 = vector("list", nrow(windows)))
  window_stats <- vector("list", nrow(windows))
  for (i in seq_len(nrow(windows))) {
    start <- windows$sampstart[i]
    end <- windows$sampend[i]
    if (progress) {
      message("Full-sample factor window ", i, "/", nrow(windows),
              " (", start, " to ", end, ")")
    }
    fitted <- fit_dm_window_full_sample_models(
      return_store, factors,
      candidates[sampstart == start & sampend == end],
      minimum_observations, alpha_threshold
    )
    publications <- unique(pairs[
      sampstart == start & sampend == end, .(pubname)
    ])
    for (model in names(panels)) {
      panels[[model]][[i]] <- cross_join_publications(
        publications, fitted$panels[[model]]
      )
    }
    window_stats[[i]] <- fitted$stats
    rm(fitted)
    gc(FALSE)
  }
  list(
    panels = lapply(panels, data.table::rbindlist, use.names = TRUE),
    window_stats = data.table::rbindlist(window_stats, use.names = TRUE)
  )
}

# Pipeline setup ----------------------------------------------------------

out_dir <- Sys.getenv("FACTOR_DM_OUT_DIR", unset = "../Data/Processed")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
cache_path <- file.path(out_dir, "factor_adjusted_dm_benchmarks.RDS")
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

return_store <- load_selected_dm_return_matrix(dm_path, base_pairs)
dm_result <- build_broad_factor_adjusted_dm(
  base_pairs, return_store, factors,
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

# Appendix full-sample specification --------------------------------------

# The appendix CAPM/FF3 specification shares the same selected strategies,
# factor panel, and dense return matrix. It differs only in estimating one
# coefficient vector from the published sample start through the end of data.
rm(
  dm_result, result, capm, ff4, published_stats, window_diagnostics,
  model_pair_counts, build_model_contract
)
invisible(gc())

raw_stats <- copy(published_raw)
capm_fs <- czret[date >= sampstart, .(
  beta_capm_fs = extract_beta(ret, mktrf)
), by = signalname]
czret <- merge(czret, capm_fs, by = "signalname", all.x = TRUE)
czret[, abnormal_capm_fs := fifelse(
  date >= sampstart, ret - beta_capm_fs * mktrf, NA_real_
)]
capm_fs_alpha <- czret[date >= sampstart & date <= sampend, .(
  abar_capm_fs = mean(abnormal_capm_fs, na.rm = TRUE),
  abar_capm_fs_t = {
    n <- sum(!is.na(abnormal_capm_fs))
    m <- mean(abnormal_capm_fs, na.rm = TRUE)
    s <- sd(abnormal_capm_fs, na.rm = TRUE)
    if (n > 1L && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret <- merge(czret, capm_fs_alpha, by = "signalname", all.x = TRUE)
czret[, abnormal_capm_fs_normalized := fifelse(
  abs(abar_capm_fs) > 1e-10,
  100 * abnormal_capm_fs / abar_capm_fs,
  NA_real_
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
ff3_fs_alpha <- czret[date >= sampstart & date <= sampend, .(
  abar_ff3_fs = mean(abnormal_ff3_fs, na.rm = TRUE),
  abar_ff3_fs_t = {
    n <- sum(!is.na(abnormal_ff3_fs))
    m <- mean(abnormal_ff3_fs, na.rm = TRUE)
    s <- sd(abnormal_ff3_fs, na.rm = TRUE)
    if (n > 1L && s > 0) m / s * sqrt(n) else NA_real_
  }
), by = signalname]
czret <- merge(czret, ff3_fs_alpha, by = "signalname", all.x = TRUE)
czret[, abnormal_ff3_fs_normalized := fifelse(
  abs(abar_ff3_fs) > 1e-10,
  100 * abnormal_ff3_fs / abar_ff3_fs,
  NA_real_
)]

full_sample_published_stats <- Reduce(
  function(x, y) merge(x, y, by = "signalname", all = TRUE),
  list(raw_stats, capm_fs, capm_fs_alpha, ff3_fs, ff3_fs_alpha)
)
full_sample_published_stats[, `:=`(
  eligible_raw_t2 = !is.na(rbar_t) & rbar_t > raw_t_threshold,
  eligible_capm_t2 = !is.na(rbar_t) & rbar_t > raw_t_threshold &
    !is.na(abar_capm_fs_t) & abar_capm_fs_t > alpha_t_threshold,
  eligible_ff3_t2 = !is.na(rbar_t) & rbar_t > raw_t_threshold &
    !is.na(abar_ff3_fs_t) & abar_ff3_fs_t > alpha_t_threshold
)]

full_sample_dm <- build_broad_full_sample_factor_adjusted_dm(
  base_pairs, return_store, factors,
  minimum_observations = minimum_observations,
  alpha_threshold = alpha_t_threshold
)

build_full_sample_contract <- function(model) {
  eligible_col <- paste0("eligible_", model, "_t2")
  published_col <- paste0("abnormal_", model, "_fs_normalized")
  eligible_signals <- full_sample_published_stats[
    get(eligible_col), signalname
  ]
  published_panel <- czret[
    signalname %in% eligible_signals & date >= sampstart,
    .(
      pubname = signalname, eventDate, calendarDate = date,
      published_return = get(published_col)
    )
  ]
  dm_panel <- full_sample_dm$panels[[model]][!is.na(dm_return)]
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

full_sample_window_diagnostics <- full_sample_dm$window_stats[, .(
  base_candidates = .N,
  capm_eligible_candidates = sum(capm_eligible, na.rm = TRUE),
  ff3_eligible_candidates = sum(ff3_eligible, na.rm = TRUE)
), by = .(sampstart, sampend)]
full_sample_result <- list(
  capm = build_full_sample_contract("capm"),
  ff3 = build_full_sample_contract("ff3"),
  published_stats = as_tibble(full_sample_published_stats),
  window_diagnostics = as_tibble(full_sample_window_diagnostics),
  metadata = list(
    schema_version = 1L,
    appendix_only = TRUE,
    base_universe = "accounting_t2",
    base_pair_count = nrow(base_pairs),
    base_pair_fingerprint_sha256 = base_fingerprint,
    coefficient_regime = "all observations from published sample start onward",
    factor_models = list(capm = "Mkt-RF", ff3 = c("Mkt-RF", "SMB", "HML")),
    minimum_factor_observations = minimum_observations,
    raw_t_threshold = raw_t_threshold,
    alpha_t_threshold = alpha_t_threshold
  )
)
stopifnot(
  !anyDuplicated(
    as.data.frame(full_sample_result$capm$panel)[c("pubname", "eventDate")]
  ),
  !anyDuplicated(
    as.data.frame(full_sample_result$ff3$panel)[c("pubname", "eventDate")]
  ),
  identical(
    full_sample_result$metadata$base_pair_fingerprint_sha256,
    raw_contract$metadata$accounting_t2$pair_fingerprint_sha256
  )
)
full_sample_cache_path <- file.path(
  out_dir, "appendix_full_sample_dm_benchmarks.RDS"
)
saveRDS(full_sample_result, full_sample_cache_path)
message("Wrote ", full_sample_cache_path)
rm(return_store)
