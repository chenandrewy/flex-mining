# Window-batched factor adjustment for broad data-mined benchmarks.
#
# How to run: source from a Chapter 3 or Appendix producer after
#   0_Environment.R.
# Inputs: compact selected pair catalogs, mined long-short returns, and factor
#   returns supplied to the functions below.
# Outputs: in-memory coefficient, alpha-statistic, and predictor/event-month
#   benchmark objects; this helper writes no files.

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
    selected_pairs, dm_path, factors, minimum_observations = 60L,
    alpha_threshold = 2L, progress = TRUE) {
  pairs <- data.table::copy(data.table::as.data.table(selected_pairs))
  pairs[, sweight := tolower(sweight)]
  window_candidates <- unique(pairs[, .(
    sampstart, sampend, sweight, dmname, rbar, tstat, orientation
  )])
  windows <- unique(pairs[, .(sampstart, sampend)])
  data.table::setorder(windows, sampend, sampstart)
  return_store <- load_selected_dm_return_matrix(dm_path, pairs)
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
  rm(return_store)
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
    selected_pairs, dm_path, factors, minimum_observations = 60L,
    alpha_threshold = 2L, progress = TRUE) {
  pairs <- data.table::copy(data.table::as.data.table(selected_pairs))
  pairs[, sweight := tolower(sweight)]
  candidates <- unique(pairs[, .(
    sampstart, sampend, sweight, dmname, rbar, tstat, orientation
  )])
  windows <- unique(pairs[, .(sampstart, sampend)])
  data.table::setorder(windows, sampend, sampstart)
  return_store <- load_selected_dm_return_matrix(dm_path, pairs)
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
  rm(return_store)
  list(
    panels = lapply(panels, data.table::rbindlist, use.names = TRUE),
    window_stats = data.table::rbindlist(window_stats, use.names = TRUE)
  )
}
