# Test window-batched factor-adjustment helpers against direct regressions.
#
# How to run: from flex-mining/, run
#   Rscript tests/test_factor_adjusted_dm_helpers.R
# Inputs: helpers/factor_adjusted_dm.R
# Outputs: none; exits nonzero on failure.

library(data.table)
source("helpers/factor_adjusted_dm.R")

set.seed(418)
n <- 180L
factors <- cbind(
  mktrf = rnorm(n), smb = rnorm(n), hml = rnorm(n), umd = rnorm(n)
)
slopes <- rbind(
  c(0.7, -0.2, 0.1, 0.3),
  c(-0.4, 0.5, 0.2, -0.1),
  c(1.2, 0.1, -0.3, 0.4)
)
y <- factors %*% t(slopes) + matrix(rnorm(n * 3L, sd = 0.7), ncol = 3L)
y[1:25, 2] <- NA_real_
y[seq(4, n, by = 13), 3] <- NA_real_

fitted <- factor_model_slopes(y, factors, minimum_observations = 60L)
direct <- t(vapply(seq_len(ncol(y)), function(j) {
  complete <- complete.cases(y[, j], factors)
  coef(lm(y[complete, j] ~ factors[complete, ]))[-1L]
}, numeric(4L)))
stopifnot(isTRUE(all.equal(
  fitted$slopes, direct, tolerance = 1e-10, check.attributes = FALSE
)))

abnormal <- factor_abnormal_returns(y, factors, fitted$slopes)
stats <- factor_alpha_stats(abnormal)
direct_stats <- rbindlist(lapply(seq_len(ncol(y)), function(j) {
  x <- abnormal[, j]
  data.table(
    alpha_mean = mean(x, na.rm = TRUE),
    alpha_sd = sd(x, na.rm = TRUE),
    alpha_n = sum(!is.na(x)),
    alpha_t = mean(x, na.rm = TRUE) / sd(x, na.rm = TRUE) *
      sqrt(sum(!is.na(x)))
  )
}))
stopifnot(isTRUE(all.equal(
  stats, direct_stats, tolerance = 1e-10, check.attributes = FALSE
)))

# Fewer than 60 observations must not produce coefficients.
short <- y
short[60:n, 1] <- NA_real_
short_fit <- factor_model_slopes(short, factors, minimum_observations = 60L)
stopifnot(all(is.na(short_fit$slopes[1, ])), short_fit$nobs[1] == 59L)

# Reusing a single full-sample slope matrix across original/post-sample rows
# must preserve the same abnormal-return calculation.
full_abnormal <- factor_abnormal_returns(y, factors, fitted$slopes)
stopifnot(isTRUE(all.equal(
  full_abnormal, abnormal, tolerance = 1e-12, check.attributes = FALSE
)))

message("Window-batched factor-adjustment helper tests passed.")
