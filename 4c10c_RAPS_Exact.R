# Exact RAPS replication — Chen & Zimmermann (2020)
# Matches their paper's implementation:
#   - Full parameter vector: (sigma_mu, nu_mu, mu_sigma, sigma_sigma, t_cut, t_slope)
#   - mu_mu = 0 (fixed)
#   - Lognormal prior on sigma_i: log(sigma_i) ~ N(mu_sigma, sigma_sigma)
#   - Numerator: quadrature for normal-t convolution
#   - Denominator (Z): Monte Carlo over (sigma, r) draws from the model
#   - Optimizer: golden section for nu_mu, Nelder-Mead for rest
#   - Posterior mean: numerical integration (Eq 16-17)

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")

# Load data  -------------------------------------------

ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>% filter(Keep)
dmcomp <- readRDS("../Data/Processed/dmcomp_sumstats.RDS")
dm_insamp <- as.data.table(dmcomp$insampsum)

dm_insamp[, se := ifelse(abs(tstat) > 0.01, abs(rbar / tstat), NA_real_)]
dm_insamp <- dm_insamp[!is.na(se) & se > 0 & is.finite(se)]

signal_info <- czsum %>%
  transmute(
    signalname,
    rbar_pub = rbar,
    tstat_pub = tstat,
    se_pub = abs(rbar / tstat),
    sampend_year = floor(as.numeric(sampend)),
    cohort = paste0(floor(sampend_year / 10) * 10, "s")
  )

# Apply Campbell filters -------------------------------------------

cal_data <- ret_for_plot0 %>%
  filter(!is.na(matchRet)) %>%
  left_join(signal_info, by = c("pubname" = "signalname"))

min_oos_months <- 60
signals_with_enough_oos <- cal_data %>%
  filter(eventDate > 0) %>%
  group_by(pubname) %>%
  summarize(max_oos = max(eventDate), .groups = "drop") %>%
  filter(max_oos >= min_oos_months) %>%
  pull(pubname)

cal_data <- cal_data %>% filter(pubname %in% signals_with_enough_oos)

min_cohort_size <- 10
cohort_sizes <- cal_data %>%
  distinct(pubname, cohort) %>%
  count(cohort) %>%
  filter(n >= min_cohort_size)

cal_data <- cal_data %>% filter(cohort %in% cohort_sizes$cohort)

cat("Signals after Campbell filters:", n_distinct(cal_data$pubname), "\n")

cal_data <- cal_data %>%
  mutate(
    ret_raw = ret * rbar_pub / 100,
    matchRet_raw = matchRet * rbar_pub / 100
  )

# ============================================================
# RAPS EXACT HELPER FUNCTIONS
# ============================================================

# Scaled Student's t density: x ~ mu_mu + sigma_mu * t_{nu}
# With mu_mu = 0: f(x | sigma_mu, nu) = dt(x/sigma_mu, nu) / sigma_mu
dst <- function(x, sigma_mu, nu) {
  dt(x / sigma_mu, df = nu) / sigma_mu
}

# Marginal density f(r | sigma, theta_mu) = integral N(r; mu, sigma^2) * f_t(mu; sigma_mu, nu) dmu
# Uses u-substitution: u = (mu - r)/sigma => phi(u) * f_t(r + sigma*u) du
marginal_density <- function(r, sigma, sigma_mu, nu, n_grid = 200) {
  u_grid <- seq(-6, 6, length.out = n_grid)
  du <- u_grid[2] - u_grid[1]
  phi_grid <- dnorm(u_grid)

  N <- length(r)
  G <- n_grid
  mu_mat <- matrix(r, N, G) + outer(sigma, u_grid)
  prior_mat <- matrix(dt(as.vector(mu_mat) / sigma_mu, df = nu) / sigma_mu, N, G)
  phi_mat <- matrix(phi_grid, N, G, byrow = TRUE)
  rowSums(phi_mat * prior_mat) * du
}

# Posterior mean E[mu | r, sigma, theta_mu] (Eq 16-17)
posterior_mean <- function(r, sigma, sigma_mu, nu, n_grid = 200) {
  u_grid <- seq(-6, 6, length.out = n_grid)
  du <- u_grid[2] - u_grid[1]
  phi_grid <- dnorm(u_grid)

  N <- length(r)
  G <- n_grid
  mu_mat <- matrix(r, N, G) + outer(sigma, u_grid)
  prior_mat <- matrix(dt(as.vector(mu_mat) / sigma_mu, df = nu) / sigma_mu, N, G)
  phi_mat <- matrix(phi_grid, N, G, byrow = TRUE)

  integrand <- phi_mat * prior_mat
  denom <- rowSums(integrand) * du
  numer <- rowSums(integrand * mu_mat) * du
  ifelse(denom > 0, numer / denom, r)
}

# ============================================================
# RAPS MLE WITH MONTE CARLO NORMALIZING CONSTANT
# ============================================================
# Parameters: theta = (sigma_mu, nu_mu, mu_sigma, sigma_sigma, t_cut, t_slope)
# mu_mu = 0 (fixed)
#
# Log-likelihood (Eq A1):
#   sum_i [ log p(t_i) + log f(r_i | sigma_i, theta_mu) + log f_N(log sigma_i | mu_sigma, sigma_sigma)
#           - log Z(theta) ]
# where Z = E_{model}[p(r/sigma)] is a SINGLE normalizing constant (not per-signal)
# because they integrate over the sigma distribution too.

fit_raps_exact <- function(rbar, se, n_mc = 50000, seed = 42) {

  N <- length(rbar)
  ti <- rbar / se
  log_se <- log(se)

  # The likelihood per observation i:
  # L_i = p(t_i | theta) * f(r_i | sigma_i, theta_mu) * f_lnorm(sigma_i | mu_sig, sig_sig) / Z
  # Z = integral integral p(r/s) * f(r | s, theta_mu) * f_lnorm(s | mu_sig, sig_sig) dr ds
  # Z is estimated by Monte Carlo: draw (sigma*, mu*) from the model, compute r* = mu* + sigma* * eps,
  # then Z ≈ mean(p(r*/sigma*))

  # Neg log-likelihood, with nu_mu FIXED (will be profiled out)
  neg_ll_fixed_nu <- function(par, nu_fixed) {
    sm <- exp(par[1])           # sigma_mu
    mu_sig <- par[2]            # mu_sigma
    sig_sig <- exp(par[3])      # sigma_sigma > 0
    t_cut <- par[4]             # t_cut
    t_slope <- exp(par[5])      # t_slope > 0

    nu <- nu_fixed

    # --- Term 1: log marginal density f(r_i | sigma_i, theta_mu) ---
    marg <- marginal_density(rbar, se, sm, nu)
    marg[marg <= 0] <- 1e-300
    log_f_r <- log(marg)

    # --- Term 2: log publication probability ---
    log_p <- -log(1 + exp(-t_slope * (ti - t_cut)))

    # --- Term 3: log prior on sigma_i ---
    log_f_sig <- dnorm(log_se, mean = mu_sig, sd = sig_sig, log = TRUE)

    # --- Term 4: normalizing constant Z by Monte Carlo ---
    set.seed(seed)
    # Draw sigma from lognormal
    log_sig_mc <- rnorm(n_mc, mean = mu_sig, sd = sig_sig)
    sig_mc <- exp(log_sig_mc)
    # Draw mu from scaled t
    mu_mc <- sm * rt(n_mc, df = nu)
    # Draw r from N(mu, sigma^2)
    r_mc <- rnorm(n_mc, mean = mu_mc, sd = sig_mc)
    # t-stat
    t_mc <- r_mc / sig_mc
    # Publication probability
    p_mc <- 1 / (1 + exp(-t_slope * (t_mc - t_cut)))
    # Z = E[p(t)] under the model
    Z <- mean(p_mc)
    if (Z <= 0) Z <- 1e-300
    log_Z <- log(Z)

    # --- Total neg log-likelihood ---
    -sum(log_f_r + log_p + log_f_sig - log_Z)
  }

  # Profile over nu_mu using golden section / grid search
  # For each nu_mu, optimize the other 5 parameters
  cat("  Profiling over nu_mu...\n")

  nu_grid <- c(2.5, 3, 3.5, 4, 5, 6, 8, 10, 15, 20, 50)
  best_ll <- Inf
  best_par <- NULL
  best_nu <- NULL

  for (nu_try in nu_grid) {
    # Starting values
    start <- c(log(0.45),   # log sigma_mu
               mean(log_se), # mu_sigma
               log(sd(log_se)), # log sigma_sigma
               1.6,          # t_cut
               log(11))      # log t_slope

    res <- tryCatch(
      optim(start, neg_ll_fixed_nu, nu_fixed = nu_try,
            method = "Nelder-Mead",
            control = list(maxit = 5000, reltol = 1e-8)),
      error = function(e) list(value = Inf, par = start, convergence = 1)
    )

    cat(sprintf("    nu=%.1f: neg_ll=%.1f, sm=%.3f, mu_sig=%.2f, sig_sig=%.2f, t_cut=%.2f, t_slope=%.1f\n",
        nu_try, res$value, exp(res$par[1]), res$par[2], exp(res$par[3]),
        res$par[4], exp(res$par[5])))

    if (res$value < best_ll) {
      best_ll <- res$value
      best_par <- res$par
      best_nu <- nu_try
    }
  }

  # Refine nu_mu around the best grid point
  cat(sprintf("  Best grid nu=%.1f, refining...\n", best_nu))
  nu_refine <- seq(max(2.1, best_nu - 1), best_nu + 1, by = 0.25)
  nu_refine <- nu_refine[nu_refine > 2]

  for (nu_try in nu_refine) {
    # Start from best_par
    res <- tryCatch(
      optim(best_par, neg_ll_fixed_nu, nu_fixed = nu_try,
            method = "Nelder-Mead",
            control = list(maxit = 3000, reltol = 1e-8)),
      error = function(e) list(value = Inf, par = best_par, convergence = 1)
    )

    if (res$value < best_ll) {
      best_ll <- res$value
      best_par <- res$par
      best_nu <- nu_try
    }
  }

  list(
    sigma_mu = exp(best_par[1]),
    nu_mu = best_nu,
    mu_sigma = best_par[2],
    sigma_sigma = exp(best_par[3]),
    t_cut = best_par[4],
    t_slope = exp(best_par[5]),
    neg_loglik = best_ll
  )
}

# ============================================================
# PART 1: PUBLISHED — Exact RAPS replication
# ============================================================
# IMPORTANT: Estimate RAPS on ALL published signals (not Campbell-filtered subset).
# The RAPS method is purely in-sample — no OOS data needed for estimation.
# Campbell filters apply only to the OOS validation step later.

pub_data_all <- signal_info %>%
  filter(!is.na(rbar_pub) & !is.na(se_pub) & se_pub > 0) %>%
  transmute(pubname = signalname, cohort,
            rbar = rbar_pub, se = se_pub, tstat = tstat_pub)

cat("\n=== PUBLISHED: Exact RAPS replication ===\n")
cat("N signals (ALL published):", nrow(pub_data_all), "\n")
cat("Mean IS return:", round(mean(pub_data_all$rbar), 3), "\n")
cat("Mean SE:", round(mean(pub_data_all$se), 3), "\n")
cat("Mean log(SE):", round(mean(log(pub_data_all$se)), 3), "\n")
cat("SD log(SE):", round(sd(log(pub_data_all$se)), 3), "\n")

tic("Exact RAPS fit")
fit_exact <- fit_raps_exact(pub_data_all$rbar, pub_data_all$se, n_mc = 100000)
toc()

cat("\n--- Estimated parameters ---\n")
cat(sprintf("  sigma_mu:    %.3f  (RAPS Table 3: 0.45)\n", fit_exact$sigma_mu))
cat(sprintf("  nu_mu:       %.2f  (RAPS Table 3: 3.89)\n", fit_exact$nu_mu))
cat(sprintf("  mu_sigma:    %.3f  (RAPS Table 3: -1.67)\n", fit_exact$mu_sigma))
cat(sprintf("  sigma_sigma: %.3f  (RAPS Table 3: 0.52)\n", fit_exact$sigma_sigma))
cat(sprintf("  t_cut:       %.2f  (RAPS Table 3: 1.61)\n", fit_exact$t_cut))
cat(sprintf("  t_slope:     %.1f  (RAPS Table 3: 11.29)\n", fit_exact$t_slope))

# Posterior means for ALL published signals
cat("\nComputing posterior means (all published)...\n")
pub_data_all$mu_hat <- posterior_mean(pub_data_all$rbar, pub_data_all$se,
                                      fit_exact$sigma_mu, fit_exact$nu_mu)
pub_data_all$shrinkage <- 1 - pub_data_all$mu_hat / pub_data_all$rbar

cat(sprintf("  Mean shrinkage:   %.1f%%  (RAPS Table 3: 12.3%%)\n",
            mean(pub_data_all$shrinkage) * 100))
cat(sprintf("  Median shrinkage: %.1f%%  (RAPS Table 3: 9.5%%)\n",
            median(pub_data_all$shrinkage) * 100))

# Cross-sectional SD of true returns
sd_true <- sqrt(fit_exact$nu_mu / (fit_exact$nu_mu - 2)) * fit_exact$sigma_mu
cat(sprintf("  SD of true returns: %.3f  (RAPS: ~0.65)\n", sd_true))

# ============================================================
# PART 2: DATA-MINED — Student's t, full universe (no selection)
# ============================================================

# For validation: Campbell-filtered subset with posterior means from the RAPS fit
pub_data <- pub_data_all %>%
  filter(pubname %in% unique(cal_data$pubname))
cat("\nSignals in Campbell-filtered validation set:", nrow(pub_data), "\n")

pub_signals <- unique(pub_data$pubname)
rep_signal <- pub_signals[which.min(abs(nchar(pub_signals) - 5))]
dm_rep <- dm_insamp[pubname == rep_signal]

cat("\n=== DATA-MINED: Fitting t-prior (no selection) ===\n")
cat("Signal:", rep_signal, "N strategies:", nrow(dm_rep), "\n")

# Fit (sigma_mu, nu_mu) from full DM universe
fit_dm_t <- function(rbar, se, n_subsample = 5000) {
  n <- length(rbar)
  if (n > n_subsample) {
    set.seed(42)
    idx <- sample(n, n_subsample)
    rbar <- rbar[idx]; se <- se[idx]
  }

  neg_ll <- function(par) {
    sm <- exp(par[1])
    nu <- exp(par[2]) + 2
    dens <- marginal_density(rbar, se, sm, nu)
    dens[dens <= 0] <- 1e-300
    -sum(log(dens))
  }

  res <- optim(c(log(0.3), log(2)), neg_ll, method = "Nelder-Mead",
               control = list(maxit = 3000, reltol = 1e-7))
  list(sigma_mu = exp(res$par[1]), nu_mu = exp(res$par[2]) + 2,
       convergence = res$convergence)
}

tic("DM t-prior fit")
fit_dm <- fit_dm_t(dm_rep$rbar, dm_rep$se)
toc()

sm_dm <- fit_dm$sigma_mu
nu_dm <- fit_dm$nu_mu
sm_dm_eff <- sm_dm * sqrt(nu_dm / (nu_dm - 2))

cat(sprintf("  sigma_mu: %.3f, nu_mu: %.2f, sm_eff: %.3f\n", sm_dm, nu_dm, sm_dm_eff))

# DM shrinkage per signal
# Units: matchRet in ret_for_plot0 scales each matched DM strategy by its OWN
# in-sample |rbar| (0_Environment.R make_DM_event_returns: ret*sign/abs(rbar)*100),
# so matchRet_raw = matchRet * rbar_pub / 100 is a retention ratio times the
# published signal's scale. For unit consistency, the DM IS and bias-adjusted
# rows must be on the same scale: IS = rbar_pub (retention 1), and the adjusted
# retention per strategy is mu_hat_j / |rbar_j| with mu_hat_j the exact
# Student-t posterior mean, matching the published columns (retention is
# sign-invariant, so |rbar| is sufficient). The strategy set replicates the
# SelectDMStrats filters used to build matchRet (|t| > t_min plus stock-count
# and coverage screens), not the plain |t| > 2 screen.
cat("Computing DM shrinkage per signal...\n")
tic("DM per-signal")
dm_results <- lapply(pub_signals, function(pname) {
  dm_full <- dm_insamp[pubname == pname]
  if (nrow(dm_full) < 100) return(NULL)

  # Same filters as SelectDMStrats with globalSettings (tolerances off)
  m_idx <- which(
    abs(dm_full$tstat) > globalSettings$t_min &
      abs(dm_full$tstat) < globalSettings$t_max &
      dm_full$min_nstock_long >= globalSettings$minNumStocks / 2 &
      dm_full$min_nstock_short >= globalSettings$minNumStocks / 2 &
      dm_full$nlastyear == 12 &
      dm_full$nmonth >= 5 * 12
  )
  if (length(m_idx) < 5) return(NULL)

  rbar_pub_i <- signal_info$rbar_pub[match(pname, signal_info$signalname)]

  rbar_m <- abs(dm_full$rbar[m_idx])
  se_m <- dm_full$se[m_idx]
  mu_hat_m <- posterior_mean(rbar_m, se_m, sm_dm, nu_dm)
  retention_m <- mu_hat_m / rbar_m
  adj_retention <- mean(retention_m)

  data.frame(pubname = pname, n_dm_t2 = length(m_idx),
             dm_is_raw = rbar_pub_i,
             dm_adj_raw = adj_retention * rbar_pub_i,
             dm_is_own = mean(rbar_m),
             dm_shrinkage_mean = mean(1 - retention_m))
})
toc()
dm_df <- bind_rows(dm_results)

# ============================================================
# PART 2b: PUBLISHED with DM prior
# ============================================================

cat("\n=== PUBLISHED with DM prior ===\n")
# Apply DM prior to all published signals, then filter for validation
pub_data_all$mu_hat_dm <- posterior_mean(pub_data_all$rbar, pub_data_all$se, sm_dm, nu_dm)
pub_data_all$shrinkage_dm <- 1 - pub_data_all$mu_hat_dm / pub_data_all$rbar
cat(sprintf("  Mean shrinkage (DM prior, all): %.1f%%\n", mean(pub_data_all$shrinkage_dm) * 100))

# Refresh the Campbell-filtered subset
pub_data <- pub_data_all %>% filter(pubname %in% unique(cal_data$pubname))
cat(sprintf("  Mean shrinkage (DM prior, Campbell): %.1f%%\n", mean(pub_data$shrinkage_dm) * 100))

# ============================================================
# PART 3: OOS returns
# ============================================================

pub_oos <- cal_data %>%
  filter(eventDate > 0) %>%
  group_by(pubname) %>%
  summarize(pub_oos = mean(ret_raw, na.rm = TRUE), n_oos = n(), .groups = "drop") %>%
  filter(n_oos >= 24)

dm_oos <- cal_data %>%
  filter(eventDate > 0) %>%
  group_by(pubname) %>%
  summarize(dm_oos = mean(matchRet_raw, na.rm = TRUE), .groups = "drop")

# ============================================================
# PART 4: Validation & Decomposition
# ============================================================

validation <- pub_data %>%
  select(pubname, cohort, pub_is = rbar, pub_adj = mu_hat, pub_adj_dm = mu_hat_dm,
         pub_shrinkage = shrinkage, pub_shrinkage_dm = shrinkage_dm) %>%
  inner_join(pub_oos, by = "pubname") %>%
  inner_join(dm_df, by = "pubname") %>%
  inner_join(dm_oos, by = "pubname")

cat("\n=== VALIDATION ===\n")

cat("\n--- Published ---\n")
cat(sprintf("%-35s %10s %10s %10s\n", "Metric", "IS", "Own prior", "DM prior"))
cat(sprintf("%-35s %10.3f %10.3f %10.3f\n", "Mean bias (pred - OOS)",
            mean(validation$pub_is - validation$pub_oos),
            mean(validation$pub_adj - validation$pub_oos),
            mean(validation$pub_adj_dm - validation$pub_oos)))
cat(sprintf("%-35s %10.3f %10.3f %10.3f\n", "RMSE",
            sqrt(mean((validation$pub_is - validation$pub_oos)^2)),
            sqrt(mean((validation$pub_adj - validation$pub_oos)^2)),
            sqrt(mean((validation$pub_adj_dm - validation$pub_oos)^2))))
cat(sprintf("%-35s %10.3f %10.3f %10.3f\n", "Correlation with OOS",
            cor(validation$pub_is, validation$pub_oos),
            cor(validation$pub_adj, validation$pub_oos),
            cor(validation$pub_adj_dm, validation$pub_oos)))

cat("\n--- Data-Mined ---\n")
cat(sprintf("%-35s %10s %10s\n", "Metric", "IS", "RAPS(t)"))
cat(sprintf("%-35s %10.3f %10.3f\n", "Mean bias (pred - OOS)",
            mean(validation$dm_is_raw - validation$dm_oos),
            mean(validation$dm_adj_raw - validation$dm_oos)))
cat(sprintf("%-35s %10.3f %10.3f\n", "RMSE",
            sqrt(mean((validation$dm_is_raw - validation$dm_oos)^2)),
            sqrt(mean((validation$dm_adj_raw - validation$dm_oos)^2))))

decomp <- validation %>%
  mutate(
    pub_total_decay = pub_is - pub_oos,
    pub_selection = pub_is - pub_adj,
    pub_learning = pub_adj - pub_oos,
    pub_selection_dm = pub_is - pub_adj_dm,
    pub_learning_dm = pub_adj_dm - pub_oos,
    dm_total_decay = dm_is_raw - dm_oos,
    dm_selection = dm_is_raw - dm_adj_raw,
    dm_learning = dm_adj_raw - dm_oos
  )

cat("\n=== DECOMPOSITION (Exact RAPS, % per month) ===\n")
cat(sprintf("\n%-35s %12s %12s %12s\n", "", "Pub(own)", "Pub(DM prior)", "Data-Mined"))
cat(sprintf("%-35s %12.3f %12.3f %12.3f\n", "IS observed",
            mean(decomp$pub_is), mean(decomp$pub_is), mean(decomp$dm_is_raw)))
cat(sprintf("%-35s %12.3f %12.3f %12.3f\n", "RAPS bias-adjusted",
            mean(decomp$pub_adj), mean(decomp$pub_adj_dm), mean(decomp$dm_adj_raw)))
cat(sprintf("%-35s %12.3f %12.3f %12.3f\n", "OOS observed",
            mean(decomp$pub_oos), mean(decomp$pub_oos), mean(decomp$dm_oos)))
cat(sprintf("%-35s %12s %12s %12s\n", "", "---", "---", "---"))
cat(sprintf("%-35s %12.3f %12.3f %12.3f\n", "Total decay (IS - OOS)",
            mean(decomp$pub_total_decay), mean(decomp$pub_total_decay), mean(decomp$dm_total_decay)))
cat(sprintf("%-35s %12.3f %12.3f %12.3f\n", "  Selection bias (IS - adj)",
            mean(decomp$pub_selection), mean(decomp$pub_selection_dm), mean(decomp$dm_selection)))
cat(sprintf("%-35s %12.3f %12.3f %12.3f\n", "  Investor learning (adj - OOS)",
            mean(decomp$pub_learning), mean(decomp$pub_learning_dm), mean(decomp$dm_learning)))

cat(sprintf("\n%-35s %11.1f%% %11.1f%% %11.1f%%\n", "Selection as % of total decay",
            mean(decomp$pub_selection) / mean(decomp$pub_total_decay) * 100,
            mean(decomp$pub_selection_dm) / mean(decomp$pub_total_decay) * 100,
            mean(decomp$dm_selection) / mean(decomp$dm_total_decay) * 100))
cat(sprintf("%-35s %11.1f%% %11.1f%% %11.1f%%\n", "Learning as % of total decay",
            mean(decomp$pub_learning) / mean(decomp$pub_total_decay) * 100,
            mean(decomp$pub_learning_dm) / mean(decomp$pub_total_decay) * 100,
            mean(decomp$dm_learning) / mean(decomp$dm_total_decay) * 100))

cat(sprintf("\n%-35s %12.3f %12.3f %12.3f\n", "sigma_mu",
            fit_exact$sigma_mu, sm_dm, sm_dm))
cat(sprintf("%-35s %12.2f %12.2f %12.2f\n", "nu_mu",
            fit_exact$nu_mu, nu_dm, nu_dm))
cat(sprintf("%-35s %11.1f%% %11.1f%% %11.1f%%\n", "Mean shrinkage",
            mean(decomp$pub_shrinkage) * 100,
            mean(decomp$pub_shrinkage_dm) * 100,
            mean(decomp$dm_shrinkage_mean) * 100))

# ============================================================
# PART 7: Save tables
# ============================================================

model_tag <- "Exact"

results_dir <- "../Results"
data_dir    <- "../Data/Processed"
if (!dir.exists(results_dir)) dir.create(results_dir, recursive = TRUE)

# 7a. Per-signal decomposition (raw data behind the table)
saveRDS(decomp, file.path(data_dir, paste0("eb_decomposition_", model_tag, ".RDS")))

# 7b. Estimated parameters
params_tbl <- tibble::tibble(
  parameter = c("sigma_mu", "nu_mu", "mu_sigma", "sigma_sigma", "t_cut", "t_slope"),
  Published = c(fit_exact$sigma_mu, fit_exact$nu_mu, fit_exact$mu_sigma,
                fit_exact$sigma_sigma, fit_exact$t_cut, fit_exact$t_slope),
  DataMined = c(sm_dm, nu_dm, NA_real_, NA_real_, NA_real_, NA_real_),
  RAPS_Table3 = c(0.45, 3.89, -1.67, 0.52, 1.61, 11.29)
)
write.csv(params_tbl,
          file.path(results_dir, paste0("EB_Params_", model_tag, ".csv")),
          row.names = FALSE)

# 7c. Headline decomposition table
pub_total <- mean(decomp$pub_total_decay)
dm_total  <- mean(decomp$dm_total_decay)

decomp_tbl <- tibble::tibble(
  row = c("IS observed",
          "RAPS bias-adjusted",
          "OOS observed",
          "Total decay (IS - OOS)",
          "  Selection bias (IS - adj)",
          "  Investor learning (adj - OOS)",
          "Selection as % of total decay",
          "Learning as % of total decay"),
  `Pub(own)` = c(
    mean(decomp$pub_is),
    mean(decomp$pub_adj),
    mean(decomp$pub_oos),
    pub_total,
    mean(decomp$pub_selection),
    mean(decomp$pub_learning),
    mean(decomp$pub_selection) / pub_total * 100,
    mean(decomp$pub_learning)  / pub_total * 100
  ),
  `Pub(DM prior)` = c(
    mean(decomp$pub_is),
    mean(decomp$pub_adj_dm),
    mean(decomp$pub_oos),
    pub_total,
    mean(decomp$pub_selection_dm),
    mean(decomp$pub_learning_dm),
    mean(decomp$pub_selection_dm) / pub_total * 100,
    mean(decomp$pub_learning_dm)  / pub_total * 100
  ),
  `Data-Mined` = c(
    mean(decomp$dm_is_raw),
    mean(decomp$dm_adj_raw),
    mean(decomp$dm_oos),
    dm_total,
    mean(decomp$dm_selection),
    mean(decomp$dm_learning),
    mean(decomp$dm_selection) / dm_total * 100,
    mean(decomp$dm_learning)  / dm_total * 100
  )
)

write.csv(decomp_tbl,
          file.path(results_dir, paste0("EB_Decomposition_", model_tag, ".csv")),
          row.names = FALSE)

# LaTeX version (booktabs)
fmt_val <- function(x, is_pct) ifelse(is_pct, sprintf("%.1f\\%%", x), sprintf("%.3f", x))
is_pct_row <- grepl("%", decomp_tbl$row)

tex_lines <- c(
  "\\begin{tabular}{lrrr}",
  "\\toprule",
  " & Pub (own prior) & Pub (DM prior) & Data-Mined \\\\",
  "\\midrule"
)
for (i in seq_len(nrow(decomp_tbl))) {
  if (decomp_tbl$row[i] == "Total decay (IS - OOS)") {
    tex_lines <- c(tex_lines, "\\midrule")
  }
  tex_lines <- c(tex_lines,
    sprintf("%s & %s & %s & %s \\\\",
            gsub("%", "\\\\%", decomp_tbl$row[i]),
            fmt_val(decomp_tbl$`Pub(own)`[i],      is_pct_row[i]),
            fmt_val(decomp_tbl$`Pub(DM prior)`[i], is_pct_row[i]),
            fmt_val(decomp_tbl$`Data-Mined`[i],    is_pct_row[i])))
}
tex_lines <- c(tex_lines, "\\bottomrule", "\\end{tabular}")

writeLines(tex_lines,
           file.path(results_dir, paste0("EB_Decomposition_", model_tag, ".tex")))

# 7d. By-cohort breakdown
cohort_tbl <- decomp %>%
  group_by(cohort) %>%
  summarize(
    n                = dplyr::n(),
    pub_is           = mean(pub_is),
    pub_adj          = mean(pub_adj),
    pub_adj_dm       = mean(pub_adj_dm),
    pub_oos          = mean(pub_oos),
    pub_selection    = mean(pub_selection),
    pub_learning     = mean(pub_learning),
    pub_selection_dm = mean(pub_selection_dm),
    pub_learning_dm  = mean(pub_learning_dm),
    dm_is            = mean(dm_is_raw),
    dm_adj           = mean(dm_adj_raw),
    dm_oos           = mean(dm_oos),
    dm_selection     = mean(dm_selection),
    dm_learning      = mean(dm_learning),
    .groups = "drop"
  )

write.csv(cohort_tbl,
          file.path(results_dir, paste0("EB_ByCohort_", model_tag, ".csv")),
          row.names = FALSE)

# 7e. By-theory decomposition (risk / mispricing / agnostic) ---
# Shared posterior, just split the per-signal decomposition by theory category.
theory_lookup <- cal_data %>% distinct(pubname, theory)

decomp_theory <- decomp %>%
  inner_join(theory_lookup, by = "pubname")

theory_tbl <- decomp_theory %>%
  group_by(theory) %>%
  summarize(
    n                = dplyr::n(),
    pub_is           = mean(pub_is),
    pub_adj          = mean(pub_adj),
    pub_adj_dm       = mean(pub_adj_dm),
    pub_oos          = mean(pub_oos),
    pub_selection    = mean(pub_selection),
    pub_learning     = mean(pub_learning),
    pub_selection_dm = mean(pub_selection_dm),
    pub_learning_dm  = mean(pub_learning_dm),
    dm_is            = mean(dm_is_raw),
    dm_adj           = mean(dm_adj_raw),
    dm_oos           = mean(dm_oos),
    dm_selection     = mean(dm_selection),
    dm_learning      = mean(dm_learning),
    .groups = "drop"
  ) %>%
  mutate(
    pub_total       = pub_is - pub_oos,
    dm_total        = dm_is - dm_oos,
    pub_sel_pct     = pub_selection    / pub_total * 100,
    pub_learn_pct   = pub_learning     / pub_total * 100,
    pub_sel_pct_dm  = pub_selection_dm / pub_total * 100,
    pub_learn_pct_dm= pub_learning_dm  / pub_total * 100,
    dm_sel_pct      = dm_selection     / dm_total * 100,
    dm_learn_pct    = dm_learning      / dm_total * 100
  )

write.csv(theory_tbl,
          file.path(results_dir, paste0("EB_ByTheory_", model_tag, ".csv")),
          row.names = FALSE)

cat("\n--- By Theory (RAPS Exact) ---\n")
cat(sprintf("%-12s %3s %8s %8s %8s %8s %8s %8s\n",
            "theory", "N", "IS", "adj(own)", "OOS",
            "sel%(own)", "lrn%(own)", "sel%(DM)"))
for (k in seq_len(nrow(theory_tbl))) {
  cat(sprintf("%-12s %3d %8.3f %8.3f %8.3f %7.1f%% %7.1f%% %7.1f%%\n",
              theory_tbl$theory[k], theory_tbl$n[k],
              theory_tbl$pub_is[k], theory_tbl$pub_adj[k], theory_tbl$pub_oos[k],
              theory_tbl$pub_sel_pct[k], theory_tbl$pub_learn_pct[k],
              theory_tbl$pub_sel_pct_dm[k]))
}

# LaTeX table for by-theory split
fmt3 <- function(x) sprintf("%.3f", x)
fmtp <- function(x) sprintf("%.1f\\%%", x)
tex_t <- c(
  "\\begin{tabular}{lrrrrrrrr}",
  "\\toprule",
  " & N & IS & $\\hat\\mu$(own) & $\\hat\\mu$(DM) & OOS & Total & Sel\\%(own) & Lrn\\%(own) \\\\",
  "\\midrule"
)
for (k in seq_len(nrow(theory_tbl))) {
  tex_t <- c(tex_t,
    sprintf("%s & %d & %s & %s & %s & %s & %s & %s & %s \\\\",
            tools::toTitleCase(theory_tbl$theory[k]),
            theory_tbl$n[k],
            fmt3(theory_tbl$pub_is[k]),
            fmt3(theory_tbl$pub_adj[k]),
            fmt3(theory_tbl$pub_adj_dm[k]),
            fmt3(theory_tbl$pub_oos[k]),
            fmt3(theory_tbl$pub_total[k]),
            fmtp(theory_tbl$pub_sel_pct[k]),
            fmtp(theory_tbl$pub_learn_pct[k])))
}
tex_t <- c(tex_t, "\\bottomrule", "\\end{tabular}")
writeLines(tex_t,
           file.path(results_dir, paste0("EB_ByTheory_", model_tag, ".tex")))

cat("\nSaved tables:\n")
cat("  ", file.path(results_dir, paste0("EB_Decomposition_", model_tag, ".csv")), "\n")
cat("  ", file.path(results_dir, paste0("EB_Decomposition_", model_tag, ".tex")), "\n")
cat("  ", file.path(results_dir, paste0("EB_Params_",        model_tag, ".csv")), "\n")
cat("  ", file.path(results_dir, paste0("EB_ByCohort_",      model_tag, ".csv")), "\n")
cat("  ", file.path(results_dir, paste0("EB_ByTheory_",      model_tag, ".csv")), "\n")
cat("  ", file.path(results_dir, paste0("EB_ByTheory_",      model_tag, ".tex")), "\n")
cat("  ", file.path(data_dir,    paste0("eb_decomposition_", model_tag, ".RDS")), "\n")

cat("\n=== Done ===\n")
