# Empirical Bayes decomposition — RAPS with Student's t prior
# Following Chen & Zimmermann (2020, RAPS) Equations 8-17.
#
# Model: r_i = mu_i + sigma_i * epsilon_i      (Eq 8-11)
#        mu_i = sigma_mu * tau_{nu_mu}          (Eq 12, scaled Student's t)
#        sigma_i observed
#        Publication: p(t_i) = logistic          (Eq 14)
#
# Bias-adjusted return: E[mu_i | r_i, sigma_i, theta] via Bayes (Eq 16-17)
# Requires numerical integration (no closed form with t prior).
#
# Published: estimate (sigma_mu, nu_mu, t_cut, t_slope) by MLE
# Data-mined: estimate (sigma_mu, nu_mu) from full universe (no selection)

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")

# Load data  -------------------------------------------

ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>% filter(Keep)
dmcomp <- readRDS("../Data/Processed/dmcomp_sumstats.RDS")
dm_insamp <- as.data.table(dmcomp$insampsum)

# Precompute SE for all DM strategies
dm_insamp[, se := ifelse(abs(tstat) > 0.01, abs(rbar / tstat), NA_real_)]
dm_insamp <- dm_insamp[!is.na(se) & se > 0 & is.finite(se)]

# Signal info
signal_info <- czsum %>%
  transmute(
    signalname,
    rbar_pub = rbar,
    tstat_pub = tstat,
    se_pub = rbar / tstat,
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

# Unscale returns to raw units
cal_data <- cal_data %>%
  mutate(
    ret_raw = ret * rbar_pub / 100,
    matchRet_raw = matchRet * rbar_pub / 100
  )

# ============================================================
# RAPS HELPER FUNCTIONS — Student's t prior
# Fast vectorized quadrature with change-of-variable trick
# ============================================================

# Fixed quadrature grid size
N_U_GRID <- 200

# Scaled Student's t density: mu ~ sigma_mu * t_{nu}
dst <- function(x, sigma_mu, nu) {
  dt(x / sigma_mu, df = nu) / sigma_mu
}

# Vectorized marginal density via change of variable u = (mu - r) / s
# f(r | s, sm, nu) = integral N(r; mu, s^2) * f_t(mu; sm, nu) dmu
#                  = integral phi(u) * f_t(r + s*u; sm, nu) du
# The phi(u) factor concentrates integrand in u ~ [-5, 5] regardless of s
marginal_density <- function(r, sigma, sigma_mu, nu, n_grid = N_U_GRID) {
  u_grid <- seq(-6, 6, length.out = n_grid)
  du <- u_grid[2] - u_grid[1]
  phi_grid <- dnorm(u_grid)  # standard normal weights, length G

  N <- length(r)
  G <- n_grid

  # mu_ij = r_i + sigma_i * u_j  (N x G matrix)
  # For each (i,j): integrand = phi(u_j) * dst(mu_ij, sm, nu)
  # density_i = sum_j integrand_ij * du

  # Build mu matrix: N x G
  mu_mat <- outer(r, u_grid * 1, `+`)  # start with r_i repeated
  # Actually: mu_ij = r_i + sigma_i * u_j
  # Need to scale u by sigma per observation
  mu_mat <- matrix(r, nrow = N, ncol = G) + outer(sigma, u_grid)

  # Evaluate t-prior at all mu values
  prior_mat <- matrix(dt(as.vector(mu_mat) / sigma_mu, df = nu) / sigma_mu,
                       nrow = N, ncol = G)

  # Integrand: phi(u_j) * prior(mu_ij), then sum over j
  phi_mat <- matrix(phi_grid, nrow = N, ncol = G, byrow = TRUE)
  rowSums(phi_mat * prior_mat) * du
}

# Posterior mean E[mu | r, sigma, sigma_mu, nu] via same change of variable
posterior_mean <- function(r, sigma, sigma_mu, nu, n_grid = N_U_GRID) {
  u_grid <- seq(-6, 6, length.out = n_grid)
  du <- u_grid[2] - u_grid[1]
  phi_grid <- dnorm(u_grid)

  N <- length(r)
  G <- n_grid

  mu_mat <- matrix(r, nrow = N, ncol = G) + outer(sigma, u_grid)
  prior_mat <- matrix(dt(as.vector(mu_mat) / sigma_mu, df = nu) / sigma_mu,
                       nrow = N, ncol = G)
  phi_mat <- matrix(phi_grid, nrow = N, ncol = G, byrow = TRUE)

  integrand <- phi_mat * prior_mat
  denom <- rowSums(integrand) * du
  numer <- rowSums(integrand * mu_mat) * du

  ifelse(denom > 0, numer / denom, r)
}

# MLE for (sigma_mu, nu_mu) on full universe — NO selection
fit_raps_t_no_selection <- function(rbar, se, n_subsample = 5000) {
  n <- length(rbar)
  if (n > n_subsample) {
    idx <- sample(n, n_subsample)
    rbar <- rbar[idx]
    se <- se[idx]
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

  list(
    sigma_mu = exp(res$par[1]),
    nu_mu = exp(res$par[2]) + 2,
    convergence = res$convergence,
    neg_loglik = res$value
  )
}

# MLE for (sigma_mu, nu_mu, t_cut, t_slope) WITH selection
# Z computed via change-of-variable: for each t on grid, marginal uses u-substitution
fit_raps_t_published <- function(rbar, se) {
  # Fixed t-stat grid for Z integration
  N_T_GRID <- 300
  t_grid <- seq(-3, 18, length.out = N_T_GRID)
  dt_step <- t_grid[2] - t_grid[1]

  # u-grid for inner integral (shared)
  n_u <- N_U_GRID
  u_grid <- seq(-6, 6, length.out = n_u)
  du <- u_grid[2] - u_grid[1]
  phi_u <- dnorm(u_grid)  # length n_u

  # Precompute unique SEs and their mapping
  unique_se <- sort(unique(se))
  se_idx <- match(se, unique_se)
  n_unique <- length(unique_se)

  neg_ll <- function(par) {
    sm <- exp(par[1])
    nu <- exp(par[2]) + 2
    t_cut <- par[3]
    t_slope <- exp(par[4])

    ti <- rbar / se

    # --- Marginal density for observed data ---
    dens <- marginal_density(rbar, se, sm, nu)
    dens[dens <= 0] <- 1e-300
    log_f <- log(dens)

    # --- Log publication probability for observed ---
    log_p <- -log(1 + exp(-t_slope * (ti - t_cut)))

    # --- Normalizing constant Z(s) for each unique SE ---
    # Z(s) = integral p(t) * f_marg(t*s | s, sm, nu) * s dt
    # f_marg(r | s, sm, nu) = integral phi(u) * dst(r + s*u, sm, nu) du

    pub_prob <- 1 / (1 + exp(-t_slope * (t_grid - t_cut)))  # length N_T_GRID

    log_Z_vec <- numeric(n_unique)
    for (k in seq_len(n_unique)) {
      s <- unique_se[k]
      r_vals <- t_grid * s  # length N_T_GRID

      # For each r_val, compute marginal via u-substitution
      # mu_mat: N_T_GRID x n_u, where mu[j,m] = r_vals[j] + s * u_grid[m]
      mu_mat <- outer(r_vals, u_grid, function(r, u) r + s * u)

      # t-prior at each mu: N_T_GRID x n_u
      prior_mat <- matrix(dt(as.vector(mu_mat) / sm, df = nu) / sm,
                           nrow = N_T_GRID, ncol = n_u)

      # marginal[j] = sum_m phi(u_m) * prior(mu[j,m]) * du
      phi_mat <- matrix(phi_u, nrow = N_T_GRID, ncol = n_u, byrow = TRUE)
      marg_at_t <- rowSums(phi_mat * prior_mat) * du  # length N_T_GRID

      # Z(s) = sum_j pub_prob[j] * marg[j] * s * dt_step
      Z_val <- sum(pub_prob * marg_at_t * s) * dt_step
      log_Z_vec[k] <- log(max(Z_val, 1e-300))
    }

    log_Z <- log_Z_vec[se_idx]
    -sum(log_f + log_p - log_Z)
  }

  # Starting values from RAPS Table 3
  start <- c(log(0.45), log(3.89 - 2), 1.6, log(11))

  res <- optim(start, neg_ll, method = "Nelder-Mead",
               control = list(maxit = 5000, reltol = 1e-7))

  list(
    sigma_mu = exp(res$par[1]),
    nu_mu = exp(res$par[2]) + 2,
    t_cut = res$par[3],
    t_slope = exp(res$par[4]),
    convergence = res$convergence,
    neg_loglik = res$value
  )
}

# ============================================================
# PART 1: PUBLISHED — RAPS with Student's t and selection
# ============================================================

pub_data <- signal_info %>%
  filter(signalname %in% unique(cal_data$pubname)) %>%
  transmute(pubname = signalname, cohort,
            rbar = rbar_pub, se = se_pub, tstat = tstat_pub)

cat("\n=== PUBLISHED: Fitting RAPS (Student's t + selection) ===\n")
cat("N signals:", nrow(pub_data), "\n")

tic("Published RAPS t-prior fit")
fit_pub <- fit_raps_t_published(pub_data$rbar, pub_data$se)
toc()

cat("\nEstimated parameters:\n")
cat("  sigma_mu:", round(fit_pub$sigma_mu, 3), "\n")
cat("  nu_mu:", round(fit_pub$nu_mu, 2), "\n")
cat("  t_cut:", round(fit_pub$t_cut, 2), "\n")
cat("  t_slope:", round(fit_pub$t_slope, 1), "\n")
cat("  Convergence:", fit_pub$convergence, "\n")

# Bias-adjusted returns via posterior mean
cat("Computing posterior means for published signals...\n")
tic("Published posterior means")
pub_data$mu_hat <- posterior_mean(pub_data$rbar, pub_data$se,
                                  fit_pub$sigma_mu, fit_pub$nu_mu)
toc()

pub_data$shrinkage <- 1 - pub_data$mu_hat / pub_data$rbar

cat("\nPublished shrinkage (own prior):\n")
cat("  Mean:", round(mean(pub_data$shrinkage) * 100, 1), "%\n")
cat("  Median:", round(median(pub_data$shrinkage) * 100, 1), "%\n")

# ============================================================
# PART 2: DATA-MINED — Student's t, full universe
# ============================================================

pub_signals <- unique(pub_data$pubname)

cat("\n=== DATA-MINED: Fitting RAPS (Student's t, full universe) ===\n")

# Estimate (sigma_mu, nu_mu) from a representative DM universe
# (stable across signals, so use one representative for speed)
rep_signal <- pub_signals[which.min(abs(nchar(pub_signals) - 5))]  # arbitrary
dm_rep <- dm_insamp[pubname == rep_signal]

cat("Estimating DM prior from", nrow(dm_rep), "strategies (signal:", rep_signal, ")...\n")
tic("DM t-prior fit")
fit_dm_global <- fit_raps_t_no_selection(dm_rep$rbar, dm_rep$se, n_subsample = 5000)
toc()

cat("\nDM estimated parameters:\n")
cat("  sigma_mu:", round(fit_dm_global$sigma_mu, 3), "\n")
cat("  nu_mu:", round(fit_dm_global$nu_mu, 2), "\n")
cat("  Convergence:", fit_dm_global$convergence, "\n")

# Now compute per-signal: shrinkage for |t|>2 subset using global DM params
# Use normal-prior shortcut for per-signal speed, with the t-estimated sigma_mu
# (since posterior_mean with t-prior on 7K strategies per signal would be slow)
#
# Actually: for the |t|>2 DM strategies, use the closed-form normal approximation
# with sigma_mu_eff = sigma_mu * sqrt(nu/(nu-2)) to match the t-prior variance

sm_dm <- fit_dm_global$sigma_mu
nu_dm <- fit_dm_global$nu_mu
# Effective sigma_mu for normal approximation matching t-prior variance:
sm_dm_eff <- sm_dm * sqrt(nu_dm / (nu_dm - 2))
cat("DM effective sigma_mu (variance-matched):", round(sm_dm_eff, 3), "\n")

cat("\nComputing DM shrinkage per signal...\n")
tic("DM per-signal")
dm_results <- lapply(pub_signals, function(pname) {
  dm_full <- dm_insamp[pubname == pname]
  if (nrow(dm_full) < 100) return(NULL)

  t2_idx <- which(abs(dm_full$tstat) > 2)
  if (length(t2_idx) < 5) return(NULL)

  rbar_t2 <- dm_full$rbar[t2_idx]
  se_t2 <- dm_full$se[t2_idx]

  # Normal-approximation shrinkage with variance-matched sigma_mu
  s_t2 <- se_t2^2 / (sm_dm_eff^2 + se_t2^2)
  adj_t2 <- (1 - s_t2) * rbar_t2

  # Sign-adjust
  sign_t2 <- sign(rbar_t2)
  aligned_is <- abs(rbar_t2)
  aligned_adj <- sign_t2 * adj_t2

  data.frame(
    pubname = pname,
    n_dm_t2 = length(t2_idx),
    dm_is_raw = mean(aligned_is),
    dm_adj_raw = mean(aligned_adj),
    dm_shrinkage_mean = mean(s_t2)
  )
})
toc()

dm_df <- bind_rows(dm_results)
cat("DM fits completed:", nrow(dm_df), "signals\n")
cat("DM |t|>2 shrinkage — mean:", round(mean(dm_df$dm_shrinkage_mean) * 100, 1), "%\n")

# Also: compute a few exact posterior means for DM to validate normal approx
cat("\nValidating normal approx vs exact t-posterior (5 signals)...\n")
for (pname in pub_signals[1:5]) {
  dm_full <- dm_insamp[pubname == pname]
  t2_pos <- dm_full[tstat > 2]
  if (nrow(t2_pos) < 5) next
  # Pick 20 random strategies for speed
  samp <- t2_pos[sample(.N, min(20, .N))]
  exact <- posterior_mean(samp$rbar, samp$se, sm_dm, nu_dm)
  approx <- (1 - samp$se^2 / (sm_dm_eff^2 + samp$se^2)) * samp$rbar
  cat(sprintf("  %s: exact=%.3f, approx=%.3f, ratio=%.3f\n",
      pname, mean(exact), mean(approx), mean(approx)/mean(exact)))
}

# ============================================================
# PART 2b: PUBLISHED with DM prior (breaks identification)
# ============================================================
# Use DM-estimated (sigma_mu, nu_mu), only fit (t_cut, t_slope) from published data

cat("\n=== PUBLISHED with DM PRIOR: Fitting selection params only ===\n")

fit_raps_t_pub_dm_prior <- function(rbar, se, sm_fixed, nu_fixed) {
  # Fixed t-stat grid for Z
  N_T_GRID <- 300
  t_grid <- seq(-3, 18, length.out = N_T_GRID)
  dt_step <- t_grid[2] - t_grid[1]

  n_u <- N_U_GRID
  u_grid <- seq(-6, 6, length.out = n_u)
  du <- u_grid[2] - u_grid[1]
  phi_u <- dnorm(u_grid)

  unique_se <- sort(unique(se))
  se_idx <- match(se, unique_se)
  n_unique <- length(unique_se)

  neg_ll <- function(par) {
    t_cut <- par[1]
    t_slope <- exp(par[2])

    ti <- rbar / se

    # Marginal density with DM prior
    dens <- marginal_density(rbar, se, sm_fixed, nu_fixed)
    dens[dens <= 0] <- 1e-300
    log_f <- log(dens)

    log_p <- -log(1 + exp(-t_slope * (ti - t_cut)))

    pub_prob <- 1 / (1 + exp(-t_slope * (t_grid - t_cut)))

    log_Z_vec <- numeric(n_unique)
    for (k in seq_len(n_unique)) {
      s <- unique_se[k]
      r_vals <- t_grid * s
      mu_mat <- outer(r_vals, u_grid, function(r, u) r + s * u)
      prior_mat <- matrix(dt(as.vector(mu_mat) / sm_fixed, df = nu_fixed) / sm_fixed,
                           nrow = N_T_GRID, ncol = n_u)
      phi_mat <- matrix(phi_u, nrow = N_T_GRID, ncol = n_u, byrow = TRUE)
      marg_at_t <- rowSums(phi_mat * prior_mat) * du
      Z_val <- sum(pub_prob * marg_at_t * s) * dt_step
      log_Z_vec[k] <- log(max(Z_val, 1e-300))
    }
    log_Z <- log_Z_vec[se_idx]
    -sum(log_f + log_p - log_Z)
  }

  start <- c(1.6, log(11))
  res <- optim(start, neg_ll, method = "Nelder-Mead",
               control = list(maxit = 3000, reltol = 1e-7))

  list(
    t_cut = res$par[1],
    t_slope = exp(res$par[2]),
    convergence = res$convergence,
    neg_loglik = res$value
  )
}

tic("Published with DM prior")
fit_pub_dm <- fit_raps_t_pub_dm_prior(pub_data$rbar, pub_data$se, sm_dm, nu_dm)
toc()

cat("  t_cut:", round(fit_pub_dm$t_cut, 2), "\n")
cat("  t_slope:", round(fit_pub_dm$t_slope, 1), "\n")
cat("  Convergence:", fit_pub_dm$convergence, "\n")

# Posterior means with DM prior
pub_data$mu_hat_dm <- posterior_mean(pub_data$rbar, pub_data$se, sm_dm, nu_dm)
pub_data$shrinkage_dm <- 1 - pub_data$mu_hat_dm / pub_data$rbar

cat("\nPublished shrinkage (DM prior):\n")
cat("  Mean:", round(mean(pub_data$shrinkage_dm) * 100, 1), "%\n")
cat("  Median:", round(median(pub_data$shrinkage_dm) * 100, 1), "%\n")

# ============================================================
# PART 3: OOS returns
# ============================================================

pub_oos <- cal_data %>%
  filter(eventDate > 0) %>%
  group_by(pubname) %>%
  summarize(pub_oos = mean(ret_raw, na.rm = TRUE),
            n_oos = n(), .groups = "drop") %>%
  filter(n_oos >= 24)

dm_oos <- cal_data %>%
  filter(eventDate > 0) %>%
  group_by(pubname) %>%
  summarize(dm_oos = mean(matchRet_raw, na.rm = TRUE), .groups = "drop")

# ============================================================
# PART 4: Validation
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
cat(sprintf("%-35s %10.3f %10.3f\n", "Correlation with OOS",
            cor(validation$dm_is_raw, validation$dm_oos),
            cor(validation$dm_adj_raw, validation$dm_oos)))

# ============================================================
# PART 5: Decomposition
# ============================================================

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

cat("\n=== DECOMPOSITION (RAPS Student's t, % per month) ===\n")
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
            fit_pub$sigma_mu, sm_dm, sm_dm))
cat(sprintf("%-35s %12.2f %12.2f %12.2f\n", "nu_mu",
            fit_pub$nu_mu, nu_dm, nu_dm))
cat(sprintf("%-35s %11.1f%% %11.1f%% %11.1f%%\n", "Mean shrinkage",
            mean(decomp$pub_shrinkage) * 100,
            mean(decomp$pub_shrinkage_dm) * 100,
            mean(decomp$dm_shrinkage_mean) * 100))

cat("\n=== Done ===\n")
