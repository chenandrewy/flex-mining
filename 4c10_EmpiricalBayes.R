# Empirical Bayes decomposition of OOS return decay — RAPS approach
# Following Chen & Zimmermann (2020, RAPS) model of biased publication.
#
# Decomposes total decay (IS - OOS) into:
#   Selection bias = IS_observed - bias_adjusted (statistical overfitting)
#   Investor learning = bias_adjusted - OOS_observed (arbitrage/publication)
#
# Model: r_i = mu_i + sigma_i * epsilon_i
#        mu_i ~ N(0, sigma_mu^2)          [true return prior]
#        sigma_i observed                  [standard error]
#        Publication: p(t_i) = logistic    [selection function]
#
# Bias-adjusted return: mu_hat_i = (1 - s_i) * r_i
#   where s_i = sigma_i^2 / (sigma_mu^2 + sigma_i^2)
#
# Published: estimate (sigma_mu, t_cut, t_slope) by MLE with selection
# Data-mined: estimate sigma_mu by MLE on FULL universe (no selection needed)
#
# All quantities in raw units (% per month).

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")

# Load data  -------------------------------------------

ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>% filter(Keep)
dmcomp <- readRDS("../Data/Processed/dmcomp_sumstats.RDS")
dm_insamp <- as.data.table(dmcomp$insampsum)

# Precompute SE for all DM strategies (once, outside loop)
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
# RAPS HELPER FUNCTIONS
# ============================================================

# Normal prior MLE for sigma_mu (no selection)
# Closed-form log-likelihood: -0.5 * sum(log(sm^2 + se^2) + rbar^2/(sm^2 + se^2))
fit_sigma_mu_normal <- function(rbar, se) {
  neg_ll <- function(log_sm) {
    sm2 <- exp(log_sm)^2
    v <- sm2 + se^2
    0.5 * sum(log(v) + rbar^2 / v)
  }
  res <- optimize(neg_ll, interval = c(-6, 2))
  exp(res$minimum)
}

# Normal prior MLE for (sigma_mu, t_cut, t_slope) WITH selection
# For published signals where we only observe those that pass publication threshold
fit_raps_published <- function(rbar, se) {
  # Log-likelihood with selection correction
  # L_i = p(t_i) * f(r_i | sigma_i, sigma_mu) / Z(sigma_i, theta)
  # where Z = integral of p(r/sigma) * f(r | sigma, sigma_mu) dr

  neg_ll <- function(par) {
    log_sm <- par[1]
    t_cut <- par[2]
    log_t_slope <- par[3]

    sm2 <- exp(log_sm)^2
    t_slope <- exp(log_t_slope)
    ti <- rbar / se
    v <- sm2 + se^2

    # Log marginal density (normal prior): log f(r_i | sigma_i, sigma_mu)
    log_f <- -0.5 * (log(v) + rbar^2 / v)

    # Log publication probability: log p(t_i)
    log_p <- -log(1 + exp(-t_slope * (ti - t_cut)))

    # Normalizing constant Z(sigma_i, theta) for each unique sigma_i
    # Z = integral p(r/sigma) * phi(r; 0, sigma_mu^2 + sigma^2) dr
    # = E[p(t_i)] where t_i ~ N(0, (sigma_mu^2 + sigma^2)/sigma^2)
    # Use Gauss-Hermite or numerical integration
    log_Z <- vapply(se, function(s) {
      sd_t <- sqrt(sm2 + s^2) / s  # SD of t-stat under prior
      # integrate p(t) * phi(t; 0, sd_t^2) dt
      integrand <- function(t) {
        1 / (1 + exp(-t_slope * (t - t_cut))) * dnorm(t, 0, sd_t)
      }
      log(integrate(integrand, lower = -10, upper = 30,
                    rel.tol = 1e-6, stop.on.error = FALSE)$value)
    }, numeric(1))

    # Negative log-likelihood
    -sum(log_f + log_p - log_Z)
  }

  # Starting values from RAPS Table 3
  start <- c(log(0.45), 1.6, log(11))

  res <- optim(start, neg_ll, method = "Nelder-Mead",
               control = list(maxit = 5000, reltol = 1e-8))

  list(
    sigma_mu = exp(res$par[1]),
    t_cut = res$par[2],
    t_slope = exp(res$par[3]),
    convergence = res$convergence,
    neg_loglik = res$value
  )
}

# Compute shrinkage and bias-adjusted return (normal prior)
bias_adjust <- function(rbar, se, sigma_mu) {
  s <- se^2 / (sigma_mu^2 + se^2)  # shrinkage factor
  mu_hat <- (1 - s) * rbar           # bias-adjusted return
  data.frame(shrinkage = s, mu_hat = mu_hat)
}

# ============================================================
# PART 1: PUBLISHED SIGNALS — RAPS with selection
# ============================================================

pub_data <- signal_info %>%
  filter(signalname %in% unique(cal_data$pubname)) %>%
  transmute(pubname = signalname, cohort,
            rbar = rbar_pub, se = se_pub, tstat = tstat_pub)

cat("\n=== PUBLISHED: Fitting RAPS with selection ===\n")
cat("N signals:", nrow(pub_data), "\n")
cat("Mean IS return:", round(mean(pub_data$rbar), 3), "\n")
cat("Mean SE:", round(mean(pub_data$se), 3), "\n")
cat("Mean |t|:", round(mean(abs(pub_data$tstat)), 2), "\n")

tic("Published RAPS fit")
fit_pub <- fit_raps_published(pub_data$rbar, pub_data$se)
toc()

cat("\nEstimated parameters:\n")
cat("  sigma_mu:", round(fit_pub$sigma_mu, 3), "\n")
cat("  t_cut:", round(fit_pub$t_cut, 2), "\n")
cat("  t_slope:", round(fit_pub$t_slope, 1), "\n")
cat("  Convergence:", fit_pub$convergence, "\n")

# Bias-adjusted returns for published
pub_adj <- bias_adjust(pub_data$rbar, pub_data$se, fit_pub$sigma_mu)
pub_data$shrinkage <- pub_adj$shrinkage
pub_data$mu_hat <- pub_adj$mu_hat

cat("\nPublished shrinkage:\n")
cat("  Mean:", round(mean(pub_data$shrinkage) * 100, 1), "%\n")
cat("  Median:", round(median(pub_data$shrinkage) * 100, 1), "%\n")
cat("  Mean bias-adjusted return:", round(mean(pub_data$mu_hat), 3), "\n")

# ============================================================
# PART 2: DATA-MINED SIGNALS — full universe, no selection
# ============================================================

pub_signals <- unique(pub_data$pubname)

cat("\n=== DATA-MINED: Fitting sigma_mu per signal (full universe) ===\n")

tic("DM fits")
dm_results <- lapply(pub_signals, function(pname) {
  dm_full <- dm_insamp[pubname == pname]
  if (nrow(dm_full) < 100) return(NULL)

  # Estimate sigma_mu from full universe (no selection)
  sm <- fit_sigma_mu_normal(dm_full$rbar, dm_full$se)

  # Shrinkage for |t|>2 subset
  t2_idx <- which(abs(dm_full$tstat) > 2)
  if (length(t2_idx) < 5) return(NULL)

  rbar_t2 <- dm_full$rbar[t2_idx]
  se_t2 <- dm_full$se[t2_idx]
  adj_t2 <- bias_adjust(rbar_t2, se_t2, sm)

  # Sign-adjust to match published direction (flip negatives)
  sign_t2 <- sign(rbar_t2)
  aligned_is <- abs(rbar_t2)
  aligned_adj <- sign_t2 * adj_t2$mu_hat

  data.frame(
    pubname = pname,
    sigma_mu_dm = sm,
    n_dm_total = nrow(dm_full),
    n_dm_t2 = length(t2_idx),
    dm_is_raw = mean(aligned_is),
    dm_adj_raw = mean(aligned_adj),
    dm_shrinkage_mean = mean(adj_t2$shrinkage)
  )
})
toc()

dm_df <- bind_rows(dm_results)
cat("DM fits completed:", nrow(dm_df), "signals\n")
cat("DM sigma_mu — median:", round(median(dm_df$sigma_mu_dm), 3), "\n")
cat("DM |t|>2 shrinkage — mean:", round(mean(dm_df$dm_shrinkage_mean) * 100, 1), "%\n")

# ============================================================
# PART 3: OOS returns (raw)
# ============================================================

# Published OOS
pub_oos <- cal_data %>%
  filter(eventDate > 0) %>%
  group_by(pubname) %>%
  summarize(pub_oos = mean(ret_raw, na.rm = TRUE),
            n_oos = n(), .groups = "drop") %>%
  filter(n_oos >= 24)

# DM OOS
dm_oos <- cal_data %>%
  filter(eventDate > 0) %>%
  group_by(pubname) %>%
  summarize(dm_oos = mean(matchRet_raw, na.rm = TRUE),
            .groups = "drop")

# ============================================================
# PART 4: Validation — does RAPS predict OOS?
# ============================================================

validation <- pub_data %>%
  select(pubname, cohort, pub_is = rbar, pub_adj = mu_hat, pub_shrinkage = shrinkage) %>%
  inner_join(pub_oos, by = "pubname") %>%
  inner_join(dm_df %>% select(pubname, dm_is_raw, dm_adj_raw, sigma_mu_dm, dm_shrinkage_mean),
             by = "pubname") %>%
  inner_join(dm_oos, by = "pubname")

cat("\n=== VALIDATION: RAPS as predictor of OOS ===\n")

cat("\n--- Published Signals ---\n")
cat(sprintf("%-35s %10s %10s\n", "Metric", "IS (raw)", "RAPS adj"))
bias_is <- mean(validation$pub_is - validation$pub_oos)
bias_adj <- mean(validation$pub_adj - validation$pub_oos)
cat(sprintf("%-35s %10.3f %10.3f\n", "Mean bias (predictor - OOS)", bias_is, bias_adj))

rmse_is <- sqrt(mean((validation$pub_is - validation$pub_oos)^2))
rmse_adj <- sqrt(mean((validation$pub_adj - validation$pub_oos)^2))
cat(sprintf("%-35s %10.3f %10.3f\n", "RMSE vs OOS", rmse_is, rmse_adj))

cor_is <- cor(validation$pub_is, validation$pub_oos)
cor_adj <- cor(validation$pub_adj, validation$pub_oos)
cat(sprintf("%-35s %10.3f %10.3f\n", "Correlation with OOS", cor_is, cor_adj))

cat(sprintf("\n%-35s %10.3f %10.3f\n", "Mean predictor",
            mean(validation$pub_is), mean(validation$pub_adj)))
cat(sprintf("%-35s %10.3f\n", "Mean OOS", mean(validation$pub_oos)))

cat("\n--- Data-Mined Signals ---\n")
cat(sprintf("%-35s %10s %10s\n", "Metric", "IS (raw)", "RAPS adj"))
bias_dm_is <- mean(validation$dm_is_raw - validation$dm_oos)
bias_dm_adj <- mean(validation$dm_adj_raw - validation$dm_oos)
cat(sprintf("%-35s %10.3f %10.3f\n", "Mean bias (predictor - OOS)", bias_dm_is, bias_dm_adj))

rmse_dm_is <- sqrt(mean((validation$dm_is_raw - validation$dm_oos)^2))
rmse_dm_adj <- sqrt(mean((validation$dm_adj_raw - validation$dm_oos)^2))
cat(sprintf("%-35s %10.3f %10.3f\n", "RMSE vs OOS", rmse_dm_is, rmse_dm_adj))

cor_dm_is <- cor(validation$dm_is_raw, validation$dm_oos)
cor_dm_adj <- cor(validation$dm_adj_raw, validation$dm_oos)
cat(sprintf("%-35s %10.3f %10.3f\n", "Correlation with OOS", cor_dm_is, cor_dm_adj))

cat(sprintf("\n%-35s %10.3f %10.3f\n", "Mean predictor",
            mean(validation$dm_is_raw), mean(validation$dm_adj_raw)))
cat(sprintf("%-35s %10.3f\n", "Mean OOS", mean(validation$dm_oos)))

# ============================================================
# PART 5: Decomposition
# ============================================================

decomp <- validation %>%
  mutate(
    # Published
    pub_total_decay = pub_is - pub_oos,
    pub_selection = pub_is - pub_adj,
    pub_learning = pub_adj - pub_oos,

    # Data-mined
    dm_total_decay = dm_is_raw - dm_oos,
    dm_selection = dm_is_raw - dm_adj_raw,
    dm_learning = dm_adj_raw - dm_oos
  )

cat("\n=== DECOMPOSITION (RAPS, raw % per month) ===\n")
cat(sprintf("\n%-35s %12s %12s\n", "", "Published", "Data-Mined"))
cat(sprintf("%-35s %12.3f %12.3f\n", "IS observed",
            mean(decomp$pub_is), mean(decomp$dm_is_raw)))
cat(sprintf("%-35s %12.3f %12.3f\n", "RAPS bias-adjusted",
            mean(decomp$pub_adj), mean(decomp$dm_adj_raw)))
cat(sprintf("%-35s %12.3f %12.3f\n", "OOS observed",
            mean(decomp$pub_oos), mean(decomp$dm_oos)))
cat(sprintf("%-35s %12s %12s\n", "", "---", "---"))
cat(sprintf("%-35s %12.3f %12.3f\n", "Total decay (IS - OOS)",
            mean(decomp$pub_total_decay), mean(decomp$dm_total_decay)))
cat(sprintf("%-35s %12.3f %12.3f\n", "  Selection bias (IS - adj)",
            mean(decomp$pub_selection), mean(decomp$dm_selection)))
cat(sprintf("%-35s %12.3f %12.3f\n", "  Investor learning (adj - OOS)",
            mean(decomp$pub_learning), mean(decomp$dm_learning)))

cat(sprintf("\n%-35s %12.1f%% %11.1f%%\n", "Selection as % of total decay",
            mean(decomp$pub_selection) / mean(decomp$pub_total_decay) * 100,
            mean(decomp$dm_selection) / mean(decomp$dm_total_decay) * 100))
cat(sprintf("%-35s %12.1f%% %11.1f%%\n", "Learning as % of total decay",
            mean(decomp$pub_learning) / mean(decomp$pub_total_decay) * 100,
            mean(decomp$dm_learning) / mean(decomp$dm_total_decay) * 100))

cat(sprintf("\n%-35s %12.3f %12.3f\n", "sigma_mu",
            fit_pub$sigma_mu, median(dm_df$sigma_mu_dm)))
cat(sprintf("%-35s %12.1f%% %11.1f%%\n", "Mean shrinkage",
            mean(decomp$pub_shrinkage) * 100, mean(decomp$dm_shrinkage_mean) * 100))

cat("\n--- By Cohort ---\n")
decomp %>%
  group_by(cohort) %>%
  summarize(
    n = n(),
    pub_is = mean(pub_is), pub_adj = mean(pub_adj), pub_oos = mean(pub_oos),
    pub_sel = mean(pub_selection), pub_learn = mean(pub_learning),
    dm_is = mean(dm_is_raw), dm_adj = mean(dm_adj_raw), dm_oos = mean(dm_oos),
    dm_sel = mean(dm_selection), dm_learn = mean(dm_learning),
    .groups = "drop"
  ) %>%
  print()

# ============================================================
# PART 6: Plots
# ============================================================

fontsizeall <- 28

# Plot 1: Stacked bar — decomposition
decomp_summary <- data.frame(
  source = rep(c("Published", "Data-Mined"), each = 3),
  component = rep(c("OOS Return", "Investor Learning", "Selection Bias"), 2),
  value = c(
    mean(decomp$pub_oos), mean(decomp$pub_learning), mean(decomp$pub_selection),
    mean(decomp$dm_oos), mean(decomp$dm_learning), mean(decomp$dm_selection)
  )
)

decomp_summary$component <- factor(decomp_summary$component,
  levels = c("Selection Bias", "Investor Learning", "OOS Return"))

p1 <- ggplot(decomp_summary, aes(x = source, y = value, fill = component)) +
  geom_col(width = 0.6) +
  scale_fill_manual(values = c(
    "Selection Bias" = "firebrick3",
    "Investor Learning" = "goldenrod2",
    "OOS Return" = "steelblue3"
  )) +
  geom_hline(yintercept = 0, linewidth = 0.5) +
  labs(x = NULL, y = "% per month", fill = NULL) +
  ggtitle("RAPS Decomposition: Selection Bias vs Investor Learning") +
  theme_minimal(base_size = fontsizeall) +
  theme(legend.position = "bottom", panel.grid.major.x = element_blank())

ggsave("../Results/Fig_EB_Decomposition_Bar.pdf", p1, width = 12, height = 8)
cat("\nSaved: Fig_EB_Decomposition_Bar.pdf\n")

# Plot 2: Boxplot — IS vs RAPS-adjusted vs OOS
eb_dist <- bind_rows(
  decomp %>% transmute(pubname, source = "Published",
                        IS = pub_is, `RAPS adj` = pub_adj, OOS = pub_oos),
  decomp %>% transmute(pubname, source = "Data-Mined",
                        IS = dm_is_raw, `RAPS adj` = dm_adj_raw, OOS = dm_oos)
) %>%
  pivot_longer(cols = c(IS, `RAPS adj`, OOS), names_to = "estimate", values_to = "value")

eb_dist$estimate <- factor(eb_dist$estimate, levels = c("IS", "RAPS adj", "OOS"))

p2 <- ggplot(eb_dist, aes(x = estimate, y = value, fill = estimate)) +
  geom_boxplot(outlier.alpha = 0.3) +
  facet_wrap(~source) +
  scale_fill_manual(values = c("IS" = "steelblue3", "RAPS adj" = "goldenrod2", "OOS" = "firebrick3")) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(x = NULL, y = "% per month", fill = NULL) +
  ggtitle("IS vs RAPS Bias-Adjusted vs OOS") +
  theme_minimal(base_size = fontsizeall) +
  theme(legend.position = "none")

ggsave("../Results/Fig_EB_ISvsEBvsOOS_Box.pdf", p2, width = 12, height = 8)
cat("Saved: Fig_EB_ISvsEBvsOOS_Box.pdf\n")

# Plot 3: Decomposition by cohort
decomp_cohort_long <- decomp %>%
  select(pubname, cohort,
         pub_selection, pub_learning, pub_oos,
         dm_selection, dm_learning, dm_oos) %>%
  pivot_longer(
    cols = -c(pubname, cohort),
    names_to = c("source", "component"),
    names_pattern = "(pub|dm)_(.*)"
  ) %>%
  mutate(
    source = ifelse(source == "pub", "Published", "Data-Mined"),
    component = case_when(
      component == "selection" ~ "Selection Bias",
      component == "learning" ~ "Investor Learning",
      component == "oos" ~ "OOS Return"
    )
  )

decomp_cohort_means <- decomp_cohort_long %>%
  group_by(cohort, source, component) %>%
  summarize(value = mean(value), .groups = "drop")

decomp_cohort_means$component <- factor(decomp_cohort_means$component,
  levels = c("Selection Bias", "Investor Learning", "OOS Return"))

p3 <- ggplot(decomp_cohort_means, aes(x = source, y = value, fill = component)) +
  geom_col(width = 0.6) +
  facet_wrap(~cohort) +
  scale_fill_manual(values = c(
    "Selection Bias" = "firebrick3",
    "Investor Learning" = "goldenrod2",
    "OOS Return" = "steelblue3"
  )) +
  geom_hline(yintercept = 0, linewidth = 0.5) +
  labs(x = NULL, y = "% per month", fill = NULL) +
  ggtitle("RAPS Decomposition by Cohort") +
  theme_minimal(base_size = fontsizeall * 0.7) +
  theme(
    legend.position = "bottom",
    panel.grid.major.x = element_blank(),
    axis.text.x = element_text(angle = 30, hjust = 1)
  )

ggsave("../Results/Fig_EB_Decomposition_ByCohort.pdf", p3, width = 14, height = 10)
cat("Saved: Fig_EB_Decomposition_ByCohort.pdf\n")

# Save
saveRDS(decomp, "../Data/Processed/eb_decomposition.RDS")
cat("\nSaved: eb_decomposition.RDS\n")
cat("\n=== Done ===\n")
