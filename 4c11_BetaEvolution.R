# Beta evolution: pre vs post-sample CAPM market beta for published vs DM
# Following Campbell (2024) discussion (slide 24, Cho 2020 funding-beta angle):
# do anomaly betas change after the original sample ends? If arbitrageurs enter
# the trade post-publication, beta should shift more for published signals than
# for matched data-mined signals.
#
# Mirrors the Campbell-filtered set used in 4c10c so the published-vs-DM
# comparison is on the same 167 signals as the EB decomposition.

# Setup --------------------------------------------------------
rm(list = ls())
source("0_Environment.R")

library(data.table)
library(dplyr)
library(ggplot2)
library(tidyr)

results_dir <- "../Results"
data_dir    <- "../Data/Processed"

DMshortname <- "CZ-style-v8b"  # matches 2d_RiskAdjustDataMinedSignals.R

# Load data ----------------------------------------------------
czret <- readRDS(file.path(data_dir, "czret_keeponly.RDS"))
czsum <- readRDS(file.path(data_dir, "czsum_allpredictors.RDS")) %>%
  filter(Keep)
ff    <- readRDS("../Data/Raw/FamaFrenchFactors.RData") %>%
  rename(date = yearm)
ret_for_plot0 <- readRDS(file.path(data_dir, "ret_for_plot0.RDS"))

# DM coefficients per (actSignal, candSignalname)
dm_coef <- readRDS(file.path(data_dir,
  paste0(DMshortname, " MatchPubCoefficients.RData")))

# Theory lookup (from cal_data, same as 4c10c)
theory_lookup <- ret_for_plot0 %>%
  filter(!is.na(matchRet)) %>%
  distinct(pubname, theory)

# Published-side betas: compute IS and post-sample CAPM beta per signal -----
czret_ff <- as.data.table(czret)[as.data.table(ff), on = "date", nomatch = 0]
czret_ff[, retx := ret - rf]  # excess returns for CAPM

beta_lm <- function(y, x, min_obs = 60) {
  ok <- complete.cases(y, x)
  if (sum(ok) < min_obs) return(NA_real_)
  coef(lm(y[ok] ~ x[ok]))[2]
}

ff3_coefs <- function(y, mkt, smb, hml, min_obs = 60) {
  ok <- complete.cases(y, mkt, smb, hml)
  if (sum(ok) < min_obs) return(c(mkt = NA_real_, smb = NA_real_, hml = NA_real_))
  cf <- coef(lm(y[ok] ~ mkt[ok] + smb[ok] + hml[ok]))
  setNames(cf[2:4], c("mkt", "smb", "hml"))
}

ff4_coefs <- function(y, mkt, smb, hml, umd, min_obs = 60) {
  ok <- complete.cases(y, mkt, smb, hml, umd)
  if (sum(ok) < min_obs) return(c(mkt = NA_real_, smb = NA_real_, hml = NA_real_, umd = NA_real_))
  cf <- coef(lm(y[ok] ~ mkt[ok] + smb[ok] + hml[ok] + umd[ok]))
  setNames(cf[2:5], c("mkt", "smb", "hml", "umd"))
}

pub_betas <- czret_ff[, {
  is_idx   <- date >= sampstart & date <= sampend
  post_idx <- date >  sampend
  ff3_is   <- ff3_coefs(retx[is_idx],   mktrf[is_idx],   smb[is_idx],   hml[is_idx])
  ff3_post <- ff3_coefs(retx[post_idx], mktrf[post_idx], smb[post_idx], hml[post_idx])
  ff4_is   <- ff4_coefs(retx[is_idx],   mktrf[is_idx],   smb[is_idx],   hml[is_idx],   umd[is_idx])
  ff4_post <- ff4_coefs(retx[post_idx], mktrf[post_idx], smb[post_idx], hml[post_idx], umd[post_idx])
  .(
    pub_beta_is     = beta_lm(retx[is_idx],   mktrf[is_idx]),
    pub_beta_post   = beta_lm(retx[post_idx], mktrf[post_idx]),
    pub_hml_is      = ff3_is["hml"],
    pub_hml_post    = ff3_post["hml"],
    pub_smb_is      = ff3_is["smb"],
    pub_smb_post    = ff3_post["smb"],
    pub_umd_is      = ff4_is["umd"],
    pub_umd_post    = ff4_post["umd"]
  )
}, by = signalname]

# DM-side loadings: average IS and post-sample factor loading across matches ---
dm_betas <- as.data.table(dm_coef)[, .(
  dm_beta_is    = mean(beta_capm_is,   na.rm = TRUE),
  dm_beta_post  = mean(beta_capm_post, na.rm = TRUE),
  dm_hml_is     = mean(h_ff3_is,       na.rm = TRUE),
  dm_hml_post   = mean(h_ff3_post,     na.rm = TRUE),
  dm_smb_is     = mean(s_ff3_is,       na.rm = TRUE),
  dm_smb_post   = mean(s_ff3_post,     na.rm = TRUE),
  dm_umd_is     = mean(u_ff4_is,       na.rm = TRUE),
  dm_umd_post   = mean(u_ff4_post,     na.rm = TRUE),
  n_matches     = .N
), by = .(pubname = actSignal)]

# Combine + Campbell filter (consistent with EB analysis) -------------------
signal_info <- czsum %>%
  transmute(signalname,
            sampend_year = floor(as.numeric(sampend)),
            cohort = paste0(floor(sampend_year / 10) * 10, "s"),
            pub_is_return = rbar)

cal_data <- ret_for_plot0 %>%
  filter(!is.na(matchRet)) %>%
  left_join(signal_info, by = c("pubname" = "signalname"))

min_oos_months <- 60
sigs_oos <- cal_data %>% filter(eventDate > 0) %>%
  group_by(pubname) %>% summarize(max_oos = max(eventDate), .groups = "drop") %>%
  filter(max_oos >= min_oos_months) %>% pull(pubname)

cohort_sizes <- cal_data %>% filter(pubname %in% sigs_oos) %>%
  distinct(pubname, cohort) %>% count(cohort) %>% filter(n >= 10)

keep_pubnames <- cal_data %>%
  filter(pubname %in% sigs_oos, cohort %in% cohort_sizes$cohort) %>%
  distinct(pubname) %>% pull(pubname)

beta_dt <- pub_betas %>%
  rename(pubname = signalname) %>%
  inner_join(dm_betas, by = "pubname") %>%
  inner_join(theory_lookup, by = "pubname") %>%
  inner_join(signal_info %>% rename(pubname = signalname), by = "pubname") %>%
  filter(pubname %in% keep_pubnames) %>%
  mutate(
    pub_dbeta = pub_beta_post - pub_beta_is,
    dm_dbeta  = dm_beta_post  - dm_beta_is,
    pub_dhml  = pub_hml_post  - pub_hml_is,
    dm_dhml   = dm_hml_post   - dm_hml_is,
    pub_dsmb  = pub_smb_post  - pub_smb_is,
    dm_dsmb   = dm_smb_post   - dm_smb_is,
    pub_dumd  = pub_umd_post  - pub_umd_is,
    dm_dumd   = dm_umd_post   - dm_umd_is
  )

cat("Signals after Campbell filter:", nrow(beta_dt), "\n")

# Saved per-signal data ------------------------------------------------------
saveRDS(beta_dt, file.path(data_dir, "beta_evolution.RDS"))
write.csv(beta_dt %>% select(pubname, theory, cohort, pub_is_return,
                              pub_beta_is, pub_beta_post, pub_dbeta,
                              dm_beta_is,  dm_beta_post,  dm_dbeta,
                              n_matches),
          file.path(results_dir, "BetaEvolution_PerSignal.csv"),
          row.names = FALSE)

# Summary table: overall + by theory ----------------------------------------
make_summary <- function(d, label) {
  diff_vec <- (d$pub_dbeta - d$dm_dbeta)
  diff_vec <- diff_vec[is.finite(diff_vec)]
  d %>% summarize(
    group         = label,
    n             = dplyr::n(),
    pub_beta_is   = mean(pub_beta_is,   na.rm = TRUE),
    pub_beta_post = mean(pub_beta_post, na.rm = TRUE),
    pub_dbeta     = mean(pub_dbeta,     na.rm = TRUE),
    dm_beta_is    = mean(dm_beta_is,    na.rm = TRUE),
    dm_beta_post  = mean(dm_beta_post,  na.rm = TRUE),
    dm_dbeta      = mean(dm_dbeta,      na.rm = TRUE),
    diff_dbeta    = mean(diff_vec),
    se_diff       = sd(diff_vec) / sqrt(length(diff_vec)),
    t_diff        = diff_dbeta / se_diff
  )
}

summary_tbl <- bind_rows(
  make_summary(beta_dt, "All"),
  beta_dt %>% group_by(theory) %>% group_modify(~ make_summary(.x, .y$theory)) %>%
    ungroup() %>% select(-theory)
)

write.csv(summary_tbl, file.path(results_dir, "BetaEvolution_Summary.csv"),
          row.names = FALSE)

cat("\n--- Mean CAPM market beta: IS vs post-sample ---\n")
print(as.data.frame(summary_tbl))

# Multi-factor summary (HML, SMB, UMD) -------------------------------------
make_factor_summary <- function(d, label, pub_d, dm_d) {
  diff_vec <- (d[[pub_d]] - d[[dm_d]])
  diff_vec <- diff_vec[is.finite(diff_vec)]
  data.frame(
    group       = label,
    n           = nrow(d),
    pub_dbeta   = mean(d[[pub_d]], na.rm = TRUE),
    dm_dbeta    = mean(d[[dm_d]],  na.rm = TRUE),
    diff_dbeta  = mean(diff_vec),
    se_diff     = sd(diff_vec) / sqrt(length(diff_vec)),
    t_diff      = mean(diff_vec) / (sd(diff_vec) / sqrt(length(diff_vec)))
  )
}

build_factor_table <- function(pub_d, dm_d) {
  bind_rows(
    make_factor_summary(beta_dt, "All", pub_d, dm_d),
    do.call(rbind, lapply(unique(beta_dt$theory), function(th) {
      make_factor_summary(beta_dt %>% filter(theory == th), th, pub_d, dm_d)
    }))
  )
}

hml_tbl <- build_factor_table("pub_dhml", "dm_dhml")
smb_tbl <- build_factor_table("pub_dsmb", "dm_dsmb")
umd_tbl <- build_factor_table("pub_dumd", "dm_dumd")

multifactor_tbl <- bind_rows(
  hml_tbl %>% mutate(factor = "HML"),
  smb_tbl %>% mutate(factor = "SMB"),
  umd_tbl %>% mutate(factor = "UMD")
)

write.csv(multifactor_tbl, file.path(results_dir, "BetaEvolution_MultiFactor.csv"),
          row.names = FALSE)

cat("\n--- Multi-factor loadings: HML, SMB, UMD ---\n")
for (f in c("HML", "SMB", "UMD")) {
  cat(sprintf("\nFactor %s:\n", f))
  print(multifactor_tbl %>% filter(factor == f) %>% select(-factor))
}

# LaTeX multi-factor table
fmt <- function(x) ifelse(is.na(x), "--", sprintf("%.3f", x))
fmtt <- function(x) ifelse(is.na(x), "--", sprintf("%.2f", x))

tex_mf <- c(
  "\\begin{tabular}{llrrrrr}",
  "\\toprule",
  "Factor & Group & $N$ & $\\Delta_{\\rm pub}$ & $\\Delta_{\\rm dm}$ & Diff & $t$ \\\\",
  "\\midrule"
)
for (k in seq_len(nrow(multifactor_tbl))) {
  if (k > 1 && multifactor_tbl$factor[k] != multifactor_tbl$factor[k-1]) {
    tex_mf <- c(tex_mf, "\\midrule")
  }
  tex_mf <- c(tex_mf,
    sprintf("%s & %s & %d & %s & %s & %s & %s \\\\",
            multifactor_tbl$factor[k],
            tools::toTitleCase(multifactor_tbl$group[k]),
            multifactor_tbl$n[k],
            fmt(multifactor_tbl$pub_dbeta[k]),
            fmt(multifactor_tbl$dm_dbeta[k]),
            fmt(multifactor_tbl$diff_dbeta[k]),
            fmtt(multifactor_tbl$t_diff[k])))
}
tex_mf <- c(tex_mf, "\\bottomrule", "\\end{tabular}")
writeLines(tex_mf, file.path(results_dir, "BetaEvolution_MultiFactor.tex"))

# Bar plot: Δβ for pub vs DM, by factor (All-signal row) --------------------
# Add CAPM market beta to the same plot for completeness
all_factor_bars <- bind_rows(
  data.frame(factor = "Market",
             pub_dbeta = mean(beta_dt$pub_dbeta, na.rm = TRUE),
             dm_dbeta  = mean(beta_dt$dm_dbeta,  na.rm = TRUE)),
  multifactor_tbl %>% filter(group == "All") %>%
    select(factor, pub_dbeta, dm_dbeta)
) %>%
  mutate(factor = factor(factor, levels = c("Market", "HML", "SMB", "UMD"))) %>%
  pivot_longer(cols = c(pub_dbeta, dm_dbeta),
               names_to = "type", values_to = "dbeta") %>%
  mutate(type = factor(type, levels = c("pub_dbeta", "dm_dbeta"),
                       labels = c("Published", "Data-mined")))

p_bar <- ggplot(all_factor_bars, aes(x = factor, y = dbeta, fill = type)) +
  geom_col(position = position_dodge(width = 0.8), width = 0.7) +
  geom_hline(yintercept = 0) +
  scale_fill_manual(values = colors[1:2]) +
  labs(x = "Factor", y = expression(Delta * beta == beta[post] - beta[IS]),
       fill = "") +
  theme_light(base_size = 22) +
  theme(legend.position = c(0.85, 0.15),
        legend.background = element_rect(fill = "white", color = "black", linewidth = 0.3))

ggsave(file.path(results_dir, "Fig_BetaEvolution_Bar.pdf"), p_bar,
       width = 10, height = 6)

# Cho-style regression: post-sample beta on IS return for published ----------
cho <- lm(pub_beta_post ~ pub_is_return, data = beta_dt)
cho_summary <- summary(cho)
cho_dm <- lm(pub_dbeta ~ pub_is_return, data = beta_dt)
cho_dm_summary <- summary(cho_dm)

cat("\n--- Cho-style: post-sample published beta predicted by IS return ---\n")
print(cho_summary$coefficients)
cat("\n--- Cho-style: change in published beta predicted by IS return ---\n")
print(cho_dm_summary$coefficients)

# LaTeX summary table --------------------------------------------------------
fmt <- function(x) ifelse(is.na(x), "--", sprintf("%.3f", x))
fmtt <- function(x) ifelse(is.na(x), "--", sprintf("%.2f", x))

tex <- c(
  "\\begin{tabular}{lrrrrrrrrr}",
  "\\toprule",
  " & N & \\multicolumn{3}{c}{Published $\\beta$} & \\multicolumn{3}{c}{Data-mined $\\beta$} & $\\Delta\\beta_{\\rm pub}-\\Delta\\beta_{\\rm dm}$ & $t$ \\\\",
  "\\cmidrule(lr){3-5}\\cmidrule(lr){6-8}",
  " & & IS & post & $\\Delta$ & IS & post & $\\Delta$ & & \\\\",
  "\\midrule"
)
for (k in seq_len(nrow(summary_tbl))) {
  tex <- c(tex,
    sprintf("%s & %d & %s & %s & %s & %s & %s & %s & %s & %s \\\\",
            tools::toTitleCase(summary_tbl$group[k]),
            summary_tbl$n[k],
            fmt(summary_tbl$pub_beta_is[k]),
            fmt(summary_tbl$pub_beta_post[k]),
            fmt(summary_tbl$pub_dbeta[k]),
            fmt(summary_tbl$dm_beta_is[k]),
            fmt(summary_tbl$dm_beta_post[k]),
            fmt(summary_tbl$dm_dbeta[k]),
            fmt(summary_tbl$diff_dbeta[k]),
            fmtt(summary_tbl$t_diff[k])))
}
tex <- c(tex, "\\bottomrule", "\\end{tabular}")
writeLines(tex, file.path(results_dir, "BetaEvolution_Summary.tex"))

# Plot 1: histogram of Δβ for pub and DM ------------------------------------
plot_long <- beta_dt %>%
  select(pubname, pub_dbeta, dm_dbeta) %>%
  pivot_longer(cols = c(pub_dbeta, dm_dbeta), names_to = "type",
               values_to = "dbeta") %>%
  mutate(type = factor(type, levels = c("pub_dbeta", "dm_dbeta"),
                       labels = c("Published", "Data-mined")))

p1 <- ggplot(plot_long, aes(x = dbeta, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.5, bins = 40) +
  scale_fill_manual(values = colors[1:2]) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  labs(x = expression(Delta * beta == beta[post] - beta[IS]),
       y = "Number of signals", fill = "") +
  theme_light(base_size = 18) +
  theme(legend.position = c(0.85, 0.85),
        legend.background = element_rect(fill = "white", color = "black", linewidth = 0.3))

ggsave(file.path(results_dir, "Fig_BetaEvolution_Hist.pdf"), p1,
       width = 10, height = 6)

# Plot 2: scatter pre vs post, pub vs DM ------------------------------------
scatter_dt <- beta_dt %>%
  select(pubname, theory,
         Published_IS = pub_beta_is, Published_post = pub_beta_post,
         DM_IS = dm_beta_is, DM_post = dm_beta_post) %>%
  pivot_longer(cols = -c(pubname, theory),
               names_to = c("type", ".value"),
               names_sep = "_")

p2 <- ggplot(scatter_dt, aes(x = IS, y = post, color = type)) +
  geom_point(alpha = 0.6, size = 2) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "grey40") +
  scale_color_manual(values = colors[1:2],
                     labels = c("Data-mined", "Published")) +
  labs(x = expression(beta[IS]), y = expression(beta[post]), color = "") +
  theme_light(base_size = 18) +
  theme(legend.position = c(0.15, 0.92),
        legend.background = element_rect(fill = "white", color = "black", linewidth = 0.3))

ggsave(file.path(results_dir, "Fig_BetaEvolution_Scatter.pdf"), p2,
       width = 10, height = 6)

# Plot 3: Cho-style — Δβ_pub on IS return -----------------------------------
p3 <- ggplot(beta_dt, aes(x = pub_is_return, y = pub_dbeta)) +
  geom_point(alpha = 0.5, size = 2, color = colors[1]) +
  geom_smooth(method = "lm", color = "black", fill = "grey80") +
  geom_hline(yintercept = 0, linetype = "dashed") +
  labs(x = "In-sample mean return (\\%/mo)",
       y = expression(Delta * beta[pub] == beta[post] - beta[IS])) +
  theme_light(base_size = 18)

ggsave(file.path(results_dir, "Fig_BetaEvolution_Cho.pdf"), p3,
       width = 10, height = 6)

cat("\nSaved tables and figures:\n")
cat("  ", file.path(results_dir, "BetaEvolution_PerSignal.csv"), "\n")
cat("  ", file.path(results_dir, "BetaEvolution_Summary.csv"), "\n")
cat("  ", file.path(results_dir, "BetaEvolution_Summary.tex"), "\n")
cat("  ", file.path(results_dir, "Fig_BetaEvolution_Hist.pdf"), "\n")
cat("  ", file.path(results_dir, "Fig_BetaEvolution_Scatter.pdf"), "\n")
cat("  ", file.path(results_dir, "Fig_BetaEvolution_Cho.pdf"), "\n")
cat("\n=== Done ===\n")
