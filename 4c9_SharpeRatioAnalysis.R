# Sharpe ratio analysis following Campbell (2024) slide 21 suggestion
# 1) Report OOS/IS Sharpe ratio by cohort — Published vs Data-Mined
# 2) SR^2-weighted portfolio returns in calendar time
#
# Campbell's point: scaling returns to same IS mean and equal-weighting
# inversely weights by SR^2. An investor would weight *proportional* to SR^2.
#
# Note: SR is invariant to the scaling (ret/rbar*100), so we can compute SR
# from ret_for_plot0's scaled returns and get the same result as from raw returns.

# Setup --------------------------------------------------------

rm(list = ls())
source("0_Environment.R")

# Load data  -------------------------------------------
# Use same data as 4c8 for consistent signal universe

ret_for_plot0 <- readRDS("../Data/Processed/ret_for_plot0.RDS")

czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep)

# Assign decade cohorts
signal_info <- czsum %>%
  transmute(
    signalname,
    sampend_year = floor(as.numeric(sampend)),
    cohort = paste0(floor(sampend_year / 10) * 10, "s")
  )

# Build dataset with cohort info
cal_data <- ret_for_plot0 %>%
  filter(!is.na(matchRet)) %>%
  left_join(signal_info, by = c("pubname" = "signalname"))

# Campbell filter: require >= 5 years of post-sample data
min_oos_months <- 60

signals_with_enough_oos <- cal_data %>%
  filter(eventDate > 0) %>%
  group_by(pubname) %>%
  summarize(max_oos = max(eventDate), .groups = "drop") %>%
  filter(max_oos >= min_oos_months) %>%
  pull(pubname)

cal_data <- cal_data %>%
  filter(pubname %in% signals_with_enough_oos)

# Drop small cohorts
min_cohort_size <- 10

cohort_sizes <- cal_data %>%
  distinct(pubname, cohort) %>%
  count(cohort) %>%
  filter(n >= min_cohort_size)

cal_data <- cal_data %>%
  filter(cohort %in% cohort_sizes$cohort)

cat("\nCohorts kept (N >=", min_cohort_size, "):\n")
cohort_sizes %>% print()
cat("Total signals:", n_distinct(cal_data$pubname), "\n")

# Part 1: Compute IS and OOS Sharpe ratios — Published AND DM -------------------

# Published signal SRs
sr_pub <- cal_data %>%
  mutate(samptype = ifelse(eventDate <= 0, "insamp", "oos")) %>%
  group_by(pubname, cohort, theory, samptype) %>%
  summarize(
    mean_ret = mean(ret, na.rm = TRUE),
    sd_ret = sd(ret, na.rm = TRUE),
    n_months = n(),
    .groups = "drop"
  ) %>%
  filter(sd_ret > 0, n_months >= 24) %>%
  mutate(sr_annual = mean_ret / sd_ret * sqrt(12))

sr_pub_wide <- sr_pub %>%
  select(pubname, cohort, theory, samptype, sr_annual) %>%
  pivot_wider(names_from = samptype, values_from = sr_annual, names_prefix = "sr_") %>%
  filter(!is.na(sr_insamp) & !is.na(sr_oos)) %>%
  mutate(sr_ratio = sr_oos / sr_insamp, source = "Published")

# Data-mined signal SRs (matchRet is already scaled, SR invariant to scaling)
sr_dm <- cal_data %>%
  mutate(samptype = ifelse(eventDate <= 0, "insamp", "oos")) %>%
  group_by(pubname, cohort, theory, samptype) %>%
  summarize(
    mean_ret = mean(matchRet, na.rm = TRUE),
    sd_ret = sd(matchRet, na.rm = TRUE),
    n_months = n(),
    .groups = "drop"
  ) %>%
  filter(sd_ret > 0, n_months >= 24) %>%
  mutate(sr_annual = mean_ret / sd_ret * sqrt(12))

sr_dm_wide <- sr_dm %>%
  select(pubname, cohort, theory, samptype, sr_annual) %>%
  pivot_wider(names_from = samptype, values_from = sr_annual, names_prefix = "sr_") %>%
  filter(!is.na(sr_insamp) & !is.na(sr_oos)) %>%
  mutate(sr_ratio = sr_oos / sr_insamp, source = "Data-Mined")

# Combine for plotting
sr_combined <- bind_rows(sr_pub_wide, sr_dm_wide)

cat("\n=== Sharpe Ratio Summary ===\n")
cat("Published signals with both IS and OOS SR:", nrow(sr_pub_wide), "\n")
cat("DM signals with both IS and OOS SR:", nrow(sr_dm_wide), "\n")

# Summary by cohort and source
cat("\nOOS/IS Sharpe ratio by cohort and source:\n")
sr_combined %>%
  group_by(source, cohort) %>%
  summarize(
    n = n(),
    median_sr_ratio = median(sr_ratio),
    mean_sr_ratio = mean(sr_ratio),
    mean_sr_is = mean(sr_insamp),
    mean_sr_oos = mean(sr_oos),
    .groups = "drop"
  ) %>%
  print(n = 20)

# Summary by theory and source
cat("\nOOS/IS Sharpe ratio by theory and source:\n")
sr_combined %>%
  group_by(source, theory) %>%
  summarize(
    n = n(),
    median_sr_ratio = median(sr_ratio),
    mean_sr_ratio = mean(sr_ratio),
    .groups = "drop"
  ) %>%
  print(n = 20)

# Plot settings
fontsizeall <- 28
linesizeall <- 1.5
rollmonths <- 60

roll_fn <- function(x) {
  if (sum(!is.na(x)) < 0.8 * length(x)) return(NA_real_)
  mean(x, na.rm = TRUE)
}

# Plot 1a: OOS/IS Sharpe ratio by cohort — Pub vs DM -------------------

p_sr_cohort <- ggplot(sr_combined, aes(x = cohort, y = sr_ratio, fill = source)) +
  geom_boxplot(outlier.alpha = 0.3) +
  scale_fill_manual(values = c("Published" = colors[1], "Data-Mined" = colors[2])) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "dimgrey") +
  geom_hline(yintercept = 0) +
  coord_cartesian(ylim = c(-1.5, 3)) +
  ggtitle("OOS/IS Sharpe Ratio by Cohort") +
  ylab("OOS / IS Sharpe Ratio") +
  xlab("Sample End Decade") +
  labs(fill = "") +
  theme_light(base_size = fontsizeall) +
  theme(
    legend.position = c(0.80, 0.90),
    legend.background = element_rect(fill = 'white', color = 'black', linewidth = 0.3)
  )

ggsave("../Results/Fig_SharpeRatio_OOSvsIS_ByCohort.pdf", p_sr_cohort,
       width = 12, height = 8)

# Plot 1b: OOS/IS Sharpe ratio by theory — Pub vs DM -------------------

p_sr_theory <- ggplot(sr_combined, aes(x = str_to_title(theory), y = sr_ratio, fill = source)) +
  geom_boxplot(outlier.alpha = 0.3) +
  scale_fill_manual(values = c("Published" = colors[1], "Data-Mined" = colors[2])) +
  geom_hline(yintercept = 1, linetype = "dashed", color = "dimgrey") +
  geom_hline(yintercept = 0) +
  coord_cartesian(ylim = c(-1.5, 3)) +
  ggtitle("OOS/IS Sharpe Ratio by Theory") +
  ylab("OOS / IS Sharpe Ratio") +
  xlab("Theory Category") +
  labs(fill = "") +
  theme_light(base_size = fontsizeall) +
  theme(
    legend.position = c(0.80, 0.90),
    legend.background = element_rect(fill = 'white', color = 'black', linewidth = 0.3)
  )

ggsave("../Results/Fig_SharpeRatio_OOSvsIS_ByTheory.pdf", p_sr_theory,
       width = 12, height = 8)

# Part 2: SR^2-weighted Pub vs DM in calendar time (OOS only) -------------------

# Compute IS MV-optimal weights separately for published and DM
# Each weighted by its own IS performance: w = mu_IS / sigma_IS^2
sr_weights_pub <- sr_pub %>%
  filter(samptype == "insamp") %>%
  transmute(pubname, w_pub = mean_ret / sd_ret^2)

sr_weights_dm <- sr_dm %>%
  filter(samptype == "insamp") %>%
  transmute(pubname, w_dm = mean_ret / sd_ret^2)

cal_oos <- cal_data %>%
  filter(eventDate >= 60) %>%
  inner_join(sr_weights_pub, by = "pubname") %>%
  inner_join(sr_weights_dm, by = "pubname") %>%
  mutate(calMonth = as.Date(calendarDate))

# MV-optimal weighted Pub vs DM portfolio returns
# Each series weighted by its own IS mu/sigma^2
portfolio_monthly <- cal_oos %>%
  group_by(calMonth) %>%
  summarize(
    ret_pub_mv = weighted.mean(ret, w = w_pub, na.rm = TRUE),
    ret_dm_mv = weighted.mean(matchRet, w = w_dm, na.rm = TRUE),
    n_signals = n_distinct(pubname),
    .groups = "drop"
  ) %>%
  complete(calMonth = seq.Date(min(calMonth), max(calMonth), by = "month")) %>%
  arrange(calMonth) %>%
  mutate(
    roll_pub = zoo::rollapply(ret_pub_mv, width = rollmonths, fill = NA,
                               align = 'right', FUN = roll_fn),
    roll_dm = zoo::rollapply(ret_dm_mv, width = rollmonths, fill = NA,
                              align = 'right', FUN = roll_fn)
  )

# Reshape for plotting
plot_pubvsdm <- portfolio_monthly %>%
  select(calMonth, roll_pub, roll_dm) %>%
  filter(!is.na(roll_pub) & !is.na(roll_dm)) %>%
  gather(key = "SignalType", value = "roll_ret", roll_pub, roll_dm) %>%
  mutate(SignalType = factor(SignalType,
                              levels = c("roll_pub", "roll_dm"),
                              labels = c("Published (Peer Reviewed)",
                                         "Data-Mined for |t|>2.0")))

p_sr2_pubvsdm <- ggplot(plot_pubvsdm, aes(x = calMonth, y = roll_ret,
                                            color = SignalType, linetype = SignalType)) +
  geom_line(linewidth = linesizeall) +
  scale_color_manual(values = colors[1:2]) +
  scale_linetype_manual(values = c('solid', 'longdash')) +
  ggtitle("MV-Optimal: Published vs Data-Mined (OOS)") +
  geom_hline(yintercept = 0) +
  scale_y_continuous(breaks = seq(-200, 200, 25)) +
  scale_x_date(date_breaks = "5 years", date_labels = "%Y") +
  ylab('Trailing 5-Year Return (bps pm)') +
  xlab('Calendar Date') +
  labs(color = '', linetype = '') +
  theme_light(base_size = fontsizeall) +
  theme(
    legend.position = c(0.35, 0.15),
    legend.spacing.y = unit(0.1, units = 'cm'),
    legend.background = element_rect(fill = 'white', color = 'black', linewidth = 0.3),
    legend.key.width = unit(1.5, units = 'cm')
  )

ggsave("../Results/Fig_SharpeRatio_SR2Weighted_PubVsDM_OOS.pdf", p_sr2_pubvsdm,
       width = 12, height = 8)

# Part 3: SR^2-weighted by cohort in calendar time (published only) -------------------

cohort_portfolio <- cal_oos %>%
  group_by(cohort, calMonth) %>%
  summarize(
    ret_sr2 = weighted.mean(ret, w = w_pub, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(cohort) %>%
  complete(calMonth = seq.Date(min(calMonth), max(calMonth), by = "month")) %>%
  arrange(cohort, calMonth) %>%
  mutate(
    roll_ret = zoo::rollapply(ret_sr2, width = rollmonths, fill = NA,
                               align = 'right', FUN = roll_fn)
  ) %>%
  ungroup() %>%
  filter(!is.na(roll_ret))

# Overall SR^2-weighted average
avg_portfolio <- cal_oos %>%
  group_by(calMonth) %>%
  summarize(
    ret_sr2 = weighted.mean(ret, w = w_pub, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  complete(calMonth = seq.Date(min(calMonth), max(calMonth), by = "month")) %>%
  arrange(calMonth) %>%
  mutate(
    roll_ret = zoo::rollapply(ret_sr2, width = rollmonths, fill = NA,
                               align = 'right', FUN = roll_fn)
  ) %>%
  filter(!is.na(roll_ret))

# Add cohort labels
cohort_counts <- cal_oos %>% distinct(pubname, cohort) %>% count(cohort)
cohort_portfolio <- cohort_portfolio %>%
  left_join(cohort_counts, by = "cohort") %>%
  mutate(cohort_label = paste0(cohort, " (N=", n, ")"))

p_sr2_cohort <- ggplot() +
  geom_line(data = cohort_portfolio,
            aes(x = calMonth, y = roll_ret, color = cohort_label),
            alpha = 0.25, linewidth = 0.8) +
  scale_color_brewer(palette = "Set1") +
  geom_line(data = avg_portfolio,
            aes(x = calMonth, y = roll_ret),
            linewidth = linesizeall, color = "black") +
  ggtitle("MV-Optimal Published Returns by Cohort (OOS)") +
  geom_hline(yintercept = 0) +
  scale_y_continuous(breaks = seq(-200, 200, 25)) +
  scale_x_date(date_breaks = "5 years", date_labels = "%Y") +
  ylab('Trailing 5-Year Return (bps pm)') +
  xlab('Calendar Date') +
  labs(color = 'Cohort') +
  theme_light(base_size = fontsizeall) +
  theme(
    legend.position = c(0.20, 0.82),
    legend.spacing.y = unit(0.1, units = 'cm'),
    legend.background = element_rect(fill = 'white', color = 'black', linewidth = 0.3),
    legend.key.width = unit(1.5, units = 'cm'),
    legend.text = element_text(size = fontsizeall * 0.55),
    legend.title = element_text(size = fontsizeall * 0.65)
  )

ggsave("../Results/Fig_SharpeRatio_SR2Weighted_ByCohort_OOS.pdf", p_sr2_cohort,
       width = 12, height = 8)

# Part 4: MV-weighted DM by cohort in calendar time -------------------

cohort_portfolio_dm <- cal_oos %>%
  group_by(cohort, calMonth) %>%
  summarize(
    ret_dm = weighted.mean(matchRet, w = w_dm, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(cohort) %>%
  complete(calMonth = seq.Date(min(calMonth), max(calMonth), by = "month")) %>%
  arrange(cohort, calMonth) %>%
  mutate(
    roll_ret = zoo::rollapply(ret_dm, width = rollmonths, fill = NA,
                               align = 'right', FUN = roll_fn)
  ) %>%
  ungroup() %>%
  filter(!is.na(roll_ret))

avg_portfolio_dm <- cal_oos %>%
  group_by(calMonth) %>%
  summarize(
    ret_dm = weighted.mean(matchRet, w = w_dm, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  complete(calMonth = seq.Date(min(calMonth), max(calMonth), by = "month")) %>%
  arrange(calMonth) %>%
  mutate(
    roll_ret = zoo::rollapply(ret_dm, width = rollmonths, fill = NA,
                               align = 'right', FUN = roll_fn)
  ) %>%
  filter(!is.na(roll_ret))

cohort_portfolio_dm <- cohort_portfolio_dm %>%
  left_join(cohort_counts, by = "cohort") %>%
  mutate(cohort_label = paste0(cohort, " (N=", n, ")"))

p_sr2_cohort_dm <- ggplot() +
  geom_line(data = cohort_portfolio_dm,
            aes(x = calMonth, y = roll_ret, color = cohort_label),
            alpha = 0.25, linewidth = 0.8) +
  scale_color_brewer(palette = "Set1") +
  geom_line(data = avg_portfolio_dm,
            aes(x = calMonth, y = roll_ret),
            linewidth = linesizeall, color = "black") +
  ggtitle("MV-Optimal Data-Mined Returns by Cohort (OOS)") +
  geom_hline(yintercept = 0) +
  scale_y_continuous(breaks = seq(-200, 200, 25)) +
  scale_x_date(date_breaks = "5 years", date_labels = "%Y") +
  ylab('Trailing 5-Year Return (bps pm)') +
  xlab('Calendar Date') +
  labs(color = 'Cohort') +
  theme_light(base_size = fontsizeall) +
  theme(
    legend.position = c(0.20, 0.82),
    legend.spacing.y = unit(0.1, units = 'cm'),
    legend.background = element_rect(fill = 'white', color = 'black', linewidth = 0.3),
    legend.key.width = unit(1.5, units = 'cm'),
    legend.text = element_text(size = fontsizeall * 0.55),
    legend.title = element_text(size = fontsizeall * 0.65)
  )

ggsave("../Results/Fig_SharpeRatio_SR2Weighted_DM_ByCohort_OOS.pdf", p_sr2_cohort_dm,
       width = 12, height = 8)

# Summary stats -------------------------------------------

cat("\n=== Summary Stats ===\n")

cat("\nOverall OOS/IS SR ratio:\n")
sr_combined %>%
  group_by(source) %>%
  summarize(
    n = n(),
    median_sr_ratio = median(sr_ratio),
    mean_sr_ratio = mean(sr_ratio),
    mean_sr_is = mean(sr_insamp),
    mean_sr_oos = mean(sr_oos),
    .groups = "drop"
  ) %>%
  print()

cat("\nSR^2-weighted portfolio mean trailing return (OOS):\n")
portfolio_monthly %>%
  filter(!is.na(roll_pub) & !is.na(roll_dm)) %>%
  summarize(
    mean_pub_sr2 = mean(roll_pub, na.rm = TRUE),
    mean_dm_sr2 = mean(roll_dm, na.rm = TRUE)
  ) %>%
  print()

cat("\nDone.\n")
