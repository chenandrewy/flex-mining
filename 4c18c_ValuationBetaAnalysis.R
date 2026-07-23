# Valuation-beta stability analysis (Campbell NBER AP 2024 discussion, sl. 17):
# Is the PREDICTIVE COEFFICIENT of returns on valuation ratios stable OOS,
# even where the long-short PORTFOLIO return decays?
#
#   r_{i,t+1} = a_t + b_t * signal_{i,t}     (monthly cross-sections, 4c18b)
#
# For each published signal: mean b_t in-sample (sampstart..sampend) vs
# out-of-sample, normalized IS = 100. Groups: PV-narrow vs non-PV (and broad).
# Mechanism: L/S ret ~= b_t * spread_t; spread compression vs coefficient decay.
#
# Inputs:  ../Data/Processed/valbeta_monthly.RDS   (4c18b)
#          ../Data/Processed/czsum_allpredictors.RDS
#          DataInput/SignalsTheoryChecked_withPV.csv
# Outputs: ../Results/ValBeta_* (csv/tex) and Fig_ValBeta_* (pdf)

# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType,
                                  topT = globalSettings$topT)

vb <- readRDS("../Data/Processed/valbeta_monthly.RDS")
vb <- vb[!is.na(b)]

czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  filter(Keep) %>%
  transmute(signalname, sampstart, sampend, rbar_pub = rbar, tstat_pub = tstat)

czpv <- fread('DataInput/SignalsTheoryChecked_withPV.csv') %>%
  transmute(signalname,
            pv_narrow = toupper(trimws(as.character(pv_narrow))) == 'TRUE',
            pv_broad  = toupper(trimws(as.character(pv_broad)))  == 'TRUE')

# yearm (year + (month-1)/12, possibly zoo::yearmon) -> yyyymm
yearm_to_yyyymm <- function(ym) {
  ym <- as.numeric(ym)
  yr <- floor(ym + 1e-6)
  mo <- round(12 * (ym - yr)) + 1
  yr * 100 + mo
}

vb <- merge(vb, as.data.table(czsum), by = "signalname")
vb <- merge(vb, as.data.table(czpv), by = "signalname", all.x = TRUE)
vb <- vb[signalname %in% inclSignals]
vb[is.na(pv_narrow), pv_narrow := FALSE]
vb[is.na(pv_broad),  pv_broad  := FALSE]

vb[, samp_start_mm := yearm_to_yyyymm(sampstart)]
vb[, samp_end_mm   := yearm_to_yyyymm(sampend)]

# event time in months since original-sample end (0 = first OOS month)
mm_to_index <- function(yyyymm) (yyyymm %/% 100) * 12 + (yyyymm %% 100)
vb[, eventDate := mm_to_index(ret_yyyymm) - mm_to_index(samp_end_mm)]
vb[, insamp := ret_yyyymm >= samp_start_mm & ret_yyyymm <= samp_end_mm]
vb[, oos := ret_yyyymm > samp_end_mm]

cat("Signals:", uniqueN(vb$signalname),
    "| PV narrow:", uniqueN(vb[pv_narrow == TRUE, signalname]),
    "| PV broad:", uniqueN(vb[pv_broad == TRUE, signalname]), "\n")

# Per-signal IS/OOS summary -------------------------------------------------
per_sig <- vb[, {
  is_b  <- b[insamp]; oos_b <- b[oos]
  list(
    n_is = sum(insamp), n_oos = sum(oos),
    b_is = mean(is_b), b_is_t = mean(is_b) / (sd(is_b) / sqrt(length(is_b))),
    b_oos = if (sum(oos) >= 12) mean(oos_b) else NA_real_,
    b_oos_t = if (sum(oos) >= 12)
      mean(oos_b) / (sd(oos_b) / sqrt(length(oos_b))) else NA_real_,
    # unit-free stability z: (mean OOS b - mean IS b) / se(diff); decay < 0
    z_stab = if (sum(oos) >= 12)
      (mean(oos_b) - mean(is_b)) /
        sqrt(var(is_b) / length(is_b) + var(oos_b) / length(oos_b))
      else NA_real_,
    spread_is  = mean(spread[insamp], na.rm = TRUE),
    spread_oos = if (sum(oos) >= 12) mean(spread[oos], na.rm = TRUE) else NA_real_,
    ls_is  = mean(ls_ret[insamp], na.rm = TRUE),
    ls_oos = if (sum(oos) >= 12) mean(ls_ret[oos], na.rm = TRUE) else NA_real_
  )
}, by = .(signalname, pv_narrow, pv_broad)]

# Require: enough IS months and a positive IS slope (published direction).
# A signal whose IS FM slope is <= 0 has no meaningful "IS = 100" baseline.
# NOTE: ls_is > 0 is NOT required here -- for discrete signals (dummies:
# ExchSwitch, ConvDebt, ...) the rank-decile L/S is meaningless while the FM
# slope is fine. ls_is > 0 gates only the L/S / mechanism series below.
min_is_months <- 24
excl <- per_sig[n_is < min_is_months | b_is <= 0 | is.na(b_oos)]
cat("\nExcluded signals:", nrow(excl), "of", nrow(per_sig), "\n")
cat("  - too few IS months (<24):", per_sig[n_is < min_is_months, .N], "\n")
cat("  - negative IS slope:      ",
    per_sig[n_is >= min_is_months & b_is <= 0, .N],
    "(", paste(per_sig[n_is >= min_is_months & b_is <= 0
                       ][order(-pv_narrow)][1:min(8, .N), signalname],
               collapse = ", "), ")\n")
cat("  - too few OOS months:     ",
    per_sig[n_is >= min_is_months & b_is > 0 & is.na(b_oos), .N], "\n")
cat("  PV-narrow among excluded: ", excl[pv_narrow == TRUE, .N], "\n")

keep_sig <- per_sig[!signalname %in% excl$signalname]
keep_sig[, ratio := b_oos / b_is]
vb <- vb[signalname %in% keep_sig$signalname]

cat("\nFinal sample:", nrow(keep_sig), "signals | PV narrow:",
    keep_sig[pv_narrow == TRUE, .N], "| non-PV (narrow spec):",
    keep_sig[pv_narrow == FALSE, .N], "\n")

# Normalize: IS coefficient = 100 --------------------------------------------
vb <- merge(vb, keep_sig[, .(signalname, b_is, spread_is, ls_is)],
            by = "signalname")
vb[, b_norm  := 100 * b / b_is]
# L/S-based series only where the IS L/S baseline is meaningful (continuous
# signals with positive IS decile L/S); dummies etc. stay in b_norm only
vb[, sp_norm := fifelse(ls_is > 0, 100 * spread / spread_is, NA_real_)]
vb[, ls_norm := fifelse(ls_is > 0, 100 * ls_ret / ls_is, NA_real_)]
vb[, bxs_norm := fifelse(ls_is > 0,
                         100 * (b * spread) / (b_is * spread_is), NA_real_)]

# winsorize normalized panels at 1/99 pctile (monthly b is noisy; keeps a
# handful of near-zero-baseline signals from dominating clustered means)
for (v in c("b_norm", "sp_norm", "ls_norm", "bxs_norm")) {
  qs <- quantile(vb[[v]], c(0.01, 0.99), na.rm = TRUE)
  vb[, (v) := pmin(pmax(get(v), qs[1]), qs[2])]
}

# Period bins as in 4c16
vb[, period := fcase(
  eventDate <= 0,                  "In-sample",
  eventDate > 0 & eventDate <= 60, "Post 0-5y",
  eventDate > 60,                  "Post 5y+"
)]
vb[, period := factor(period, levels = c("In-sample", "Post 0-5y", "Post 5y+"))]
# In-sample bin: restrict to actual IS window (pre-sampstart months excluded)
vb_periods <- vb[period != "In-sample" | insamp == TRUE]

# Clustered mean + SE (cluster by month + signal), as in 4c16 -----------------
clustered_mean_se <- function(d, yvar) {
  d <- d[is.finite(d[[yvar]])]
  if (nrow(d) == 0 || uniqueN(d$signalname) < 2 || uniqueN(d$ret_yyyymm) < 2)
    return(list(m = NA_real_, se = NA_real_))
  m  <- lm(reformulate("1", yvar), data = d)
  se <- tryCatch(sqrt(sandwich::vcovCL(m, cluster = ~ ret_yyyymm + signalname)[1, 1]),
                 error = function(e) NA_real_)
  list(m = unname(coef(m)[1]), se = se)
}

build_summary <- function(flagcol, spec_label) {
  d <- copy(vb_periods)
  d[, PVgroup := fifelse(get(flagcol), "Present-value", "Non-PV")]
  out <- list()
  for (g in unique(d$PVgroup)) for (p in levels(d$period)) {
    sub <- d[PVgroup == g & period == p]
    for (series in c("FM coefficient", "L/S return", "Valuation spread")) {
      yv <- c("FM coefficient" = "b_norm", "L/S return" = "ls_norm",
              "Valuation spread" = "sp_norm")[series]
      r  <- clustered_mean_se(sub, yv)
      out[[length(out) + 1]] <- data.table(
        spec = spec_label, PVgroup = g, period = p, series = series,
        n_sig = uniqueN(sub[is.finite(sub[[yv]]), signalname]),
        mean = r$m, se = r$se)
    }
  }
  rbindlist(out)
}

summ <- rbindlist(list(build_summary("pv_narrow", "Narrow"),
                       build_summary("pv_broad",  "Broad")))
summ[, `:=`(lo = mean - 1.96 * se, hi = mean + 1.96 * se)]
summ[, PVgroup := factor(PVgroup, levels = c("Present-value", "Non-PV"))]

cat("\nPeriod means, IS = 100, clustered (month + signal) 95% CI:\n")
print(summ[order(spec, PVgroup, series, period),
           .(spec, PVgroup, series, period, n_sig,
             mean = round(mean, 1),
             ci = paste0("[", round(lo), ",", round(hi), "]"))],
      nrows = 100)

fwrite(summ, "../Results/ValBeta_PeriodMeans.csv")

# Pooled stability test: b_norm on OOS dummy, PV vs non-PV -------------------
cat("\n=== Pooled tests: normalized FM coefficient on OOS dummy ===\n")
library(fixest)
for (spec in c("pv_narrow", "pv_broad")) {
  d <- copy(vb_periods)
  d[, PV := get(spec)]
  fit <- feols(b_norm ~ oos * PV, data = d,
               cluster = ~ ret_yyyymm + signalname)
  cat("\n---", spec, "---\n")
  print(summary(fit))
}

# Headline point-range figure (Narrow spec) ----------------------------------
make_fig <- function(spec_label, outfile) {
  dd <- summ[spec == spec_label & series != "Valuation spread"]
  # label facets by the group's max n_sig (series can differ slightly in
  # coverage; per-row n_sig would create labels outside the factor levels)
  nlab <- dd[, .(n = max(n_sig)), by = PVgroup][order(PVgroup)]
  levs <- paste0(nlab$PVgroup, " (", nlab$n, " signals)")
  dd[, PVfacet := factor(levs[match(PVgroup, nlab$PVgroup)], levels = levs)]

  p <- ggplot(dd, aes(x = period, y = mean, color = series, group = series)) +
    geom_hline(yintercept = 100, color = "dimgrey") +
    geom_hline(yintercept = 0) +
    geom_line(position = position_dodge(width = 0.35), size = 1) +
    geom_pointrange(aes(ymin = lo, ymax = hi),
                    position = position_dodge(width = 0.35), size = 0.9) +
    facet_wrap(~ PVfacet) +
    scale_color_manual(values = colors) +
    labs(x = NULL, y = "Relative to In-Sample (=100)", color = NULL,
         title = paste0("Return-on-valuation coefficient vs L/S return (",
                        spec_label, " spec)")) +
    theme_light(base_size = 18) +
    theme(legend.position = "bottom",
          plot.title = element_text(size = 15),
          panel.grid.minor = element_blank())

  ggsave(outfile, p, width = 11, height = 6)
}

make_fig("Narrow", "../Results/Fig_ValBeta_PeriodMeans_Narrow.pdf")
make_fig("Broad",  "../Results/Fig_ValBeta_PeriodMeans_Broad.pdf")

# Mechanism figure (PV narrow only): b, spread, L/S, b*spread ----------------
mech <- list()
for (p in levels(vb_periods$period)) {
  sub <- vb_periods[pv_narrow == TRUE & period == p]
  for (series in c("FM coefficient", "Valuation spread",
                   "Coef x spread", "L/S return")) {
    yv <- c("FM coefficient" = "b_norm", "Valuation spread" = "sp_norm",
            "Coef x spread" = "bxs_norm", "L/S return" = "ls_norm")[series]
    r <- clustered_mean_se(sub, yv)
    mech[[length(mech) + 1]] <- data.table(period = p, series = series,
                                           mean = r$m, se = r$se)
  }
}
mech <- rbindlist(mech)
mech[, `:=`(lo = mean - 1.96 * se, hi = mean + 1.96 * se)]
mech[, period := factor(period, levels = c("In-sample", "Post 0-5y", "Post 5y+"))]
mech[, series := factor(series, levels = c("FM coefficient", "Valuation spread",
                                           "Coef x spread", "L/S return"))]
fwrite(mech, "../Results/ValBeta_Mechanism_Narrow.csv")

p2 <- ggplot(mech, aes(x = period, y = mean, color = series, group = series)) +
  geom_hline(yintercept = 100, color = "dimgrey") +
  geom_hline(yintercept = 0) +
  geom_line(position = position_dodge(width = 0.4), size = 1) +
  geom_pointrange(aes(ymin = lo, ymax = hi),
                  position = position_dodge(width = 0.4), size = 0.8) +
  labs(x = NULL, y = "Relative to In-Sample (=100)", color = NULL,
       title = "Mechanism: coefficient vs spread vs portfolio return (PV narrow)") +
  theme_light(base_size = 18) +
  theme(legend.position = "bottom",
        plot.title = element_text(size = 15),
        panel.grid.minor = element_blank())
ggsave("../Results/Fig_ValBeta_Mechanism_Narrow.pdf", p2, width = 11, height = 6)

# Per-signal table ------------------------------------------------------------
fwrite(keep_sig[order(-pv_narrow, signalname)],
       "../Results/ValBeta_PerSignal.csv")

cat("\nPer-signal OOS/IS coefficient ratio, medians:\n")
print(keep_sig[, .(median_ratio = median(ratio), mean_ratio = mean(ratio),
                   N = .N), by = pv_narrow])

cat("\nPer-signal stability z (unit-free; z < -1.96 = significant decay):\n")
print(keep_sig[, .(mean_z = mean(z_stab), median_z = median(z_stab),
                   frac_sig_decay = mean(z_stab < -1.96), N = .N),
               by = pv_narrow])
cat("Wilcoxon PV vs non-PV (z):",
    round(wilcox.test(keep_sig[pv_narrow == TRUE, z_stab],
                      keep_sig[pv_narrow == FALSE, z_stab])$p.value, 3), "\n")
cat("Restricted t_IS > 2:\n")
print(keep_sig[b_is_t > 2, .(mean_z = mean(z_stab), median_z = median(z_stab),
                             frac_sig_decay = mean(z_stab < -1.96), N = .N),
               by = pv_narrow])

cat("\nDone. Wrote ValBeta_* tables and Fig_ValBeta_* figures to ../Results/\n")
