# DM side of the valuation-beta test (Campbell NBER AP 2024, slide 17):
# "How many of the ... data-mined anomalies are in this category?" and do
# data-mined PV-type signals show the same coefficient stability as published?
#
# PV-type DM signal (mechanical): form v1/v2 with v2 = me_datadate, i.e. a
# price-scaled level ratio -- the data-mined analog of the narrow PV class.
# Count: 237 of 29,315 DM signals (0.8%) vs 25 of 212 published (12%).
#
# Design: FM monthly cross-sections r_{i,t+1} = a_t + b_t x_{i,t} for
#   - all 237 PV-type DM signals
#   - a random sample of non-PV v1/v2 DM signals (same functional form)
# Event time / sign / matched-pub assignment exactly as the paper's DM
# benchmark: corr-filtered matched pairs (|cor_aligned| <= 0.10, as 4c15/4c16),
# signed by in-sample rbar, IS window = matched pub's sampstart..sampend.
#
# Inputs:  ../Data/tmpAllDat.fst, CZ-style-v8b LongShort.RData (signal_list),
#          plotdat0.RDS (comp_matched), ValBeta_PerSignal.csv (pub-side sample)
# Outputs: ../Results/ValBetaDM_* and Fig_ValBetaDM_*

# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')
library(fst)

n_nonpv_sample <- 1500
corr_threshold <- 0.10
min_obs_month  <- 50
min_is_months  <- 24

# DM signal universe --------------------------------------------------------
LS <- readRDS(paste0('../Data/Processed/', globalSettings$dataVersion,
                     ' LongShort.RData'))
sl <- as.data.table(LS$signal_list)
sl[, pv_type := signal_form == "v1/v2" & v2 == "me_datadate"]

pv_sigs <- sl[pv_type == TRUE]
set.seed(42)
nonpv_pool <- sl[signal_form == "v1/v2" & pv_type == FALSE]
nonpv_sigs <- nonpv_pool[sample(.N, min(.N, n_nonpv_sample))]
todo <- rbind(pv_sigs, nonpv_sigs)
cat("DM signals to process:", nrow(todo),
    "( PV-type:", nrow(pv_sigs), ", non-PV sample:", nrow(nonpv_sigs), ")\n")

# Load stock-month panel (only needed columns) -------------------------------
need_cols <- unique(c("permno", "ret_yearm", "ret", "signalyearm",
                      todo$v1, todo$v2))
cat("Loading tmpAllDat (", length(need_cols), "columns )...\n")
tic("load fst")
dat <- read_fst("../Data/tmpAllDat.fst", columns = need_cols, as.data.table = TRUE)
toc()
dat <- dat[!is.na(ret) & !is.na(signalyearm)]
dat[, ret_yyyymm := {
  yr <- floor(ret_yearm + 1e-6)
  as.integer(yr * 100 + round(12 * (ret_yearm - yr)) + 1)
}]
setkey(dat, ret_yyyymm)
cat("Panel rows:", nrow(dat), "\n")

# FM engine over DM signals ---------------------------------------------------
fm_one <- function(v1, v2) {
  x <- dat[[v1]] / dat[[v2]]
  ok <- is.finite(x)
  if (sum(ok) < 1000) return(NULL)
  d <- data.table(ret_yyyymm = dat$ret_yyyymm[ok], x = x[ok], ret = dat$ret[ok])
  # winsorize 1/99 within month, then monthly closed-form OLS slope
  d[, `:=`(x_lo = quantile(x, 0.01), x_hi = quantile(x, 0.99)), by = ret_yyyymm]
  d[, x := pmin(pmax(x, x_lo), x_hi)]
  d[, {
    n <- .N
    sdx <- sd(x)
    if (n >= min_obs_month && !is.na(sdx) && sdx > 0) {
      list(n = n, b = cov(x, ret) / var(x))
    } else list(n = n, b = NA_real_)
  }, by = ret_yyyymm]
}

tic("DM FM engine")
res <- vector("list", nrow(todo))
for (i in seq_len(nrow(todo))) {
  out <- tryCatch(fm_one(todo$v1[i], todo$v2[i]), error = function(e) NULL)
  if (!is.null(out)) {
    out[, signalid := todo$signalid[i]]
    res[[i]] <- out
  }
  if (i %% 100 == 0) cat("  ...", i, "/", nrow(todo), "\n")
}
toc()
dmb <- rbindlist(res)
dmb <- dmb[!is.na(b)]
cat("DM signals with FM series:", uniqueN(dmb$signalid), "\n")
saveRDS(dmb, "../Data/Processed/valbeta_dm_monthly.RDS")

# Matched pairs (paper's DM benchmark construction) ---------------------------
pd <- readRDS("../Data/Processed/plotdat0.RDS")
cm <- as.data.table(pd$comp_matched)
cm[, cor_aligned := cor * sign(rbar)]
cm <- cm[cor_aligned <= corr_threshold & sweight == "ew"]
cm <- cm[dmname %in% unique(dmb$signalid)]

# pub-side sample: signals used in 4c18c
pub_keep <- fread("../Results/ValBeta_PerSignal.csv")
cm <- cm[pubname %in% pub_keep$signalname]
cm[, dm_pv := dmname %in% pv_sigs$signalid]
cat("Matched pairs kept:", nrow(cm),
    "( PV-type DM:", cm[dm_pv == TRUE, .N], ")\n")
cat("Unique DM in pairs:", uniqueN(cm$dmname),
    "| unique pubs:", uniqueN(cm$pubname), "\n")

# Pair-level panel: sign by IS rbar, event time by pub's sample window --------
ym2mm <- function(ym) {
  ym <- as.numeric(ym); yr <- floor(ym + 1e-6)
  as.integer(yr * 100 + round(12 * (ym - yr)) + 1)
}
cm[, `:=`(s0 = ym2mm(sampstart), s1 = ym2mm(sampend), sgn = sign(rbar))]

pairs <- cm[, .(pubname, dmname, dm_pv, s0, s1, sgn)]
panel <- merge(pairs, dmb, by.x = "dmname", by.y = "signalid",
               allow.cartesian = TRUE)
panel[, b_signed := sgn * b]
panel[, insamp := ret_yyyymm >= s0 & ret_yyyymm <= s1]
panel[, oos := ret_yyyymm > s1]

# per-pair IS baseline
pair_is <- panel[insamp == TRUE,
                 .(b_is = mean(b_signed), n_is = .N), by = .(pubname, dmname)]
pair_oos_n <- panel[oos == TRUE, .(n_oos = .N), by = .(pubname, dmname)]
pair_is <- merge(pair_is, pair_oos_n, by = c("pubname", "dmname"))
keep_pairs <- pair_is[n_is >= min_is_months & n_oos >= 12 & b_is > 0]
cat("Pairs after IS-baseline screen (b_is > 0):", nrow(keep_pairs),
    "of", nrow(pair_is), "\n")

panel <- merge(panel, keep_pairs[, .(pubname, dmname, b_is)],
               by = c("pubname", "dmname"))
panel[, b_norm := 100 * b_signed / b_is]
qs <- quantile(panel$b_norm, c(0.01, 0.99), na.rm = TRUE)
panel[, b_norm := pmin(pmax(b_norm, qs[1]), qs[2])]

panel[, eventDate := (ret_yyyymm %/% 100) * 12 + ret_yyyymm %% 100 -
        ((s1 %/% 100) * 12 + s1 %% 100)]
panel[, period := fcase(
  eventDate <= 0,                  "In-sample",
  eventDate > 0 & eventDate <= 60, "Post 0-5y",
  eventDate > 60,                  "Post 5y+"
)]
panel <- panel[period != "In-sample" | insamp == TRUE]
panel[, period := factor(period, levels = c("In-sample", "Post 0-5y", "Post 5y+"))]

# Clustered period means (month + pub) ---------------------------------------
clustered_mean_se <- function(d, yvar) {
  d <- d[is.finite(d[[yvar]])]
  if (nrow(d) == 0) return(list(m = NA_real_, se = NA_real_))
  m  <- lm(reformulate("1", yvar), data = d)
  se <- tryCatch(sqrt(sandwich::vcovCL(m, cluster = ~ ret_yyyymm + pubname)[1, 1]),
                 error = function(e) NA_real_)
  list(m = unname(coef(m)[1]), se = se)
}

out <- list()
for (g in c(TRUE, FALSE)) for (p in levels(panel$period)) {
  sub <- panel[dm_pv == g & period == p]
  r <- clustered_mean_se(sub, "b_norm")
  out[[length(out) + 1]] <- data.table(
    DMgroup = ifelse(g, "PV-type DM", "Non-PV DM"), period = p,
    n_dm = uniqueN(sub$dmname), n_pairs = uniqueN(sub[, .(pubname, dmname)]),
    mean = r$m, se = r$se)
}
summ <- rbindlist(out)
summ[, `:=`(lo = mean - 1.96 * se, hi = mean + 1.96 * se)]

cat("\nDM FM-coefficient period means (pair-level, IS = 100):\n")
print(summ[, .(DMgroup, period, n_dm, n_pairs, mean = round(mean, 1),
               ci = paste0("[", round(lo), ",", round(hi), "]"))])
fwrite(summ, "../Results/ValBetaDM_PeriodMeans.csv")

# Pooled test ------------------------------------------------------------------
library(fixest)
fit <- feols(b_norm ~ oos * dm_pv, data = panel,
             cluster = ~ ret_yyyymm + pubname)
cat("\nPooled: b_norm ~ oos * PV-type (month+pub clustered)\n")
print(summary(fit))

# Figure: DM PV-type vs non-PV, alongside published values --------------------
pub_summ <- fread("../Results/ValBeta_PeriodMeans.csv")
pub_n <- pub_summ[spec == "Narrow" & series == "FM coefficient",
                  .(PVfacet = fifelse(PVgroup == "Present-value",
                                      "Present-value", "Non-PV"),
                    period, mean, lo, hi, Source = "Published")]
dm_n <- summ[, .(PVfacet = fifelse(DMgroup == "PV-type DM",
                                   "Present-value", "Non-PV"),
                 period, mean, lo, hi, Source = "Data-mined")]
both <- rbind(pub_n, dm_n)
both[, period := factor(period, levels = c("In-sample", "Post 0-5y", "Post 5y+"))]
both[, PVfacet := factor(PVfacet, levels = c("Present-value", "Non-PV"))]

p <- ggplot(both, aes(x = period, y = mean, color = Source, group = Source)) +
  geom_hline(yintercept = 100, color = "dimgrey") +
  geom_hline(yintercept = 0) +
  geom_line(position = position_dodge(width = 0.35), linewidth = 1) +
  geom_pointrange(aes(ymin = lo, ymax = hi),
                  position = position_dodge(width = 0.35), size = 0.9) +
  facet_wrap(~ PVfacet) +
  scale_color_manual(values = colors) +
  labs(x = NULL, y = "FM coefficient relative to In-Sample (=100)", color = NULL,
       title = "Return-on-valuation coefficient: published vs data-mined (narrow spec)") +
  theme_light(base_size = 18) +
  theme(legend.position = "bottom",
        plot.title = element_text(size = 15),
        panel.grid.minor = element_blank())
ggsave("../Results/Fig_ValBetaDM_PubVsDM.pdf", p, width = 11, height = 6)

cat("\nDone. Wrote ValBetaDM_PeriodMeans.csv and Fig_ValBetaDM_PubVsDM.pdf\n")
