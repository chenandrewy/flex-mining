# Companion cut to 4c18d: group the DM FM-coefficient panel by the MATCHED
# PUB's PV classification (paper-consistent, as in 4c14-4c16), plus the 2x2
# (DM's own mechanical PV-type x matched pub's PV class).
#
# Question: does the DM twin of a PV paper show stable coefficients like the
# paper does? (4c18d answered the different question: are mined valuation
# ratios intrinsically stable? -> no.)
#
# Inputs:  ../Data/Processed/valbeta_dm_monthly.RDS   (4c18d)
#          plotdat0.RDS (comp_matched), LongShort signal_list,
#          ValBeta_PerSignal.csv (pub sample), SignalsTheoryChecked_withPV.csv
# Outputs: ../Results/ValBetaDM_ByPubPV.csv, ValBetaDM_2x2.csv

# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')

corr_threshold <- 0.10
min_is_months  <- 24

dmb <- readRDS("../Data/Processed/valbeta_dm_monthly.RDS")

LS <- readRDS(paste0('../Data/Processed/', globalSettings$dataVersion,
                     ' LongShort.RData'))
sl <- as.data.table(LS$signal_list)
pv_dm_ids <- sl[signal_form == "v1/v2" & v2 == "me_datadate", signalid]

pd <- readRDS("../Data/Processed/plotdat0.RDS")
cm <- as.data.table(pd$comp_matched)
cm[, cor_aligned := cor * sign(rbar)]
cm <- cm[cor_aligned <= corr_threshold & sweight == "ew"]
cm <- cm[dmname %in% unique(dmb$signalid)]

pub_keep <- fread("../Results/ValBeta_PerSignal.csv")
cm <- cm[pubname %in% pub_keep$signalname]

czpv <- fread('DataInput/SignalsTheoryChecked_withPV.csv')[
  , .(pubname = signalname,
      pub_pv = toupper(trimws(as.character(pv_narrow))) == 'TRUE')]
cm <- merge(cm, czpv, by = "pubname", all.x = TRUE)
cm[is.na(pub_pv), pub_pv := FALSE]
cm[, dm_pv := dmname %in% pv_dm_ids]

cat("Pairs:", nrow(cm), "\n")
cat("\n2x2 pair counts (rows: pub PV, cols: DM PV-type):\n")
print(table(pub_pv = cm$pub_pv, dm_pv = cm$dm_pv))
cat("\nShare of matched DM that is PV-type, by pub group:\n")
print(cm[, .(share_dm_pvtype = round(mean(dm_pv), 3),
             n_pairs = .N, n_pubs = uniqueN(pubname)), by = pub_pv])

# Pair-level panel (identical construction to 4c18d) ---------------------------
ym2mm <- function(ym) {
  ym <- as.numeric(ym); yr <- floor(ym + 1e-6)
  as.integer(yr * 100 + round(12 * (ym - yr)) + 1)
}
cm[, `:=`(s0 = ym2mm(sampstart), s1 = ym2mm(sampend), sgn = sign(rbar))]

panel <- merge(cm[, .(pubname, dmname, pub_pv, dm_pv, s0, s1, sgn)],
               dmb, by.x = "dmname", by.y = "signalid",
               allow.cartesian = TRUE)
panel[, b_signed := sgn * b]
panel[, insamp := ret_yyyymm >= s0 & ret_yyyymm <= s1]
panel[, oos := ret_yyyymm > s1]

pair_is <- panel[insamp == TRUE,
                 .(b_is = mean(b_signed), n_is = .N), by = .(pubname, dmname)]
pair_oos_n <- panel[oos == TRUE, .(n_oos = .N), by = .(pubname, dmname)]
pair_is <- merge(pair_is, pair_oos_n, by = c("pubname", "dmname"))
keep_pairs <- pair_is[n_is >= min_is_months & n_oos >= 12 & b_is > 0]

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

clustered_mean_se <- function(d, yvar = "b_norm") {
  d <- d[is.finite(d[[yvar]])]
  if (nrow(d) == 0) return(list(m = NA_real_, se = NA_real_))
  m  <- lm(reformulate("1", yvar), data = d)
  se <- tryCatch(sqrt(sandwich::vcovCL(m, cluster = ~ ret_yyyymm + pubname)[1, 1]),
                 error = function(e) NA_real_)
  list(m = unname(coef(m)[1]), se = se)
}

# Cut 1: by matched pub's PV class (paper-consistent DM benchmark) -----------
out <- list()
for (g in c(TRUE, FALSE)) for (p in levels(panel$period)) {
  sub <- panel[pub_pv == g & period == p]
  r <- clustered_mean_se(sub)
  out[[length(out) + 1]] <- data.table(
    PubGroup = ifelse(g, "DM matched to PV pubs", "DM matched to non-PV pubs"),
    period = p, n_pubs = uniqueN(sub$pubname),
    n_pairs = uniqueN(sub[, .(pubname, dmname)]), mean = r$m, se = r$se)
}
s1_tbl <- rbindlist(out)
s1_tbl[, `:=`(lo = mean - 1.96 * se, hi = mean + 1.96 * se)]
cat("\n=== DM coefficient stability, grouped by MATCHED PUB PV class ===\n")
print(s1_tbl[, .(PubGroup, period, n_pubs, n_pairs, mean = round(mean, 1),
                 ci = paste0("[", round(lo), ",", round(hi), "]"))])
fwrite(s1_tbl, "../Results/ValBetaDM_ByPubPV.csv")

# Cut 2: 2x2 (pub PV x DM PV-type) --------------------------------------------
out <- list()
for (gp in c(TRUE, FALSE)) for (gd in c(TRUE, FALSE)) for (p in levels(panel$period)) {
  sub <- panel[pub_pv == gp & dm_pv == gd & period == p]
  if (nrow(sub) == 0) next
  r <- clustered_mean_se(sub)
  out[[length(out) + 1]] <- data.table(
    pub_pv = gp, dm_pv = gd, period = p,
    n_pairs = uniqueN(sub[, .(pubname, dmname)]), mean = r$m, se = r$se)
}
s2_tbl <- rbindlist(out)
cat("\n=== 2x2: pub PV x DM PV-type ===\n")
print(dcast(s2_tbl, pub_pv + dm_pv ~ period, value.var = "mean",
            fun.aggregate = function(x) round(mean(x), 1)))
fwrite(s2_tbl, "../Results/ValBetaDM_2x2.csv")

cat("\nDone.\n")
