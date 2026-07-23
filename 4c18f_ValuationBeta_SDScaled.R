# Robustness for 4c18c: rescale FM slopes to RETURN PER 1-SD OF THE SIGNAL
# (b_t * sd_x,t), which is comparable across signals WITHOUT dividing by the
# noisy in-sample mean slope (no "IS = 100" ratio, no denominator selection).
# Report raw period means (% per month per 1 sd) and the OOS/IS ratio of
# GROUP-LEVEL means (ratio of averages, not average of ratios).
#
# Input:  ../Data/Processed/valbeta_monthly.RDS, czsum, PV classification
# Output: ../Results/ValBeta_SDScaled.csv

rm(list = ls())
source('0_Environment.R')

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType,
                                  topT = globalSettings$topT)

vb <- readRDS("../Data/Processed/valbeta_monthly.RDS")[!is.na(b)]
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>% filter(Keep)
czpv <- fread('DataInput/SignalsTheoryChecked_withPV.csv')[
  , .(signalname, pvn = toupper(trimws(as.character(pv_narrow))) == 'TRUE')]

ym2mm <- function(ym) {
  ym <- as.numeric(ym); yr <- floor(ym + 1e-6)
  as.integer(yr * 100 + round(12 * (ym - yr)) + 1)
}
info <- data.table(signalname = czsum$signalname,
                   s0 = ym2mm(czsum$sampstart), s1 = ym2mm(czsum$sampend))

vb <- merge(vb, info, by = "signalname")[signalname %in% inclSignals]
vb <- merge(vb, czpv, by = "signalname", all.x = TRUE)
vb[is.na(pvn), pvn := FALSE]
vb[, insamp := ret_yyyymm >= s0 & ret_yyyymm <= s1]

# same coefficient-sample screens as 4c18c (b_is>0 needed only for signing —
# here we sign by the IS slope's sign, not divide by its magnitude)
per <- vb[, .(n_is = sum(insamp), n_oos = sum(ret_yyyymm > s1),
              b_is = mean(b[insamp])), by = signalname]
keep <- per[n_is >= 24 & n_oos >= 12 & !is.na(b_is)]
vb <- merge(vb, keep[, .(signalname, sgn_is = sign(b_is))], by = "signalname")
cat("Signals:", uniqueN(vb$signalname), "\n")

# return per 1 sd of signal, signed by IS direction
vb[, b_sd := sgn_is * b * sd_x]

vb[, eventDate := (ret_yyyymm %/% 100) * 12 + ret_yyyymm %% 100 -
     ((s1 %/% 100) * 12 + s1 %% 100)]
vb[, period := fcase(eventDate <= 0, "In-sample",
                     eventDate > 0 & eventDate <= 60, "Post 0-5y",
                     eventDate > 60, "Post 5y+")]
vb <- vb[period != "In-sample" | insamp == TRUE]
vb[, period := factor(period, levels = c("In-sample", "Post 0-5y", "Post 5y+"))]

clustered_mean_se <- function(d, yvar = "b_sd") {
  d <- d[is.finite(d[[yvar]])]
  m  <- lm(reformulate("1", yvar), data = d)
  se <- tryCatch(sqrt(sandwich::vcovCL(m, cluster = ~ ret_yyyymm + signalname)[1, 1]),
                 error = function(e) NA_real_)
  list(m = unname(coef(m)[1]), se = se)
}

out <- list()
for (g in c(TRUE, FALSE)) for (p in levels(vb$period)) {
  sub <- vb[pvn == g & period == p]
  r <- clustered_mean_se(sub)
  out[[length(out) + 1]] <- data.table(
    PVgroup = ifelse(g, "Present-value", "Non-PV"), period = p,
    n_sig = uniqueN(sub$signalname), mean = r$m, se = r$se)
}
summ <- rbindlist(out)
summ[, `:=`(lo = mean - 1.96 * se, hi = mean + 1.96 * se)]

cat("\nReturn per 1-sd of signal (% pm), clustered 95% CI — NO IS normalization:\n")
print(summ[, .(PVgroup, period, n_sig, mean = round(mean, 3),
               ci = paste0("[", round(lo, 2), ",", round(hi, 2), "]"))])

wide <- dcast(summ, PVgroup ~ period, value.var = "mean")
wide[, `:=`(ret_0_5y = round(100 * `Post 0-5y` / `In-sample`),
            ret_5yplus = round(100 * `Post 5y+` / `In-sample`))]
cat("\nGroup-level retention (ratio of period means, IS = 100):\n")
print(wide[, .(PVgroup, ret_0_5y, ret_5yplus)])

fwrite(summ, "../Results/ValBeta_SDScaled.csv")
cat("\nDone.\n")
