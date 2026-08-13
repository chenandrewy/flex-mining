# Robust alternative to the noisy PV decay curve: period-MEAN returns with
# clustered 95% CIs, for Published vs corr-filtered Data-Mined, split by
# PRESENT-VALUE (PV) vs non-PV published signals (both specs).
#
# The event-time decay curve is jumpy for the PV subset (N=19-27), but the
# story is a clean set of period means. This is the "mean return by PV or not"
# summary. DM benchmark is the corr-filtered (|cor|<=0.10) one, as in 4c6/4c15.

# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType,
                                  topT = globalSettings$topT)

# Correlation-filtered DM means (as in 4c6/4c15) ------------------------------
corr_threshold <- 0.10
plotdat0 <- readRDS("../Data/Processed/plotdat0.RDS")
matchinfo_filtered <- plotdat0$comp_matched %>%
  mutate(cor_aligned = cor * sign(rbar)) %>%
  filter(cor_aligned <= corr_threshold) %>%
  transmute(pubname, sweight, dmname, sign = sign(rbar), rbar) %>% setDT()
DMname  <- paste0('../Data/Processed/', globalSettings$dataVersion, ' LongShort.RData')
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list
dm_rets <- dm_rets %>%
  left_join(dm_info %>% select(portid, sweight), by = "portid") %>%
  transmute(sweight, dmname = signalid, yearm, ret) %>% setDT()
rm(dm_info)
dm_rets <- dm_rets[unique(matchinfo_filtered[, .(sweight, dmname)]),
                   on = c("sweight", "dmname"), nomatch = 0]
dmPanel <- matchinfo_filtered[dm_rets, on = c("sweight", "dmname"),
                              allow.cartesian = TRUE, nomatch = 0]
rm(dm_rets)
dmPanel[, `:=`(ret_scaled = ret * sign / abs(rbar) * 100, calendarDate = yearm)]
dm_means_excl <- dmPanel[, .(matchRet_excl = mean(ret_scaled, na.rm = TRUE)),
                         by = .(pubname, calendarDate)]
rm(dmPanel, matchinfo_filtered, plotdat0); gc()

# PV flags + assemble ---------------------------------------------------------
czpv <- fread('DataInput/SignalsTheoryChecked_withPV.csv') %>%
  transmute(pubname = signalname,
            pv_narrow = toupper(trimws(as.character(pv_narrow))) == 'TRUE',
            pv_broad  = toupper(trimws(as.character(pv_broad)))  == 'TRUE') %>%
  filter(pubname %in% inclSignals)

rp0 <- readRDS("../Data/Processed/ret_for_plot0.RDS") %>% setDT()
rp0 <- merge(rp0, dm_means_excl, by = c("pubname", "calendarDate"), all.x = TRUE)
rp0 <- merge(rp0, czpv,          by = "pubname",                    all.x = TRUE)
rp0 <- rp0[!is.na(matchRet_excl) & !is.na(pv_narrow) & !is.na(pv_broad)]

# Period bins (event time; trailing-5yr scale, normalized to 100 in-sample)
rp0[, period := fcase(
  eventDate <  0,                 "In-sample",
  eventDate >= 0 & eventDate < 60,"Post 0-5y",
  eventDate >= 60,                "Post 5y+"
)]
rp0[, period := factor(period, levels = c("In-sample", "Post 0-5y", "Post 5y+"))]

# Clustered mean + SE (cluster by calendarDate + pubname) --------------------
clustered_mean_se <- function(d, yvar) {
  d <- d[is.finite(d[[yvar]])]
  if (nrow(d) == 0 || uniqueN(d$pubname) < 2 || uniqueN(d$calendarDate) < 2)
    return(list(m = NA_real_, se = NA_real_))
  m  <- lm(reformulate("1", yvar), data = d)
  se <- tryCatch(sqrt(sandwich::vcovCL(m, cluster = ~ calendarDate + pubname)[1, 1]),
                 error = function(e) NA_real_)
  list(m = unname(coef(m)[1]), se = se)
}

build_summary <- function(flagcol, spec_label) {
  d <- copy(rp0)
  d[, PVgroup := fifelse(get(flagcol), "Present-value", "Non-PV")]
  out <- list()
  for (g in unique(d$PVgroup)) for (p in levels(d$period)) {
    sub <- d[PVgroup == g & period == p]
    for (series in c("Published", "Data-mined")) {
      yv <- if (series == "Published") "ret" else "matchRet_excl"
      r  <- clustered_mean_se(sub, yv)
      out[[length(out) + 1]] <- data.table(spec = spec_label, PVgroup = g,
        period = p, series = series, n_sig = uniqueN(sub$pubname),
        mean = r$m, se = r$se)
    }
  }
  rbindlist(out)
}

summ <- rbindlist(list(build_summary("pv_narrow", "Narrow"),
                       build_summary("pv_broad",  "Broad")))
summ[, `:=`(lo = mean - 1.96 * se, hi = mean + 1.96 * se)]
summ[, period := factor(period, levels = c("In-sample", "Post 0-5y", "Post 5y+"))]
summ[, PVgroup := factor(PVgroup, levels = c("Present-value", "Non-PV"))]

cat("Period means (corr-filtered DM), clustered 95% CI:\n")
print(summ[order(spec, PVgroup, period, series),
           .(spec, PVgroup, period, series, n_sig,
             mean = round(mean, 1), ci = paste0("[", round(lo), ",", round(hi), "]"))])

# Point-range figure per spec -------------------------------------------------
make_fig <- function(spec_label, outfile) {
  dd <- summ[spec == spec_label]
  nlab <- dd[, .(n = max(n_sig)), by = PVgroup]
  levs <- paste0(nlab$PVgroup, " (", nlab$n, " signals)")
  dd[, PVfacet := factor(paste0(PVgroup, " (", n_sig, " signals)"),
                         levels = levs)]

  p <- ggplot(dd, aes(x = period, y = mean, color = series, group = series)) +
    geom_hline(yintercept = 100, color = "dimgrey") +
    geom_hline(yintercept = 0) +
    geom_line(position = position_dodge(width = 0.35), size = 1) +
    geom_pointrange(aes(ymin = lo, ymax = hi),
                    position = position_dodge(width = 0.35), size = 0.9) +
    facet_wrap(~ PVfacet) +
    scale_color_manual(values = colors) +
    labs(x = NULL, y = "Trailing 5-Year Return (bps pm)", color = NULL,
         title = paste0("Mean return by present-value vs not (", spec_label, " spec)")) +
    theme_light(base_size = 18) +
    theme(legend.position = "bottom",
          plot.title = element_text(size = 15),
          panel.grid.minor = element_blank())

  ggsave(outfile, p, width = 11, height = 6)
}

make_fig("Broad",  "../Results/Fig_PVPeriodMeans_Broad.pdf")
make_fig("Narrow", "../Results/Fig_PVPeriodMeans_Narrow.pdf")

cat("\nDone. Wrote Fig_PVPeriodMeans_{Broad,Narrow}.pdf to ../Results/\n")
