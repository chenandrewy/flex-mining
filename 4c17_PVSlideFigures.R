# Consolidated figure builder for the PV slide deck (BROAD spec, consistent
# y-limits). Produces, off a single corr-filtered DM build:
#   (1) Fig_PVslide_PeriodMeans   - headline: mean return by period, pub vs DM, PV vs non-PV
#   (2) Fig_PVslide_DM_PV / _NonPV - corr-filtered DM decay curves w/ shaded SE
#   (3) Fig_PVslide_PVvsNonPV      - published-only, PV vs non-PV decay
# All return/decay panels share y-limits YL..YH so slides are visually consistent.

rm(list = ls())
source('0_Environment.R')

# ---- consistent y-limits for all return/decay panels ----
YL <- 0; YH <- 150
XL <- -360; XH <- 300
fontsizeall <- 28; linesizeall <- 1.5
ylaball <- 'Trailing 5-Year Return (bps pm)'
leglabels <- c("Published (and Peer Reviewed)", "Data-Mined (|t|>2.0), corr-filtered", "N/A")

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType, topT = globalSettings$topT)

# PV flags
czpv <- fread('DataInput/SignalsTheoryChecked_withPV.csv') %>%
  transmute(pubname = signalname,
            pv_broad = toupper(trimws(as.character(pv_broad))) == 'TRUE') %>%
  filter(pubname %in% inclSignals)

# ---- corr-filtered DM means (as in 4c6/4c15) ----
corr_threshold <- 0.10
plotdat0 <- readRDS("../Data/Processed/plotdat0.RDS")
mf <- plotdat0$comp_matched %>% mutate(cor_aligned = cor * sign(rbar)) %>%
  filter(cor_aligned <= corr_threshold) %>%
  transmute(pubname, sweight, dmname, sign = sign(rbar), rbar) %>% setDT()
DMname  <- paste0('../Data/Processed/', globalSettings$dataVersion, ' LongShort.RData')
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list
dm_rets <- dm_rets %>% left_join(dm_info %>% select(portid, sweight), by = "portid") %>%
  transmute(sweight, dmname = signalid, yearm, ret) %>% setDT()
rm(dm_info)
dm_rets <- dm_rets[unique(mf[, .(sweight, dmname)]), on = c("sweight", "dmname"), nomatch = 0]
dmPanel <- mf[dm_rets, on = c("sweight", "dmname"), allow.cartesian = TRUE, nomatch = 0]
rm(dm_rets)
dmPanel[, `:=`(ret_scaled = ret * sign / abs(rbar) * 100, calendarDate = yearm)]
dm_means_excl <- dmPanel[, .(matchRet_excl = mean(ret_scaled, na.rm = TRUE)), by = .(pubname, calendarDate)]
rm(dmPanel, mf, plotdat0); gc()

rp0 <- readRDS("../Data/Processed/ret_for_plot0.RDS") %>% setDT()
rp0 <- merge(rp0, dm_means_excl, by = c("pubname", "calendarDate"), all.x = TRUE)
rp0 <- merge(rp0, czpv, by = "pubname", all.x = TRUE)
rp0 <- rp0[!is.na(matchRet_excl) & !is.na(pv_broad)]
rp0[, matchRet := matchRet_excl]

# ============================================================================
# (1) HEADLINE: period means, clustered 95% CI
# ============================================================================
rp0[, period := factor(fcase(eventDate < 0, "In-sample",
                             eventDate >= 0 & eventDate < 60, "Post 0-5y",
                             eventDate >= 60, "Post 5y+"),
                       levels = c("In-sample", "Post 0-5y", "Post 5y+"))]
cms <- function(d, yv) {
  d <- d[is.finite(d[[yv]])]
  if (nrow(d) == 0 || uniqueN(d$pubname) < 2 || uniqueN(d$calendarDate) < 2) return(c(NA, NA))
  m <- lm(reformulate("1", yv), data = d)
  se <- tryCatch(sqrt(sandwich::vcovCL(m, cluster = ~calendarDate + pubname)[1, 1]), error = function(e) NA_real_)
  c(unname(coef(m)[1]), se)
}
rows <- list()
for (g in c("Present-value", "Non-PV")) for (p in levels(rp0$period)) {
  sub <- rp0[fifelse(pv_broad, "Present-value", "Non-PV") == g & period == p]
  for (s in c("Published", "Data-mined")) {
    r <- cms(sub, if (s == "Published") "ret" else "matchRet")
    rows[[length(rows)+1]] <- data.table(PVgroup = g, period = p, series = s,
      n = uniqueN(sub$pubname), mean = r[1], se = r[2])
  }
}
summ <- rbindlist(rows)
summ[, `:=`(lo = mean - 1.96*se, hi = mean + 1.96*se)]
summ[, period := factor(period, levels = c("In-sample","Post 0-5y","Post 5y+"))]
nlab <- summ[, .(n = max(n)), by = PVgroup]
summ[, PVfacet := factor(paste0(PVgroup, " (", n, " signals)"),
                         levels = paste0(nlab$PVgroup, " (", nlab$n, " signals)"))]
pHead <- ggplot(summ, aes(period, mean, color = series, group = series)) +
  geom_hline(yintercept = 100, color = "dimgrey") + geom_hline(yintercept = 0) +
  geom_line(position = position_dodge(0.35), size = 1) +
  geom_pointrange(aes(ymin = lo, ymax = hi), position = position_dodge(0.35), size = 0.9) +
  facet_wrap(~PVfacet) + scale_color_manual(values = colors) +
  coord_cartesian(ylim = c(YL, 155)) +
  labs(x = NULL, y = ylaball, color = NULL) +
  theme_light(base_size = 18) +
  theme(legend.position = "bottom", panel.grid.minor = element_blank())
ggsave("../Results/Fig_PVslide_PeriodMeans.pdf", pHead, width = 11, height = 6)

# ============================================================================
# (2) corr-filtered DM decay curves w/ shaded SE (consistent YL..YH)
# ============================================================================
rp0df <- as.data.frame(rp0)
dm_fig <- function(sub, outfile, title) {
  tb <- '../Results/tmpPVs'; ts <- gsub('[^A-Za-z0-9]', '', title)
  p <- ReturnPlotsWithDM_std_errors_indicators(dt = sub, basepath = tb, suffix = ts,
        rollmonths = 60, colors = colors, yl = YL, yh = YH, xl = XL, xh = XH,
        legendlabels = leglabels, legendpos = c(35,20)/100, fontsize = fontsizeall,
        yaxislab = ylaball, linesize = linesizeall)
  (p + labs(title = title) +
     theme(legend.background = element_rect(fill="white", color="black", size=0.3),
           legend.position = c(44,18)/100, legend.spacing.y = unit(0.2,"cm"),
           plot.title = element_text(size = fontsizeall*0.7)) +
     guides(color = guide_legend(byrow = TRUE))) %>%
    ggsave(filename = outfile, width = 10, height = 8)
  suppressWarnings(file.remove(paste0(tb,'_',ts,'.pdf')))
}
np <- length(unique(rp0df$pubname[rp0df$pv_broad]))
nn <- length(unique(rp0df$pubname[!rp0df$pv_broad]))
dm_fig(rp0df[rp0df$pv_broad, ], "../Results/Fig_PVslide_DM_PV.pdf",
       sprintf("Present-value signals (%d signals)", np))
dm_fig(rp0df[!rp0df$pv_broad, ], "../Results/Fig_PVslide_DM_NonPV.pdf",
       sprintf("Non-present-value signals (%d signals)", nn))

# ============================================================================
# (3) Published-only, PV vs non-PV (consistent YL..YH)
# ============================================================================
czret <- readRDS('../Data/Processed/czret_keeponly.RDS') %>%
  left_join(czpv, by = c('signalname' = 'pubname')) %>%
  mutate(ret = ret/rbar*100) %>%
  filter(signalname %in% inclSignals, !is.na(pv_broad)) %>%
  mutate(catID = ifelse(pv_broad, 'PV', 'non-PV'))
prep <- czret %>% group_by(catID) %>% summarise(n = n_distinct(signalname), .groups='drop')
plotme <- czret %>% group_by(catID, eventDate) %>% summarise(rbar = mean(ret), .groups='drop_last') %>%
  arrange(catID, eventDate) %>%
  mutate(roll = zoo::rollmean(rbar, 60, fill = NA, align='right')) %>% ungroup() %>%
  mutate(catID = factor(catID, levels = c('PV','non-PV'),
    labels = paste0(c('PV','non-PV'), ' (', prep$n[match(c('PV','non-PV'), prep$catID)], ' signals)')))
pPV <- ggplot(plotme, aes(eventDate, roll, color = catID, linetype = catID)) +
  geom_line(size = 1.1) + scale_color_manual(values = colors) +
  scale_linetype_manual(values = c('solid','dashed')) +
  geom_vline(xintercept = 0) + geom_hline(yintercept = 100, color='dimgrey') + geom_hline(yintercept = 0) +
  coord_cartesian(xlim = c(XL, XH), ylim = c(YL, YH)) +
  scale_x_continuous(breaks = seq(-360,360,60)) + scale_y_continuous(breaks = seq(0,150,25)) +
  labs(x = 'Months Since Original Sample Ended', y = ylaball, color='', linetype='') +
  theme_light(base_size = fontsizeall) +
  theme(legend.position = c(30,20)/100, legend.background = element_rect(fill='transparent'))
ggsave("../Results/Fig_PVslide_PVvsNonPV.pdf", pPV, width = 10, height = 8)

cat("\nDone. Wrote Fig_PVslide_*.pdf (consistent y-limits", YL, "..", YH, ") to ../Results/\n")
