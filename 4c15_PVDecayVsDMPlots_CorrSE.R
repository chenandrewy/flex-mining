# Main-spec decay figure (Published vs Matched Data-Mined) with:
#   (1) the CORRELATION-FILTERED DM benchmark (in-sample |cor| <= 0.10), i.e. the
#       same DM construction as the paper's main MP table (4c6), and
#   (2) SHADED clustered standard-error bands (ReturnPlotsWithDM_std_errors_indicators),
# split by PRESENT-VALUE (PV) vs non-PV published signals, under both specs.
#
# The corr-filtered DM means (dm_means_excl) are built exactly as in
# 4c6_MPStyleDecayTables.R, then joined onto ret_for_plot0 on (pubname,
# calendarDate) to replace the raw matchRet. PV flags come from the NON-destructive
# merge DataInput/SignalsTheoryChecked_withPV.csv (original untouched).

# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')

inclSignals = restrictInclSignals(restrictType = globalSettings$restrictType,
                                  topT = globalSettings$topT)

# Correlation-filtered DM means (copied from 4c6_MPStyleDecayTables.R) ---------
corr_threshold <- 0.10

plotdat0 <- readRDS("../Data/Processed/plotdat0.RDS")
matchinfo_filtered <- plotdat0$comp_matched %>%
  mutate(cor_aligned = cor * sign(rbar)) %>%
  filter(cor_aligned <= corr_threshold) %>%
  transmute(pubname, sweight, dmname, sign = sign(rbar), rbar) %>%
  setDT()

DMname  <- paste0('../Data/Processed/', globalSettings$dataVersion, ' LongShort.RData')
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list
dm_rets <- dm_rets %>%
  left_join(dm_info %>% select(portid, sweight), by = "portid") %>%
  transmute(sweight, dmname = signalid, yearm, ret) %>%
  setDT()
rm(dm_info)

# keep only matched strategies, then cartesian-expand to (pubname, dmname, yearm)
dm_rets <- dm_rets[unique(matchinfo_filtered[, .(sweight, dmname)]),
                   on = c("sweight", "dmname"), nomatch = 0]
dmPanel <- matchinfo_filtered[dm_rets, on = c("sweight", "dmname"),
                              allow.cartesian = TRUE, nomatch = 0]
rm(dm_rets)

# scaled DM return (same scaling as 4c6), aggregate to (pubname, calendarDate)
dmPanel[, `:=`(ret_scaled = ret * sign / abs(rbar) * 100, calendarDate = yearm)]
dm_means_excl <- dmPanel[, .(matchRet_excl = mean(ret_scaled, na.rm = TRUE)),
                         by = .(pubname, calendarDate)]
rm(dmPanel, matchinfo_filtered, plotdat0); gc()

# PV flags --------------------------------------------------------------------
czpv <- fread('DataInput/SignalsTheoryChecked_withPV.csv') %>%
  transmute(pubname = signalname,
            pv_narrow = toupper(trimws(as.character(pv_narrow))) == 'TRUE',
            pv_broad  = toupper(trimws(as.character(pv_broad)))  == 'TRUE') %>%
  filter(pubname %in% inclSignals)

# Assemble plotting data: swap raw matchRet for the corr-filtered one ----------
rp0 <- readRDS("../Data/Processed/ret_for_plot0.RDS") %>% setDT()
rp0 <- merge(rp0, dm_means_excl, by = c("pubname", "calendarDate"), all.x = TRUE)
rp0 <- merge(rp0, czpv,          by = "pubname",                    all.x = TRUE)

# SANITY: in-sample (eventDate < 0) the raw and corr-filtered DM should both be
# ~100 (same normalization as published ret). If these diverge wildly the scaling
# is inconsistent and the figure is not trustworthy.
cat("In-sample scaling check (eventDate < 0):\n")
print(rp0[eventDate < 0, .(mean_ret          = mean(ret,           na.rm = TRUE),
                            mean_matchRet_raw = mean(matchRet,      na.rm = TRUE),
                            mean_matchRet_excl= mean(matchRet_excl, na.rm = TRUE))])

rp0[, matchRet := matchRet_excl]
rp0 <- rp0[!is.na(matchRet) & !is.na(pv_narrow) & !is.na(pv_broad)]

# Aesthetics copied from 4c2 main figure --------------------------------------
fontsizeall = 28
ylaball     = 'Trailing 5-Year Return (bps pm)'
linesizeall = 1.5
global_xl   = -360
global_xh   = 300
leglabels   = c("Published (and Peer Reviewed)",
                "Data-Mined (|t|>2.0), corr-filtered",
                "N/A")

# Wrapper: corr-filtered, shaded-SE main-spec figure for one PV subset ---------
pv_dm_se_figure <- function(dt_sub, outfile, title) {
  tmp_base   <- '../Results/temp_PVDMSE'
  tmp_suffix <- gsub('[^A-Za-z0-9]', '', title)

  p <- ReturnPlotsWithDM_std_errors_indicators(
    dt = dt_sub,
    basepath = tmp_base,
    suffix = tmp_suffix,
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl = 0, yh = 125,
    xl = global_xl, xh = global_xh,
    legendlabels = leglabels,
    legendpos = c(35, 20)/100,
    fontsize = fontsizeall,
    yaxislab = ylaball,
    linesize = linesizeall
  )

  (p + labs(title = title) +
      theme(legend.background = element_rect(fill = "white", color = "black", size = 0.3),
            legend.margin = margin(-1.0, 0.5, 0.5, 0.5, "cm"),
            legend.position = c(44, 18)/100,
            legend.spacing.y = unit(0.2, "cm"),
            plot.title = element_text(size = fontsizeall * 0.7)) +
      guides(color = guide_legend(byrow = TRUE))) %>%
    ggsave(filename = outfile, width = 10, height = 8)

  suppressWarnings(file.remove(paste0(tmp_base, '_', tmp_suffix, '.pdf')))
  invisible(NULL)
}

# Figures: 2 specs x {PV, non-PV} ---------------------------------------------
rp0df <- as.data.frame(rp0)
specs <- list(list(flag = 'pv_narrow', label = 'Narrow'),
              list(flag = 'pv_broad',  label = 'Broad'))

for (sp in specs) {
  pv_sub    <- rp0df[rp0df[[sp$flag]], ]
  nonpv_sub <- rp0df[!rp0df[[sp$flag]], ]
  n_pv    <- length(unique(pv_sub$pubname))
  n_nonpv <- length(unique(nonpv_sub$pubname))
  cat(sprintf('[%s] PV signals: %d | non-PV signals: %d\n', sp$label, n_pv, n_nonpv))

  pv_dm_se_figure(pv_sub,
                  sprintf('../Results/Fig_PVDecayDM_CorrSE_%sPV.pdf', sp$label),
                  sprintf('Present-value signals (%s spec, %d signals)', sp$label, n_pv))
  pv_dm_se_figure(nonpv_sub,
                  sprintf('../Results/Fig_PVDecayDM_CorrSE_%sNonPV.pdf', sp$label),
                  sprintf('Non-present-value signals (%s spec, %d signals)', sp$label, n_nonpv))
}

cat('\nDone. Wrote Fig_PVDecayDM_CorrSE_{Narrow,Broad}{PV,NonPV}.pdf to ../Results/\n')
