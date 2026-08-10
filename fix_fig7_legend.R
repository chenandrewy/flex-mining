# Fix Figure 7 Panel (a) legend labels
# Run from flex-mining/flex-mining directory
# Takes ~10-15 minutes

library(parallel)
library(doParallel)
source("0_Environment.R")
load("../Data/9a_DMThemes_with_pca.RData")

# Regenerate correlation event time returns
cat("Making spanned event time returns...\n")
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dmpred$matched[spanned_ever==TRUE],
  npubmax = plotdat$npubmax, czsum = czsum, use_sign_info = plotdat$use_sign_info)
plotdat$comp_event_time_cor <- dmpred$event_time

cat("Making unspanned t > t_op...\n")
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dmpred$matched[spanned_ever==FALSE & abs(tstat) > tstat_op],
  npubmax = plotdat$npubmax, czsum = czsum, use_sign_info = plotdat$use_sign_info)
plotdat$unspan_event_time_t_g_cor <- dmpred$event_time

cat("Making unspanned t <= t_op...\n")
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dmpred$matched[spanned_ever==FALSE & abs(tstat) <= tstat_op],
  npubmax = plotdat$npubmax, czsum = czsum, use_sign_info = plotdat$use_sign_info)
plotdat$unspan_event_time_t_l_cor <- dmpred$event_time

# Build plotting data
ret_for_plotting_cor <- czret %>%
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
  left_join(plotdat$comp_event_time_cor %>% transmute(pubname, eventDate, matchRet = dm_mean)) %>%
  left_join(plotdat$unspan_event_time_t_g_cor %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)) %>%
  left_join(plotdat$unspan_event_time_t_l_cor %>% transmute(pubname, eventDate, newRet = dm_mean)) %>%
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

cat("Generating plot...\n")
ReturnPlotsWithDM4series(
  dt = ret_for_plotting_cor,
  basepath = "../Results/Fig_DM",
  suffix = "unspan_match_t_g_cor",
  rollmonths = 60,
  colors = c(colors, "#7E2F8E"),
  labelmatch = FALSE,
  yl = 0,
  yh = 125,
  legendlabels = c(
    "Published",
    "Cor > 0.50",
    "Cor < 0.50 \n& t-stat > t pub",
    "Cor < 0.50 \n& t-stat <= t pub"
  ),
  legendpos = c(25,30)/100,
  fontsize = 48,
  yaxislab = "Trailing 5-Year Return (bps pm)",
  linesize = 2
)

cat("\nDone! Now copy the figure to exhibits:\n")
cat("cp ../Results/Fig_DM_unspan_match_t_g_cor.pdf ../risk-vs-rfs-sub/latex-risk-vs/exhibits/\n")
