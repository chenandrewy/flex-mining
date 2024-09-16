# Setup  ----------------------------------------------

load('../Data/tmp_4e_DMThemes.RData')
source('0_Environment.R')
library('doParallel')

# Declare functions ---------------------------------------------
ReturnPlotsWithDM <- function(dt, suffix = '', rollmonths = 60, colors = NA,
                              xl = -360, xh = 240, yl = -10, yh = 130, fig.width = 15,
                              fig.height = 12, fontsize = 18, basepath = NA_character_,
                              labelmatch = FALSE, hideoos = FALSE,
                              legendlabels = c('Published', 'Matched data-mined', 'Alt data-mined', 'New data-mined'),
                              legendpos = c(80, 85) / 100,
                              yaxislab = 'Trailing 5-Year Mean Return (bps p.m.)',
                              filetype = '.pdf',
                              linesize = 1.1) {
  
  #' @param dt Table with columns (eventDate, ret, matchRet, matchRetAlt, newRet)
  #' @param suffix String to attach to saved pdf figure 
  #' @param rollmonths Number of months over which moving average is computed
  #' @param xl, xh, yl, yh Upper and lower limits for x and y axes  
  
  # check if you have matchRetAlt and newRet, and adjust accordingly
  if (all(c('matchRetAlt', 'newRet') %in% names(dt))) {
    select_cols <- c('eventDate', 'ret', 'matchRet', 'matchRetAlt', 'newRet')
  } else if ('matchRetAlt' %in% names(dt)) {
    select_cols <- c('eventDate', 'ret', 'matchRet', 'matchRetAlt')
  } else {
    select_cols <- c('eventDate', 'ret', 'matchRet')
  }
  
  dt <- dt %>% 
    select(all_of(select_cols)) %>% 
    gather(key = 'SignalType', value = 'return', -eventDate) %>% 
    group_by(SignalType, eventDate) %>% 
    summarise(rbar = mean(return, na.rm = TRUE)) %>% 
    arrange(SignalType, eventDate) %>% 
    mutate(
      roll_rbar = zoo::rollmean(rbar, k = rollmonths, fill = NA, align = 'right')
    )
  
  if (hideoos == TRUE) {
    dt <- dt %>% 
      filter(!(SignalType == 'matchRet' & eventDate > 0))
  }
  
  printme <- dt %>% 
    mutate(SignalType = factor(SignalType, levels = select_cols[-1], labels = legendlabels)) %>% 
    ggplot(aes(x = eventDate, y = roll_rbar, color = SignalType, linetype = SignalType)) +
    geom_line(size = linesize) +
    scale_color_manual(values = colors) + 
    scale_linetype_manual(values = c('solid', 'longdash', 'dashed', 'dotdash')) +
    geom_vline(xintercept = 0) +
    coord_cartesian(
      xlim = c(xl, xh), ylim = c(yl, yh)
    ) +
    scale_y_continuous(breaks = seq(-200, 180, 25)) +
    scale_x_continuous(breaks = seq(-360, 360, 60)) +  
    geom_hline(yintercept = 100, color = 'dimgrey') +
    geom_hline(yintercept = 0) +
    ylab(yaxislab) +
    xlab('Months Since Original Sample Ended') +
    labs(color = '', linetype = '') +
    theme_light(base_size = fontsize) +
    theme(
      legend.position = legendpos,
      legend.spacing.y = unit(0.1, units = 'cm'),
      legend.background = element_rect(fill = 'transparent'),
      legend.key.width = unit(1.5, units = 'cm')
    )
  
  if (labelmatch == TRUE) {
    printme <- printme +
      annotate('text', x = -90, y = 12, fontface = 'italic',
               label = '<- matching region',
               color = 'grey40', size = 5) +
      annotate('text', x = 70, y = 12, fontface = 'italic',
               label = 'unmatched ->',
               color = 'grey40', size = 5)
  }
  
  ggsave(paste0(basepath, '_', suffix, filetype), width = fig.width, height = fig.height)
  
  return(printme)
  
}

# Plot decay --------------------------------------------------

## make event time returns -----------------------
print("Making spanned accounting event time returns")
print("Can take a few minutes...")
start_time <- Sys.time()
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dmpred$matched[spanned_ever==TRUE]
  , npubmax = plotdat$npubmax, 
  czsum = czsum, use_sign_info = plotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat$comp_matched <- dmpred$matched[spanned_ever==TRUE]
plotdat$comp_event_time <- dmpred$event_time

# Plot decay t > t op --------------------------------------------------

print("Making unspanned accounting event time returns t > t op")
print("Can take a few minutes...")
start_time <- Sys.time()
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dmpred$matched[spanned_ever==FALSE  & abs(tstat) > tstat_op]
  , npubmax = plotdat$npubmax, 
  czsum = czsum, use_sign_info = plotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat$unspan_matched_t_g <- dmpred$matched[spanned_ever==FALSE   & abs(tstat) > tstat_op]
plotdat$unspan_event_time_t_g <- dmpred$event_time

print("Making unspanned accounting event time returns t < t op")
print("Can take a few minutes...")
start_time <- Sys.time()
dmpred$event_time <- make_DM_event_returns(
  DMname = dmcomp$name, match_strats = dmpred$matched[spanned_ever==FALSE  & abs(tstat) <= tstat_op]
  , npubmax = plotdat$npubmax, 
  czsum = czsum, use_sign_info = plotdat$use_sign_info
)
stop_time <- Sys.time()
print(stop_time - start_time)

plotdat$unspan_matched_t_l <- dmpred$matched[spanned_ever==FALSE & abs(tstat) <= tstat_op]
plotdat$unspan_event_time_t_l <- dmpred$event_time

# join and reformat for plotting function
ret_for_plotting <- czret %>%
  transmute(pubname = signalname, eventDate, ret = ret_scaled) %>%
  left_join(
    plotdat$comp_event_time %>% transmute(pubname, eventDate, matchRet = dm_mean)
  ) %>%
  left_join(
    plotdat$unspan_event_time_t_g %>% transmute(pubname, eventDate, matchRetAlt = dm_mean)
  ) %>%
  left_join(
    plotdat$unspan_event_time_t_l %>% transmute(pubname, eventDate, newRet = dm_mean)
  ) %>%
  select(eventDate, ret, matchRet, matchRetAlt, newRet, pubname) %>%
  # keep only rows where both matchrets are observed
  filter(!is.na(matchRet) & !is.na(matchRetAlt))

## actually plot ----------------------------------------------
printme = ReturnPlotsWithDM(
  dt = ret_for_plotting, 
  basepath = "../Results/Fig_DM",
  suffix = 'unspan_match_t_g',
  rollmonths = 60,
  colors = c(colors, "#7E2F8E"),
  labelmatch = FALSE,
  yl = -0,
  yh = 125,
  legendlabels =
    c(
      paste0("Published"),
      paste0("Cor > 0.5 w/ a current or previous pub"),
      paste0("Cor < 0.5 w/ all current and previous pubs and t-stat > t pub"),
      paste0("Cor < 0.5 w/ all current and previous pubs and t-stat <= t pub")
    ),
  legendpos = c(35,20)/100,
  fontsize = fontsizeall,
  yaxislab = ylaball,
  linesize = linesizeall
)

