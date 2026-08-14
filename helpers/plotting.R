# Plotting helpers: return-decay figures (ggplot).
#
# Sourced by 0_Environment.R after packages and config.R. These functions
# rely on objects from the sourcing environment (e.g. globalSettings and
# chapter-local data frames); do not source this file in isolation.



# Create a plot by category without data-mining benchmark
ReturnPlotsNoDM = function(dt, suffix = '', rollmonths = 60, filetype = '.pdf',
                           xl = -360, xh = 240, yl = -10, yh = 130, 
                           basepath = NA_character_,
                           fontsize = 18,
                           legpos = c(85,85)/100) {
  
  #' @param dt Table with four columns (signalname, ret, eventDate, catID)
  #' @param suffix String to attach to saved pdf figure 
  #' @param rollmonths Number of months over which moving average is computed
  
  # Prep legend
  prepLegend = dt %>% 
    group_by(catID) %>% 
    summarise(nSignals = n_distinct(signalname))
  
  # Plot    
  plotme = dt %>% 
    group_by(catID, eventDate) %>% 
    summarise(rbar = mean(ret)) %>% 
    arrange(catID, eventDate) %>% 
    mutate(
      roll_rbar = zoo::rollmean(rbar, k = rollmonths, fill = NA, align = 'right')
    ) %>% 
    mutate(catID = factor(catID, levels = c('risk', 'mispricing', 'agnostic'), 
                          labels = c(paste0('Risk (', prepLegend$nSignals[prepLegend$catID == 'risk'], ' signals)'),
                                     paste0('Mispricing (', prepLegend$nSignals[prepLegend$catID == 'mispricing'], ' signals)'), 
                                     paste0('Agnostic (', prepLegend$nSignals[prepLegend$catID == 'agnostic'], ' signals)')))) 
  
  catfac = plotme$catID %>% unique() %>% sort()
  
  print( plotme %>% 
           ggplot(aes(x = eventDate, y = roll_rbar, color = catID, linetype = catID)) +
           geom_line(size = 1.1) +
           # scale_color_brewer(palette = 'Dark2') + 
           scale_color_manual(values = colors, breaks = catfac) +
           scale_linetype_manual(values = c('solid','longdash','dashed'), breaks = catfac) +
           geom_vline(xintercept = 0) +
           coord_cartesian(
                xlim = c(xl, xh), ylim = c(yl, yh)
           ) +
           scale_y_continuous(breaks = seq(-200,180,25)) +
           scale_x_continuous(breaks = seq(-360,360,60)) +  
           geom_hline(yintercept = 100, color = 'dimgrey') +
           # annotate(geom="text",
           #          label='In-Sample Mean', x=16, y=95, vjust=-1,
           #          family = "Palatino Linotype", color = 'dimgrey'
           # )  +
           geom_hline(yintercept = 0) +
           ylab('Trailing 5-Year Return (bps p.m.)') +
           xlab('Months Since Original Sample Ended') +
           labs(color = '', linetype = '') +
           theme_light(base_size = fontsize) +
           theme(
             legend.position = legpos
             , legend.spacing.y = unit(0, units = 'cm')
             #    , legend.box.background = element_rect(fill='transparent')
             ,legend.background = element_rect(fill='transparent')
           ) 
  )
  
  ggsave(paste0(basepath, '_', suffix, filetype), width = 10, height = 8)
  
}



# Create a plot by category without data-mining benchmark for CAPM returns
ReturnPlotsNoDMAlpha = function(dt, suffix = '', rollmonths = 60, filetype = '.pdf',
                                xl = -360, xh = 240, yl = -10, yh = 130, 
                                basepath = NA_character_) {
  
  #' @param dt Table with four columns (signalname, ret, eventDate, catID)
  #' @param suffix String to attach to saved pdf figure 
  #' @param rollmonths Number of months over which moving average is computed
  
  # Prep legend
  prepLegend = dt %>% 
    group_by(catID) %>% 
    summarise(nSignals = n_distinct(signalname))
  
  # Plot    
  plotme = dt %>%
    group_by(catID, eventDate) %>% 
    summarise(abar = mean(alpha, na.rm = TRUE)) %>% 
    arrange(catID, eventDate) %>% 
    mutate(
      roll_abar = zoo::rollmean(abar, k = rollmonths, fill = NA, align = 'right')
    ) %>% 
    mutate(catID = factor(catID, levels = c('risk', 'mispricing', 'agnostic'), 
                          labels = c(paste0('Risk (', prepLegend$nSignals[prepLegend$catID == 'risk'], ' signals)'),
                                     paste0('Mispricing (', prepLegend$nSignals[prepLegend$catID == 'mispricing'], ' signals)'), 
                                     paste0('Agnostic (', prepLegend$nSignals[prepLegend$catID == 'agnostic'], ' signals)')))) 
  
  catfac = plotme$catID %>% unique() %>% sort()
  
  print( plotme %>% 
           ggplot(aes(x = eventDate, y = roll_abar, color = catID, linetype = catID)) +
           geom_line(size = 1.1) +
           # scale_color_brewer(palette = 'Dark2') + 
           scale_color_manual(values = colors, breaks = catfac) +
           scale_linetype_manual(values = c('solid','longdash','dashed'), breaks = catfac) +
           geom_vline(xintercept = 0) +
           coord_cartesian(
             xlim = c(xl, xh), ylim = c(yl, yh)
           ) +
           scale_y_continuous(breaks = seq(-200,180,25)) +
           scale_x_continuous(breaks = seq(-360,360,60)) +  
           geom_hline(yintercept = 100, color = 'dimgrey') +
           # annotate(geom="text",
           #          label='In-Sample Mean', x=16, y=95, vjust=-1,
           #          family = "Palatino Linotype", color = 'dimgrey'
           # )  +
           geom_hline(yintercept = 0) +
           ylab('Trailing 5-Year Abnormal Return (bps p.m.)') +
           xlab('Months Since Original Sample Ended') +
           labs(color = '', linetype = '') +
           theme_light(base_size = 18) +
           theme(
             legend.position = c(85,85)/100
             , legend.spacing.y = unit(0, units = 'cm')
             #    , legend.box.background = element_rect(fill='transparent')
             ,legend.background = element_rect(fill='transparent')
           ) 
  )
  
  ggsave(paste0(basepath, '_', suffix, filetype), width = 10, height = 8)
  
}



# Create a plot that compares the average predictor return with the average data-mined return
ReturnPlotsWithDM = function(dt, suffix = '', rollmonths = 60, colors = NA,
                             xl = -360, xh = 240, yl = -10, yh = 130, fig.width = 10,
                             fig.height = 8, fontsize = 18, basepath = NA_character_,
                             labelmatch = FALSE, hideoos = FALSE,
                             legendlabels = c('Published','Matched data-mined','Alt data-mined'),
                             legendpos = c(80,85)/100,
                             yaxislab = 'Trailing 5-Year Mean Return (bps p.m.)',
                             filetype = '.pdf',
                             linesize = 1.1
                             ) {
  
  #' @param dt Table with columns (eventDate, ret, matchRet, matchRetAlt)
  #' @param suffix String to attach to saved pdf figure 
  #' @param rollmonths Number of months over which moving average is computed
  #' @param xl, xh, yl, yh Upper and lower limits for x and y axes  

  # check if you have matchRetAlt and adjust accordingly
  if (any(names(dt)=='matchRetAlt')){
    select_cols = c('eventDate','ret','matchRet','matchRetAlt')
  } else if (any(names(dt)=='matchRet')){
    select_cols = c('eventDate','ret','matchRet')
  } else {
    select_cols = c('eventDate','ret')
  }
    
  dt = dt %>% 
    select(all_of(select_cols))  %>% 
    gather(key = 'SignalType', value = 'return', -eventDate) %>% 
    group_by(SignalType, eventDate) %>% 
    summarise(rbar = mean(return), na.rm=TRUE) %>% 
    arrange(SignalType, eventDate) %>% 
    mutate(
      roll_rbar = zoo::rollmean(rbar, k = rollmonths, fill = NA, align = 'right')
    ) 
  
  if (hideoos==TRUE){
    dt = dt %>% 
      filter(!(SignalType == 'matchRet' & eventDate > 0))
  }
  
  printme = dt %>% 
      mutate(SignalType 
             = factor(SignalType, levels = c('ret', 'matchRet','matchRetAlt')
               , labels = legendlabels)) %>% 
      ggplot(aes(x = eventDate, y = roll_rbar, color = SignalType, linetype = SignalType)) +
      geom_line(size = linesize) +
      #  scale_color_grey() + 
      # scale_color_brewer(palette = 'Dark2') + 
      scale_color_manual(values = colors) + 
      scale_linetype_manual(values = c('solid', 'longdash','dashed')) +
      # scale_linetype(guide = 'none') +
      geom_vline(xintercept = 0) +
      coord_cartesian(
        xlim = c(xl, xh), ylim = c(yl, yh)
      ) +
      scale_y_continuous(breaks = seq(-200,180,25)) +
      scale_x_continuous(breaks = seq(-360,360,60)) +  
      geom_hline(yintercept = 100, color = 'dimgrey') +
      geom_hline(yintercept = 0) +
      ylab(yaxislab) +
      xlab('Months Since Original Sample Ended') +
      labs(color = '', linetype = '') +
      theme_light(base_size = fontsize) +
      theme(
        legend.position = legendpos
        , legend.spacing.y = unit(0.1, units = 'cm')
        , legend.background = element_rect(fill='transparent')
        , legend.key.width = unit(1.5, units = 'cm')
      ) 
  
  if (labelmatch == TRUE){
   printme = printme +
    annotate('text', x = -90, y = 12, fontface = 'italic'
             , label = '<- matching region'
             , color = 'grey40' , size = 5) +
    annotate('text', x =   70, y = 12, fontface = 'italic'
             , label = 'unmatched ->'
             , color = 'grey40' , size = 5)
  }
  
  # print(printme)
  
  ggsave(paste0(basepath, '_', suffix, filetype), width = fig.width, height = fig.height)

  return(printme)
  
}



ReturnPlotsWithDM4series <- function(dt, suffix = '', rollmonths = 60, colors = NA,
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
      legend.spacing.y = unit(0.15, units = 'cm'),
      legend.background = element_rect(fill = 'transparent'),
      legend.key.width = unit(2.5, units = 'cm'),
      legend.key.height = unit(1.5,"cm")
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


ReturnPlotsWithDM_std_errors_indicators = function(dt, suffix = '', rollmonths = 60, colors = NA,
                             xl = -360, xh = 240, yl = -10, yh = 130, fig.width = 10,
                             fig.height = 8, fontsize = 18, basepath = NA_character_,
                             labelmatch = FALSE, hideoos = FALSE,
                             legendlabels = c('Published','Matched data-mined','Alt data-mined'),
                             legendpos = c(80,85)/100,
                             yaxislab = 'Trailing 5-Year Mean Return (bps p.m.)',
                             filetype = '.pdf',
                             linesize = 1.1
) {
  # Check available columns and ensure calendarDate is included
  if (any(names(dt)=='matchRetAlt')){
    select_cols = c('eventDate','calendarDate','ret','matchRet','matchRetAlt','pubname')
  } else if (any(names(dt)=='matchRet')){
    select_cols = c('eventDate','calendarDate','ret','matchRet','pubname')
  } else {
    select_cols = c('eventDate','calendarDate','ret','pubname')
  }
  
  # Just add window indicators to original data
  dt = dt %>% 
    select(all_of(select_cols)) %>%
    mutate(
      nonoverlap_window = floor(eventDate/rollmonths)  # same for all pubnames at each eventDate
    )

  # First gather the returns into long format
  dt_long = dt %>%
    pivot_longer(
      cols = c("ret", "matchRet", if("matchRetAlt" %in% names(.)) "matchRetAlt"),
      names_to = "SignalType",
      values_to = "return"
    )
  
  get_clustered_se = function(data) {
      # Add checks with informative messages
      if (nrow(data) == 0) {
          warning("Empty data received")
          return(NA_real_)
      }
      if (length(unique(data$calendarDate)) < 2) {
          warning("Less than 2 unique calendarDates")
          return(NA_real_)
      }
      if (length(unique(data$pubname)) < 2) {
          # If only one pubname, use regular time series clustering
          print(c("only one pubname: ", unique(data$pubname), "nrow: ", nrow(data)))
          return(NA_real_)
      } else {
          # If multiple pubnames, use double clustering by calendar month instead of event date
          mod = lm(return ~ 1, data = data)
          se = tryCatch({
              sqrt(vcovCL(mod, cluster = ~calendarDate + pubname))[1,1]
          }, error = function(e) {
              warning("Error in vcovCL: ", e$message)
              return(NA_real_)
          })
      }
      
      if (is.nan(se)) {
          warning(sprintf("NaN SE produced: nobs=%d, n_dates=%d, n_pubnames=%d", 
                      nrow(data %>% filter(!is.na(return))), 
                      length(unique(data$calendarDate)), 
                      length(unique(data$pubname))))
          return(NA_real_)
      }
      
      return(se)
  }
  
  dt_plot = dt_long %>% 
      # First get mean return for each period
      group_by(SignalType, eventDate) %>% 
      summarise(
          period_mean = mean(return, na.rm=TRUE),
          .groups = 'drop'
      ) %>% 
      # Now get rolling means and compute SEs for non-overlapping windows
      group_by(SignalType) %>%
      arrange(eventDate) %>%
      mutate(
          roll_rbar = zoo::rollmean(period_mean, k = rollmonths, fill = NA, align = 'right'),
          window_end = eventDate,
          window_start = eventDate - rollmonths + 1,
          nonoverlap_window = floor(eventDate/rollmonths)  # window indicator
      ) %>%
      # Now compute SEs only for non-overlapping windows
      group_by(SignalType, nonoverlap_window) %>%
      summarise(
          roll_rbar = last(roll_rbar),  # take end of window value
          window_end = last(eventDate),
          window_start = window_end - rollmonths + 1,
          # NOTE: the filter below must compare against the *group's* SignalType.
          # Referencing first(SignalType) inside filter() resolves against
          # dt_long's own column (always the first series, 'ret'), which made
          # every series' SE come from the published series' rows. Capture the
          # group value outside the filter mask instead.
          se = {
              sig_grp = SignalType[1]
              window_data = dt_long %>%
                  filter(!is.na(return),
                        SignalType == sig_grp,
                        eventDate > window_start,
                        eventDate <= window_end)
              get_clustered_se(window_data)
          },
          unique_pubnames = {
              sig_grp = SignalType[1]
              dt_long %>%
                  filter(!is.na(return),
                        SignalType == sig_grp,
                        eventDate > window_start,
                        eventDate <= window_end) %>%
                  select(pubname) %>%
                  distinct() %>%
                  nrow()
          },
          unique_eventdates = {
              sig_grp = SignalType[1]
              dt_long %>%
                  filter(!is.na(return),
                        SignalType == sig_grp,
                        eventDate > window_start,
                        eventDate <= window_end) %>%
                  select(eventDate) %>%
                  distinct() %>%
                  nrow()
          },
          .groups = 'drop') %>%
      # Now join back to get SE for each event date
      select(SignalType, nonoverlap_window, se, unique_pubnames, unique_eventdates) %>%
      right_join(
          dt_long %>% 
              group_by(SignalType, eventDate) %>% 
              summarise(
                  period_mean = mean(return, na.rm=TRUE),
                  .groups = 'drop'
              ) %>% 
              group_by(SignalType) %>%
              arrange(eventDate) %>%
              mutate(
                  roll_rbar = zoo::rollmean(period_mean, k = rollmonths, fill = NA, align = 'right'),
                  nonoverlap_window = floor(eventDate/rollmonths)
              ),
          by = c("SignalType", "nonoverlap_window")
      ) %>%
      # Add confidence intervals
      mutate(
          upper = roll_rbar + 1 * se,
          lower = roll_rbar - 1 * se
      )

  # dt_plot %>% filter(!is.na(se) & !is.na(roll_rbar) & SignalType == 'ret') %>% head()
  # dt_plot %>% filter(!is.na(se) & !is.na(roll_rbar) & SignalType == 'ret') %>% tail()
  # Create plot
  printme = dt_plot %>% 
    mutate(SignalType = factor(SignalType, 
                              levels = c('ret', 'matchRet','matchRetAlt'),
                              labels = legendlabels)) %>% 
    ggplot(aes(x = eventDate, y = roll_rbar, color = SignalType, linetype = SignalType)) +
    # plot point est
    geom_line(size = linesize) +
    scale_color_manual(values = colors) + 
    scale_fill_manual(values = colors) +
    scale_linetype_manual(values = c('solid', 'longdash','dashed')) +
    # Add CI only for published signals
    geom_ribbon(
      data = . %>% filter(SignalType == legendlabels[1]),
      aes(ymin = lower, ymax = upper), 
      fill = colors[1],
      alpha = 0.1, 
      color = NA
    ) +    
    geom_vline(xintercept = 0) +
    coord_cartesian(xlim = c(xl, xh), ylim = c(yl, yh)) +
    scale_y_continuous(breaks = seq(-200,180,25)) +
    scale_x_continuous(breaks = seq(-360,360,60)) +  
    geom_hline(yintercept = c(0, 100), color = c('black', 'dimgrey')) +
    labs(x = 'Months Since Original Sample Ended',
         y = yaxislab,
         color = '', 
         linetype = '') +
    theme_light(base_size = fontsize) +
    theme(
      legend.position = legendpos,
      legend.spacing.y = unit(0.1, units = 'cm'),
      legend.background = element_rect(fill='transparent'),
      legend.key.width = unit(1.5, units = 'cm')
    ) +
    guides(fill = "none")
  
  if (labelmatch == TRUE){
    printme = printme +
      annotate('text', x = -90, y = 12, fontface = 'italic',
               label = '<- matching region',
               color = 'grey40' , size = 5) +
      annotate('text', x = 70, y = 12, fontface = 'italic',
               label = 'unmatched ->',
               color = 'grey40' , size = 5)
  }
  
  ggsave(paste0(basepath, '_', suffix, filetype), width = fig.width, height = fig.height)
  
  return(printme = printme)
}

