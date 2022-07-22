
# function for creating a list of possible variable combinations used in strategies
strategy_list = function(xvars, signalnum, scale_vars = NULL, rs) {
  
  #' @param xvars Unique names of variables used for creating strategies
  #' @param signalnum Number of signals to sample from full list
  #' @param scale_vars Scaling variables used in ratios (or NULL for unrestricted)
  #' @param rs Random seed
  
  set.seed(rs)
  
  # make list of all possible xused combinations
  tmp = expand.grid(v1 = xnames, v2 = xnames, stringsAsFactors = FALSE) %>% 
    filter(v1 != v2)
  
  # Deal with scaling variables if provided
  if (!is.null(scale_vars)) {
    tmp = tmp %>% 
      filter(v2 %in% scale_vars, !(v1 %in% scale_vars))
  }
  
  # Deal with duplicates (from including all combinations e.g. "ab" and "ba")
  if (is.null(scale_vars)) {
    tmp = tmp %>% 
      filter(v1 >v2)
  }
  
  # sample from full list
  if (signalnum == TRUE) {
    tmp = tmp
  } else if (nrow(tmp) > signalnum) {
    tmp = tmp %>% 
      sample_n(signalnum) 
  }
  
  return(tmp)
}


# function for turning xused into a signal
dataset_to_signal = function(form, dt, v1, v2){
  
  stopifnot("form must be one of ratio, ratioChange, ratioChangePct,
            levelChangePct, levelChangeScaled, levelsChangePct_Change, noise" = 
              form %in% c('ratio', 'ratioChange', 'ratioChangePct',
                          'levelChangePct', 'levelChangeScaled', 'levelsChangePct_Change', 'noise'))
  
  if (form == 'ratio'){
    
    dt[,'tmp'] = dt[, v1]/dt[, v2]
    return(dt %>% pull(tmp))
    
  } else if (form == 'ratioChange') {
    
    dt[,'tmp'] = dt[, v1]/dt[, v2]
    return(
      dt %>% 
        arrange(permno, ret_yearm) %>% 
        group_by(permno) %>%
        mutate(tmp2 = tmp - lag(tmp, 12)) %>% 
        ungroup() %>% 
        pull(tmp2)
    )
    
  } else if (form == 'ratioChangePct') {
    
    dt[,'tmp'] = dt[, v1]/dt[, v2]
    return(
      dt %>% 
        arrange(permno, ret_yearm) %>% 
        group_by(permno) %>%
        mutate(tmp2 = 100*(tmp - lag(tmp, 12))/lag(tmp, 12)) %>% 
        ungroup() %>% 
        pull(tmp2)
    )
    
  } else if (form == 'levelChangePct') {
    
    dt[,'tmp'] = dt[, v1]
    return(
      dt %>% 
        arrange(permno, ret_yearm) %>% 
        group_by(permno) %>%
        mutate(tmp2 = 100*(tmp - lag(tmp, 12))/lag(tmp, 12)) %>% 
        ungroup() %>% 
        pull(tmp2)
    )
    
  } else if (form == 'levelChangeScaled') {
    dt[,'tmp'] = dt[, v1]
    df[,'tmp2'] = df[, v2]
    return(
      dt %>% 
        arrange(permno, ret_yearm) %>% 
        group_by(permno) %>%
        mutate(tmp3 = (tmp - lag(tmp, 12))/lag(tmp2, 12)) %>% 
        ungroup() %>% 
        pull(tmp3)
    )
    
  } else if (form == 'levelsChangePct_Change') {
    dt[,'tmp'] = dt[, v1]
    df[,'tmp2'] = df[, v2]
    return(
      dt %>% 
        arrange(permno, ret_yearm) %>% 
        group_by(permno) %>%
        mutate(tmp3 = 100*(tmp - lag(tmp, 12))/lag(tmp, 12),
               tmp4 = 100*(tmp2 - lag(tmp2, 12))/lag(tmp2, 12)) %>% 
        ungroup() %>% 
        mutate(tmp5 = tmp3 - tmp4) %>% 
        pull(tmp5)
    )
    
  } else if (form == 'noise'){ 
    # pure noise
    return = runif(dim(xusedcurr)[1])
  } # end if form
  
} # end dataset_to_signal



signal_to_longshort = function(dt, form, portnum, sweight, trim = NULL){
  
  if (form == 'ls_extremes'){
    
    # sweight is zero if data is missing
    if (sweight == 'ew'){
      dt$weight = !is.na(dt$ret)
    } else if (sweight == 'vw'){
      dt$weight = dt$me_monthly
      dt$weight[is.na(dt$weight)] = 0
    }
    
    # Potential preprocessing of signal values
    if (!is.null(trim)) {
      
      dt = dt %>% 
        filter(signal >= quantile(dt$signal, trim, na.rm = TRUE),
               signal <= quantile(dt$signal, 1-trim, na.rm = TRUE)
        )
    }
    
    
    # find portfolio, rename date (only ret is still left)
    portdat = dt %>%
      filter(!is.na(signal), is.finite(signal)) %>% 
      mutate(port = ntile(signal, portnum)) %>%
      group_by(ret_yearm, port) %>%
      summarize(
        ret = weighted.mean(ret,weight, na.rm=T), .groups = 'drop'
      ) %>%
      rename(date = ret_yearm)

    # Add long-short return (this works when the number of portfolios created is less than portnum)
    return(portdat %>% 
      arrange(date, port) %>% 
      group_by(date) %>% 
      slice(1, n()) %>% 
      transmute(ret_ls = ret[2] - ret[1]) %>% 
      slice(1) %>% 
      ungroup()
    )

    # return(
    #   portdat %>%
    #     pivot_wider(
    #       id_cols = c(port,date), names_from = port, values_from = ret, names_prefix = 'port') %>%
    #     left_join(tmp)
    # )
  } # if form
  
} # end signal_to_longshort


# Function for calculating alpha and shrinkage

calculate_alpha_shrinkage = function(dt, train_start, train_end, test_start, test_end) {
  
  #' @param dt Table with columns signalname, date and ret
  #' @param train_start Start year for training sample, analogously for other parameters
  
  # add sample indicators
  retdat = dt %>% 
    mutate(
      samp = case_when(
        year(date) >= train_start & year(date) <= train_end ~ 'train'
        , year(date) >= test_start & year(date) <= test_end ~ 'test'
      )
    ) %>% 
    filter(!is.na(ret), !is.na(samp))  %>% 
    setDT()
  
  
  # find alphas
  stratsum = retdat[
    , list(
      alpha = summary(lm(ret ~ 1))$coefficients['(Intercept)' , 'Estimate']
      , tstat = summary(lm(ret ~ 1))$coefficients['(Intercept)' , 't value']
    )
    , by = .(signalname, samp)
  ] 
  
  # find shrinkage
  sampsum = stratsum %>% 
    group_by(samp) %>% 
    summarize(
      shrinkage = min((mean(tstat^2, na.rm = TRUE))^(-1), 1.0) # var(tstat, na.rm = TRUE)^-1
    )
  
  # shrink strat summary
  stratshrink = stratsum %>% 
    left_join(sampsum) %>% 
    mutate(
      alpha = alpha*(1-shrinkage)
      , tstat = tstat*(1-shrinkage)
    ) %>% 
    select(-shrinkage)
  
  # Combine classical and shrink strat summaries
  stratSumAll = stratshrink %>% 
    mutate(stattype = 'stein_ez') %>% 
    bind_rows(stratsum %>% 
                mutate(stattype = 'classical')
    )
  
  return(stratSumAll)
  
}
