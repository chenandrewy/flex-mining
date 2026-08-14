# Mining helpers: signal-list construction, signals, and portfolio sorts.
#
# Sourced by 0_Environment.R after packages and config.R. These functions
# rely on objects from the sourcing environment (e.g. globalSettings and
# chapter-local data frames); do not source this file in isolation.




# function for creating a list of possible variable combinations used in strategies
make_signal_list = function(signal_form, xvars, scale_vars, validDenoms = NULL) {
  
  #' @param xvars Unique names of variables used for creating strategies
  #' @param scale_vars Scaling variables used in ratios (or NULL for unrestricted)
  #' @param validDenoms Dataset of valid denominator for each combination of signals (created in 1_Download_and_Clean.R)
  
  # make list of all possible xused combinations
  tmp = expand.grid(signal_form = signal_form, 
                    v1 = xvars, 
                    v2 = scale_vars, stringsAsFactors = FALSE) %>% 
    as_tibble()
  
  # Remove v1=v2 for functions where this does not make sense
  # and remove inverse  (e.g. keep only v1/v2 not v2/v1)
  if (!is.null(validDenoms)) {  # new version that removes based on non-zero freq in denominator
    
    tmp = tmp %>% 
      # paste name alphabetically
      mutate(combName = ifelse(v1 < v2, paste(v1, v2, sep='|'), 
                               paste(v2, v1, sep='|')) %>% as.character()) %>% 
      left_join(validDenoms)  %>% 
      mutate(keep = case_when(
        # Keep all growth rate signals
        signal_form  %in% c('diff(v1)/lag(v2)') ~ 1,
        # For ratio signals, keep version with more non-zeros in denominator
        !(v1 %in% scale_vars) ~ 1,
        v1 %in% scale_vars & v2 == denom ~ 1,
        TRUE ~ 0
      )) %>% 
      filter(keep == 1) %>% 
      select(signal_form, v1, v2)
    
  } else {  # old version that just removes alphabetically
    
    tmp = tmp %>%
      mutate(keep = case_when(
        signal_form  %in% c('diff(v1)/lag(v2)') ~ 1,
        # For ratio signals
        v1 %in% scale_vars & v1<= v2 ~ 0,
        TRUE ~ 1)
      ) %>% 
      filter(keep == 1) %>% 
      select(-keep)
  }
  
  # remove v2 for signal_forms that use only 1 variable
  tmp = tmp %>% 
    mutate(v2 = if_else(signal_form %in% c('pdiff(v1)'), NA_character_, v2)) %>% 
    arrange(signal_form, v1, v2) %>% 
    distinct(signal_form, v1, v2, .keep_all = T)
  
  # # sample from full list
  # signalnum = min(signalnum, nrow(tmp))
  # tmp = tmp %>% sample_n(signalnum)
  
  # clean up
  tmp = tmp %>%
    arrange(across(everything())) %>% 
    mutate(signalid = row_number()) %>% 
    select(signalid, everything())
  
  return(tmp)
}







# function for creating Yan-Zheng's 18,113 signal list
make_signal_list_yz = function(signal_form, x1list, x2list, signalnum, seed){
  
  # ac: this works to replicate yz strat list
  # first make 240*76 = 18,240 combinations
  # use yz.denom for me_datadate, yz.denom_alt for me (most recent)
  signal_list = expand.grid(
    signal_form = signal_form
    , v1 = x1list
    , v2 = x2list
    , stringsAsFactors = F
  ) %>% 
    mutate(
      v2 = if_else(signal_form == 'pdiff(v1)', NA_character_, v2)
    ) %>% 
    distinct(across(everything()), .keep_all = T) %>% 
    # remove 13 vboth x 5 two variable fun where v1 == v2 leads to constant signals  
    mutate(
      dropme = v1 %in% intersect(x1list, x2list) 
      & signal_form != 'pdiff(v1)' 
      &  v1 == v2 
    ) %>% 
    # remove selected strategies (2 vodd x 31 pd_var funs) based on yz sas data
    mutate(
      dropme2 = v1 %in% c('rdip', 'txndbr')  
      & signal_form %in% c('pdiff(v1/v2)','pdiff(v1)','pdiff(v1)-pdiff(v2)')
    ) %>%
    filter(!(dropme | dropme2)) %>% 
    select(-starts_with('drop')) %>% 
    as_tibble()
  
  
  # sample and add id
  set.seed(seed)
  signal_list = signal_list %>% 
    sample_n(min(dim(signal_list)[1],signalnum)) %>% 
    arrange(across(everything())) %>% 
    mutate(signalid = row_number()) %>% 
    select(signalid, everything())    
}



# function for turning xused into a signal
dataset_to_signal = function(form, dt, v1, v2){
  
  stopifnot("form must be one of ratio, ratioChange, ratioChangePct,
            levelChangePct, levelChangeScaled, levelsChangePct_Change, noise" = 
              form %in% c('v1/v2', 'diff(v1/v2)', 'pdiff(v1/v2)',
                          'pdiff(v1)', 'diff(v1)/lag(v2)', 'pdiff(v1)-pdiff(v2)', 'noise'))
  
  if (form == 'v1/v2'){
    
    dt[,'tmp'] = dt[, 'v1']/dt[, 'v2']
    return(dt %>% pull(tmp))
    
  } else if (form == 'diff(v1/v2)') {
    
    dt[,'tmp'] = dt[, 'v1']/dt[, 'v2']
    return(
      dt %>% 
        arrange(permno, ret_yearm) %>% 
        group_by(permno) %>%
        mutate(tmp2 = tmp - lag(tmp, 12)) %>% 
        ungroup() %>% 
        pull(tmp2)
    )
    
  } else if (form == 'pdiff(v1/v2)') {
    
    dt[,'tmp'] = dt[, 'v1']/dt[, 'v2']
    return(
      dt %>% 
        arrange(permno, ret_yearm) %>% 
        group_by(permno) %>%
        mutate(tmp2 = 100*(tmp - lag(tmp, 12))/lag(tmp, 12)) %>% 
        ungroup() %>% 
        pull(tmp2)
    )
    
  } else if (form == 'diff(v1)/lag(v2)') {
    dt[,'tmp'] = dt[, 'v1']
    dt[,'tmp2'] = dt[, 'v2']
    return(
      dt %>% 
        arrange(permno, ret_yearm) %>% 
        group_by(permno) %>%
        mutate(tmp3 = (tmp - lag(tmp, 12))/lag(tmp2, 12)) %>% 
        ungroup() %>% 
        pull(tmp3)
    )
    
  } else if (form == 'pdiff(v1)-pdiff(v2)') {
    dt[,'tmp'] = dt[, 'v1']
    dt[,'tmp2'] = dt[, 'v2']
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
    
    
  } else if (form == 'pdiff(v1)') {
    
    dt[,'tmp'] = dt[, 'v1']
    return(
      dt %>% 
        arrange(permno, ret_yearm) %>% 
        group_by(permno) %>%
        mutate(tmp2 = 100*(tmp - lag(tmp, 12))/lag(tmp, 12)) %>% 
        ungroup() %>% 
        pull(tmp2)
    )    
    
  } else if (form == 'noise'){ 
    # pure noise
    return = runif(dim(xusedcurr)[1])
  } # end if form
  
} # end dataset_to_signal




signal_to_ports = function(dt0, form, portnum, sweight, trim = NULL){
  
  dt = dt0 %>% filter(!is.na(signal), !is.na(ret), is.finite(signal))
  
  if (form == 'ls_extremes'){
    
    # sweight is zero if data is missing
    if (sweight == 'ew'){
      dt$weight = !is.na(dt$ret)
    } else if (sweight == 'vw'){
      dt$weight = dt$me_monthly
      dt$weight[is.na(dt$weight)] = 0
    }
    
    # Potential preprocessing of signal values
    if (!is.na(trim)) {
      
      dt = dt %>% 
        filter(signal >= quantile(dt$signal, trim, na.rm = TRUE),
               signal <= quantile(dt$signal, 1-trim, na.rm = TRUE)
        )
    }
    
    # # find breakpoints
    # # based on email with LingLing Zheng 2023 01
    # # she used proc rank with group output and ties = min
    # # see https://blogs.sas.com/content/sgf/2019/07/19/how-the-rank-procedure-calculates-ranks-with-groups-and-ties/
    dt = dt %>%
      group_by(ret_yearm) %>%
      mutate(
        rank = rank(signal, ties.method = 'min')
        , group = floor(rank*portnum / (n()+1)) + 1
        , port = case_when(
          group == 1 ~ 'short'
          , group == portnum ~ 'long'
        )
      ) %>%
      ungroup()
    
    # find long-short return, rename date (only ret is still left)
    dt = dt %>% 
      filter(!is.na(port)) %>% 
      group_by(ret_yearm, port) %>%
      summarize(
        ret = weighted.mean(ret,weight, na.rm=T)
        , nstock = n()
        , .groups = 'drop'
      ) %>%
      rename(yearm = ret_yearm)
    
    # more error checking
    if (dim(dt %>% filter(port == 'short'))[1] == 0){
      print('No short portfolios, returning empty tibble')
      return(tibble())
    }    
    if (dim(dt %>% filter(port == 'long'))[1] == 0){
      print('No long portfolios, returning empty tibble')
      return(tibble())
    }    
    
    # find long-short return
    dt_ls = dt %>% 
      select(yearm, port, ret, nstock) %>% 
      pivot_wider(names_from = port, values_from = c(ret, nstock)) %>% 
      mutate(ret_ls = ret_long - ret_short) %>% 
      filter(!is.na(ret_ls)) %>% 
      transmute(yearm, ret = ret_ls, nstock_long, nstock_short)
    
    return(dt_ls)
    
  } # if form
  
} # end signal_to_ports

make_many_ls = function(){
  ### make one portdat ===
  
  # extract current settings 
  signal_cur = signal_list[signali,]
  
  # import small dataset with return, me, xusedcurr, and add signal
  if (is.na(signal_cur$v2) | signal_cur$v1 == signal_cur$v2) { # If only one variable needed to construct signal
    smalldat = fst::read_fst('../Data/tmpAllDat.fst', 
                             columns = c('permno', 'ret_yearm', 'ret', 'me_monthly',
                                         signal_cur$v1)) %>%
      as_tibble()
  } else {
    smalldat = fst::read_fst('../Data/tmpAllDat.fst', 
                             columns = c('permno', 'ret_yearm', 'ret', 'me_monthly',
                                         signal_cur$v1, signal_cur$v2)) %>%
      as_tibble()
  }
  
  smalldat = smalldat %>% mutate(ret_yearm = as.yearmon(ret_yearm))
  
  # Unify column names for processing
  if (is.na(signal_cur$v2)) {
    colnames(smalldat) = c('permno', 'ret_yearm', 'ret', 'me_monthly', 'v1')
  } else if (signal_cur$v1 == signal_cur$v2) {
    colnames(smalldat) = c('permno', 'ret_yearm', 'ret', 'me_monthly', 'v1')
    smalldat = smalldat %>% mutate(v2 = v1)
  } else {
    colnames(smalldat) = c('permno', 'ret_yearm', 'ret', 'me_monthly', 'v1', 'v2')
  }
  
  tic = Sys.time() #
  smalldat$signal = dataset_to_signal(form = signal_cur$signal_form, 
                                      dt = smalldat) # makes a signal
  toc = Sys.time() #
  print('signal done')
  print(toc - tic) #
  
  
  tic = Sys.time() #
  # assign to portfolios
  portdat = tibble()
  for (porti in 1:dim(port_list)[1]){
    tempport = signal_to_ports(dt0 = smalldat, 
                               form = port_list[porti,]$longshort_form, 
                               portnum = port_list[porti,]$portnum, 
                               sweight = port_list[porti,]$sweight,
                               trim = port_list[porti,]$trim)
    tempport = tempport %>% mutate(portid = porti)
    portdat = rbind(portdat, tempport)
  }
  
  toc = Sys.time() #
  print('ports done')
  print(toc - tic)  #
  
  # Clean up and save
  ls_dat = portdat %>% mutate(signalid = signali) 
  
  # feedback
  print(paste0(
    'signali = ', signali, ' of ', nrow(signal_list)
    , ' | signalform = ', signal_cur$signal_form
    , ' | v1 = ', signal_cur$v1
    , ' | v2 = ', signal_cur$v2
    #      , ' | Var(tstat) = ', round(var_tstat,2)
  ))
  
  ## end make one portdat ===
  
  return(ls_dat)
  
} # make_many_ls

### Form nchoose2 long-short strategies by going long-short every ntile combination
nchoose2ports <- function(n, big_trade_months = 6) { 
  # n=50 will lead to 50*50/2 - 50 = 1200 long-short portfolios. 
  
  # change date notation (this should be done earlier)
  CCM = CCM %>% 
    mutate(
      date = as.Date(paste0(as.character(yyyymm), '28'), format='%Y%m%d')
    )
  
  # have signals update only on big_trade_months  
  # - note: filling early here helps ensures signal isn't super stale, as long
  #         as the signal data is constructed nicely
  signal = CCM %>% 
    mutate(
      signal = if_else(month %in% big_trade_months, signal, NA_character_)
    ) %>% 
    arrange(permno,date) %>% 
    group_by(permno) %>% 
    fill(signal)
  
  # sort stocks into bins, change date notation
  signal = signal %>% 
    group_by(date) %>% 
    mutate(bin1 = ntile(signal, n)) %>% 
    ungroup() %>% 
    transmute(permno, signal_avail = date, bin1, signal)
  
  # merge last month's signal on current month's return
  ret = CCM %>% 
    select(permno,yyyymm,date,ret,lag_me) %>% 
    left_join(
      signal %>% mutate( date  =  signal_avail %m+% months(1) ) 
      ,  by = c('permno','date')
    )
  
  # find portfolio returns
  portfolio_returns <- ret %>%
    group_by(yyyymm, bin1) %>% 
    dplyr::summarize(ew_mean = mean(ret, na.rm=TRUE),
                     vw_mean = weighted.mean(ret, lag_me, na.rm=TRUE),
                     N = n()) %>% 
    filter(is.na(bin1) == FALSE) %>% 
    ungroup()
  
  ### generate long-short portfolios of all possible combinations
  
  for (ii in 1:n) {
    portfolio_returns <- portfolio_returns %>% 
      group_by(yyyymm) %>% 
      mutate("ew_ls_x.{ii}":= ew_mean - ew_mean[ii],
             "vw_ls_x.{ii}":= vw_mean - vw_mean[ii])
  }  
  
  ### reshape to long
  portfolio_returns_ew <- portfolio_returns %>% 
    ungroup() %>% 
    select(yyyymm, bin1, starts_with("ew_ls"), ) %>% 
    pivot_longer(cols = starts_with("ew_ls"),
                 names_to = "bin2", 
                 values_to = "return_ew") %>% 
    mutate(bin2 = as.numeric(substr(bin2, 9, 11))) %>% 
    filter(bin1 < bin2) %>%  # drop whenever return is always 0 and whenever long short return of one portfolio is the negative return of another portfolio
    unite(bin, bin1:bin2, sep=",")
  
  portfolio_returns_vw <- portfolio_returns %>% 
    ungroup() %>% 
    select(yyyymm, bin1, starts_with("vw_ls"), ) %>% 
    pivot_longer(cols = starts_with("vw_ls"),
                 names_to = "bin2", 
                 values_to = "return_vw") %>% 
    mutate(bin2 = as.numeric(substr(bin2, 9, 11))) %>% 
    filter(bin1 < bin2) %>%  # drop whenever return is always 0 and whenever long short return of one portfolio is the negative return of another portfolio
    unite(bin, bin1:bin2, sep=",")  
  
  output <- full_join(portfolio_returns_ew, portfolio_returns_vw, by = c("bin", "yyyymm"))
  return(output)
  
} # end function



# "form portfolios based on the first, second, and third letters of the ticker symbol"
tic_kth_letter_port <- function(k) {
  # create portfolio assignments
  crsp2 <- copy(crsp)
  crsp2[!is.na(lag_tic), `:=`(port = paste0("tic", k, substr(lag_tic, k, k)))]
  
  # find EW and VW returns
  port <- crsp2[!is.na(ret) & !is.na(port) & !is.na(lag_me),
                .(
                  ret_ew = mean(ret), ret_vw = weighted.mean(ret, lag_me),
                  nstock = .N
                ),
                by = c("yearm", "port")
  ] %>%
    pivot_longer(
      cols = c("ret_ew", "ret_vw"), names_to = "sweight", values_to = "ret",
      names_prefix = "ret_"
    ) %>%
    setDT()
  
  return(port)
} # end tic_kth_letter_port

