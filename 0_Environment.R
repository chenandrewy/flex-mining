# Libraries ====
library(data.table)
library(RPostgres)
library(haven)
library(getPass)
library(dplyr)
library(fst)
library(lubridate)
library(splitstackshape)
library(zoo)
library(tidyr)
library(tictoc)
library(readr)
library(lme4)
library(stringr)
library(ggplot2)
library(gridExtra)


# Paths -------------------------------------------------------------------

# create data folders (separate to avoid storage problems)
dir.create('../Data/', showWarnings = F)
dir.create('../Data/Intermediate/', showWarnings = F)
dir.create('../Data/LongShortPortfolios/', showWarnings = F)
dir.create('../Results', showWarnings = F)
dir.create('../Data/CZ', showWarnings = F)
dir.create('../Data/Processed', showWarnings = F)

# Globals ====
options(stringsAsFactors = FALSE)


# code assumes that working directory is the directory with the R scripts
# check that working directory is correct
if (!file.exists('0_Environment.R')){
  stop('error: 0_Environment.R not found.  Please set working directory to the folder with the script')
}


# Set seed for random sampling
set.seed(1337)

# Yan-Zheng numerator and denominator names
# YZ list MKTCAP in Table B.1, which we call me_datadate  mkvalt is not available earlier in the data

compnames = list()
compnames$yz.numer = c("acchg", "aco", "acox", "act", "am", "ao", "aoloch", "aox", "ap", "apalch",
                          "aqc", "aqi", "aqs", "at", "bast", "caps", "capx", "capxv", "ceq", "ceql", "ceqt", "ch", "che", "chech",
                          "cld2", "cld3", "cld4", "cld5", "cogs", "cstk", "cstkcv", "cstke", "dc", "dclo", "dcom", "dcpstk",
                          "dcvsr", "dcvsub", "dcvt", "dd", "dd1", "dd2", "dd3", "dd4", "dd5", "dfs", "dfxa", "diladj", "dilavx",
                          "dlc", "dlcch", "dltis", "dlto", "dltp", "dltr", "dltt", "dm", "dn", "do", "donr", "dp", "dpact", "dpc",
                          "dpvieb", "dpvio", "dpvir", "drc", "ds", "dudd", "dv", "dvc", "dvp", "dvpa", "dvpibb", "dvt", "dxd2", "dxd3",
                          "dxd4", "dxd5", "ebit", "ebitda", "esopct", "esopdlt", "esopt", "esub", "esubc", "exre", "fatb", "fatc", "fate",
                          "fatl", "fatn", "fato", "fatp", "fiao", "fincf", "fopo", "fopox", "fopt", "fsrco", "fsrct", "fuseo", "fuset", "gdwl",
                          "gp", "ib", "ibadj", "ibc", "ibcom", "icapt", "idit", "intan", "intc", "intpn", "invch", "invfg", "invo", "invrm",
                          "invt", "invwip", "itcb", "itci", "ivaco", "ivaeq", "ivao", "ivch", "ivncf", "ivst", "ivstch", "lco", "lcox",
                          "lcoxdr", "lct", "lifr", "lo", "lt", "mib", "mii", "mrc1", "mrc2", "mrc3", "mrc4", "mrc5", "mrct", "msa", "ni",
                          "niadj", "nieci", "nopi", "nopio", "np", "oancf", "ob", "oiadp", "pi", "pidom", "pifo", "ppegt", "ppenb",
                          "ppenc", "ppenli", "ppenme", "ppennr", "ppeno", "ppent", "ppevbb", "ppeveb", "ppevo", "ppevr", "prstkc",
                          "pstk", "pstkc", "pstkl", "pstkn", "pstkr", "pstkrv", "rdip", "re", "rea", "reajo", "recch", "recco", "recd", "rect",
                          "recta", "rectr", "reuna", "sale", "seq", "siv", "spi", "sppe", "sppiv", "sstk", "tlcf", "tstk", "tstkc", "tstkp",
                          "txach", "txbco", "txc", "txdb", "txdba", "txdbca", "txdbcl", "txdc", "txdfed", "txdfo", "txdi", "txditc",
                          "txds", "txfed", "txfo", "txndb", "txndba", "txndbl", "txndbr", "txo", "txp", "txpd", "txr", "txs", "txt", "txw",
                          "wcap", "wcapc", "wcapch", "xacc", "xad", "xdepl", "xi", "xido", "xidoc", "xint", "xopr", "xpp", "xpr", "xrd", "xrent",
                          "xsga")


compnames$yz.denom <- c("at", "act",  "invt", "ppent", "lt", "lct", "dltt",
                      "ceq", "seq", "icapt", "sale", "cogs", "xsga", "emp", 'me_datadate')


compnames$yz.denom_alt <- c("at", "act",  "invt", "ppent", "lt", "lct", "dltt",
                        "ceq", "seq", "icapt", "sale", "cogs", "xsga", "emp", 'me')


# Functions ---------------------------------------------------------------
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

## MEMORY CHECK

# improved list of objects
.ls.objects <- function (pos = 1, pattern, order.by,
                         decreasing=FALSE, head=FALSE, n=5) {
  napply <- function(names, fn) sapply(names, function(x)
    fn(get(x, pos = pos)))
  names <- ls(pos = pos, pattern = pattern)
  obj.class <- napply(names, function(x) as.character(class(x))[1])
  obj.mode <- napply(names, mode)
  obj.type <- ifelse(is.na(obj.class), obj.mode, obj.class)
  obj.prettysize <- napply(names, function(x) {
    format(utils::object.size(x), units = "auto") })
  obj.size <- napply(names, object.size)
  obj.dim <- t(napply(names, function(x)
    as.numeric(dim(x))[1:2]))
  vec <- is.na(obj.dim)[, 1] & (obj.type != "function")
  obj.dim[vec, 1] <- napply(names, length)[vec]
  out <- data.frame(obj.type, obj.size, obj.prettysize, obj.dim)
  names(out) <- c("Type", "Size", "PrettySize", "Length/Rows", "Columns")
  if (!missing(order.by))
    out <- out[order(out[[order.by]], decreasing=decreasing), ]
  if (head)
    out <- head(out, n)
  out
}

# shorthand
lsos <- function(..., n=10) {
  .ls.objects(..., order.by="Size", decreasing=TRUE, head=TRUE, n=n)
}



# function for creating a list of possible variable combinations used in strategies
make_signal_list = function(signal_form, xvars, signalnum, scale_vars = NULL, rs) {
  
  #' @param xvars Unique names of variables used for creating strategies
  #' @param signalnum Number of signals to sample from full list
  #' @param scale_vars Scaling variables used in ratios (or NULL for unrestricted)
  #' @param rs Random seed
  
  set.seed(rs)
  
  # make list of all possible xused combinations
  # (is v1 != v2 what we want here??)
  tmp = expand.grid(signal_form = signal_form, v1 = xvars, v2 = xvars, stringsAsFactors = FALSE) %>% 
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
  
  # remove v2 for signal_forms that use only 1 variable
  tmp = tmp %>% 
    mutate(v2 = if_else(signal_form %in% c('levelChangePct'), NA_character_, v2)) %>% 
    arrange(signal_form, v1, v2) %>% 
    distinct(signal_form, v1, v2, .keep_all = T)
  
  # sample from full list
  signalnum = min(signalnum, nrow(tmp))
  tmp = tmp %>% sample_n(signalnum)
  
  # clean up
  tmp = tmp %>%
    arrange(across(everything())) %>% 
    mutate(signalid = row_number()) %>% 
    select(signalid, everything())

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
    dt[,'tmp2'] = dt[, v2]
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
    dt[,'tmp2'] = dt[, v2]
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



signal_to_ports = function(dt, form, portnum, sweight, trim = NULL){
  
  dt = dt %>% filter(!is.na(signal), !is.na(ret), is.finite(signal))
  
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
    
    # find breakpoints, with backup plan of getting 20 stocks
    breakdat = dt %>% 
      group_by(ret_yearm) %>% 
      summarize(
        ntot = n()
        , qlo_alt = quantile(signal, 20/pmax(ntot,20))
        , qlo = quantile(signal, 1/portnum)
        , qhi = quantile(signal, 1-1/portnum)
        , qhi_alt = quantile(signal, 1-20/pmax(ntot,20))    
      ) %>% 
      # backup: replace simple quantile with the alts if not enough stocks
      mutate(
        qlo = pmax(qlo, qlo_alt), qhi = pmin(qhi, qhi_alt)
      ) %>%
      # backup 2: if qlo == qhi, adjust
      mutate(
        qlo = if_else(qlo == qhi, pmin(qlo, qlo_alt), qlo)
        , qhi = if_else(qlo == qhi, pmax(qhi, qhi_alt), qhi)
      )
    
    # assign to legs, pedantically
    #   stocks assigned to both legs get dropped
    dt = dt %>% 
      inner_join(breakdat, by = 'ret_yearm') %>% 
      mutate(
        short = signal <= qlo
        , long = signal >= qhi
      ) %>% 
      mutate(
        port = case_when(
          short & !long ~ 'short'
          , !short & long ~ 'long'
        )
      )

    # find long-short return, rename date (only ret is still left)
    dt = dt %>% 
      filter(!is.na(port)) %>% 
      group_by(ret_yearm, port) %>%
      summarize(
        ret = weighted.mean(ret,weight, na.rm=T)
        , nstock = n()
        , .groups = 'drop'
      ) %>%
      filter(nstock >= 20) %>%  # drop undiversified junk (should make this an option)
      rename(yearm = ret_yearm)
    
    # more error checking
    if (dim(dt %>% filter(port == 'short'))[1] == 0){
      print('No short portfolios with at least 20 nstocks, returning empty tibble')
      return(tibble())
    }    
    if (dim(dt %>% filter(port == 'long'))[1] == 0){
      print('No long portfolios with at least 20 nstocks, returning empty tibble')
      return(tibble())
    }    
    
    # find long-short return
    return(
      dt %>% 
      select(yearm, port, ret, nstock) %>% 
      pivot_wider(names_from = port, values_from = c(ret, nstock)) %>% 
      mutate(ret_ls = ret_long - ret_short,
             nstocks_ls = nstock_long + nstock_short) %>% 
      filter(!is.na(ret_ls)) %>% 
      transmute(yearm, ret = ret_ls, nstock = nstocks_ls)
    )

  } # if form
  
} # end signal_to_ports


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


# Function for reading fst and auto-converting yearmon variables
# for some reason fst format saves yearmon as a rounded decimal

read_fst_yearm = function(filename, yearm_names = c('yearm')){
  dat = setDT(read_fst(filename))
  dat[ , yearm_names := lapply(yearm_names, as.yearmon), .SDcols = yearm_names ]
  return(dat)
}


# Create a plot by category without data-mining benchmark
ReturnPlotsNoDM = function(dt, suffix = '', rollmonths = 60, 
                           basepath = NA_character_) {
  
  #' @param dt Table with four columns (signalname, ret, eventDate, catID)
  #' @param suffix String to attach to saved pdf figure 
  #' @param rollmonths Number of months over which moving average is computed
  
  # Prep legend
  prepLegend = dt %>% 
    group_by(catID) %>% 
    summarise(nSignals = n_distinct(signalname))
  
  # Plot    
  print(dt %>% 
          group_by(catID, eventDate) %>% 
          summarise(rbar = mean(ret)) %>% 
          arrange(catID, eventDate) %>% 
          mutate(
            roll_rbar = zoo::rollmean(rbar, k = rollmonths, fill = NA, align = 'right')
          ) %>% 
          mutate(catID = factor(catID, levels = c('risk', 'mispricing', 'agnostic'), 
                                labels = c(paste0('Risk (', prepLegend$nSignals[prepLegend$catID == 'risk'], ' signals)'),
                                           paste0('Mispricing (', prepLegend$nSignals[prepLegend$catID == 'mispricing'], ' signals)'), 
                                           paste0('Agnostic (', prepLegend$nSignals[prepLegend$catID == 'agnostic'], ' signals)')))) %>% 
          ggplot(aes(x = eventDate, y = roll_rbar, color = catID, linetype = catID)) +
          geom_line(size = 1.1) +
          # scale_color_brewer(palette = 'Dark2') + 
          scale_color_manual(values = colors) + 
          # scale_linetype(guide = 'none') +
          geom_vline(xintercept = 0) +
          coord_cartesian(
            xlim = c(-360, 240), ylim = c(-60, 170)
          ) +
          scale_y_continuous(breaks = seq(-200,180,25)) +
          scale_x_continuous(breaks = seq(-360,360,60)) +  
          geom_hline(yintercept = 100, color = 'dimgrey') +
          # annotate(geom="text",
          #          label='In-Sample Mean', x=16, y=95, vjust=-1,
          #          family = "Palatino Linotype", color = 'dimgrey'
          # )  +
          geom_hline(yintercept = 0) +
          ylab('Trailing 5-Year Mean Return (bps p.m.)') +
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
  
  ggsave(paste0(basepath, '_', suffix, '.pdf'), width = 10, height = 8)
  
}


# Create a plot that compares the average predictor return with the average data-mined return
ReturnPlotsWithDM = function(dt, suffix = '', rollmonths = 60, colors = NA,
                             xl = -360, xh = 240, yl = -10, yh = 130, fig.width = 10,
                             fig.height = 8, basepath = NA_character_) {
  
  #' @param dt Table with three columns (eventDate, ret, matchRet)
  #' @param suffix String to attach to saved pdf figure 
  #' @param rollmonths Number of months over which moving average is computed
  #' @param xl, xh, yl, yh Upper and lower limits for x and y axes  
  
  print(
    dt %>% 
      gather(key = 'SignalType', value = 'return', -eventDate) %>% 
      group_by(SignalType, eventDate) %>% 
      summarise(rbar = mean(return)) %>% 
      arrange(SignalType, eventDate) %>% 
      mutate(
        roll_rbar = zoo::rollmean(rbar, k = rollmonths, fill = NA, align = 'right')
      ) %>% 
      mutate(SignalType = factor(SignalType, levels = c('ret', 'matchRet'), labels = c('Published', 'Matched data-mined'))) %>% 
      ggplot(aes(x = eventDate, y = roll_rbar, color = SignalType, linetype = SignalType)) +
      geom_line(size = 1.1) +
      #  scale_color_grey() + 
      # scale_color_brewer(palette = 'Dark2') + 
      scale_color_manual(values = colors) + 
      scale_linetype_manual(values = c('solid', 'twodash')) +
      # scale_linetype(guide = 'none') +
      geom_vline(xintercept = 0) +
      coord_cartesian(
        xlim = c(xl, xh), ylim = c(yl, yh)
      ) +
      scale_y_continuous(breaks = seq(-200,180,25)) +
      scale_x_continuous(breaks = seq(-360,360,60)) +  
      geom_hline(yintercept = 100, color = 'dimgrey') +
      geom_hline(yintercept = 0) +
      ylab('Trailing 5-Year Mean Return (bps p.m.)') +
      xlab('Months Since Original Sample Ended') +
      labs(color = '', linetype = '') +
      theme_light(base_size = 18) +
      theme(
        legend.position = c(80,85)/100
        , legend.spacing.y = unit(0, units = 'cm')
        , legend.background = element_rect(fill='transparent')
      ) 
  )
  
  ggsave(paste0(basepath, '_', suffix, '.pdf'), width = fig.width, height = fig.height)
  
}


# Finds matching strategies for a predictor from the universe of strategies
matchedReturns = function(bm_rets,
                          actSignalname, 
                          actSampleStart, # TBD: Unify dates to end of calendar month (current workaround: year(date))
                          actSampleEnd, 
                          actTStat,
                          actRBar,
                          tol_t = .3,
                          tol_r = .3,
                          minStocks = 50) {
  
  #' @param bm_rets Table of universe of strategies
  #' @param actSignalname String of actual predictor name
  #' @param tol_t Tolerance level for difference in t-stats
  #' @param tol_r Tolerance level for difference in mean returns
  
  # Restrict benchmark sample to in-sample dates and compute summary stats
  tmpSumStats = bm_rets %>% 
    filter(
      yearm >= actSampleStart, yearm <= actSampleEnd
    )
  # Make sure that samples are the spanning the entire length (and not just small subsets)
  # group_by(signalname) %>% 
  # mutate(minDate = min(date),
  #        maxDate = max(date)) %>% 
  # ungroup() %>% 
  # filter(year(minDate) == year(actSampleStart),
  #        year(maxDate) == year(actSampleEnd)) %>% 
  # Make sure that samples are available for at least 80% of predictor in-sample period
  # group_by(signalname) %>% 
  # mutate(tmpN = n()) %>% 
  # ungroup() %>% 
  # filter(tmpN > .8*12*(year(actSampleEnd) - year(actSampleStart)))
  
  # Make sure at least minStocks stocks in each month of the sample period
  tmpAtLeastNStocks = tmpSumStats %>% 
    group_by(signalname) %>% 
    summarise(minN = min(nstock)) %>% 
    ungroup() %>% 
    filter(minN >= minStocks)

  tmpSumStats = tmpSumStats %>% 
    filter(signalname %in% tmpAtLeastNStocks$signalname)
  
  # Make sure predictors fully available in last in-sample year
  tmpFullyLastYear = tmpSumStats %>% 
    filter(year(yearm) == year(actSampleEnd)) %>% 
    group_by(signalname) %>% 
    filter(n() == 12) %>% 
    ungroup() %>% 
    select(signalname) %>% 
    distinct()
  
  tmpSumStats = tmpSumStats %>% 
    filter(signalname %in% tmpFullyLastYear$signalname)
  
  # Sum stats
  tmpSumStats = tmpSumStats %>% 
    group_by(signalname) %>%
    summarize(
      rbar = mean(ret)
      #    , vol = sd(ret)
      , tstat = mean(ret)/sd(ret)*sqrt(dplyr::n())) %>% 
    ungroup()
  
  # Find matches (can define different metrics here if desired. Currently, all strats with t-stat difference within tol)
  tmpCandidates = tmpSumStats %>% 
    mutate(diff_t = abs(tstat) - abs(actTStat),
           diff_r = abs(rbar)  - abs(actRBar)) %>% 
    filter(abs(diff_t) < tol_t,
           abs(diff_r) < tol_r)
  
  # Return candidate strategy returns
  #   ac: should be a better way to find eventDate directly from yearm
  bm_rets %>% 
    filter(signalname %in% tmpCandidates$signalname) %>% 
    inner_join(tmpSumStats %>% 
                 filter(signalname %in% tmpCandidates$signalname)) %>% 
    transmute(candSignalname = signalname,
              eventDate = interval(as.Date(tmpSampleEnd), as.Date(yearm)) %/% months(1),
              # Sign returns
              ret = ifelse(rbar >0, ret, -ret),
              samptype = case_when(
                (yearm >= actSampleStart) & (yearm <= actSampleEnd) ~ 'insamp'
                , (yearm > actSampleEnd) ~ 'oos' 
                , TRUE ~ NA_character_
              ),
              actSignal = actSignalname
    )
  
}

