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
dir.create('../Data/RollingStats/', showWarnings = F)
dir.create('../Results', showWarnings = F)



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
# mkvalt is not exactly me, but for our framework it makes more sense

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
                          "xsga", "mkvalt")


compnames$yz.denom <- c("at", "act",  "invt", "ppent", "lt", "lct", "dltt",
                      "ceq", "seq", "icapt", "sale", "cogs", "xsga", "mkvalt", "emp")


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


# Function for reading fst and auto-converting yearmon variables
# for some reason fst format saves yearmon as a rounded decimal

read_fst_yearm = function(filename, yearm_names = c('yearm')){
  dat = setDT(read_fst(filename))
  dat[ , yearm_names := lapply(yearm_names, as.yearmon), .SDcols = yearm_names ]
  return(dat)
}


