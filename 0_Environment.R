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
library(stringr)
library(ggplot2)
library(gridExtra)
library(xtable)
if("lme4" %in% (.packages())){
  detach("package:lme4", unload=TRUE) 
}
if("multcomp" %in% (.packages())){
  detach("package:multcomp", unload=TRUE) 
}
if("TH.data" %in% (.packages())){
  detach("package:TH.data", unload=TRUE) 
}
if("MASS" %in% (.packages())){
  detach("package:MASS", unload=TRUE) 
}



# Paths -------------------------------------------------------------------

# code assumes that working directory is the directory with the R scripts
# check that working directory is correct
if (!file.exists('0_Environment.R')){
  stop('error: 0_Environment.R not found.  Please set working directory to the folder with the script')
}

# create data folders (separate to avoid storage problems)
dir.create('../Data/', showWarnings = F)
dir.create('../Data/Raw/', showWarnings = F)
dir.create('../Data/Processed/', showWarnings = F)
dir.create('../Results', showWarnings = F)
dir.create('../Results/Extra/', showWarnings = F)


# Globals ====
options(stringsAsFactors = FALSE)

globalSettings = list(
  dataVersion = 'CZ-style-v7',
  minNumStocks = 20 # Minimum number of stocks in any month over the in-sample period to include a DM strategy for matching to published strategies (ie minNumStocks/2 in each leg)
)

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

# 63 denominators with at least 25% non-missing observations in 1963
compnames$pos_in_1963 <- c("aco", "acox","act","ao","aox","at","caps","capx","capxv","ceq","ceql","ceqt","che","cogs",
                          "cstk","dlc","dltt","dp","dpact","dvc","dvp","dvt","ebit","ebitda","gp","ib","ibadj","ibcom",
                          "icapt","intan","invt","itci","ivaeq","ivao","lct","lo","lt","ni","nopi","nopio","np",
                          "oiadp","pi","ppegt","ppent","pstkl","pstkrv","re","recco","rect","sale","seq","txdb",
                          "txditc","txt","wcap","xint","xopr","xpr","xrent","xsga","emp", "me_datadate")

compnames$all = unique(Reduce(c, compnames))

# nice colors
colors = c(rgb(0,0.4470,0.7410), # MATBLUE
           rgb(0.8500, 0.3250, 0.0980), # MATRED
           rgb(0.9290, 0.6940, 0.1250) # MATYELLOW
)


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
make_signal_list = function(signal_form, xvars, scale_vars) {
  
  #' @param xvars Unique names of variables used for creating strategies
  #' @param scale_vars Scaling variables used in ratios (or NULL for unrestricted)
  
  # make list of all possible xused combinations
  tmp = expand.grid(signal_form = signal_form, 
                    v1 = xvars, 
                    v2 = scale_vars, stringsAsFactors = FALSE) %>% 
    as_tibble()
  
  # Remove v1=v2 for functions where this does not make sense
  # and remove inverse  (e.g. keep only v1/v2 not v2/v1)
  tmp = tmp %>%
    mutate(keep = case_when(
      signal_form  %in% c('diff(v1)/lag(v2)') ~ 1,
      # For ratio signals
      v1 %in% scale_vars & v1<= v2 ~ 0,
      TRUE ~ 1)
    ) %>% 
    filter(keep == 1) %>% 
    select(-keep)
  
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
ReturnPlotsNoDM = function(dt, suffix = '', rollmonths = 60, filetype = '.pdf',
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
  } else{
    select_cols = c('eventDate','ret','matchRet')
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


#' # Finds matching strategies for a predictor from the universe of strategies
#' matchedReturns = function(bm_rets,
#'                           actSignalname, 
#'                           actSampleStart, # TBD: Unify dates to end of calendar month (current workaround: year(date))
#'                           actSampleEnd, 
#'                           actTStat,
#'                           actRBar,
#'                           tol_t = .3,
#'                           tol_r = .3,
#'                           minStocks = 50) {
#'   
#'   #' @param bm_rets Table of universe of strategies
#'   #' @param actSignalname String of actual predictor name
#'   #' @param tol_t Tolerance level for difference in t-stats
#'   #' @param tol_r Tolerance level for difference in mean returns
#'   
#'   # Restrict benchmark sample to in-sample dates and compute summary stats
#'   tmpSumStats = bm_rets %>% 
#'     filter(
#'       yearm >= actSampleStart, yearm <= actSampleEnd
#'     )
#'   # Make sure that samples are the spanning the entire length (and not just small subsets)
#'   # group_by(signalname) %>% 
#'   # mutate(minDate = min(date),
#'   #        maxDate = max(date)) %>% 
#'   # ungroup() %>% 
#'   # filter(year(minDate) == year(actSampleStart),
#'   #        year(maxDate) == year(actSampleEnd)) %>% 
#'   # Make sure that samples are available for at least 80% of predictor in-sample period
#'   # group_by(signalname) %>% 
#'   # mutate(tmpN = n()) %>% 
#'   # ungroup() %>% 
#'   # filter(tmpN > .8*12*(year(actSampleEnd) - year(actSampleStart)))
#'   
#'   # Make sure at least minStocks stocks in each month of the sample period
#'   if (minStocks > 0){
#'     tmpAtLeastNStocks = tmpSumStats %>% 
#'       filter(floor(yearm) !=1963) %>%   #Somewhat quick and dirty way to deal with the fact that in early 1963 we have few obs
#'       group_by(signalname) %>% 
#'       summarise(minN = min(nstock)) %>% 
#'       ungroup() %>% 
#'       filter(minN >= minStocks)
#'   } else {
#'     tmpAtLeastNStocks = tmpSumStats %>% 
#'       select(signalname) %>% 
#'       distinct()
#'   }
#'   
#'   tmpSumStats = tmpSumStats %>% 
#'     filter(signalname %in% tmpAtLeastNStocks$signalname)
#'   
#'   # Make sure predictors fully available in last in-sample year
#'   tmpFullyLastYear = tmpSumStats %>% 
#'     filter(floor(yearm) == floor(actSampleEnd)) %>% 
#'     group_by(signalname) %>% 
#'     filter(n() == 12) %>% 
#'     ungroup() %>% 
#'     select(signalname) %>% 
#'     distinct()
#'   
#'   tmpSumStats = tmpSumStats %>% 
#'     filter(signalname %in% tmpFullyLastYear$signalname)
#'   
#'   # Sum stats
#'   tmpSumStats = tmpSumStats %>% 
#'     group_by(signalname) %>%
#'     summarize(
#'       rbar = mean(ret)
#'       #    , vol = sd(ret)
#'       , tstat = mean(ret)/sd(ret)*sqrt(dplyr::n())) %>% 
#'     ungroup()
#'   
#'   # Find matches (can define different metrics here if desired. Currently, all strats with t-stat difference within tol)
#'   tmpCandidates = tmpSumStats %>% 
#'     mutate(diff_t = abs(tstat) - abs(actTStat),
#'            diff_r = abs(rbar)  - abs(actRBar)) %>% 
#'     filter(abs(diff_t) < tol_t,
#'            abs(diff_r) < tol_r)
#'   
#'   # Return candidate strategy returns
#'   bm_rets %>% 
#'     filter(signalname %in% tmpCandidates$signalname) %>% 
#'     inner_join(tmpSumStats %>% 
#'                  filter(signalname %in% tmpCandidates$signalname)) %>% 
#'     transmute(candSignalname = signalname,
#'               eventDate = as.integer(round(12*(yearm-tmpSampleEnd))),
#'               # Sign returns
#'               ret = ifelse(rbar >0, ret, -ret),
#'               samptype = case_when(
#'                 (yearm >= actSampleStart) & (yearm <= actSampleEnd) ~ 'insamp'
#'                 , (yearm > actSampleEnd) ~ 'oos' 
#'                 , TRUE ~ NA_character_
#'               ),
#'               actSignal = actSignalname
#'     )
#'   
#' }


f.custom.t <- function(x){
  if(length(x[!is.na(x)]) > 1 & sd(x[!is.na(x)] > 1e-8)){
    return(t.test(x, na.action = na.omit)$statistic)
  }else{return(NaN)}
  
}

# Annualized Sharpe ratio

f.sharp <- function(x, na.rm = TRUE){
  return(mean(x, na.rm = na.rm)*sqrt(12)/sd(x, na.rm = na.rm))
}

# Describe function

f.describe_numeric <- function(.x) {
  if(!is.numeric(.x)) stop(".x must be a numeric vector!")
  if(!is.atomic(.x)) stop(".x must be an atomic vector!")
  describe_functions <- list(
    N = function(.x, ...) length(.x),
    mean = function(.x, ...) mean(.x, ...),
    median = function(.x, ...) median(.x, ...),
    sd = function(.x, ...) stats::sd(.x, ...),
    min = function(.x, ...) min(.x, ...),
    q1 = function(.x, ...) stats::quantile(.x, probs = 0.01, ...),
    q5 = function(.x, ...) stats::quantile(.x, probs = 0.02, ...),
    q10 = function(.x, ...) stats::quantile(.x, probs = 0.10, ...),
    q25 = function(.x, ...) stats::quantile(.x, probs = 0.25, ...),
    q50 = function(.x, ...) stats::median(.x, ...),
    q75 = function(.x, ...) stats::quantile(.x, probs = 0.75, ...),
    q90 = function(.x, ...) stats::quantile(.x, probs = 0.90, ...),
    q95 = function(.x, ...) stats::quantile(.x, probs = 0.95, ...),
    q99 = function(.x, ...) stats::quantile(.x, probs = 0.99, ...),
    max = function(.x, ...) max(.x, ...)
  )
  return(as.data.frame(lapply(describe_functions,
                              function(.f) .f(.x, na.rm = TRUE)),
                       row.names = "1",
                       stringsAsFactors = FALSE))
}

fntile <- function(x, n) {
  x.length <- length(x)
  return(as.integer(n * {frank(x, ties.method = "first") - 1} / x.length + 1))
}

f.desc.returns <- function(returns_dt){
  sumsignal_rets = returns_dt %>% 
    group_by(bin) %>% 
    summarize(rbar_is = mean(ret_is, na.rm = TRUE),
              avg_tstat_is = mean(t_is, na.rm = TRUE),
              rbar_oos = mean(ret_oos) 
              # ,tstat_oos_portfolio = rbar_oos/sd(ret_oos)*sqrt(n())
    ) %>% 
    ungroup()
  
  return(sumsignal_rets)
}

f.ls.past.returns <- function(n_tiles, name_var){
  
  
  yz_dt[, sort_var := get(name_var)]
  
  yz_dt[!is.na(sort_var) & month(date) == 6,
        var_sort := as.factor(fntile(sort_var, n_tiles)), by = date]
  
  yz_dt[ ,
         var_sort :=  zoo::na.locf(var_sort,na.rm =  FALSE),
         by = signalname]
  
  yz_dt[!is.na(var_sort), bin := var_sort]
  
  yz_dt[month(date) != 6, sort_var := NA]
  
  returns_dt <- yz_dt[!is.na(bin) & !is.na(ret),
                      .(ret_oos = mean(ret, na.rm=TRUE),
                        ret_is = mean(sort_var, na.rm=TRUE),
                        t_is = mean(t_30y_l, na.rm = TRUE),
                        .N),
                      by = .(date, bin)]
  
  sumsignal_oos <- f.desc.returns(returns_dt)
  sumsignal_oos_pre_2003 <- f.desc.returns(returns_dt[date < '2003-06-30'])
  sumsignal_oos_post_2003 <- f.desc.returns(returns_dt[date >= '2003-06-30'])
  
  return(list(sumsignal_oos = sumsignal_oos,
              sumsignal_oos_pre_2003 = sumsignal_oos_pre_2003,
              sumsignal_oos_post_2003 = sumsignal_oos_post_2003,
              rets = returns_dt))
  
}


# function for computing DM strat sumstats in pub samples
sumstats_for_DM_Strats <- function(
    DMname = paste0('../Data/Processed/',
                    globalSettings$dataVersion, 
                    ' LongShort.RData'),
    nsampmax = Inf) {

  # convert czsum to data.table (this should be done more globally)
  setDT(czsum)
  
  # read in DM strats
  dm_rets <- readRDS(DMname)$ret
  dm_info <- readRDS(DMname)$port_list
  
  dm_rets <- dm_rets %>%
    left_join(
      dm_info %>% select(portid, sweight), by = c("portid")
    ) %>%
    transmute(
      sweight, dmname = signalid, yearm, ret, nstock_long, nstock_short
    ) %>%
    setDT()
  
  # Finds sum stats for dm in each pub sample
  # the output for this can be used for all dm selection methods
  samplist <- czsum %>%
    distinct(sampstart, sampend) %>%
    arrange(sampstart, sampend)
  
  # set up for parallel
  cl <- makePSOCKcluster(ncores)
  registerDoParallel(cl)
  
  # loop setup
  nsamp <- dim(samplist)[1]
  nsamp <- min(nsamp, nsampmax)
  dm_insamp <- list()
  
  # dopar in a function needs some special setup
  # https://stackoverflow.com/questions/6689937/r-problem-with-foreach-dopar-inside-function-called-by-optim
  dm_insamp <- foreach(
    sampi = 1:nsamp,
    .combine = rbind,
    .packages = c("data.table", "tidyverse", "zoo"),
    .export = ls(envir = globalenv())
  ) %dopar% {
  # ) %do% {
    # feedback
    print(paste0("DM sample stats for sample ", sampi, " of ", nsamp))
    
    # find sum stats for the current sample
    sampcur <- samplist[sampi, ]
    sumcur <- dm_rets[
      yearm >= sampcur$sampstart & yearm <= sampcur$sampend &
        !is.na(ret),
      .(
        rbar = mean(ret), tstat = mean(ret) / sd(ret) * sqrt(.N),
        min_nstock_long = min(nstock_long),
        min_nstock_short = min(nstock_short),
        nmonth = sum(!is.na(ret))
      ),
      by = c("sweight", "dmname")
    ]
    # find number of obs in the last year of the sample
    filtcur <- dm_rets[
      floor(yearm) == year(sampcur$sampend) &
        !is.na(ret),
      .(nlastyear = .N),
      by = c("sweight", "dmname")
    ]
    
    # combine sum stats with last year nobs
    sumcur <- sumcur %>%
      left_join(filtcur, by = c("sweight", "dmname")) %>%
      mutate(
        sampstart = sampcur$sampstart, sampend = sampcur$sampend
      )

    # expand with published signalnames and reorg
    pubnamelist = czsum[sampstart == sampcur$sampstart
        & sampend == sampcur$sampend, .(signalname)] %>% 
        rename(pubname = signalname)
    pubsumcur = expand_grid(pubnamelist, sumcur) %>% 
      select(pubname, sampstart, sampend, everything()) %>% 
      setDT()
    
    # add pairwise correlations    
    # with data.table takes only 2 sec per pubname
    for (pubi in 1:nrow(pubnamelist)) {      
      pubname = pubnamelist[pubi, ]$pubname

      # merge pub returns onto dm returns, temporarily
      tempret = czret[signalname == pubname & date >= sampcur$sampstart
        & date <= sampcur$sampend, .(date,ret)]
      dm_rets[tempret, temppubret := i.ret, on = .(yearm = date)]
      
      # Perform PPCA on the wide version of tempret
      tempret_wide <- dcast(tempret, date ~ pubname, value.var = "ret")
      pca_model <- pca(tempret_wide[,-1, with=FALSE], method = "ppca", nPcs = 5)
      pca_scores <- scores(pca_model)
      # Add the date back to PCA scores
      pca_scores <- data.table(date = tempret_wide$date, pca_scores)
      
      # compute correlation
      tempcor = dm_rets[yearm >= sampcur$sampstart & yearm <= sampcur$sampend
        , .(cor = cor(ret, temppubret, use = "pairwise")), by = c("dmname", "sweight")]
      tempcor$pubname = pubname

      # merge back onto sumcur
      pubsumcur[tempcor, cor := i.cor, on = c("pubname", "sweight", "dmname")]

      # clean up
      dm_rets[ , temppubret := NULL]
    } # end for pubi

    return(pubsumcur)
  } # end dm_insamp loop
  stopCluster(cl)
  
  # Merge with czsum
  # insampsum key is c(pubname,dmname). Each row is a dm strat that matches a pub
  insampsum <- czsum %>%
    transmute(
      pubname = signalname, rbar_op = rbar, tstat_op = tstat, sampstart, sampend,
      sweight = tolower(sweight)
    ) %>%
    left_join(
      dm_insamp,
      by = c('pubname', 'sweight','sampstart', 'sampend'),
    ) %>%
    arrange(pubname, desc(tstat))
  
  setDT(insampsum)
  
  return(insampsum)
} # end Sumstats function



SelectDMStrats <- function(insampsum, settings) {
  # input:
  #     insampsum = summary stats for each pubname, dmname combination
  #     dmset = settings for selection
  # output: matchcur = all pubname, dmname that satisfy dmset
  
  # add derivative statistics
  insampsum <- insampsum %>%
    group_by(sweight, sampstart, sampend) %>%
    arrange(desc(abs(tstat))) %>%
    mutate(rank_tstat = row_number()) %>%
    arrange(desc(abs(rbar))) %>%
    mutate(rank_rbar = row_number(), n_dm_tot = n()) %>%
    mutate(
      diff_rbar = abs(rbar * sign(rbar) - rbar_op),
      diff_tstat = abs(tstat * sign(rbar) - tstat_op)
    ) %>%
    setDT()
  
  # filter
  matchcur <- insampsum[
    diff_rbar <= settings$r_tol &
      diff_tstat <= settings$t_tol &
      diff_rbar / rbar_op <= settings$r_reltol &
      diff_tstat / tstat_op <= settings$t_reltol &
      min_nstock_long >= settings$minNumStocks/2 &
      min_nstock_short >= settings$minNumStocks/2 &
      abs(tstat) > settings$t_min &
      abs(tstat) < settings$t_max &
      rank_tstat / n_dm_tot <= settings$t_rankpct_min / 100 &
      nlastyear == 12 &   # tbc: make flexible
      nmonth >= 5*12 # tbc: make flexible
  ]
  
  print("summary of matching:")
  matchcur[, .(n_dm_match = .N, sampstart = min(sampstart), sampend = min(sampend)), by = "pubname"] %>%
    arrange(-n_dm_match) %>%
    print()
  
  return(matchcur)
  
  print("end selectStrats")
}

make_DM_event_returns <- function(
    match_strats,
    DMname = paste0('../Data/Processed/',
                    globalSettings$dataVersion, 
                    ' LongShort.RData'),
    npubmax = Inf,
    czsum,
    use_sign_info = TRUE
) {
  # input: match_strats = summary stats for each selected pubname, dmname pair
  #     outname = name of RDS output
  # you need to pass in czsum (can't use the global) because of
  # a mysterious dopar error (object 'czsum' not found)
  # output: for each pubname-eventDate, average dm returns
  gc()
  
  # read in DM strats
  dm_rets <- readRDS(DMname)$ret
  dm_info <- readRDS(DMname)$port_list
  
  dm_rets <- dm_rets %>%
    left_join(
      dm_info %>% select(portid, sweight),
      by = c("portid")
    ) %>%
    transmute(
      sweight,
      dmname = signalid,
      yearm,
      ret,
      nstock_long,
      nstock_short
    ) %>%
    setDT()
  
  cl <- makePSOCKcluster(ncores)
  registerDoParallel(cl)
  npub <- dim(czsum)[1]
  npub <- min(npub, npubmax)
  event_dm_scaled <- foreach(
    pubi = 1:npub,
    .combine = rbind,
    .packages = c("data.table", "tidyverse", "zoo")
  ) %dopar% {
    # feedback
    print(paste0("pubi ", pubi, " of ", npub))
    
    pubcur <- czsum[pubi, ]
    
    # select matching dm strats for the current pubname
    matchcur <- match_strats[pubname == pubcur$signalname]
    
    matchcur <- matchcur %>%
      transmute(sweight, dmname, sign = sign(rbar), rbar)
    
    # make an event time panel
    eventpan <- dm_rets %>%
      inner_join(matchcur, by = c("sweight", "dmname")) %>%
      transmute(
        candSignalname = dmname,
        eventDate = as.integer(round(12 * (yearm - pubcur$sampend))),
        sign,
        # scale returns
        ret_scaled = ret * sign / abs(rbar) * 100,
        # # sign returns (sanity check)
        # ret_scaled = ifelse(use_sign_info, sign*ret_scaled, ret_scaled),
        samptype = case_when(
          (yearm >= pubcur$sampstart) & (yearm <= pubcur$sampend) ~ "insamp",
          (yearm > pubcur$sampend) ~ "oos",
          TRUE ~ NA_character_
        )
      )
    
    if (use_sign_info==FALSE){
      # remove sign_info if requested (for testing)
      eventpan[ , ret_scaled := sign*ret_scaled]
    }
    
    # average down to one matched return per event date
    eventsumscaled <- eventpan[, .(dm_mean = mean(ret_scaled), 
                                   dm_sd = sd(ret_scaled), dm_n = .N),
                               by = c("eventDate",'samptype')
    ] %>%
      mutate(pubname = pubcur$signalname)
    
    return(eventsumscaled)
  } # end do pubi = 1:npub
  
  stopCluster(cl)
  
  return(event_dm_scaled)
} # end MakeMatchedPanel

adj_R2_with_PPCA <- function(
    DMname = paste0('../Data/Processed/',
                    globalSettings$dataVersion, 
                    ' LongShort.RData'),
    nsampmax = Inf) {
  # convert czsum to data.table (this should be done more globally)
  setDT(czsum)
  
  # read in DM strats
  dm_rets <- readRDS(DMname)$ret
  dm_info <- readRDS(DMname)$port_list
  
  dm_rets <- dm_rets %>%
    left_join(
      dm_info %>% select(portid, sweight), by = c("portid")
    ) %>%
    transmute(
      sweight, dmname = signalid, yearm, ret, nstock_long, nstock_short
    ) %>%
    setDT()
  
  # Finds sum stats for dm in each pub sample
  # the output for this can be used for all dm selection methods
  samplist <- czsum %>%
    distinct(sampstart, sampend) %>%
    arrange(sampstart, sampend)
  
  # set up for parallel
  cl <- makePSOCKcluster(ncores)
  registerDoParallel(cl)
  
  # loop setup
  nsamp <- dim(samplist)[1]
  nsamp <- min(nsamp, nsampmax)
  dm_insamp <- list()
  
  # dopar in a function needs some special setup
  # https://stackoverflow.com/questions/6689937/r-problem-with-foreach-dopar-inside-function-called-by-optim
  start_time <- Sys.time()
  print(start_time)
  dm_insamp <- foreach(
    sampi = 1:nsamp,
    .combine = rbind,
    .packages = c("data.table", "tidyverse", "zoo", "pcaMethods", "broom"),
    .export = c("samplist", "dm_rets", "czsum", "czret", "nsamp")
  ) %dopar% {
    #) %do% {
    # feedback
    print(paste0("DM sample stats for sample ", sampi, " of ", nsamp))
    print(Sys.time())
    # find sum stats for the current sample
    sampcur <- samplist[sampi, ]
    sumcur <- dm_rets[
      yearm >= sampcur$sampstart & yearm <= sampcur$sampend &
        !is.na(ret),
      .(
        rbar = mean(ret), tstat = mean(ret) / sd(ret) * sqrt(.N),
        min_nstock_long = min(nstock_long),
        min_nstock_short = min(nstock_short),
        nmonth = sum(!is.na(ret))
      ),
      by = c("sweight", "dmname")
    ]
    # find number of obs in the last year of the sample
    filtcur <- dm_rets[
      floor(yearm) == year(sampcur$sampend) &
        !is.na(ret),
      .(nlastyear = .N),
      by = c("sweight", "dmname")
    ]
    # Perform the left join
    sumcur <- sumcur[filtcur, on = .(sweight, dmname)]
    
    # Add sampstart and sampend columns
    sumcur[, `:=`(sampstart = sampcur$sampstart, sampend = sampcur$sampend)]
    
    # expand with published signalnames available by then and reorg
    pubnamelist = czsum[sampend <= sampcur$sampend, .(signalname)] %>% 
      rename(pubname = signalname)
    # pubsumcur = expand_grid(pubnamelist, sumcur) %>% 
    #   select(pubname, sampstart, sampend, everything()) %>% 
    #   setDT()
    # merge pub returns onto dm returns, temporarily
    tempret_pca = czret[signalname %in% pubnamelist$pubname & date >= sampcur$sampstart
                        & date <= sampcur$sampend, .(signalname, date, ret)]
    npcs <- min(5, pubnamelist[, .N])
    # Pivot the data to wide format
    # Pivot the data to wide format
    tempret_wide <- dcast(tempret_pca, date ~ signalname, value.var = "ret")
    
    # Check the number of columns
    if (ncol(tempret_wide) < 7) {
      # Run regression with the columns in tempret_wide
      formula_temp <- paste('ret ~ ',  colnames(tempret_wide)[-1] %>% paste(., collapse = ' + ')) %>% as.formula()
      dm_rets2 <- dm_rets[tempret_wide, on = .(yearm = date)]
      dm_rets2 <- dm_rets2[!is.na(dmname)]
      dm_rets2[, available_obs := .N, by  = c("dmname", "sweight")]
      sumcur[, npcs := 0]
      
    } else {
      # Perform PCA and run regression with PCA scores
      pca_model <- pca(tempret_wide[,-1, with=FALSE] %>% as.matrix(), method = "ppca", nPcs = npcs)
      pca_scores <- scores(pca_model)
      formula_pca <- paste('ret ~ ',  colnames(pca_scores) %>% paste(., collapse = ' + ')) %>% as.formula()
      pca_scores <- data.table(date = tempret_wide$date, pca_scores)
      dm_rets2 <- dm_rets[pca_scores, on = .(yearm = date)]
      dm_rets2 <- dm_rets2[!is.na(dmname)]
      dm_rets2[, available_obs := .N, by  = c("dmname", "sweight")]
      dm_rets2[available_obs > 30, adj_r2 := summary(lm(formula = formula_pca, data = .SD))$adj.r.squared, by = c("dmname", "sweight")]
      sumcur[, npcs := npcs]
    }
    
    adj_r2_dt <- dm_rets2[, {
      model <- lm(formula = if (ncol(tempret_wide) < 7) formula_temp else formula_pca, data = .SD)
      model_summary <- summary(model)
      .(r2 = model_summary$r.squared,
        adj_r2 = model_summary$adj.r.squared,
        N_pca = .N)
    }, by = c("dmname", "sweight")]
    
    test <- merge(sumcur, adj_r2_dt)
    return(test)
  } # end dm_insamp loop
  stop_time <- Sys.time()
  print(stop_time - start_time)
  stopCluster(cl)
  
  return(dm_insamp)
} # end Sumstats function


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
# Define a function to handle repeated tasks with different conditions
process_event_time_returns <- function(dm_name, match_strats, npubmax, czsum, use_sign_info) {
  start_time <- Sys.time()
  event_time <- make_DM_event_returns(
    DMname = dm_name,
    match_strats = match_strats,
    npubmax = npubmax,
    czsum = czsum,
    use_sign_info = use_sign_info
  )
  stop_time <- Sys.time()
  print(stop_time - start_time)
  
  return(event_time)
}
