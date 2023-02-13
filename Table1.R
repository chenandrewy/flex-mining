################################
# Setup
################################
# Setup -------------------------------------------------------------------
cat("\f")  
rm(list=ls())
gc()
library(MASS)
library(tidyverse)
library(data.table)
library(haven)
library(xtable)
require(RcppRoll)
library(glue)
library(janitor)
library(lubridate)
library(stringr)
################################
# Functions
################################

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

f.desc.returns <- function(dt_portfolio_returns){
  sumsignal_isl = dt_portfolio_returns %>% 
    filter(sample == 'IS') %>% 
    group_by( bin) %>% 
    summarize(rbar_is = mean(ret),
              tstat_is = mean(t_30y_l)
    ) %>% 
    ungroup()  
  
  sumsignal_oos = dt_portfolio_returns %>% 
    filter(sample == 'OOS') %>% 
    group_by( bin) %>% 
    summarize(rbar_oos = mean(ret), 
              tstat_oos = rbar/sd(ret)*sqrt(n()),
              
    ) %>% 
    ungroup()  %>%
    left_join(sumsignal_isl) %>% arrange(bin) %>%
    relocate(rbar_is, .after = bin) %>%
    # mutate(decay = 1 - rbar/rbar_is) %>%
    # mutate(decay = ifelse(decay > 1, NA, decay)) %>%
    relocate(c(rbar_is,tstat,  tstat_is), .after = last_col())
  
  return(sumsignal_oos)
}

f.ls.past.returns <- function(n_tiles, name_var){
  
  yz_dt[!is.na(get(name_var)) & month(date) == 6,
        var_sort := as.factor(fntile(get(name_var), n_tiles)), by = date]
  
  
  yz_dt[ ,
         var_sort :=  zoo::na.locf(var_sort,na.rm =  FALSE),
         by = signalname]
  
  
  yz_dt[!is.na(var_sort), bin := paste0('bin_', var_sort)]
  
  returns_dt <- yz_dt[!is.na(bin) & !is.na(ret),
                      .(ret_oos = mean(ret, na.rm=TRUE),
                        ret_is = mean(get(name_var), na.rm=TRUE),
                        .N),
                      by = .(date, bin)]
  
  yz_dt[, bin := NULL]
  yz_dt[, var_sort := NULL]
  
  
  dt_ret_melted <- dcast(returns_dt, date ~ bin, value.var = c('ret_oos', 'ret_is', 't_30y_l'))
  
  port_h_oos <- paste0('ret_oos_bin_', n_tiles)
  port_h_is <- paste0('ret_is_bin_', n_tiles)
  dt_ret_melted[, ret_oos_LS := get(port_h_oos) - ret_oos_bin_1]
  dt_ret_melted[, ret_is_LS := get(port_h_is) - ret_is_bin_1]
  dt_portfolio_returns <- melt(dt_ret_melted, id.vars = c('date'),
                               measure.vars = c(paste0('ret_oos_bin_', 1:n_tiles),
                                                paste0('ret_is_bin_', 1:n_tiles),
                                                'ret_oos_LS', 'ret_is_LS'
                               ), 
                               variable.name = 'type',
                               value.name = 'ret')
  
  dt_portfolio_returns[, sample := ifelse(grepl('oos', type), 'OOS', 'IS') ]
  dt_portfolio_returns[, bin :=   str_extract(type, '\\d+|LS')]
  
  sumsignal_oos <- f.desc.returns(dt_portfolio_returns)
  sumsignal_oos_pre_2003 <- f.desc.returns(dt_portfolio_returns[date < '2003-06-30'])
  sumsignal_oos_post_2003 <- f.desc.returns(dt_portfolio_returns[date >= '2003-06-30'])
  
  
  
  
  
  return(list(sumsignal_oos = sumsignal_oos,
              sumsignal_oos_pre_2003 = sumsignal_oos_pre_2003,
              sumsignal_oos_post_2003 = sumsignal_oos_post_2003,
              rets = dt_portfolio_returns))
  
}

################################
# Table 1a
################################

signaldoc <- fread('SignalsTheoryChecked.csv') %>%
  mutate(theory1 = theory2) %>% mutate(sampstart = ymd(sampstart),
                                       sampend = ymd(sampend)) %>%
  mutate(sample_size = time_length(difftime(sampend, sampstart), "years") ) 

signaldoc%>% summarise( mean(sample_size), median(sample_size) ) %>%
  mutate_if(is.numeric, round)


temp = read_sas('../Data Yan-Zheng/Yan_Zheng_RFS_Data.sas7bdat')

#####
# EW
####

dir.create('../Tables1')

var_types <- c('ddiff_vw', 'ddiff_ew')
var_type <- var_types[1]
for (var_type in var_types) {
  
  str_to_add  <- str_extract(var_type, '_.*')
  
  
  yz = temp %>%
    mutate(
      signalname = paste(transformation, fsvariable, sep = '.')
    ) %>%
    transmute(
      signalname, date = DATE, ret = 100*get(var_type)
    )
  
  
  sumsignal_all = yz %>% 
    group_by(signalname) %>% 
    summarize(rbar = mean(ret), nmonth = n(), stdev = sd(ret),
              sharpe = f.sharp(ret),
              tstat = rbar/sd(ret)*sqrt(nmonth)) %>% 
    ungroup() %>% as.data.table()
  
  Summary_Statistics <- sumsignal_all %>% 
    summarise(across(where(is.numeric), .fns = 
                       list(Count =  ~  n(),
                            Mean = mean,
                            SD = sd,
                            Min = min,
                            q01 = ~quantile(., 0.01), 
                            q05 = ~quantile(., 0.01), 
                            q25 = ~quantile(., 0.25), 
                            Median = median,
                            q75 = ~quantile(., 0.75),
                            q95 = ~quantile(., 0.95),
                            q99 = ~quantile(., 0.99),
                            Max = max ))) %>%
    pivot_longer(everything(), names_sep = "_", names_to = c( "variable", ".value")) 
  # %>%  mutate_if(is.numeric, round, 2)
  
  fwrite(Summary_Statistics, glue('../Tables1/Summary_Statistics{str_to_add}.csv'))
  
  Summary_Statistics
  
  print(xtable(Summary_Statistics, caption = 'Summary Statistics YZ All',
               type = "latex", include.rownames=FALSE))
  
  
  ################################
  # Table 1b
  ################################
  
  # Returns based on past returns
  # Basically creating a portfolio
  
  yz_dt <- yz %>% as.data.table() %>% setkey(signalname, date)
  
  yz_dt[, ret_30y_l := shift(frollmean(ret, 12*30, NA)), by = signalname]
  yz_dt[, t_30y_l := shift(frollapply(ret, 12*30, f.custom.t, fill = NA)), by = signalname]
  
  
  ############################
  
  n_tiles <- 5
  
  
  
  name_var <- 'ret_30y_l'
  test <- f.ls.past.returns(n_tiles, name_var)
  
  # print(xtable(test$sumsignal_oos, 
  #              caption = 'Out-of-Sample Portfolios of Strategies Sorted on Past 30 Years of Returns',
  #              type = "latex"), include.colnames=FALSE)
  
  fwrite(test$sumsignal_oos,  glue('../Tables1/sumsignal_oos_30y{str_to_add}.csv'))
  
  fwrite(test$sumsignal_oos_pre_2003,  glue('../Tables1/sumsignal_oos_30y_pre_2003_{str_to_add}.csv'))
  
  fwrite(test$sumsignal_oos_post_2003,  glue('../Tables1/sumsignal_oos_30y_post_2003_{str_to_add}.csv'))
  
}

