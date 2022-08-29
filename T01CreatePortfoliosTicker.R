# SETUP ====
rm(list=ls())
gc()

setwd('/home/alex/GitProjects/flex-mining')

source('0_Environment.R')
# Supress summary message
options(dplyr.summarise.inform = FALSE)
#####
library(fastTextR)
library(wordspace)
library(janeaustenr)
library(tidytext)
# library(dplyr)
# library(data.table)

################
# Functions
###############

# Faster ntile

fntile <- function(x, n) {
  x.length <- length(x)
  return(as.integer(n * {frank(x, ties.method = "first") - 1} / x.length + 1))
}


# Create portfolios for one word and companies' names

create_single_set_returns_placebo <- function(seed = 1, n_tiles = 5 ){
  
  # n_tiles is the number of groups
  
  # word_vec <- sentence_to_vec(word_to_compare)
  
  unique_tickers[, signal := NULL]
  set.seed(seed)
  unique_tickers[, signal := runif(.N)]
  
  setkey(unique_tickers, lag_ticker)
  
  setkey(crspm_dt, lag_ticker)
  
  crspm_dt[, signal := NULL]
  
  crspm_dt[, bin1 := NULL]
  
  crspm_dt[unique_tickers, signal := signal]
  
  crspm_dt[!is.na(signal), bin1 := fntile(signal, 5), by = yyyymm]
  
  returns_dt <- crspm_dt[!is.na(bin1) & !is.na(lag_me)
                                   & !is.na(ret),
                                   .(ew_mean = mean(ret, na.rm=TRUE),
                                     vw_mean = weighted.mean(ret, lag_me,
                                                             na.rm=TRUE)),
                                   by = .(yyyymm, bin1)]
  
  dt_ret_melted <- dcast(returns_dt, yyyymm ~ bin1, value.var = c('ew_mean', 'vw_mean'))
  port_h_ew <- paste0('ew_mean_', n_tiles)
  port_h_vw <- paste0('vw_mean_', n_tiles)
  dt_ret_melted[, ls_ew := get(port_h_ew) - ew_mean_1]
  dt_ret_melted[, ls_vw := get(port_h_vw) - vw_mean_1]
  dt_portfolio_returns <- melt(dt_ret_melted, id.vars = c('yyyymm'),
                               measure.vars = c('ls_vw', 'ls_ew'
                                                ,port_h_ew, port_h_vw,
                                                'ew_mean_1', 'vw_mean_1'
                               ),
                               variable.name = 'type',
                               value.name = 'ret'
  )
  
  dt_portfolio_returns[, seed := seed]
  
  
  return(dt_portfolio_returns)
}

create_single_set_returns_edit <- function(word_to_compare, n_tiles = 5 ){
  
  # n_tiles is the number of groups
  
  unique_tickers[, signal := NULL]
  
  unique_tickers[, signal := adist(lag_ticker,
                                   word_to_compare)/pmax(nchar(lag_ticker),
                                                         nchar(word_to_compare))]
  
  setkey(unique_tickers, lag_ticker)
  
  setkey(crspm_dt, lag_ticker)
  
  crspm_dt[, signal := NULL]
  
  crspm_dt[, bin1 := NULL]
  
  crspm_dt[unique_tickers, signal := signal]
  
  crspm_dt[!is.na(signal), bin1 := fntile(signal, 5), by = yyyymm]
  
  
  returns_dt <- crspm_dt[!is.na(bin1) & !is.na(lag_me)
                                   & !is.na(ret),
                                   .(ew_mean = mean(ret, na.rm=TRUE),
                                            vw_mean = weighted.mean(ret, lag_me,
                                                                    na.rm=TRUE),
                                     .N),
                          by = .(yyyymm, bin1)][N > 30, ]
  
  dt_ret_melted <- dcast(returns_dt, yyyymm ~ bin1, value.var = c('ew_mean', 'vw_mean'))
  port_h_ew <- paste0('ew_mean_', n_tiles)
  port_h_vw <- paste0('vw_mean_', n_tiles)
  dt_ret_melted[, ls_ew := get(port_h_ew) - ew_mean_1]
  dt_ret_melted[, ls_vw := get(port_h_vw) - vw_mean_1]
  dt_portfolio_returns <- melt(dt_ret_melted, id.vars = c('yyyymm'),
                               measure.vars = c('ls_vw', 'ls_ew'
                                                ,port_h_ew, port_h_vw,
                                                'ew_mean_1', 'vw_mean_1'
                                                ),
                               variable.name = 'type',
                               value.name = 'ret'
                               )
  
  dt_portfolio_returns[, word := word_to_compare]
  
  
  return(dt_portfolio_returns)
}


# Create many ticker portfolios

create_ticker_portfolios_edit <- function(nports = 600,
                                    n_tiles = 5,
                                    word_list  = finance_words[, word_lower]){
  print(Sys.time())
  # nports = Number of portfolios
  # n_tiles = Number of groups per order
  
  # Number of combinations 
  n_ports_per_order = 2
  # 3 different letters to choose in the tickers
  max_n_words = ceiling(nports/( n_ports_per_order)) 
  # Count Portfolios
  current_n_portfolios <- 0
  # Empty dt
  ticker_ports_long <- data.table()
  
  # Loop in seeds and letter from ticker
  for (i_word in 1:max_n_words) {
    skip_to_next <- FALSE
    finance_word <- word_list[i_word]
    # Create new portfolio
    
    new_port <- tryCatch(create_single_set_returns_edit(
      word_to_compare = finance_word,
      n_tiles = n_tiles),
      error = function(e) { skip_to_next <<- TRUE})
    
    
    
    if(skip_to_next) {
      print('Skipping')
      print(finance_word)
      next }   
    # Bind new and old set of portfolios
    ticker_ports_long <- rbind(ticker_ports_long,  new_port)
    # Count new portfolios
    current_n_portfolios <- current_n_portfolios + n_ports_per_order
    
    if(i_word %% 100 == 0){
      print(Sys.time())
      print('N portfolios')
      print(current_n_portfolios)
      print(finance_word)
    }
    
    # Early stop if current # portfolios > desired
    if(current_n_portfolios > nports){
      print('done')
      print('N portfolios')
      print(current_n_portfolios)
      return(ticker_ports_long)
    }
  }
  return(ticker_ports_long)
}


create_placebo_portfolios <- function(nports = 600,
                                    n_tiles = 5){
  # nports = Number of portfolios
  # n_tiles = Number of groups per order
  
  # Number of combinations 
  n_ports_per_order = 2
  # 3 different letters to choose in the tickers
  max_seeds = ceiling(nports/( n_ports_per_order)) 
  # Count Portfolios
  current_n_portfolios <- 0
  # Empty dt
  ticker_ports_long <- data.table()
  
  # Loop in seeds and letter from ticker
  for (i_seed in 1:max_seeds) {
    skip_to_next <- FALSE
    # Create new portfolio
    
    new_port <- tryCatch(create_single_set_returns_placebo(
      seed = i_seed,
      n_tiles = n_tiles),
      error = function(e) { skip_to_next <<- TRUE})
    
    
    
    if(skip_to_next) {
      print('Skipping')
      print(finance_word)
      next }   
    # Bind new and old set of portfolios
    ticker_ports_long <- rbind(ticker_ports_long,  new_port)
    # Count new portfolios
    current_n_portfolios <- current_n_portfolios + n_ports_per_order
    
    if(i_seed %% 100 == 0){
      print(Sys.time())
      print('N portfolios')
      print(current_n_portfolios)
    }
    
    # Early stop if current # portfolios > desired
    if(current_n_portfolios > nports){
      print('done')
      print('N portfolios')
      print(current_n_portfolios)
      return(ticker_ports_long)
    }
  }
  return(ticker_ports_long)
}

# t-stat with removed NA


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


###############
# Prepare data
###############

finance_words <- read_fst('../Data/Intermediate/finance_words.fst') %>%
  as.data.table() %>%
  setorder(-doc_prop)
# Read data
crspm_dt <- as.data.table(read_fst('../Data/Intermediate/crspm.fst'))

# Month format
crspm_dt[, month := month(date)]

setkey(crspm_dt, permno, date)

crspm_dt[ ,
          ticker :=  zoo::na.locf(ticker,na.rm =  FALSE),
          by = permno]

# Temporary ticker so that new stocks are added in June
crspm_dt[month(date) == 5, tic_temp := tolower(ticker)]

# Ticker available in real time
crspm_dt[, lag_ticker := shift(tic_temp, 1),
         by = .(permno)]

# Fill-forward missing tickers
crspm_dt[ ,
          lag_ticker :=  zoo::na.locf(lag_ticker,na.rm =  FALSE),
          by = permno]

# Lagged market cap
crspm_dt[, lag_me := shift(me), by = permno]

# Remove if missing lagged ticker or lagged market cap

crspm_dt <- crspm_dt[!is.na(lag_me) & !is.na(lag_ticker) & nchar(lag_ticker) >= 3, ]


unique_tickers <- crspm_dt[, .(lag_ticker = unique(lag_ticker))]

##########################################

ticker_porfolios_dt <- create_ticker_portfolios_edit(6000, 5)

write_fst(ticker_porfolios_dt, '../Data/Intermediate/ticker_ports_dt.fst')

summary_stats_per_port <- ticker_porfolios_dt[,
                                              .(t_stat = f.custom.t(ret),
                                                mean_ret = mean(ret, na.rm = TRUE),
                                                sharpe = f.sharp(ret)
                                              ),
                                              by = .(word, type)]


# summary_stats_per_port[, f.describe_numeric(t_stat), by = type]
# 
# summary_stats_per_port[, f.describe_numeric(mean_ret), by = type]
# 
# summary_stats_per_port[, f.describe_numeric(sharpe), by = type]

ggplot(summary_stats_per_port[type == 'ls_ew' |
                                type == 'ls_vw',], aes(x = t_stat, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-4,4))

ggplot(summary_stats_per_port[type == 'ls_ew' |
                                type == 'ls_vw',], aes(x = mean_ret, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-0.5,0.5))

ggplot(summary_stats_per_port[type == 'ls_ew' |
                                type == 'ls_vw',], aes(x = sharpe, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-0.5,0.5))

##########################################

jane_words <- austen_books() %>%
  group_by(book) %>%
  mutate(line = row_number(),
         chapter = cumsum(str_detect(text, regex("^chapter [\\divxlc]",
                                                 ignore_case = TRUE)))) %>%
  ungroup() %>%
  unnest_tokens(word, text) %>%
  anti_join(get_stopwords()) %>%
  count(word, sort = TRUE)   %>%
  as.data.table()

word_list_jane <- jane_words[, word]

ticker_porfolios_jane_dt <- create_ticker_portfolios_edit(6000, 5,
                                               word_list = word_list_jane)

write_fst(ticker_porfolios_jane_dt,
          '../Data/Intermediate/names_ports_dt_5_1_jane.fst')

# Portfolio Statistics ====

summary_stats_per_port_jane <- ticker_porfolios_jane_dt[,
                                              .(t_stat = f.custom.t(ret),
                                                mean_ret = mean(ret, na.rm = TRUE),
                                                sharpe = f.sharp(ret)
                                              ),
                                              by = .(word, type)]

write_fst(summary_stats_per_port_jane,
          '../Data/Intermediate/summary_stats_per_port_5_1_jane.fst')

ggplot(summary_stats_per_port_jane[type == 'ls_ew' |
                                type == 'ls_vw',], aes(x = t_stat, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-4,4))

ggplot(summary_stats_per_port_jane[type == 'ls_ew' |
                                type == 'ls_vw',], aes(x = mean_ret, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-0.5,0.5))

ggplot(summary_stats_per_port_jane[type == 'ls_ew' |
                                type == 'ls_vw',], aes(x = sharpe, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-0.5,0.5))

##################################
# Placebo Portfolios 
##################################

placebo_porfolios_dt <- create_placebo_portfolios(6000, 5)

write_fst(placebo_porfolios_dt,
          '../Data/Intermediate/placebo_porfolios_5_1.fst')

# Portfolio Statistics ====

summary_stats_per_port_placebo <- placebo_porfolios_dt[,
                                                        .(t_stat = f.custom.t(ret),
                                                          mean_ret = mean(ret, na.rm = TRUE),
                                                          sharpe = f.sharp(ret)
                                                        ),
                                                        by = .(seed, type)]

write_fst(summary_stats_per_port_placebo,
          '../Data/Intermediate/summary_stats_per_port_5_1_placebo.fst')

summary_stats_per_port_placebo[, f.describe_numeric(t_stat), by = type]

ggplot(summary_stats_per_port_placebo[type == 'ls_ew' |
                                     type == 'ls_vw',], aes(x = t_stat, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-4,4))

ggplot(summary_stats_per_port_placebo[type == 'ls_ew' |
                                     type == 'ls_vw',], aes(x = mean_ret, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-0.5,0.5))

ggplot(summary_stats_per_port_placebo[type == 'ls_ew' |
                                     type == 'ls_vw',], aes(x = sharpe, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-0.5,0.5))
