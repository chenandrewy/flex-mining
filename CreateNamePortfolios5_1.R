# SETUP ====
setwd('/home/alex/Github/flex-mining')

source('0_Environment.R')
# Supress summary message
options(dplyr.summarise.inform = FALSE)
#####
library(fastTextR)
library(wordspace)
# library(dplyr)
# library(data.table)
#####
# Faster ntile

fntile <- function(x, n) {
  x.length <- length(x)
  return(as.integer(n * {frank(x, ties.method = "first") - 1} / x.length + 1))
}


# Create portfolios for one word and companies' names

create_single_set_returns_placebo <- function(seed, n_tiles = 5 ){
  
  # n_tiles is the number of groups
  
  setkey(crspm_dt_processed, permno, yyyymm)
  
  crspm_dt_processed[, signal := NULL]
  
  crspm_dt_processed[, bin1 := NULL]
  
  set.seed(seed)
  crspm_dt_processed[, signal := sample(1:.N, .N), by = yyyymm]
  
  crspm_dt_processed[!is.na(signal), bin1 := fntile(signal, 5), by = yyyymm]
  
  returns_dt <- crspm_dt_processed[!is.na(signal), .(ew_mean = mean(ret, na.rm=TRUE),
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
                                                # ,port_h_ew, port_h_vw,
                                                # 'ew_mean_1', 'vw_mean_1'
                               ),
                               variable.name = 'type',
                               value.name = 'ret'
  )
  
  dt_portfolio_returns[, seed := seed]
  
  
  return(dt_portfolio_returns)
}
# a <- create_single_set_returns_placebo(1)
create_single_set_returns <- function(word_to_compare, n_tiles = 5 ){
  
  # n_tiles is the number of groups
  
  word_vec <- sentence_to_vec(word_to_compare)
  
  unique_names[, signal := NULL]
  
  unique_names[, signal := sim_from_list(vector, word_vec)]
  
  setkey(unique_names, lag_name)
  
  setkey(crspm_dt_processed, lag_name)
  
  crspm_dt_processed[, signal := NULL]
  
  crspm_dt_processed[, bin1 := NULL]
  
  crspm_dt_processed[unique_names, signal := signal]
  
  crspm_dt_processed[!is.na(signal), bin1 := fntile(signal, 5), by = yyyymm]
  
  returns_dt <- crspm_dt_processed[!is.na(bin1) & !is.na(lag_me)
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
                                                # ,port_h_ew, port_h_vw,
                                                # 'ew_mean_1', 'vw_mean_1'
                                                ),
                               variable.name = 'type',
                               value.name = 'ret'
                               )
  
  dt_portfolio_returns[, word := word_to_compare]
  
  
  return(dt_portfolio_returns)
}

# Create many ticker portfolios

create_names_portfolios <- function(nports = 600,
                                    n_tiles = 5,
                                    word_list  = finance_words[, word_lower]){
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
    
    new_port <- tryCatch(create_single_set_returns(
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

# t-stat with removed NA


f.custom.t <- function(x){
  return(t.test(x, na.action = na.omit)$statistic)
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

#####
name_sim <-function(string, word_to_compare){
  words <- unlist(strsplit(string, ' ', TRUE))
  sims <- word_sim(words, word_to_compare)
  text_sim <- mean( sims, na.rm = TRUE)
  return(text_sim)
}
name_sim_vec <- function(vec_strings, word_to_compare){
  return(sapply(vec_strings, name_sim, word_to_compare))
}
word_sim <- function(words_1, word_2){
  vec_1 <- ft_word_vectors(model, words_1)
  # print(dim(vec_1))
  vec_2 <- as.numeric(ft_word_vectors(model, word_2))
  # print(dim(vec_2))
  return(cos_sim(vec_1, vec_2))
  
}

sentence_to_vec <- function(string){
  words <- unlist(strsplit(string, ' ', TRUE))
  vec  <- apply(ft_word_vectors(model, words), 2, mean)
  nvec <- norm(vec, type="2")
  if(nvec == 0){
    # print(string)
    return(NA)
  }
  vec <- vec/nvec
  return(vec)
}


cos_sim <- function(x, y){
  sim <- crossprod(t(x), y)/(rowNorms(x)*norm(y, type="2"))
  print(sim)
  return(sim)
  
  
}

vec_from_word <- function(x){
  return(ft_word_vectors(model, x))
}

sim_from_list <- function(list_of_vecs, word_vec){
  return(sapply(list_of_vecs, cos_sim_list, word_vec))
  
}

cos_sim_list <- function(x, y){
  x <- unlist(x)
  sim <- sum(x*y)
  
  return(sim)
}



######
finance_words <- read_fst('../Data/Intermediate/finance_words.fst') %>%
  as.data.table() %>%
  setorder(-doc_prop)

crspm_dt_processed <- read_fst('../Data/Intermediate/crsp_with_names.fst') %>%
  as.data.table()

model <- ft_load('../Data/Intermediate/cc.en.300.bin')


unique_names <- crspm_dt_processed[, .(lag_name = unique(lag_name))]




unique_names[, vector :=   lapply(lag_name, sentence_to_vec)  ]

ticker_porfolios_dt <- create_names_portfolios(6000, 5)

write_fst(ticker_porfolios_dt, '../Data/Intermediate/names_ports_dt_5_1.fst')

# Portfolio Statistics ====

summary_stats_per_port <- ticker_porfolios_dt[,
                                            .(t_stat = f.custom.t(ret),
                                              mean = mean(ret, na.rm = TRUE),
                                              sharpe = f.sharp(ret)
                                            ),
                                            by = .(word, type)]

write_fst(summary_stats_per_port, '../Data/Intermediate/summary_stats_per_port_5_1.fst')

summary_stats_per_port[, f.describe_numeric(t_stat), by = type]

ggplot(summary_stats_per_port, aes(x = t_stat, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-4,4))


# Jane Austin
library(janeaustenr)
library(tidytext)

original_books <- austen_books() %>%
  group_by(book) %>%
  mutate(line = row_number(),
         chapter = cumsum(str_detect(text, regex("^chapter [\\divxlc]",
                                                 ignore_case = TRUE)))) %>%
  ungroup()


tidy_books <- original_books %>%
  unnest_tokens(word, text)

cleaned_books <- tidy_books %>%
  anti_join(get_stopwords())

freq_words <- cleaned_books %>%
  count(word, sort = TRUE) 

ticker_porfolios_jane_dt <- create_names_portfolios(6000, 5,
                                               word_list = freq_words$word)

write_fst(ticker_porfolios_jane_dt,
          '../Data/Intermediate/names_ports_dt_5_1_jane.fst')

# Portfolio Statistics ====

summary_stats_per_port_jane <- ticker_porfolios_jane_dt[,
                                              .(t_stat = f.custom.t(ret),
                                                mean = mean(ret, na.rm = TRUE),
                                                sharpe = f.sharp(ret)
                                              ),
                                              by = .(word, type)]

write_fst(summary_stats_per_port_jane,
          '../Data/Intermediate/summary_stats_per_port_5_1_jane.fst')

summary_stats_per_port_jane[, f.describe_numeric(t_stat), by = type]

ggplot(summary_stats_per_port_jane, aes(x = t_stat, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-4,4))



# Shuffle

# 5 - 1
