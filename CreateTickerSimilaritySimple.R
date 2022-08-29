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
# library(dplyr)
# library(data.table)
#####
# Faster ntile
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


######
finance_words <- read_fst('../Data/Intermediate/finance_words.fst') %>%
  as.data.table() %>%
  setorder(-doc_prop)
# Read data
crspm_dt <- as.data.table(read_fst('../Data/Intermediate/crspm.fst'))

# Month format
crspm_dt[, month := month(date)]

setkey(crspm_dt, permno, date)

# Fill-forward missing tickers
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

ns <- crspm_dt[, .N, by = yyyymm]

crspm_dt_finance_words <- crspm_dt[, .(permno, yyyymm, lag_ticker)]

tickers_dt <- crspm_dt_finance_words[, .(lag_ticker = unique(lag_ticker))]


##########################################

setkey(tickers_dt, lag_ticker)

for (word_to_compare in finance_words[1:6000, word_lower]) {
  
  tickers_dt[, (word_to_compare) := adist(lag_ticker, word_to_compare)]
  
  # print(word_to_compare)
}

write_fst(ticker_porfolios_jane_dt,
          '../Data/Intermediate/finance_words_char.fst')

#########################

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