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
library(parallel)
library(janeaustenr)
library(tidytext)
numCores <- detectCores()

#####


custom_dist <- function(word_to_compare, ticker, len_ticker){
  return(adist(ticker, word_to_compare)/pmax(len_ticker, nchar(word_to_compare)))
  
}
######
finance_words <- read_fst('../Data/Intermediate/finance_words.fst') %>%
  as.data.table() %>%
  setorder(-doc_prop)
# Read data
crspm_dt <- as.data.table(read_fst('../Data/Intermediate/crsp_with_names.fst'))

tickers_dt <- crspm_dt[!is.na(ticker), .(ticker_id = tolower(unique(ticker)))]

tickers_dt[, len_ticker := nchar(ticker_id)]

text_df <- copy(tickers_dt)


word_list_finance <- finance_words[1:6000, word_lower]

text_df[,
        (word_list_finance) := 
          mclapply(word_list_finance, custom_dist, ticker_id, len_ticker,
                   mc.cores = numCores)]

text_df[, len_ticker :=  NULL]



finance_df_melt <- melt(text_df, id.vars = 'ticker_id',
                        variable.name = 'word',
                        value.name = "distance")

write_fst(finance_df_melt,
          '../Data/Intermediate/finance_words_ticker.fst')
##################
# Jane Austen
##################

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

text_df_jane <- copy(tickers_dt)

word_list_jane <- jane_words[1:6000, word]

text_df_jane[,
        (word_list_jane) := 
          mclapply(word_list_jane, custom_dist, ticker_id, len_ticker,
                   mc.cores = numCores)]

text_df_jane[, len_ticker :=  NULL]

jane_df_melt <- melt(text_df_jane, id.vars = 'ticker_id',
                        variable.name = 'word',
                        value.name = "distance")

write_fst(jane_df_melt,
          '../Data/Intermediate/jane_words_ticker.fst')
