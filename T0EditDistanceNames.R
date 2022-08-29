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


custom_dist <- function(word_to_compare, processed_conm, len_name){
  return(adist(processed_conm, word_to_compare)/pmax(len_name, nchar(word_to_compare)))
  
}
######
finance_words <- read_fst('../Data/Intermediate/finance_words.fst') %>%
  as.data.table() %>%
  setorder(-doc_prop)
# Read data
crspm_dt <- as.data.table(read_fst('../Data/Intermediate/crsp_with_names.fst'))

comnams_dt <- crspm_dt[!is.na(lag_name), .(processed_conm = unique(lag_name))]



text_df <- copy(comnams_dt)


text_df[, len_name := nchar(processed_conm)]

word_list_finance <- finance_words[1:6000, word_lower]

text_df[,
        (word_list_finance) := 
          mclapply(word_list_finance, custom_dist, processed_conm, len_name,
                   mc.cores = numCores)]

text_df[, len_name :=  NULL]

write_fst(text_df,
          '../Data/Intermediate/finance_words_conm.fst')


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

text_df_jane <- copy(comnams_dt)

text_df_jane[, len_name := nchar(processed_conm)]

word_list_jane <- jane_words[1:6000, word]

text_df_jane[,
        (word_list_jane) := 
          mclapply(word_list_jane, custom_dist, processed_conm, len_name,
                   mc.cores = numCores)]


text_df_jane[, len_name :=  NULL]

write_fst(text_df_jane,
          '../Data/Intermediate/jane_words_conm.fst')
