# remove incorporated consolidated

# SETUP ====
# setwd('/home/alex/GitProjects/flex-mining')
source('0_Environment.R')
# Supress summary message
options(dplyr.summarise.inform = FALSE)
#####
library(googledrive)
library(stopwords)
library(tidytext)
#####
clean_company_name <- function(dt){
  text_df <- dt[!is.na(comnam), ] %>%
    unnest_tokens(word, comnam)%>%
    anti_join(stop_words, by= c("word" = "word"))%>%
    filter(nchar(word) > 2) %>%
    filter(word != 'corp') %>%
    group_by(I) %>%
    summarise(processed_conm = paste0(word, collapse = ' ')) %>%
    as.data.table()  %>%
    setkey(I)
  return(text_df)
  
  
}
#####
dir_save_words <- '../Data/Intermediate/finance_words.csv'
if(!file.exists(dir_save_words)){
  url_finance_words <- 'https://drive.google.com/file/d/17CmUZM9hGUdGYjCXcjQLyybjTrcjrhik/'
  drive_download(url_finance_words, dir_save_words, overwrite = TRUE)
}

finance_words <- fread(dir_save_words)
setnames(finance_words, names(finance_words), tolower(gsub(' ', '_', names(finance_words))))
names(finance_words)

finance_words[, doc_prop := doc_count/max(doc_count)]
finance_words[, word_lower := tolower(word)]

setorder(finance_words, -doc_prop)

stopwords_en <- stopwords(source = 'smart')
finance_words <- finance_words[!word_lower %in%  stopwords_en, ]

write_fst(finance_words, '../Data/Intermediate/finance_words.fst')

# Read data
crspm_dt <- as.data.table(read_fst('../Data/Intermediate/crspm.fst'))
crspm_dt[, I := .I]
crspm_dt <- crspm_dt[!is.na(ret), ]

clean_names_dt <- crspm_dt[!is.na(comnam), clean_company_name(.SD)]
setkey(crspm_dt, I)
crspm_dt[clean_names_dt, clean_name := processed_conm]
setkey(crspm_dt, permno, date)
crspm_dt[, lag_name := shift(clean_name, 1),
         by = .(permno)]
crspm_dt[ ,
          lag_name :=  zoo::na.locf(lag_name,na.rm =  FALSE),
          by = permno]
crspm_dt[, lag_me := shift(me), by = permno]

write_fst(crspm_dt[nchar(lag_name) > 2,], '../Data/Intermediate/crsp_with_names.fst')