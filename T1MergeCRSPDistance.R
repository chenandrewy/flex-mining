# SETUP ====
rm(list=ls())
gc()

setwd('/home/alex/GitProjects/flex-mining')

source('0_Environment.R')
# Supress summary message
options(dplyr.summarise.inform = FALSE)

finance_tickers_dt <- read_fst('../Data/Intermediate/finance_words_ticker.fst') %>%
  as.data.table()

finance_tickers_dt[, word := as.character(word)]

finance_words <- finance_tickers_dt[, unique(word)]

finance_wide <- dcast(finance_tickers_dt, ticker_id ~ word, value.var = 'distance')

gc()

setnames(finance_wide, 'ticker_id', 'lag_ticker')

crspm_dt <- as.data.table(read_fst('../Data/Intermediate/crsp_with_names.fst'))

crspm_dt <- crspm_dt[month(date) == 6, .(permno, yyyymm, lag_ticker)]

setkey(finance_wide, lag_ticker)

setkey(crspm_dt, lag_ticker)

crsp_with_distance_finance <- merge(crspm_dt, finance_wide)

write_fst(crsp_with_distance_finance, '../Data/Intermediate/crsp_with_distance.fst')

gc()

melted_crsp <- melt(crsp_with_distance_finance, id.vars = c('permno', 'yyyymm', 'lag_ticker'),
                    measure.vars = finance_words[1:1000],
                    variable.name = 'word',
                    value.name = 'distance')

gc()

rm(melted_crsp)

gc()


