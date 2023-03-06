# exports data as csv for sharing

# Setup -------------------------------------------------------------------


rm(list = ls())

source('0_Environment.R')

dmdat = readRDS('../Data/Processed/CZ-style-v6 LongShort.RData')


dir.create('../Data/Export/', showWarnings = F)


# Reformat  -------------------------------------------------------------

# sanity check
# temp2 = dmdat$ret  %>% filter(signalid <= 100)
# 
# checkdat = temp2 %>% 
#   mutate(
#     year = as.integer(floor(yearm))
#     , month = round(12*(as.numeric(yearm) - year) +  1)
#   ) %>% 
#   mutate(
#     yearcheck = year(yearm), monthcheck = month(yearm)
#     , zero = abs(year*100 + month - yearcheck*100-monthcheck)
#   ) %>% 
#   print()
# 
# checkdat$zero %>% max

# yearmon is awful slow so we do this workaround
temp = dmdat$ret %>% 
    mutate(
      year = as.integer(floor(yearm))
      , month = round(12*(as.numeric(yearm) - year) +  1)
    ) %>%
  left_join(
    dmdat$port_list %>% select(portid, sweight), by = 'portid'
  ) 

# split EW / VW
ret_ew = temp %>% filter(sweight == 'ew') %>% 
  select(signalid, year, month, ret, nstock) %>% 
  arrange(signalid, year, month, )


ret_vw = temp %>% filter(sweight == 'vw') %>% 
  select(signalid, year, month, ret, nstock) %>% 
  arrange(signalid, year, month, )



# Write to disk -------------------------------------------------------------------------


fwrite(ret_ew, '../Data/Export/DataMinedLongShortReturnsEW.csv', row.names = F)
fwrite(ret_vw, '../Data/Export/DataMinedLongShortReturnsVW.csv', row.names = F)
fwrite(dmdat$signal_list, '../Data/Export/DataMinedSignalList.csv', row.names = F)

# write a small sample
fwrite(ret_ew %>% filter(signalid <= 10) 
  , '../Data/Export/DataMinedLongShortReturnsSample.csv', row.names = F)