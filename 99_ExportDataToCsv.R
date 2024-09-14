# exports data as csv for sharing

# Setup -------------------------------------------------------------------

rm(list = ls())
source('0_Environment.R')
dir.create('../Data/Export/', showWarnings = F)

# function for writing to disk
rdata_to_csv = function(rdataname, csvname){
  
  # read rdata / rds
  dmdat = readRDS(rdataname)
  
  # Reformat  
  # yearmon is awful slow so we do this workaround
  temp = dmdat$ret %>% 
    mutate(
      year = as.integer(floor(yearm))
      , month = round(12*(as.numeric(yearm) - year) +  1)
    ) %>%
    left_join(
      dmdat$port_list %>% select(portid, sweight), by = 'portid'
    ) 
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
  
  # split EW / VW
  ret_ew = temp %>% filter(sweight == 'ew') %>% 
    select(signalid, year, month, ret, nstock_long, nstock_short) %>% 
    arrange(signalid, year, month, )
  
  ret_vw = temp %>% filter(sweight == 'vw') %>% 
    select(signalid, year, month, ret, nstock_long, nstock_short) %>% 
    arrange(signalid, year, month, )
  
  # Write to disk 
  fwrite(ret_ew, paste0(csvname, 'EW.csv'), row.names = F)
  fwrite(ret_vw, paste0(csvname, 'VW.csv'), row.names = F)
  fwrite(dmdat$signal_list, paste0(csvname, 'SignalList.csv'), row.names = F)
  
  # write a small sample
  fwrite(ret_ew %>% filter(signalid <= 10) 
         , paste0(csvname,'Sample.csv'), row.names = F)
  
}

# Compustat Mining ---------------------------------------------------------------

rdataname = paste0('../Data/Processed/',
                   globalSettings$dataVersion, 
                   ' LongShort.RData')
csvname = '../Data/Export/DataMinedLongShortReturns'

rdata_to_csv(rdataname = rdataname, csvname = csvname)


# Ticker Mining -----------------------------------------------------------

rdataname = '../Data/Processed/ticker_Harvey2017JF.RDS'
csvname = '../Data/Export/tickerHarvey2017'

rdata_to_csv(rdataname = rdataname, csvname = csvname)

