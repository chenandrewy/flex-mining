# Precompute DM OOS summary statistics.
#
# How to run: source from 3_Precompute.R with the working directory set to
#   flex-mining/.
# Inputs:  ../Data/Processed/<dataVersion> LongShort.RData
# Outputs: ../Data/Processed/Summary_StatisticsDM_{ew,vw}.csv
#          ../Data/Processed/sumsignal_oos_30y_*.csv
#
# This script intentionally does not render exhibits. The corresponding tables
# are written by S2b_DataMiningSummaryTables.R.

# Load environment
source('0_Environment.R')
source("helpers/stats.R")

# Settings ---------------------------------------------------------------------
var_types <- c('vw', 'ew')

DMname = paste0('../Data/Processed/',
                globalSettings$dataVersion, 
                ' LongShort.RData')

dmcomp <- list()
dmcomp$name <- paste0('../Data/Processed/',
                      globalSettings$dataVersion, 
                      ' LongShort.RData')


# Load data ---------------------------------------------------------------
stratdat <- readRDS(DMname)
dm_rets <- stratdat$ret
dm_info <- stratdat$port_list
rm(stratdat)

dm_rets <- dm_rets %>%
  left_join(
    dm_info %>% select(portid, sweight),
    by = c("portid")
  ) %>%
  transmute(
    sweight,
    dmname = signalid,
    yearm,
    ret,
    nstock_long,
    nstock_short
  ) %>%
  setDT()


# Compute summary stats ---------------------------------------------------
for (var_type in var_types) {
  
  str_to_add  <- var_type
  
  yz = dm_rets %>%
    filter(sweight == var_type) %>% 
    transmute(
    dmname, date = yearm, ret
  )
  
  sumsignal_all = yz %>% 
    group_by(dmname) %>% 
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
  
  fwrite(Summary_Statistics, glue::glue('../Data/Processed/Summary_StatisticsDM_{str_to_add}.csv'))
  
  Summary_Statistics
  
  print(xtable::xtable(Summary_Statistics, caption = 'Summary Statistics YZ All',
                       type = "latex", include.rownames=FALSE))
  
  
  ############################### # 
  # Table 1b
  ############################### #
  
  # Returns based on past returns
  # Basically creating a portfolio
  
  yz_dt <- yz %>% as.data.table() %>% setkey(dmname, date)
  
  yz_dt[, ret_30y_l := data.table::shift(frollmean(ret, 12*30, NA)), by = dmname]
  
  yz_dt[, t_30y_l   := data.table::shift(frollapply(ret, 12*30, f.custom.t, fill = NA)), by = dmname]
  
  yz_dt[, head(month(date))]
  
  yz_dt[month(date) != 6, t_30y_l := NA]
  
  ########################### #
  
  n_tiles <- 5
  
  name_var <- 'ret_30y_l'
  
  test <- f.ls.past.returns(n_tiles, name_var)
  
  print(xtable::xtable(test$sumsignal_oos, 
                       caption = 'Out-of-Sample Portfolios of Strategies Sorted on Past 30 Years of Returns',
                       type = "latex"), include.colnames=FALSE)
  
  fwrite(test$sumsignal_oos,  glue::glue('../Data/Processed/sumsignal_oos_30y_{str_to_add}_unit_level.csv'))
  fwrite(test$sumsignal_oos_pre_2003,  glue::glue('../Data/Processed/sumsignal_oos_30y_pre_2003_{str_to_add}_unit_level.csv'))
  fwrite(test$sumsignal_oos_post_2003,  glue::glue('../Data/Processed/sumsignal_oos_30y_post_2003_{str_to_add}_unit_level.csv'))
  
}
