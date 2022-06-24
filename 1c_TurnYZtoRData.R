# Prepare YZ data
source('0_Environment.R')



# number of strategies to sample
nstrat = 1000

temp = read_sas('../Data Yan-Zheng/Yan_Zheng_RFS_Data.sas7bdat')

# EW
temp1 = temp %>%
  mutate(
    signalname = paste(transformation, fsvariable, sep = '.')
  ) %>%
  transmute(
    signalname, date = DATE, ret = 100*ddiff_ew
  )

yz_ew = list(
  ret = temp1
  , usedData = 'yz'
#  , signal_form = NULL
#  , longshort_form = longshort_form
#  , portnum = portnum
#  , trim = trim
#  , reup_months = reup_months
  , sweight = 'ew'
#  , data_avail_lag = data_avail_lag
#  , scaling_variables = scaling_variables
)

saveRDS(yz_ew, paste0('../Data/LongShortPortfolios/yz_ew.RData'))
        
# VW
temp1 = temp %>%
  mutate(
    signalname = paste(transformation, fsvariable, sep = '.')
  ) %>%
  transmute(
    signalname, date = DATE, ret = 100*ddiff_vw
)

yz_vw = list(
  ret = temp1
  , usedData = 'yz'
  #  , signal_form = NULL
  #  , longshort_form = longshort_form
  #  , portnum = portnum
  #  , trim = trim
  #  , reup_months = reup_months
  , sweight = 'vw'
  #  , data_avail_lag = data_avail_lag
  #  , scaling_variables = scaling_variables
)

saveRDS(yz_vw, paste0('../Data/LongShortPortfolios/yz_vw.RData'))
        



