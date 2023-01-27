# finds why YZ signal count is not 240*76 = 18,240 but 18,113

# Find YZ exceptions -------------------------------------------------------------------------

rm(list = ls())

source('0_Environment.R')

yzraw = read_sas('../Data Yan-Zheng/Yan_Zheng_RFS_Data.sas7bdat')



signals = yzraw %>% transmute(fun = transformation, x = fsvariable) %>% distinct(fun, x)


xsum = signals  %>% 
  group_by(x) %>% 
  summarize(
    n = n()
  ) %>% 
  arrange(n)

# print out altered ones
#   there are 15 altered ones
#   2 are strange: rdip and txndbr
#   13 are from the denominators, but omits emp and mktcap (since they're not in the numerator)
altered = xsum %>% filter(n < 76)
altered
compnames$yz.denom[!compnames$yz.denom %in% altered$x]


# compare selected x 
#   the 13 altered from the denominators seem to drop x == y, leading to a decrease of 13 * 5 = 65 signals
#   the 2 altered strange ones drop the pd_var_ transformations,
#     and there are 31 pd_var transformations leading to a decrease of 2 * 31 = 62 signals
#   this explains the 240*76 = 18,240 - 18,113 = 127

signals %>% filter(x == 'aco') %>% 
  full_join(
    signals %>% filter(x == 'act'), by = c('fun')
  ) %>% 
  full_join(
    signals %>% filter(x == 'at'), by = c('fun')
  ) %>%   
  full_join(
    signals %>% filter(x == 'rdip'), by = c('fun')
  ) %>%     
  full_join(
    signals %>% filter(x == 'txndbr'), by = c('fun')
  ) %>%       
  print(n=Inf)


