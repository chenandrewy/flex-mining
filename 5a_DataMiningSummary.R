# The OOS Sumstats can take several minutes



# DM OOS Sumstats to CSV  ----------------------------
var_types <- c('vw', 'ew')
var_type <- var_types[1]


bm_rets = readRDS(DMname)$ret
bm_info = readRDS(DMname)$port_list

bm_rets = bm_rets %>% 
  left_join(
    bm_info %>% select(portid, sweight), by = c('portid')
  )  %>%
  transmute(
    sweight
    , signalname = signalid
    , yearm
    , ret
    , nstock) %>% 
  filter(signalname %in% filteredCandidates)


for (var_type in var_types) {
  
  str_to_add  <- var_type
  
  yz = bm_rets %>%
    filter(sweight == var_type) %>% 
    transmute(
      signalname, date = as.Date(yearm), ret
    )
  
  
  sumsignal_all = yz %>% 
    group_by(signalname) %>% 
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
  
  fwrite(Summary_Statistics, glue::glue('../Results/Summary_StatisticsDM_{str_to_add}.csv'))
  
  Summary_Statistics
  
  print(xtable::xtable(Summary_Statistics, caption = 'Summary Statistics YZ All',
                       type = "latex", include.rownames=FALSE))
  
  
  ############################### # 
  # Table 1b
  ############################### #
  
  # Returns based on past returns
  # Basically creating a portfolio
  
  yz_dt <- yz %>% as.data.table() %>% setkey(signalname, date)
  
  yz_dt[, ret_30y_l := data.table::shift(frollmean(ret, 12*30, NA)), by = signalname]
  
  yz_dt[, t_30y_l   := data.table::shift(frollapply(ret, 12*30, f.custom.t, fill = NA)), by = signalname]
  
  yz_dt[, head(month(date))]
  
  yz_dt[month(date) != 6, t_30y_l := NA]
  
  ########################### #
  
  n_tiles <- 5
  
  name_var <- 'ret_30y_l'
  
  test <- f.ls.past.returns(n_tiles, name_var)
  
  print(xtable::xtable(test$sumsignal_oos, 
                       caption = 'Out-of-Sample Portfolios of Strategies Sorted on Past 30 Years of Returns',
                       type = "latex"), include.colnames=FALSE)
  
  fwrite(test$sumsignal_oos,  glue::glue('../Results/sumsignal_oos_30y_{str_to_add}_unit_level.csv'))
  fwrite(test$sumsignal_oos_pre_2003,  glue::glue('../Results/sumsignal_oos_30y_pre_2003_{str_to_add}_unit_level.csv'))
  fwrite(test$sumsignal_oos_post_2003,  glue::glue('../Results/sumsignal_oos_30y_post_2003_{str_to_add}_unit_level.csv'))
  
}


# DM OOS Sumstats to LaTeX ----------------------------------------------------


# to TeX
fs_ew = read_csv('../Results/sumsignal_oos_30y_ew_unit_level.csv')
fs_vw = read_csv('../Results/sumsignal_oos_30y_vw_unit_level.csv')

fs_ew = fs_ew %>% 
  transmute(bin = as.integer(bin),
            empty1 = NA_character_,
            rbar_is = round(100*rbar_is, 1),
            avg_tstat_is = round(avg_tstat_is, 2),
            empty2 = NA_character_,
            rbar_oos = round(100*rbar_oos, 1),
            Decay = ifelse(bin !=4, 
                           round(100*(1 - rbar_oos/rbar_is), 1),
                           NA_real_),
            empty3 = NA_character_
  )

fs_vw = fs_vw %>% 
  transmute(rbar_isvw = round(100*rbar_is, 1),
            avg_tstat_isvw = round(avg_tstat_is, 2),
            empty1vw = NA_character_,
            rbar_oosvw = round(100*rbar_oos, 1),
            Decayvw = ifelse(bin !=4, 
                             round(100*(1 - rbar_oos/rbar_is), 1),
                             NA_real_)
  )

bind_cols(fs_ew, fs_vw) %>% 
  xtable(digits = c(0, 0, 0, 1, 2,0, 1, 1, 0, 1, 2, 0, 1, 1)) %>% 
  print(
    include.rownames = FALSE,
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = paste0('../Results/dm-sortsFull.tex')
  )

# post 2003
fs_ew = read_csv('../Results/sumsignal_oos_30y_post_2003_ew_unit_level.csv')
fs_vw = read_csv('../Results/sumsignal_oos_30y_post_2003_vw_unit_level.csv')

fs_ew = fs_ew %>% 
  transmute(bin = as.integer(bin),
            empty1 = NA_character_,
            rbar_is = round(100*rbar_is, 1),
            avg_tstat_is = round(avg_tstat_is, 2),
            empty2 = NA_character_,
            rbar_oos = round(100*rbar_oos, 1),
            Decay = ifelse(bin !=4, 
                           round(100*(1 - rbar_oos/rbar_is), 1),
                           NA_real_),
            empty3 = NA_character_
  )

fs_vw = fs_vw %>% 
  transmute(rbar_isvw = round(100*rbar_is, 1),
            avg_tstat_isvw = round(avg_tstat_is, 2),
            empty1vw = NA_character_,
            rbar_oosvw = round(100*rbar_oos, 1),
            Decayvw = ifelse(bin !=4, 
                             round(100*(1 - rbar_oos/rbar_is), 1),
                             NA_real_)
  )

bind_cols(fs_ew, fs_vw) %>% 
  xtable(digits = c(0, 0, 0, 1, 2,0, 1, 1, 0, 1, 2, 0, 1, 1)) %>% 
  print(
    include.rownames = FALSE,
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = paste0('../Results/dm-sortsPost2003.tex')
  )

