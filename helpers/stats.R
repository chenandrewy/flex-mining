# Statistical helpers: summary stats, EZ-theme sorts, and PCA.
#
# Sourced by 0_Environment.R after packages and config.R. These functions
# rely on objects from the sourcing environment (e.g. globalSettings and
# chapter-local data frames); do not source this file in isolation.



f.custom.t <- function(x){
  if(length(x[!is.na(x)]) > 1 & sd(x[!is.na(x)] > 1e-8)){
    return(t.test(x, na.action = na.omit)$statistic)
  }else{return(NaN)}
  
}


# Annualized Sharpe ratio

f.sharp <- function(x, na.rm = TRUE){
  return(mean(x, na.rm = na.rm)*sqrt(12)/sd(x, na.rm = na.rm))
}


fntile <- function(x, n) {
  x.length <- length(x)
  return(as.integer(n * {frank(x, ties.method = "first") - 1} / x.length + 1))
}


f.desc.returns <- function(returns_dt){
  sumsignal_rets = returns_dt %>% 
    group_by(bin) %>% 
    summarize(rbar_is = mean(ret_is, na.rm = TRUE),
              avg_tstat_is = mean(t_is, na.rm = TRUE),
              rbar_oos = mean(ret_oos) 
              # ,tstat_oos_portfolio = rbar_oos/sd(ret_oos)*sqrt(n())
    ) %>% 
    ungroup()
  
  return(sumsignal_rets)
}


f.ls.past.returns <- function(n_tiles, name_var){
  
  
  yz_dt[, sort_var := get(name_var)]
  
  yz_dt[!is.na(sort_var) & month(date) == 6,
        var_sort := as.factor(fntile(sort_var, n_tiles)), by = date]
  
  yz_dt[ ,
         var_sort :=  zoo::na.locf(var_sort,na.rm =  FALSE),
         by = dmname]
  
  yz_dt[!is.na(var_sort), bin := var_sort]
  
  yz_dt[month(date) != 6, sort_var := NA]
  
  returns_dt <- yz_dt[!is.na(bin) & !is.na(ret),
                      .(ret_oos = mean(ret, na.rm=TRUE),
                        ret_is = mean(sort_var, na.rm=TRUE),
                        t_is = mean(t_30y_l, na.rm = TRUE),
                        .N),
                      by = .(date, bin)]
  
  sumsignal_oos <- f.desc.returns(returns_dt)
  sumsignal_oos_pre_2003 <- f.desc.returns(returns_dt[date < '2003-06-30'])
  sumsignal_oos_post_2003 <- f.desc.returns(returns_dt[date >= '2003-06-30'])
  
  return(list(sumsignal_oos = sumsignal_oos,
              sumsignal_oos_pre_2003 = sumsignal_oos_pre_2003,
              sumsignal_oos_post_2003 = sumsignal_oos_post_2003,
              rets = returns_dt))
  
}




# Function to compute principal components given returns
compute_pca = function(ret1){
  
  # make wide matrix
  temp = dcast(ret1, yearm ~ dmname, value.var = 'ret') 
  retmat0 = as.matrix(temp[ , -1])
  rownames(retmat0) = temp$yearm
  
  # drop signals with missing values
  nmonthmissmax = 0
  signalmiss = colSums(is.na(retmat0))
  retmat = retmat0[ , signalmiss <= nmonthmissmax] 
  
  # drop months with missing values (redundant right now)
  # nstratmissmax = 0.1*nstrat
  # monthmiss = rowSums(is.na(retmat)) 
  # retmat = retmat[monthmiss <= nstratmissmax , ]
  
  # PCA
  A = (retmat - colMeans(retmat))/ sqrt(nrow(retmat))
  Asvd = svd(A)
  pcadat = tibble(n_pc = 1:length(Asvd$d) , eval = Asvd$d^2)  %>% 
    mutate(cum_pct_exp = cumsum(eval)/sum(eval)*100) %>% 
    mutate(nstrat = dim(retmat)[2])
  
  # # sanity check (this requires a lot more compute)
  # coveig = eigen(cov(retmat))
  # temp = cumsum(coveig$values)/sum(coveig$values)*100
  # temp %>% head()
  # pcadat$cum_pct_exp %>% head()
  
  return(pcadat)
} # end compute_pca 
