# Statistical helpers: shrinkage, summary stats, EZ-theme sorts, PCA.
#
# Sourced by 0_Environment.R after packages and config.R. These functions
# rely on objects from the sourcing environment (e.g. globalSettings and
# chapter-local data frames); do not source this file in isolation.



# Function for calculating alpha and shrinkage

calculate_alpha_shrinkage = function(dt, train_start, train_end, test_start, test_end) {
  
  #' @param dt Table with columns signalname, date and ret
  #' @param train_start Start year for training sample, analogously for other parameters
  
  # add sample indicators
  retdat = dt %>% 
    mutate(
      samp = case_when(
        year(date) >= train_start & year(date) <= train_end ~ 'train'
        , year(date) >= test_start & year(date) <= test_end ~ 'test'
      )
    ) %>% 
    filter(!is.na(ret), !is.na(samp))  %>% 
    setDT()
  
  
  # find alphas
  stratsum = retdat[
    , list(
      alpha = summary(lm(ret ~ 1))$coefficients['(Intercept)' , 'Estimate']
      , tstat = summary(lm(ret ~ 1))$coefficients['(Intercept)' , 't value']
    )
    , by = .(signalname, samp)
  ] 
  
  # find shrinkage
  sampsum = stratsum %>% 
    group_by(samp) %>% 
    summarize(
      shrinkage = min((mean(tstat^2, na.rm = TRUE))^(-1), 1.0) # var(tstat, na.rm = TRUE)^-1
    )
  
  # shrink strat summary
  stratshrink = stratsum %>% 
    left_join(sampsum) %>% 
    mutate(
      alpha = alpha*(1-shrinkage)
      , tstat = tstat*(1-shrinkage)
    ) %>% 
    select(-shrinkage)
  
  # Combine classical and shrink strat summaries
  stratSumAll = stratshrink %>% 
    mutate(stattype = 'stein_ez') %>% 
    bind_rows(stratsum %>% 
                mutate(stattype = 'classical')
    )
  
  return(stratSumAll)
  
}

f.custom.t <- function(x){
  if(length(x[!is.na(x)]) > 1 & sd(x[!is.na(x)] > 1e-8)){
    return(t.test(x, na.action = na.omit)$statistic)
  }else{return(NaN)}
  
}


# Annualized Sharpe ratio

f.sharp <- function(x, na.rm = TRUE){
  return(mean(x, na.rm = na.rm)*sqrt(12)/sd(x, na.rm = na.rm))
}


# Describe function

f.describe_numeric <- function(.x) {
  if(!is.numeric(.x)) stop(".x must be a numeric vector!")
  if(!is.atomic(.x)) stop(".x must be an atomic vector!")
  describe_functions <- list(
    N = function(.x, ...) length(.x),
    mean = function(.x, ...) mean(.x, ...),
    median = function(.x, ...) median(.x, ...),
    sd = function(.x, ...) stats::sd(.x, ...),
    min = function(.x, ...) min(.x, ...),
    q1 = function(.x, ...) stats::quantile(.x, probs = 0.01, ...),
    q5 = function(.x, ...) stats::quantile(.x, probs = 0.02, ...),
    q10 = function(.x, ...) stats::quantile(.x, probs = 0.10, ...),
    q25 = function(.x, ...) stats::quantile(.x, probs = 0.25, ...),
    q50 = function(.x, ...) stats::median(.x, ...),
    q75 = function(.x, ...) stats::quantile(.x, probs = 0.75, ...),
    q90 = function(.x, ...) stats::quantile(.x, probs = 0.90, ...),
    q95 = function(.x, ...) stats::quantile(.x, probs = 0.95, ...),
    q99 = function(.x, ...) stats::quantile(.x, probs = 0.99, ...),
    max = function(.x, ...) max(.x, ...)
  )
  return(as.data.frame(lapply(describe_functions,
                              function(.f) .f(.x, na.rm = TRUE)),
                       row.names = "1",
                       stringsAsFactors = FALSE))
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

