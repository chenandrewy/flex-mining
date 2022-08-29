# SETUP ====
source('0_Environment.R')
# Supress summary message
options(dplyr.summarise.inform = FALSE)

# Functions ====

# Faster ntile

fntile <- function(x, n) {
  x.length <- length(x)
  return(as.integer(n * {frank(x, ties.method = "first") - 1} / x.length + 1))
}

# Simplified create portfolios from ntiles

nchoose2ports_fast <- function(CCM, n, big_trade_months = 6) {
  
  # n=50 will lead to 50*50/2 - 50 = 1200 long-short portfolios.
  
  # find portfolio returns
  
  portfolio_returns <- CCM %>%
    
    select(permno,yyyymm,date,ret,lag_me, bin1, signal) %>%
    
    group_by(yyyymm, bin1) %>%
    
    dplyr::summarize(ew_mean = mean(ret, na.rm=TRUE),
                     
                     vw_mean = weighted.mean(ret, lag_me, na.rm=TRUE),
                     
                     N = n()) %>%
    
    filter(N > n)  %>%
    
    ungroup()
  
  ### generate long-short portfolios of all possible combinations
  
  for (ii in 1:n) {
    
    portfolio_returns <- portfolio_returns %>%
      
      group_by(yyyymm) %>%
      
      mutate("ew_ls_x.{ii}":= ew_mean - ew_mean[ii],
             
             "vw_ls_x.{ii}":= vw_mean - vw_mean[ii])
    
  } 
  
  
  
  ### reshape to long
  
  portfolio_returns_ew <- portfolio_returns %>%
    
    ungroup() %>%
    
    select(yyyymm, bin1, starts_with("ew_ls"), ) %>%
    
    pivot_longer(cols = starts_with("ew_ls"),
                 
                 names_to = "bin2",
                 
                 values_to = "return_ew") %>%
    
    mutate(bin2 = as.numeric(substr(bin2, 9, 11))) %>%
    
    filter(bin1 < bin2) %>%  # drop whenever return is always 0 and whenever long short return of one portfolio is the negative return of another portfolio
    
    unite(bin, bin1:bin2, sep=",")
  
  
  
  portfolio_returns_vw <- portfolio_returns %>%
    
    ungroup() %>%
    
    select(yyyymm, bin1, starts_with("vw_ls"), ) %>%
    
    pivot_longer(cols = starts_with("vw_ls"),
                 
                 names_to = "bin2",
                 
                 values_to = "return_vw") %>%
    
    mutate(bin2 = as.numeric(substr(bin2, 9, 11))) %>%
    
    filter(bin1 < bin2) %>%  # drop whenever return is always 0 and whenever long short return of one portfolio is the negative return of another portfolio
    
    unite(bin, bin1:bin2, sep=",") 
  
  
  
  output <- full_join(portfolio_returns_ew, portfolio_returns_vw, by = c("bin", "yyyymm"))
  
  return(output)
  
  
  
} # end function

# Create portfolios for one seed and one letter of the ticker

create_single_set_returns <- function(seedn, n_tiles = 5, nticker = 1 ){
  
  # n_tiles is the number of groups
  
  # nticker in {1,2,3} is which letter of the ticker to take for the order
  
  set.seed(seedn)
  
  current_permutation <- paste0(sample(LETTERS, length(LETTERS), replace = FALSE))
  
  crsp_dt[, signal := as.numeric(factor(substr(lag_ticker, nticker, nticker),
                                        levels =  current_permutation))]
  
  crsp_dt[, bin1 := fntile(signal, n_tiles), by = date]
  
  port_rets_dt <- as.data.table(nchoose2ports_fast(crsp_dt, n_tiles))
  
  port_rets_dt[, seed := seedn]
  
  port_rets_dt[, ntic := nticker]
  
  return(port_rets_dt)
}

# Create many ticker portfolios

create_ticker_portfolios <- function(nports = 600, n_tiles = 5){
  # nports = Number of portfolios
  # n_tiles = Number of groups per order
  
  # Number of combinations 
  n_ports_per_order = n_tiles*(n_tiles-1)/2
  # 3 different letters to choose in the tickers
  max_n_seed = ceiling(nports/( n_ports_per_order *3)) 
  # Count Portfolios
  current_n_portfolios <- 0
  # Empty dt
  ticker_ports_long <- data.table()
  
  # Loop in seeds and letter from ticker
  for (i_seed in 1:max_n_seed) {
    for(j_tic in 1:3){
      # Create new portfolio
      new_port <- create_single_set_returns(seedn = i_seed, n_tiles = n_tiles,
                                      nticker = j_tic)
      # Bind new and old set of portfolios
      ticker_ports_long <- rbind(ticker_ports_long,  new_port)
      # Count new portfolios
      current_n_portfolios <- current_n_portfolios + n_ports_per_order
      
      # print(c(current_n_portfolios, nports))
      
      # Early stop if current # portfolios > desired
      if(current_n_portfolios > nports){
        return(ticker_ports_long)
      }
    }
  }
  return(ticker_ports_long)
}

# t-stat with removed NA

f.custom.t <- function(x){
  return(t.test(x, na.action = na.omit)$statistic)
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


# Data ====

# Read data
crspm_dt <- as.data.table(read_fst('../Data/Intermediate/crspm.fst'))

# Remove if ticker missing or length ticker < 3
crspm_dt <- crspm_dt[!is.na(ticker) & nchar(ticker) >= 3, ]

# Month format
crspm_dt[, month := month(date)]

setkey(crspm_dt, permno, date)

# Temporary ticker so that new stocks are added in June
crspm_dt[month(date) == 5, tic_temp := ticker]

# Ticker available in real time
crspm_dt[, lag_ticker := shift(tic_temp, 1),
         by = .(permno)]

# Fill-forward missing tickers
crspm_dt[ ,
          lag_ticker :=  zoo::na.locf(lag_ticker,na.rm =  FALSE),
          by = permno]

# Lagged market cap
crspm_dt[, lag_me := shift(me), by = permno]

# Remove if missing lagged ticker or lagged market cap

crsp_dt <- crspm_dt[!is.na(lag_me) & !is.na(lag_ticker), ]

# Create Portfolios ====


ticker_porfolios_dt <- create_ticker_portfolios(6, 5)

write_fst(ticker_porfolios_dt, '../Data/Intermediate/ticker_ports_dt.fst')

ticker_ports_long <- melt(ticker_porfolios_dt,
                          id.vars = c('yyyymm', 'bin', 'seed', 'ntic'),
                          measure.vars = c('return_ew', 'return_vw'),
                          variable.name = "type",
                          value.name = 'return')

write_fst(ticker_ports_long, '../Data/Intermediate/ticker_ports_long.fst')

# Portfolio Statistics ====

summary_stats_per_port <- ticker_ports_long[,
                               .(t_stat = f.custom.t(return),
                                 mean = mean(return, na.rm = TRUE),
                                 sharpe = f.sharp(return)
                                 ),
                               by = .(bin, seed, ntic, type)]

write_fst(summary_stats_per_port, '../Data/Intermediate/summary_stats_per_port.fst')

summary_stats_per_port[, f.describe_numeric(t_stat), by = type]

ggplot(summary_stats_per_port, aes(x = t_stat, fill = type)) +
  geom_histogram(position = "identity", alpha = 0.4, bins = 50)+ xlim(c(-4,4))

