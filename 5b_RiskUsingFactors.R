# Setup -------------------------------------------------------------------
rm(list = ls())
source('0_Environment.R')


czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS')

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, Year, theory)

czret = readRDS('../Data/Processed/czret_keeponly.RDS') %>% 
  left_join(czcat, by = 'signalname') %>% 
  mutate(
    retOrig = ret
    , ret = ret/rbar*100
  )



FamaFrenchFactors <- readRDS('../Data/Raw/FamaFrenchFactors.RData') %>%
  rename(date = yearm)

czret <- czret %>% left_join(FamaFrenchFactors, by  = c('date'))


mkt_implied_category <- function(data, notused){
  # print(data)
  data_reg <- data[date >= sampstart & date <= sampend & !is.na(retOrig), ]
  mean_ret <- mean(data_reg$retOrig, na.rm = TRUE)
  linear_fit <- lm(retOrig ~ mktrf, data = data_reg)
  mean_mkt <-  mean(data_reg$mktrf, na.rm = TRUE)
  coeffs <- linear_fit$coefficients
  expected_returns <-  coeffs[2] * mean_mkt
  # print(mean_mkt)
  return(expected_returns/mean_ret)
}

ff3_implied_category <- function(data, sampstart, sampend){
  # Filter the data
  data_reg <- data[data$date >= sampstart &
                     data$date <= sampend & !is.na(retOrig), ]
  
  # Calculate mean return
  mean_ret <- mean(data_reg$retOrig, na.rm = TRUE)
  
  # Fit a linear regression model with additional features
  linear_fit <- lm(retOrig ~ mktrf + smb + hml, data = data_reg)
  
  # Calculate mean market return
  mean_mkt <- mean(data_reg$mktrf, na.rm = TRUE)
  
  # Calculate means of additional features
  mean_feature1 <- mean(data_reg$smb, na.rm = TRUE)
  mean_feature2 <- mean(data_reg$hml, na.rm = TRUE)
  
  # Return the modified calculation accounting for additional features
  coeffs <- linear_fit$coefficients
  expected_returns <- (coeffs[2] * mean_mkt +
                         coeffs[3] * mean_feature1 +
                         coeffs[4] * mean_feature2)
  return(( expected_returns/ mean_ret))
}

ff5_implied_category <- function(data, sampstart, sampend){
  # Filter the data
  data_reg <- data[data$date >= sampstart &
                     data$date <= sampend & !is.na(data$retOrig), ]
  
  # Calculate mean return
  mean_ret <- mean(data_reg$retOrig, na.rm = TRUE)
  
  # Fit a linear regression model with additional features
  linear_fit <- lm(retOrig ~ mktrf + smb + hml + cma + rmw, data = data_reg)
  
  # Calculate mean market return
  mean_mkt <- mean(data_reg$mktrf, na.rm = TRUE)
  
  # Calculate means of additional features
  mean_feature1 <- mean(data_reg$smb, na.rm = TRUE)
  mean_feature2 <- mean(data_reg$hml, na.rm = TRUE)
  mean_feature3 <- mean(data_reg$cma, na.rm = TRUE)
  mean_feature4 <- mean(data_reg$rmw, na.rm = TRUE)
  
  # Return the modified calculation accounting for additional features
  coeffs <- linear_fit$coefficients
  expected_returns <- (coeffs[2] * mean_mkt +
                         coeffs[3] * mean_feature1 +
                         coeffs[4] * mean_feature2 +
                         coeffs[5] * mean_feature3 +
                         coeffs[6] * mean_feature4)
  return((expected_returns / mean_ret))
}

add_catID <- function(df, risk_measure, n_tiles = 3) {
  # Create quantile breaks
  breaks <- quantile(df[!is.na(get(risk_measure)),
                           median(get(risk_measure)),
                           by = signalname][, V1],
                     probs = -0:3/3,  
                     include.lowest = TRUE, 
                     type = 2)
  breaks <- c(-Inf, -1.5, -1, -.5, 0,  0.5, 1, 1.5, Inf)
  breaks[1] <- breaks[1] - .Machine$double.eps^0.5
  print(breaks)
  # Apply cut to create new column and convert to factor with sequential labels
  df[!is.na(get(risk_measure)), (paste("catID", risk_measure, sep="_")) :=
       factor(cut(get(risk_measure),
                  breaks = breaks)
              # ,labels = c('low or negative', 'good', 'too high')
              , ordered = TRUE)]
  
}

czret %>% setDT()

czret[, risk_via_mkt :=  mkt_implied_category(.SD), by = signalname]
czret[, risk_via_ff3 :=  ff3_implied_category(.SD), by = signalname]
czret[, risk_via_ff5 :=  ff5_implied_category(.SD), by = signalname]

add_catID(czret, "risk_via_mkt", 3)
add_catID(czret, "risk_via_ff3", 3)
add_catID(czret, "risk_via_ff5", 3)

average_by_ntile <- czret[eventDate >0 ,
                          .(ret = mean(ret)/100,
                            catID_risk_via_mkt = first(catID_risk_via_mkt),
                            catID_risk_via_ff3 = first(catID_risk_via_ff3),
                            catID_risk_via_ff5 = first(catID_risk_via_ff5),
                            risk_via_mkt = first(risk_via_mkt),
                            risk_via_ff3 = first(risk_via_ff3),
                            risk_via_ff5 = first(risk_via_ff5)
                            ), by = signalname]

library(ggplot2)
library(gridExtra)
ylab <- "[Post-Sample]/[In-Sample]"
xlab <- "[Predicted by Risk Model]/[In-Sample]"
# Create the individual plots
xax <- c(-.5, 1)
repelsize = 6
repelcolor = 'royalblue4'
reg_camp = lm(ret ~ risk_via_mkt, average_by_ntile) %>% 
  summary()
p1 <- ggplot(average_by_ntile, aes(x=risk_via_mkt, y=ret)) +
  geom_hline(yintercept = 0, color = 'gray', size =2) +
  geom_vline(xintercept =  0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 2) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  labs(
    x=xlab,
    y=ylab) +
  xlim(xax) + ylim(c(-1, 2))    +
  geom_abline(
    aes(intercept = reg_camp$coefficients[1], slope = reg_camp$coefficients[2])
    , color = colors[1], size = 2
  ) 
p1
ggsave('../Results/Fig_Risk_via_CAPM.pdf', p1, width = 10, height = 8)
reg_ff3 = lm(ret ~ risk_via_ff3, average_by_ntile) %>% 
  summary()
p2 <- ggplot(average_by_ntile, aes(x=risk_via_ff3, y=ret)) +
  geom_hline(yintercept = 0, color = 'gray', size = 2) +
  geom_vline(xintercept =  0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 2) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  labs(
    x=xlab,
    y=ylab) +
  xlim(xax) + ylim(c(-1, 2))    +
  geom_abline(
    aes(intercept = reg_ff3$coefficients[1], slope = reg_ff3$coefficients[2])
    , color = colors[1], size = 2
  ) 
p2
ggsave('../Results/Fig_Risk_via_FF3.pdf', p2, width = 10, height = 8)

reg_ff5 = lm(ret ~ risk_via_ff5, average_by_ntile) %>% 
  summary()
p3 <- ggplot(average_by_ntile, aes(x=risk_via_ff5, y=ret)) +
  geom_hline(yintercept = 0, color = 'gray', size = 2) +
  geom_vline(xintercept =  0, color = 'gray', size = 1) +
  geom_hline(yintercept = 1, color = 'gray', size = 2) +
  geom_point(size = 2.5) +
  theme_light(base_size = 26) +
  theme(
    legend.position = c(80,85)/100
    , legend.spacing.y = unit(0, units = 'cm')
    , legend.background = element_rect(fill='transparent')) +
  labs(
    x=xlab,
    y=ylab) +
  xlim(xax) + ylim(c(-1, 2))    +
  geom_abline(
    aes(intercept = reg_ff5$coefficients[1], slope = reg_ff5$coefficients[2])
    , color = colors[1], size = 2
  ) 
p3
ggsave('../Results/Fig_Risk_via_FF5.pdf', p3, width = 10, height = 8)