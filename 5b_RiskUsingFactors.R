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


# risk_measure <- 'risk_via_mkt'
# breaks <- quantile(czret[!is.na(get(risk_measure)),
#                          median(get(risk_measure)),
#                          by = signalname][, V1],
#                    probs = -0:3/3,  
#                    include.lowest = TRUE, 
#                    type = 2)
# 

add_catID(czret, "risk_via_mkt", 3)
add_catID(czret, "risk_via_ff3", 3)
add_catID(czret, "risk_via_ff5", 3)

average_by_ntile <- czret[eventDate >0 ,
                          .(ret = mean(ret),
                            catID_risk_via_mkt = first(catID_risk_via_mkt),
                            catID_risk_via_ff3 = first(catID_risk_via_ff3),
                            catID_risk_via_ff5 = first(catID_risk_via_ff5),
                            risk_via_mkt = first(risk_via_mkt),
                            risk_via_ff3 = first(risk_via_ff3),
                            risk_via_ff5 = first(risk_via_ff5)
                            ), by = signalname]

library(ggplot2)
library(gridExtra)

# Create the individual plots
p1 <- ggplot(average_by_ntile, aes(x=risk_via_mkt, y=ret)) + 
  geom_point() +
  labs(title="Average Return by Risk via Mkt",
       x="b'r/mu",
       y="Average Return") + xlim(c(-3, 3)) + ylim(c(-300, 300))

p2 <- ggplot(average_by_ntile, aes(x=risk_via_ff3, y=ret)) + 
  geom_point() +
  labs(title="Average Return by Risk via FF3",
       x="b'r/mu",
       y="Average Return") + xlim(c(-3, 3)) + ylim(c(-300, 300))

p3 <- ggplot(average_by_ntile, aes(x=risk_via_ff5, y=ret)) + 
  geom_point() +
  labs(title="Average Return by Risk via FF5",
       x="b'r/mu",
       y="Average Return") + xlim(c(-3,3)) + ylim(c(-300, 300))

# Combine the plots into a single plot with three panels
# Specify the file name and open the PDF device
pdf("combined_plots.pdf", width=7, height=10)  # Adjust width and height as necessary

# Draw the combined plots
grid.arrange(p1, p2, p3, ncol=2)

# Close the PDF device
dev.off()

pdf("combined_plots3.pdf", width=10, height=7)  # Adjust width and height as necessary

# Draw the combined plots in two columns
grid.arrange(p1, p2, p3, ncol=3)

# Close the PDF device
dev.off()
capm_cat <- average_by_ntile[,.(capm = mean(ret), N_capm = .N), by = .(catID_risk_via_mkt)] 
ff3_cat <- average_by_ntile[,.(ff3 = mean(ret), N_ff3 = .N), by = .(catID_risk_via_ff3)]
ff5_cat <- average_by_ntile[, .(ff5 =mean(ret), N_ff5 = .N), by = .(catID_risk_via_ff5)]

# Convert to data.tables for easier merging and set keys for the merge
setDT(capm_cat, key = "catID_risk_via_mkt")
setDT(ff3_cat, key = "catID_risk_via_ff3")
setDT(ff5_cat, key = "catID_risk_via_ff5")

# Merge the data.tables
results <- capm_cat[ff3_cat][ff5_cat]



# Convert data.table to matrix
results_mat <- as.matrix(results)

# Transpose the matrix
transposed_results <- t(results_mat)

# Convert back to data.frame for pretty printing
transposed_results <- as.data.frame(transposed_results)[-1,]
colnames(transposed_results) <- c('negative', 'positive', 'Too high')
