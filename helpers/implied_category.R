# Implied-category helpers: CAPM/FF factor adjustments and binning.
#
# Sourced by 0_Environment.R after packages and config.R. These functions
# rely on objects from the sourcing environment (e.g. globalSettings and
# chapter-local data frames); do not source this file in isolation.


# Function to compute CAPM adjustment
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


# Function to compute FF3 adjustment
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


# Function to compute FF4 adjustment (Carhart 4-factor with momentum)
ff4_implied_category <- function(data, sampstart, sampend){
  # Filter the data
  data_reg <- data[data$date >= sampstart &
                     data$date <= sampend & !is.na(retOrig), ]

  # Calculate mean return
  mean_ret <- mean(data_reg$retOrig, na.rm = TRUE)

  # Fit a linear regression model with FF4 factors (market, size, value, momentum)
  linear_fit <- lm(retOrig ~ mktrf + smb + hml + umd, data = data_reg)

  # Calculate mean market return
  mean_mkt <- mean(data_reg$mktrf, na.rm = TRUE)

  # Calculate means of additional features
  mean_feature1 <- mean(data_reg$smb, na.rm = TRUE)
  mean_feature2 <- mean(data_reg$hml, na.rm = TRUE)
  mean_feature3 <- mean(data_reg$umd, na.rm = TRUE)

  # Return the modified calculation accounting for additional features
  coeffs <- linear_fit$coefficients
  expected_returns <- (coeffs[2] * mean_mkt +
                         coeffs[3] * mean_feature1 +
                         coeffs[4] * mean_feature2 +
                         coeffs[5] * mean_feature3)
  return(( expected_returns/ mean_ret))
}


# Function to compute FF5 adjustment
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

