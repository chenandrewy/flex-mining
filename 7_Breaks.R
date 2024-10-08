# Setup --------------------------------------------------------
# https://lindeloev.github.io/mcp/articles/packages.html
# https://cran.r-project.org/web/packages/strucchange/index.html
# https://kevin-kotze.gitlab.io/tsm/ts-2-tut/
rm(list = ls())
source("0_Environment.R")
library(strucchange)
library(changepoint)
# Load libraries
library(future)
library(future.apply)
library(progressr)
# number of cores
ncores = globalSettings$num_cores
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
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list

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
dm_rets[, monthsobservation := .N, by = .(sweight, dmname)]
t_all <- dm_rets[, .(t_all = f.custom.t(ret)), by = .(sweight, dmname)]

t_90 <- dm_rets[year(yearm) <= 1990, .(t_90 = f.custom.t(ret)), by = .(sweight, dmname)]


# All stratrgies

# Set up the future plan
plan(multisession, workers = ncores)

# Enable progress handlers
handlers(global = TRUE)
handlers(handler_progress(format = "[:bar] :percent Elapsed: :elapsed ETA: :eta"))
# Define your function
f.break <- function(dt) {
  fit_bp <- breakpoints(ret ~ 1, data = dt, breaks = 1, h = 12*5)
  a <- summary(fit_bp)
  date_break <- dt$yearm[a$breakpoints[1]]
  sweight <- unique(dt$sweight)
  dmname <- unique(dt$dmname)
  data.table(sweight = sweight, dmname = dmname, date_break = date_break)
}

# Split the data into groups
grouped_data <- split(dm_rets[monthsobservation > 120, ], by = c("sweight", "dmname"))

with_progress({
  p <- progressr::progressor(steps = length(grouped_data))
  
  results <- future_lapply(
    grouped_data,
    function(dt_group) {
      # Update progress
      p()
      # Apply the function
      f.break(dt_group)
    },
    future.globals = c("p", "f.break"),
    future.packages = c("strucchange", "data.table")
  )
})

# Combine the results
breakpoints_dt <- rbindlist(results)

# Convert yearmon dates back to Date format for easier plotting
breakpoints_dt[, date_break_numeric := as.Date(date_break)]
# Create a ggplot histogram with cluster centers and sparse x-axis labels
ggplot(breakpoints_dt, aes(x = date_break_numeric)) +
  geom_histogram(binwidth = 30, fill = "lightblue", color = "black", alpha = 0.7) +  # Histogram
  labs(title = "Date Distribution All Strategies", 
       x = "Date", 
       y = "Frequency") +
  theme_minimal() +
  scale_x_date(date_labels = "%b %Y", date_breaks = "3 years")  # Adjusted to show labels every 3 years
ggsave(paste0("../Results/BreaksAll.pdf"), width = 16, height = 8)

###################################################
# All strategies means
###################################################
setkey(breakpoints_dt, sweight, dmname)
setkey(dm_rets, sweight, dmname)

test <- breakpoints_dt[dm_rets, ]
test[, before_break := yearm < date_break]
means_before_after <- test[, .(mean_ret = mean(ret),
                               sharpe =  f.sharp(ret),
                               t_stat = f.custom.t(ret),
                               .N, date_break = first(date_break)),
                           by = .(sweight, dmname, before_break)]

# Reshape the data to have columns for before and after the break
reshaped_dt <- dcast(means_before_after, sweight + dmname+ date_break  ~ before_break, 
                     value.var = c("mean_ret", "sharpe", "t_stat"))

# Rename columns for clarity
setnames(reshaped_dt, 
         c("mean_ret_TRUE", "mean_ret_FALSE", "sharpe_TRUE", "sharpe_FALSE", "t_stat_TRUE", "t_stat_FALSE"),
         c("mean_ret_before", "mean_ret_after", "sharpe_before", "sharpe_after", "t_stat_before", "t_stat_after"))

# Calculate the differences
reshaped_dt[, mean_ret_diff := mean_ret_after - mean_ret_before]
reshaped_dt[, sharpe_diff := sharpe_after - sharpe_before]
reshaped_dt[, t_stat_diff := t_stat_after - t_stat_before]

# View the result
reshaped_dt[, mean(mean_ret_diff, na.rm  = TRUE)]
reshaped_dt[, mean(abs(mean_ret_diff), na.rm  = TRUE)]
reshaped_dt[abs(t_stat_before) > 2, mean(abs(mean_ret_diff), na.rm  = TRUE)]

reshaped_dt[, sign_before := sign(mean_ret_before)]
reshaped_dt[, ret_before_scaled :=  100]
reshaped_dt[, ret_after_scaled :=  100*sign_before*mean_ret_after/mean_ret_before]
reshaped_dt[abs(t_stat_before) > 2, median(ret_after_scaled)]
reshaped_dt[abs(t_stat_before) > 2, mean(abs(mean_ret_diff), na.rm  = TRUE)]

# View the final result
reshaped_dt

###################################################
# All strategies means
###################################################
bp_t_stat_dt <- merge(breakpoints_dt, t_all)[abs(t_all) > 2, ]

# Convert yearmon dates back to Date format for easier plotting
bp_t_stat_dt[, date_break_numeric := as.Date(date_break)]

# Create a ggplot histogram with cluster centers and sparse x-axis labels
ggplot(bp_t_stat_dt, aes(x = date_break_numeric)) +
  geom_histogram(binwidth = 30, fill = "lightblue", color = "black", alpha = 0.7) +  # Histogram
  labs(title = "Date Distribution, |t| > 2 full sample", 
       x = "Date", 
       y = "Frequency") +
  theme_minimal() +
  scale_x_date(date_labels = "%b %Y", date_breaks = "3 years")  # Adjusted to show labels every 3 years
ggsave(paste0("../Results/BreaksTG2FullSample.pdf"), width = 16, height = 8)

#########################
# In 1990
##########################

bp_t_stat_dt_90 <- merge(breakpoints_dt, t_90)[abs(t_90) > 2, ]

# Convert yearmon dates back to Date format for easier plotting
bp_t_stat_dt_90[, date_break_numeric := as.Date(date_break)]

# Create a ggplot histogram with cluster centers and sparse x-axis labels
ggplot(bp_t_stat_dt_90, aes(x = date_break_numeric)) +
  geom_histogram(binwidth = 30, fill = "lightblue", color = "black", alpha = 0.7) +  # Histogram
  labs(title = "Date Distribution, |t| > 2 in 1990", 
       x = "Date", 
       y = "Frequency") +
  theme_minimal() +
  scale_x_date(date_labels = "%b %Y", date_breaks = "3 years")  # Adjusted to show labels every 3 years

ggsave(paste0("../Results/BreaksTG2in1990.pdf"), width = 16, height = 8)
