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

bp_t_stat_dt <- merge(breakpoints_dt, t_all)[abs(t_all) > 2, ]

# Convert the date into numeric format (months since the earliest date)
min_date <- min(bp_t_stat_dt$date_break)
bp_t_stat_dt[, months_since_min := as.numeric((date_break - min_date) * 12)]  # Distance in months

# Step 2: Apply K-means clustering (you can tune `centers` to the number of clusters you expect)
set.seed(123)  # For reproducibility
kmeans_result <- kmeans(bp_t_stat_dt$months_since_min, centers = 2)  # 3 clusters is just an example
# Step 3: Get cluster centers and convert them back to yearmon format
cluster_centers <- kmeans_result$centers  # These are in months since the earliest date
cluster_centers_yearmon <- as.yearmon(min_date + (cluster_centers / 12))  # Convert back to yearmon

# Step 4: Plot the histogram with cluster center lines

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


# Step 4: Plot the histogram with cluster center lines

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

############################################################

test <- dm_rets[sweight == 'ew' & dmname == 1, ]
fit_bp = breakpoints(ret ~ 1, data = test, breaks = 2, h = 12*5)
a <- summary(fit_bp)
test$yearm[a$breakpoints[2, ] ]
# 
# f.break <- function(dt){
#   fit_bp = breakpoints(ret ~ 1, data = dt, breaks = 1, h = 12*5)
#   a <- summary(fit_bp)
#   date_break <-dt$yearm[a$breakpoints[1]]
#   return(date_break)
# }
# test2 <- dm_rets[1:20000, f.break(.SD), by = .(sweight, dmname)]
# 
# # Load necessary libraries
# library(data.table)
# library(future)
# library(future.apply)
# library(strucchange)  # Ensure this is loaded on each worker
# # number of cores
# ncores = globalSettings$num_cores
# # Set up the future plan for parallel execution
# plan(multisession, workers = ncores)  # Adjust the number of workers
# 
# # Define your function (make sure all dependencies are within the function or loaded)
# f.break <- function(dt){
#   fit_bp = breakpoints(ret ~ 1, data = dt, breaks = 1, h = 12*5)
#   a <- summary(fit_bp)
#   date_break <- dt$yearm[a$breakpoints[1]]
#   return(date_break)
# }
# # 
# # # Split the data into a list of data.tables by group
# # grouped_data <- split(dm_rets[monthsobservation > 120, ], by = c("sweight", "dmname"))
# # 
# # # Apply f.break over the list in parallel
# # results <- future_lapply(grouped_data, function(dt_group) {
# #   date_break <- f.break(dt_group)
# #   # Extract group identifiers
# #   sweight <- unique(dt_group$sweight)
# #   dmname <- unique(dt_group$dmname)
# #   # Return results as a data.table
# #   data.table(sweight = sweight, dmname = dmname, date_break = date_break)
# # })
# # 
# # # Combine the results into a single data.table
# # test2 <- rbindlist(results)
# # 
# # 
# # 
# # m_binseg <- cpt.mean(test$ret, penalty = "BIC", method = "BinSeg", Q = 1, minseglen = 12*5)
# # plot(m_binseg, type = "l", xlab = "Index", cpt.width = 4)

##################################




dates_cluster <- breakpoints_dt[, .N, by = date_break]

# Step 2: Calculate distance in months or years from January 2000
reference_date <- as.yearmon("Jan 2000", format = "%b %Y")
breakpoints_dt[, distance_from_2000 := as.numeric((date_break - reference_date) * 12)]  # distance in months

# Step 3: Summarize or visualize the results
print(breakpoints_dt)


# Optional: Plot the distribution of distances to see clustering
hist(breakpoints_dt$distance_from_2000, 
     main = "Distribution of Distance from January 2000", 
     xlab = "Months from January 2000", 
     breaks = 10, 
     col = "lightblue")


# Add cluster assignment to the data.table
breakpoints_dt[, cluster := as.factor(kmeans_result$cluster)]

# Step 3: Plot the clusters
ggplot(breakpoints_dt, aes(x = months_since_min, y = 0, color = cluster)) +
  geom_point(size = 5) +
  labs(title = "Clustering of Dates Based on Natural Centers", 
       x = "Months Since Earliest Date", 
       y = "") +
  theme_minimal()

# Step 3: Get cluster centers and convert them back to yearmon format
cluster_centers <- kmeans_result$centers  # These are in months since the earliest date
cluster_centers_yearmon <- as.yearmon(min_date + (cluster_centers / 12))  # Convert back to yearmon

# Display the cluster centers as yearmon
cluster_centers_yearmon

# 
# 
# # Apply K-means clustering (adjust centers based on your preference)
# set.seed(123)
# kmeans_result <- kmeans(breakpoints_dt$months_since_min, centers = 2)
# 
# # Add cluster labels to the data
# breakpoints_dt[, cluster := as.factor(kmeans_result$cluster)]
# 
# # Step 1: Calculate the boundaries (min and max) for each cluster
# cluster_boundaries <- breakpoints_dt[, .(min_date = min(date_break), max_date = max(date_break)), by = cluster]
# 
# # Step 2: Convert boundaries to numeric date format for plotting
# cluster_boundaries[, min_date_numeric := as.Date(min_date)]
# cluster_boundaries[, max_date_numeric := as.Date(max_date)]
# 
# # Step 3: Plot histogram with shaded regions for cluster boundaries
# ggplot(breakpoints_dt, aes(x = as.Date(date_break))) +
#   geom_histogram(binwidth = 30, fill = "lightblue", color = "black", alpha = 0.7) +
#   geom_rect(data = cluster_boundaries, 
#             aes(xmin = min_date_numeric, xmax = max_date_numeric, ymin = -Inf, ymax = Inf, fill = cluster),
#             alpha = 0.2) +  # Shaded regions for cluster boundaries
#   geom_vline(xintercept = as.Date(cluster_centers_yearmon), color = "red", linetype = "dashed", size = 1) +
#   labs(title = "Date Distribution with Cluster Centers and Boundaries", 
#        x = "Date", 
#        y = "Frequency") +
#   theme_minimal() +
#   scale_x_date(date_labels = "%b %Y", date_breaks = "3 years")  # Adjust for sparser x-axis labels
