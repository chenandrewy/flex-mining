# Download CZ data

rm(list = ls())
library(tidyverse)
library(data.table)
library(googledrive)
library(lubridate)

# March 2022 cz data
url <- "https://drive.google.com/drive/folders/1O18scg9iBTiBaDiQFhoGxdn4FdsbMqGo" 


## Download Baseline CZ Data ====

if (!file.exists('../Data/CZ/PredictorPortsFull.csv')){
  
  SUBDIR = 'Full Sets OP'; FILENAME = 'PredictorPortsFull.csv'
  
  url %>% drive_ls() %>%
    filter(name == "Portfolios") %>% drive_ls() %>% 
    filter(name == SUBDIR) %>% drive_ls() %>% 
    filter(name == FILENAME) %>% 
    drive_download(path = paste0("../Data/CZ/",FILENAME), overwrite = TRUE)
  
  # signal doc 
  url %>% drive_ls() %>% 
    filter(name == "SignalDoc.csv") %>% 
    drive_download(path = "../Data/CZ/SignalDoc.csv", overwrite = TRUE)
}
