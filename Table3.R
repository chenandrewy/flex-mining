################################
# Setup
################################
# Setup -------------------------------------------------------------------
cat("\f")  
rm(list=ls())
gc()
library(MASS)
library(tidyverse)
library(data.table)
library(haven)
library(xtable)
require(RcppRoll)
library(glue)
library(janitor)
library(lubridate)
library(stringr)
library(lubridate)
library(zoo)
library(latex2exp)
library(extrafont)
library(lmtest)
library(sandwich)
library(broom)
library(huxtable)
################################
# Functions
################################
# Setup ------------------------------------------------------------------





MATBLUE = rgb(0,0.4470,0.7410)
MATRED = rgb(0.8500, 0.3250, 0.0980)
MATYELLOW = rgb(0.9290, 0.6940, 0.1250)

NICEBLUE = "#619CFF"
NICEGREEN = "#00BA38"
NICERED = "#F8766D"

chen_theme =   theme_minimal() +
  theme(
    text = element_text(family = "Palatino Linotype")
    , panel.border = element_rect(colour = "black", fill=NA, size=1)
    
    # Font sizes
    , axis.title.x = element_text(size = 26),
    axis.title.y = element_text(size = 26),
    axis.text.x = element_text(size = 22),
    axis.text.y = element_text(size = 22),
    legend.text = element_text(size = 18),
    
    # Tweaking legend
    legend.position = c(0.7, 0.8),
    legend.text.align = 0,
    legend.background = element_rect(fill = "white", color = "black"),
    legend.margin = margin(t = 5, r = 20, b = 5, l = 5), 
    legend.key.size = unit(1.5, "cm")
    , legend.title = element_blank()    
  ) 


# signaldoc + cat data
# should we use theory or theory1?
signaldoc = fread('DataInput/SignalsTheoryChecked.csv') %>%
  mutate(theory1 = theory) %>%
  filter(Keep == 1)


# czreturns
cz_all = fread("../Data/CZ/PredictorPortsFull.csv")
# czret (monthly returns)
czret = cz_all %>%                                         
  filter(!is.na(ret), port == 'LS') %>%                                                           
  left_join(signaldoc) %>% 
  filter(date >= sampstart) %>%
  dplyr::select(signalname, date, ret,  sampstart, sampend,theory1, pubdate) %>%
  mutate(in_samp = (date >= sampstart) & (date <= sampend)) %>%
  mutate(post_samp = (date > sampend)) %>%
  mutate(post_pub = (date > pubdate)) %>%
  mutate(risk_theory = theory1 == 'risk')  %>%
  as.data.table()

czret[in_samp == TRUE, mean(ret)]


tempsum = czret %>% 
  filter(in_samp == TRUE) %>% 
  group_by(signalname) %>% 
  summarize(rbar_insamp = mean(ret))

tempret = czret %>% 
  dplyr::select(signalname, date, ret, sampstart, sampend, theory1,
                in_samp, risk_theory, post_pub, post_samp) %>% 
  mutate(
    samp_time = year(date) + month(date)/12
    - (year(sampend) + month(sampend)/12)
  ) %>% 
  left_join(tempsum, by = 'signalname')  %>% 
  filter(!is.na(theory1)) %>% 
  mutate(
    ret_n = 100*ret/rbar_insamp
  )%>% as.data.table()

tempret[in_samp == TRUE, mean(ret_n), by = risk_theory]

tempret[, mean(ret), by = risk_theory]


a1 <- tempret %>%
  lm(ret_n ~ post_samp +  I(I(post_samp == TRUE)*I(theory1 == 'risk')) , data = .) %>%
  coeftest(., vcov = vcovCL, cluster = ~ date) %>% round(10)

# rownames(a1) <- gsub('post_samp == TRUE', 'post_samp == TRUE', rownames(a1)) %>%
#   gsub('theory1 == 'risk'', 'theory1 == "risk"', .)

a1

a2 <- tempret %>%
  lm(ret_n ~ post_samp + post_pub + I(I(post_samp == TRUE)*I(theory1 == 'risk')) +
       I(I(post_pub == TRUE)*I(theory1 == 'risk')), data = .) %>%
  coeftest(., vcov = vcovCL, cluster = ~ date) %>% round(10)
a2
# rownames(a2) <- gsub('theory1 == 'risk'', 'theory1 == "risk"', rownames(a2))

a3 <- tempret %>%
  lm(ret_n ~ post_samp +  I(I(post_samp == TRUE)*I(theory1 == 'risk'))+
       I(I(post_samp == TRUE)*I(theory1 == 'mispricing')), data = .) %>%
  coeftest(., vcov = vcovCL, cluster = ~ date) %>% round(10)

a3
# rownames(a3) <- gsub('post_samp == TRUE', 'post_samp == TRUE', rownames(a3)) %>%
#   gsub('post_sampTRUE', 'samptypepostsample',.)



a4 <- tempret %>%
  lm(ret_n ~ post_samp + post_pub + I(I(post_samp == TRUE)*I(theory1 == "risk")) +
       I(I(post_pub == TRUE)*I(theory1 == "risk")) +
       I(I(post_samp == TRUE)*I(theory1 == "mispricing")) +
       I(I(post_pub == TRUE)*I(theory1 == "mispricing")) , data = .) %>%
  coeftest(., vcov = vcovCL, cluster = ~ date) %>% round(10)


a4

reg_save <- huxreg(a1, a2, a3, a4, coefs = c(
  "Intercept" = "(Intercept)",
  "Post-Sample" = "post_sampTRUE",
  "Post-Pub" = 'post_pubTRUE',
  'Post-Sample x Risk' = 'I(I(post_samp == TRUE) * I(theory1 == "risk"))',
  'Post-Pub x Risk' = 'I(I(post_pub == TRUE) * I(theory1 == "risk"))',
  'Post-Sample x Mispricing' = 'I(I(post_samp == TRUE) * I(theory1 == "mispricing"))',
  'Post-Pub x Mispricing' = 'I(I(post_pub == TRUE) * I(theory1 == "mispricing"))'),
  statistics = c('nobs'), stars = NULL)
reg_save

fwrite(reg_save, '../Results/RegressionMultiColumns.csv', sep = ',')
