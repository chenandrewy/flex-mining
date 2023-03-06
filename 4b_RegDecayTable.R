############################### #
# Setup ====
############################### #

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
library(multcomp)



############################### #
# Functions ====
############################### #


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



# Do stuff ----------------------------------------------------------------



signalcat = fread('DataInput/SignalsTheoryChecked.csv') 

czret_1 = readRDS('../Data/Processed/czsum_all207.RDS') %>% setDT()

setkey(czret_1, signalname)
setkey(signalcat, signalname)

signalcat[czret_1, Keep := Keep]

signalcat <- signalcat[Keep == TRUE,]  
# # czreturns

# redundant, but fix me carefully later
czret = readRDS('../Data/Processed/czret.RDS') %>% 
  filter(!is.na(samptype)) %>% 
  mutate(in_samp = samptype == 'insamp') %>%
  mutate(post_samp = samptype %in% c('oos','postpub')) %>% 
  mutate(post_pub = samptype == 'postpub') %>%  
  left_join(
    signalcat %>% dplyr::select(signalname, theory1)
    , by = 'signalname'
  ) %>% 
  mutate(risk_theory = theory1 == 'risk')  %>%  
  filter(Keep == TRUE) %>%
  setDT()

czret[in_samp == TRUE, mean(ret)]


tempsum = czret %>% 
  filter(in_samp == TRUE) %>% 
  group_by(signalname) %>% 
  summarize(rbar_insamp = mean(ret)) %>% 
  setDT()

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
  )%>% setDT()


# tempret[in_samp == TRUE, mean(ret_n), by = risk_theory] # triggers bizarre data.table bug

tempret[, mean(ret), by = risk_theory]


a1 <- tempret %>%
  lm(ret_n ~ post_samp +  I(I(post_samp == TRUE)*I(theory1 == 'risk')) , data = .) %>%
  coeftest(., vcov = vcovCL, cluster = ~ date) %>% round(10)

a2_model <- tempret %>%
  lm(ret_n ~ post_samp + post_pub + I(I(post_samp == TRUE)*I(theory1 == 'risk')) +
       I(I(post_pub == TRUE)*I(theory1 == 'risk')), data = .)
a2 <- a2_model %>%
  coeftest(., vcov = vcovCL, cluster = ~ date) %>% round(10)


coefeq_a2 <- matrix(data=0, nrow=1, ncol=length(a2_model$coefficients))
colnames(coefeq_a2) <- names(a2_model$coefficients)
coefeq_a2[1,-1] <- 1
coefeq_a2
ametest_2<- glht(model=a2_model, linfct=coefeq_a2, rhs=0, alternative="less",
                 vcov = vcovCL(a2_model, cluster = ~ date))
sum_a2 <- summary(ametest_2)
p_val_a2 <- sum_a2$test$pvalues[1]

a3 <- tempret %>%
  lm(ret_n ~ post_samp +  I(I(post_samp == TRUE)*I(theory1 == 'risk'))+
       I(I(post_samp == TRUE)*I(theory1 == 'mispricing')), data = .) %>%
  coeftest(., vcov = vcovCL, cluster = ~ date) %>% round(10)

a3


a4_model <- tempret %>%
  lm(ret_n ~ post_samp + post_pub + I(I(post_samp == TRUE)*I(theory1 == "risk")) +
       I(I(post_pub == TRUE)*I(theory1 == "risk")) +
       I(I(post_samp == TRUE)*I(theory1 == "mispricing")) +
       I(I(post_pub == TRUE)*I(theory1 == "mispricing")) , data = .)

a4 <- a4_model %>%
  coeftest(., vcov = vcovCL, cluster = ~ date) %>% round(10)


coefeq_4 <- matrix(data=0, nrow=1, ncol=length(a4_model$coefficients))
colnames(coefeq_4) <- names(a4_model$coefficients)
coefeq_4[1,-c(1,6, 7)] <- 1
coefeq_4
ametest_4_risk <- glht(model=a4_model,
                  linfct=coefeq_4, rhs=0,
                  alternative="less",
                  vcov = vcovCL(a4_model,  cluster = ~ date))
sum_a4 <- summary(ametest_4_risk)
p_val_a4_risk <- sum_a4$test$pvalues[1]
p_table_4 <- ifelse(p_val_a4_risk < 0.01, '< 1%', p_val_a4_risk)
p_table_2 <- ifelse(p_val_a2 < 0.01, '< 1%', p_val_a2)

coefeq_4_misp <- matrix(data=0, nrow=1, ncol=length(a4_model$coefficients))
colnames(coefeq_4_misp) <- names(a4_model$coefficients)
coefeq_4_misp[1,-c(1,4, 5)] <- 1
coefeq_4_misp
ametest_4_misp <- glht(model=a4_model,
                       linfct=coefeq_4_misp, rhs=0,
                       alternative="greater",
                       vcov = vcovCL(a4_model,  cluster = ~ date))
sum_a4_misp <- summary(ametest_4_misp)
p_val_a4_misp <- sum_a4_misp$test$pvalues[1]
p_table_4_misp <- ifelse(p_val_a4_misp < 0.01, '< 1%', round(p_val_a4_misp, 2))

reg_save <- huxreg(a1, a2, a3, a4, coefs = c(
  "Intercept" = "(Intercept)",
  "Post-Sample" = "post_sampTRUE",
  "Post-Pub" = 'post_pubTRUE',
  'Post-Sample x Risk' = 'I(I(post_samp == TRUE) * I(theory1 == "risk"))',
  'Post-Pub x Risk' = 'I(I(post_pub == TRUE) * I(theory1 == "risk"))',
  'Post-Sample x Mispricing' = 'I(I(post_samp == TRUE) * I(theory1 == "mispricing"))',
  'Post-Pub x Mispricing' = 'I(I(post_pub == TRUE) * I(theory1 == "mispricing"))'),
  statistics = c('nobs'), stars = NULL, number_format = 2) %>%
  insert_row(c('p-val Risk', '', p_table_2, '', p_table_4)
             , after = 16) %>%
  insert_row(c('p-val Mispricing', '', '', '', p_table_4_misp)
             , after = 17)
reg_save
round_numbers_in_strings <- function(strings_with_numbers) {
  regex_pattern <- "\\d+\\.?\\d*" # matches any number with or without decimal point
  rounded_strings <- c() # create an empty vector to store the results
  
  for (string_with_number in strings_with_numbers) {
    # Use regular expressions to extract the number from the string
    number_in_string <- as.numeric(gsub("[^[:digit:].]", "", regmatches(string_with_number, regexpr(regex_pattern, string_with_number))))
    
    # Round the number to two decimal places
    rounded_number <- round(number_in_string, 1)
    
    # Replace the original number in the string with the rounded number
    string_with_rounded_number <- gsub(regex_pattern, toString(rounded_number), string_with_number)
    
    # Add the result to the output vector
    rounded_strings <- c(rounded_strings, string_with_rounded_number)
  }
  
  return(rounded_strings)
}


data_new1 <- reg_save[reg_save$names != 'nobs',] %>% as.data.frame()
rownames(data_new1) <- NULL
df_rounded <- data.frame(lapply(data_new1,round_numbers_in_strings))
df_rounded[1,1] <- 'RHS Variables'

xt <- xtable(df_rounded)
print.xtable(xt, digits = 2, include.rownames=FALSE, include.colnames = FALSE, hline.after = c(0,1, 15, 17))
fwrite(reg_save, '../Results/RegressionMultiColumns.csv', sep = ',')
