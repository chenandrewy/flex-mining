# Setup ------------------------------------------------------------------
rm(list = ls())
gc()
library(tidyverse)
library(data.table)
library(lubridate)
library(zoo)
library(latex2exp)
library(extrafont)
library("readxl")
library(janitor)
library(stringr)
Mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

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


signal_text <- fread('DataIntermediate/TextClassification.csv')
signal_text[, Keep := NULL]
# redundant, but fix me carefully later
czret = readRDS('../Data/Processed/czsum_all207.RDS') %>% setDT()

setkey(czret, signalname)
setkey(signal_text, signalname)

signal_text[czret, Keep := Keep]

signal_text <- signal_text[Keep == TRUE,]  


sub_sample_text <- signal_text %>% dplyr::select(signalname, Journal,
                                                 Authors, Year, theory1,
                                                 misp_count, risk_count,
                                                 misprice_risk_ratio) 





sub_sample_text[, anom := (theory1 == 'mispricing')*1]

sub_sample_text[theory1 == 'risk', anom := -1]

sub_sample_text[, cor(misprice_risk_ratio, anom)]

sub_sample_text[, plot(misprice_risk_ratio, anom)]

sub_sample_text[, plot(anom, misprice_risk_ratio)]
# 
# View(sub_sample_text[anom > 0, ])
# 
# View(sub_sample_text[anom < 0, ])
# 
# View(sub_sample_text[anom > 0, ])

sub_sample_text[, unique(theory1)]

signal_text[,hist(sampend, breaks = 20)]

signal_text[,hist(sampstart, breaks = 20)]

signal_text[, Mode(sampend)]

signal_text[, Mode(sampstart)]

signal_text[, mean(sampend)]

signal_text[, mean(sampstart)]

signal_text[, median(sampend)]

signal_text[, median(sampstart)]

signal_text_to_table <- signal_text[, .(signalname, theory1,
                                        Predictor = desc,
        ExampleText = str_replace_all(quote, "[\r\n]" , "") %>%
          str_replace_all("[.] " , ".\n"),
        Authors,
        PubYear = Year,
        Journal,
        word_count, misp_count, risk_count,
        misprice_risk_ratio)]

text_examples <- signal_text_to_table[signalname == 'ShareRepurchase', ] %>%
  rbind(signal_text_to_table[signalname == 'SurpriseRD', ]) %>%
  rbind(signal_text_to_table[signalname == 'cfp', ])  %>%
  rbind(signal_text_to_table[signalname == 'VolSD', ])  %>%
  rbind(signal_text_to_table[signalname == 'NetPayoutYield', ])  %>%
  rbind(signal_text_to_table[signalname == 'Size', ])  %>%
  rbind(signal_text_to_table[signalname == 'ChEQ', ])  %>%
  rbind(signal_text_to_table[signalname == 'hire', ])  %>%
  rbind(signal_text_to_table[signalname == 'realestate', ])  %>%
  rename(Theory = theory1) %>%
  mutate(Reference = paste(Authors, PubYear))  %>%
  dplyr::select(!c(signalname, word_count, misp_count, risk_count, Authors, PubYear, Journal)) %>%
  arrange(misprice_risk_ratio)  %>%
  mutate(RiskMispricingRatio = round(1/misprice_risk_ratio, 2))  %>%
    dplyr::select(Theory, Reference, Predictor, ExampleText, RiskMispricingRatio)

text_examples


fwrite(text_examples, '../Results/text_examples.csv')




summary_text <- bind_rows(
  signal_text,
  signal_text %>% mutate(theory1 = "Any")
) %>% group_by(theory1) %>%
  mutate(RiskMispricingRatio = (1/misprice_risk_ratio) %>% round(2)) %>%
  arrange(-RiskMispricingRatio)  %>%
  summarise(holder_0 = '', Count = n(), CountPre2004 = sum(Year < 2004),
            CountPost2004 = sum(Year >= 2004),
            holder_1 = '',
            # TypicalSampleStart = median(ymd(sampstart)) %>% as.character(),
            # TypicalSampleEnd = median(ymd(sampend))  %>% as.character(),
            # MeanRiskMispricingRatio = mean(RiskMispricingRatio) %>% round(2)  %>% as.character(),
            # MinRiskMispricingRatio = min(RiskMispricingRatio) %>% round(2)  %>% as.character(),
            q05RiskMispricingRatio = quantile(RiskMispricingRatio, 0.05) %>% round(2)  %>% as.character(),
            # q25RiskMispricingRatio = quantile(RiskMispricingRatio, 0.25) %>% round(2)  %>% as.character(),
            MedianRiskMispricingRatio = median(RiskMispricingRatio) %>% round(2)  %>% as.character(),
            # q75RiskMispricingRatio = quantile(RiskMispricingRatio, 0.75) %>% round(2)  %>% as.character(),
            q95RiskMispricingRatio = quantile(RiskMispricingRatio, 0.95) %>% round(2)  %>% as.character()
            # MaxRiskMispricingRatio = max(RiskMispricingRatio) %>% round(2)  %>% as.character()
            )  %>%
  rename(Theory = theory1) %>%
  arrange(factor(Theory,
                 levels = c('risk', 'mispricing', 'agnostic', 'Any'))) %>% 
  mutate(Theory = str_to_title(Theory))

x_summ <-  summary_text %>% xtable()

print(x_summ, include.rownames=FALSE, include.colnames = FALSE, hline.after = c(3))

fwrite(summary_text, '../Results/summary_text.csv')

