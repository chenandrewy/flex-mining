# Setup ------------------------------------------------------------------
cat("\f")  
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
library(xtable)
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

# Use dcast to create the two-way frequency table
freq_table <- dcast(signal_text_to_table, Journal ~ theory1, fun.aggregate = length)

names_theory <- colnames(freq_table)[-1]
theory_counts <- freq_table[, colSums(.SD), .SDcols = names_theory]
freq_table[, Row_Total := rowSums(.SD) %>% round(0), .SDcols = names_theory] # -1 to exclude the first column (Journal)
print(xtable(freq_table, digits = 0), include.rownames=FALSE, type="html", file="count_journal_type.html")
prop_table_per_journal <- freq_table[,cbind(Journal, (100*.SD/Row_Total) %>% round(0)), .SDcols = names_theory] 
prop_table_per_theory <- freq_table[,cbind(Journal, (100*.SD/theory_counts) %>% round(0)), .SDcols = names_theory] 

total_sum <- sum(freq_table[,-1])

prop_table <- freq_table %>%
  mutate(across(where(is.numeric), ~ . / total_sum * 100))




summary_df <- signal_text_to_table %>% 
  group_by(theory1, Journal) %>% 
  summarise(n=n()) %>% 
  group_by(Journal) %>% 
  mutate(perc = n / sum(n) * 100)

summary_matrix <- summary_df %>%
  pivot_wider(names_from = theory1, values_from = perc, values_fill = 0) 
%>%
  column_to_rownames(var = "Journal") 
%>%
  as.matrix()

journal_type_table <- signal_text_to_table %>% group_by(Journal, theory1) %>% summarise(n=n()) %>% mutate(perc = n / sum(n) * 100)

journal_type_table <- signal_text_to_table[, table(Journal, theory1)] %>% as.data.frame.matrix()

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

