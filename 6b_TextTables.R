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
library(tools)

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
# signal_text[, theory := NULL]

czcat = fread('DataInput/SignalsTheoryChecked.csv') %>% 
  select(signalname, theory)

czsum = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>% setDT()

czcat <- merge(czsum[Keep == TRUE, .(signalname)], czcat)
czcat[, theory2 := theory]
setkey(signal_text, signalname)
setkey(czcat, signalname)

czcat[, .N, by = theory]
signal_text[, .N, by = theory]

signal_text[czcat, theory2 := theory2]
signal_text[, .N, by = theory2]

signal_text[, Keep := NULL]
# redundant, but fix me carefully later
czret = readRDS('../Data/Processed/czsum_allpredictors.RDS') %>% setDT()
setkey(czret, signalname)


signal_text[czret, Keep := Keep]
signal_text <- signal_text[Keep == TRUE,]  



sub_sample_text <- signal_text %>% dplyr::select(signalname, Journal,
                                                 Authors, Year, theory,
                                                 misp_count, risk_count,
                                                 misprice_risk_ratio) 





sub_sample_text[, anom := (theory == 'mispricing')*1]

sub_sample_text[theory == 'risk', anom := -1]

sub_sample_text[, cor(misprice_risk_ratio, anom)]

sub_sample_text[, plot(misprice_risk_ratio, anom)]

sub_sample_text[, plot(anom, misprice_risk_ratio)]
# 
# View(sub_sample_text[anom > 0, ])
# 
# View(sub_sample_text[anom < 0, ])
# 
# View(sub_sample_text[anom > 0, ])

# sub_sample_text[, unique(theory)]
# 
# signal_text[,hist(sampend, breaks = 20)]
# 
# signal_text[,hist(sampstart, breaks = 20)]
# 
# signal_text[, Mode(sampend)]
# 
# signal_text[, Mode(sampstart)]
# 
# signal_text[, mean(sampend)]
# 
# signal_text[, mean(sampstart)]
# 
# signal_text[, median(sampend)]
# 
# signal_text[, median(sampstart)]

signal_text_to_table <- signal_text[, .(signalname, theory,
                                        Predictor = LongDescription,
        ExampleText = str_replace_all(quote, "[\r\n]" , "") %>%
          str_replace_all("[.] " , ".\n"),
        Authors,
        PubYear = Year,
        Journal,
        word_count, misp_count, risk_count,
        misprice_risk_ratio)]

# Use dcast to create the two-way frequency table
freq_table <- dcast(signal_text_to_table, Journal ~ theory, fun.aggregate = length)

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
  group_by(theory, Journal) %>% 
  summarise(n=n()) %>% 
  group_by(Journal) %>% 
  mutate(perc = n / sum(n) * 100)

summary_matrix <- summary_df %>%
  pivot_wider(names_from = theory, values_from = perc, values_fill = 0)

# %>%
#   column_to_rownames(var = "Journal") %>%
#   as.matrix()

journal_type_table <- signal_text_to_table %>% group_by(Journal, theory) %>% summarise(n=n()) %>% mutate(perc = n / sum(n) * 100)

journal_type_table <- signal_text_to_table[, table(Journal, theory)] %>% as.data.frame.matrix()

setnames(journal_type_table, names(journal_type_table), toTitleCase(names(journal_type_table)))

# Assuming journal_type_table is your data frame or matrix
xt <- xtable(journal_type_table)

# Print the xtable object to LaTeX
# include.rownames=TRUE ensures row names are included
# include.colnames=TRUE ensures column names are included
print(xtable(xt), include.rownames=TRUE, include.colnames=TRUE, comment=FALSE)

text_examples <- signal_text_to_table[signalname == 'ShareRepurchase', ] %>%
  rbind(signal_text_to_table[signalname == 'SurpriseRD', ]) %>%
  rbind(signal_text_to_table[signalname == 'cfp', ])  %>%
  rbind(signal_text_to_table[signalname == 'VolSD', ])  %>%
  rbind(signal_text_to_table[signalname == 'NetPayoutYield', ])  %>%
  rbind(signal_text_to_table[signalname == 'Size', ])  %>%
  rbind(signal_text_to_table[signalname == 'ChEQ', ])  %>%
  rbind(signal_text_to_table[signalname == 'hire', ])  %>%
  rbind(signal_text_to_table[signalname == 'realestate', ])  %>%
  rename(Theory = theory) %>%
  mutate(Reference = paste(Authors, PubYear))  %>%
  dplyr::select(!c(signalname, word_count, misp_count, risk_count, Authors, PubYear, Journal)) %>%
  arrange(misprice_risk_ratio)  %>%
  mutate(RiskMispricingRatio = round(1/misprice_risk_ratio, 2))  %>%
    dplyr::select(Theory, Reference, Predictor, ExampleText, RiskMispricingRatio)

text_examples


fwrite(text_examples, '../Results/text_examples.csv')




summary_text <- bind_rows(
  signal_text,
  signal_text %>% mutate(theory = "Any")
) %>% group_by(theory) %>%
  mutate(RiskMispricingRatio = (1/misprice_risk_ratio) %>% round(2)) %>%
  arrange(-RiskMispricingRatio)  %>%
  summarise(holder_0 = '', Count = n(), CountPre2004 = sum(Year < 2004),
            CountPost2004 = sum(Year >= 2004),
            holder_1 = '',
            holder_2 = '',
            # TypicalSampleStart = median(ymd(sampstart)) %>% as.character(),
            # TypicalSampleEnd = median(ymd(sampend))  %>% as.character(),
            # MeanRiskMispricingRatio = mean(RiskMispricingRatio) %>% round(2)  %>% as.character(),
            # MinRiskMispricingRatio = min(RiskMispricingRatio) %>% round(2)  %>% as.character(),
            q05RiskMispricingRatio = quantile(RiskMispricingRatio, 0.05, na.rm = TRUE) %>% sprintf("%.2f",.)  %>% as.character(),
            # q25RiskMispricingRatio = quantile(RiskMispricingRatio, 0.25) %>% round(2)  %>% as.character(),
            MedianRiskMispricingRatio = median(RiskMispricingRatio, na.rm = TRUE) %>% sprintf("%.2f",.)  %>% as.character(),
            # q75RiskMispricingRatio = quantile(RiskMispricingRatio, 0.75) %>% round(2)  %>% as.character(),
            q95RiskMispricingRatio = quantile(RiskMispricingRatio, 0.95, na.rm = TRUE) %>% sprintf("%.2f",.)  %>% as.character()
            # MaxRiskMispricingRatio = max(RiskMispricingRatio) %>% round(2)  %>% as.character()
            )  %>%
  rename(Theory = theory) %>%
  arrange(factor(Theory,
                 levels = c('risk', 'mispricing', 'agnostic', 'Any'))) %>% 
  mutate(Theory = str_to_title(Theory)) %>% setDT()

to_merge_csv <- fread('DataIntermediate/FollowUpToMerge.csv')

setkey(to_merge_csv, Theory)

setkey(summary_text, Theory)

##############
# First panel
##############


# Custom order for column1
custom_order <- c("Risk", "Mispricing", "Agnostic", "Any")

# Factor the column with levels specified by custom order
summary_text[, Theory := factor(Theory, levels = custom_order)]
setorder(summary_text, Theory)


x_summ <- xtable(summary_text, align = c("l", "l", rep("r", 9)))
x_summ
# Define the LaTeX code to add
headerRow <- c("   \\toprule
   \\multicolumn{10}{c}{Panel (a)} \\\\",
               "\\cmidrule{1-11}",
               "Source of &   & \\multicolumn{3}{c}{Num Published Predictors} &    \\multicolumn{6}{c}{Risk Words to Mispricing Words}  \\\\",
               "Predictability &   & \\multicolumn{1}{c}{Total} & \\multicolumn{1}{c}{1981-2004} & \\multicolumn{1}{c}{2005-2016} & \\phantom  & & \\multicolumn{1}{c}{p05} & \\multicolumn{1}{c}{p50} & p95 & \\phantom{a}\\\\",
               "\\cmidrule{1-1} \\cmidrule{3-5} \\cmidrule{7-11}
"
)
# Calculate the position for the \hline before the last row
nrows <- nrow(summary_text)
hline_pos <- nrows - 1  # Since indexing starts at zero

# Specify where to add the custom header and the \hline
addtorow <- list(
  pos = list(0, hline_pos, nrows),  # Add header at the beginning and hline before the last row
  command = c(paste(headerRow, collapse = "\n"), "\\hline\n", "\\bottomrule")
)
# Start the sink to redirect output to a file
sink("../Results/table_theory_follow_up_a.tex")

# Print the xtable with the custom LaTeX header and specified alignment
print(x_summ, include.rownames = FALSE, include.colnames = FALSE,
      hline.after = NULL,
      add.to.row = addtorow,
      booktabs = TRUE, sanitize.text.function = function(x){x},
      only.contents = TRUE)
sink()
print(x_summ, include.rownames = FALSE, include.colnames = FALSE,
      hline.after = NULL,
      add.to.row = addtorow,
      booktabs = TRUE, sanitize.text.function = function(x){x},
      only.contents = FALSE)

print(x_summ, include.rownames = FALSE, include.colnames = FALSE,
      hline.after = NULL,
      add.to.row = addtorow,
      booktabs = TRUE, sanitize.text.function = function(x){x},
      only.contents = TRUE)


##############
# Second panel
##############

merged_summary <-to_merge_csv %>% copy()
merged_summary[, holder_1 := '']
setcolorder(merged_summary, c('Theory', 'holder_1'))
# Custom order for column1
custom_order <- c("Risk", "Mispricing", "Agnostic", "Any")

# Factor the column with levels specified by custom order
merged_summary[, Theory := factor(Theory, levels = custom_order)]
setorder(merged_summary, Theory)


x_summ <- xtable(merged_summary, align = c("l", "l", rep("r", 5)))

# Define the LaTeX code to add
headerRow <- c("\\toprule
\\multicolumn{6}{c}{Panel (b)} \\\\",
"\\cmidrule{1-6}",
               "Source of &   & \\multicolumn{4}{c}{Follow-Up Citations Category}\\\\",
               "Predictability &   &Incidental &  Methodological & Substantial&   Other \\\\",
               "\\cmidrule{1-1} \\cmidrule{3-6}
"
)
# Calculate the position for the \hline before the last row
nrows <- nrow(summary_text)
hline_pos <- nrows - 1  # Since indexing starts at zero

# Specify where to add the custom header and the \hline
addtorow <- list(
  pos = list(0, hline_pos),  # Add header at the beginning and hline before the last row
  command = c(paste(headerRow, collapse = "\n"), "\\hline
")
)
# Start the sink to redirect output to a file
sink("../Results/table_theory_follow_up_b.tex")

# Print the xtable with the custom LaTeX header and specified alignment
print(x_summ, include.rownames = FALSE, include.colnames = FALSE,
      hline.after = NULL,
      add.to.row = addtorow,
      booktabs = TRUE, sanitize.text.function = function(x){x},
      only.contents = TRUE)
sink()
print(x_summ, include.rownames = FALSE, include.colnames = FALSE,
      hline.after = NULL,
      add.to.row = addtorow,
      booktabs = TRUE, sanitize.text.function = function(x){x},
      only.contents = FALSE)
fwrite(summary_text, '../Results/summary_text.csv')

