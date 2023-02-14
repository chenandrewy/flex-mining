# Setup ------------------------------------------------------------------

library(tidyverse)
library(data.table)
library(lubridate)
library(zoo)
library(latex2exp)
library(extrafont)
library("readxl")

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
library(stringr)
subset_text <- fread('../IntermediateText/TextAnalysis.csv')  %>%  rename(Journal = journal) %>%
  # dplyr::select(Authors, Year, Journal, file_names)%>%
  mutate(Journal = gsub('^RF$', 'ROF', Journal)) %>%
  mutate(Journal = gsub('^TAR$', 'AR', Journal)) %>%
  mutate(Authors = gsub('et al.?|and |,', '', Authors)) %>%
  mutate(FirstAuthor = word(Authors)) %>%
  filter(Authors != 'Ang et al') %>%
  filter(Authors != 'Chen Jegadeesh Lakonishok')



signaldoc_orig = fread('../data-cz/SignalDoc.csv')

# signaldoc <- fread('../../SignalsTheoryChecked.csv') %>%
#   mutate(theory1 = theory2) %>% merge(signaldoc_orig  %>%
#                                         transmute(
#                                           signalname = Acronym,
#                                           desc = LongDescription,
#                                           Journal = Journal))

signaldoc = readxl::read_excel('SignalsTheoryChecked.xlsx') %>%
  mutate(theory1 = theory) %>% merge(signaldoc_orig  %>%
                                        transmute(
                                          signalname = Acronym,
                                          desc = LongDescription,
                                          Journal = Journal)) %>%
  filter(Keep == 1)

signalcat = readxl::read_excel('OP-text.xlsx',
                               sheet = 'Risk or Mispricing') %>% 
  transmute(signalname = Acronym, quote = Quote)

signal_text <-  signaldoc %>% merge(signalcat) %>% as.data.table() %>%
  mutate(post_2004 = Year >= 2004) %>%
  mutate(author_merge = gsub('et al.?|and |,', '', Authors))

library(fuzzyjoin)
joined_inner <- signal_text %>% dplyr::select(signalname, Year, Journal, author_merge, Authors) %>%
  rename(Authors2 = Authors, Authors = author_merge) %>%
  stringdist_inner_join(subset_text ,
                        by = c('Authors', 'Journal'),
                        max_dist = 1) %>% filter(abs(Year.x - Year.y) < 1)

names_in_inner <- unique(joined_inner$signalname.x)


to_join <- signal_text %>% filter(!signalname %in% names_in_inner )  %>%
  dplyr::select(signalname, Year, Journal, author_merge, Authors) %>%
  rename(Authors2 = Authors, Authors = author_merge) %>%
  stringdist_inner_join(subset_text , by = c('Authors'),
                        max_dist = 1)  %>%
  filter(abs(Year.x - Year.y) < 1)

joined_so_far <- rbind(joined_inner, to_join)

names_so_far <- unique(joined_so_far$signalname.x)

to_join <- signal_text %>% filter(!signalname %in% names_so_far )  %>%
  dplyr::select(signalname, Year, Journal, author_merge, Authors) %>%
  rename(Authors2 = Authors, Authors = author_merge) %>%
  stringdist_left_join(subset_text , by = c('Authors'),
                        max_dist = 2) %>%
  filter(abs(Year.x - Year.y) < 5)

joined_so_far <- rbind(joined_so_far, to_join)

names_so_far <- unique(joined_so_far$signalname.x)

authors_y <- unique(joined_so_far$Authors.y)

to_join <- signal_text %>% filter(!signalname %in% names_so_far )  %>%
  dplyr::select(signalname, Year, Journal, author_merge, Authors) %>%
  rename(Authors2 = Authors, Authors = author_merge) %>%
  stringdist_left_join(subset_text , by = c('Authors' = 'FirstAuthor'),
                       max_dist = 1) %>%
  filter(abs(Year.x - Year.y) < 1) %>% filter(Journal.x == Journal.y)

joined_so_far <- rbind(joined_so_far, to_join)

names_so_far <- unique(joined_so_far$signalname.x)

to_join <- signal_text %>% filter(!signalname %in% names_so_far ) %>%
  dplyr::select(signalname, Year, Journal, author_merge, Authors) %>%
  rename(Authors2 = Authors, Authors = author_merge) %>%
  stringdist_left_join(subset_text , by = c('Authors' = 'FirstAuthor'),
                       max_dist = 1) %>%
  filter(abs(Year.x - Year.y) < 2) %>% filter(Journal.x == Journal.y)


joined_final <- rbind(joined_so_far, to_join) %>%
  rename(signalname = signalname.x) %>%
  arrange(signalname) %>%
  dplyr::select(signalname, word_count, misp_count, risk_count, misprice_risk_ratio)

signal_text <- signal_text %>% merge(joined_final)

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
  mutate(misprice_risk_ratio = round(misprice_risk_ratio, 2)) %>%
  rename(Theory = theory1) %>%
  mutate(Reference = paste(Authors, PubYear))  %>%
  select(!c(signalname, word_count, misp_count, risk_count, Authors, PubYear, Journal)) %>%
  mutate(not_log_risk_misprice_ratio = exp(-misprice_risk_ratio) %>% round(2)) %>%
  arrange(-not_log_risk_misprice_ratio)  %>%
  rename(RiskMispricingRatio = not_log_risk_misprice_ratio)  %>%
    select(Theory, Reference, Predictor, ExampleText, RiskMispricingRatio)

text_examples
dir.create('../TablesText/')

fwrite(text_examples, '../TablesText/text_examples.csv')


library(janitor)

summary_text <- bind_rows(
  signal_text,
  signal_text %>% mutate(theory1 = "Total")
) %>% group_by(theory1) %>%
  mutate(RiskMispricingRatio = exp(-misprice_risk_ratio) %>% round(2)) %>%
  arrange(-RiskMispricingRatio)  %>%
  summarise(Count = n(), CountPre2004 = sum(Year < 2004),
            CountPost2004 = sum(Year >= 2004),
            TypicalSampleStart = median(ymd(sampstart)) %>% as.character(),
            TypicalSampleEnd = median(ymd(sampend))  %>% as.character(),
            MeanRiskMispricingRatio = mean(RiskMispricingRatio) %>% round(2)  %>% as.character(),
            MinRiskMispricingRatio = min(RiskMispricingRatio) %>% round(2)  %>% as.character(),
            q05RiskMispricingRatio = quantile(RiskMispricingRatio, 0.05) %>% round(2)  %>% as.character(),
            q25RiskMispricingRatio = quantile(RiskMispricingRatio, 0.25) %>% round(2)  %>% as.character(),
            MedianRiskMispricingRatio = median(RiskMispricingRatio) %>% round(2)  %>% as.character(),
            q75RiskMispricingRatio = quantile(RiskMispricingRatio, 0.75) %>% round(2)  %>% as.character(),
            q95RiskMispricingRatio = quantile(RiskMispricingRatio, 0.95) %>% round(2)  %>% as.character(),
            MaxRiskMispricingRatio = max(RiskMispricingRatio) %>% round(2)  %>% as.character())  %>%
  rename(Theory = theory1)

# %>%
#   adorn_totals("row") 

summary_text

fwrite(summary_text, '../TablesText/summary_text.csv')

