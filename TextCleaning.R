gc()

library(haven)
library(dplyr)
library(data.table)
library(stringr)
library(quanteda)
library(lexicon)
library(stm)  
library(stopwords)

library("quanteda.textstats")
library("spacyr")

spacy_initialize(model = "en_core_web_sm")

texts <- fread('../IntermediateText/text_with_pdf_name.csv', header = TRUE)

sngl_quot_rx = "[ʻʼʽ٬‘’‚‛՚︐]"

# texts[, clean_text := str_replace_all(text, '</?.+?>', ' ')]

# texts[, clean_text := str_replace_all(clean_text, '&[a-z]{1,6}', ' ')]

# consolidate different apostrophe variants.
texts[, clean_text :=  gsub(sngl_quot_rx, "\"", text)]

texts[, clean_text :=  gsub("'s", "", clean_text, ignore.case = TRUE)]



# texts[, clean_text := gsub(pattern = "[ [:alnum:][:space:].!,?-]", " ", clean_text)]



# texts[, clean_text := str_replace_all(clean_text, "[\\s-][IVXLCM]+ ", ' ')]

# texts[, clean_text := str_replace(clean_text, 'i\\.e\\.', ' ')]

pattern <- '( [:alnum:]+)(\\-\\n[:space:]*)([:alnum:]+)'

texts[, clean_text := str_replace_all(clean_text, pattern, ' \\1\\3')]

# pattern <- '( not)([:space:]+)([:alnum:]+ )'
# 
# texts[, clean_text := str_replace_all(clean_text, pattern, ' \\1_\\3')]

pattern <- "[^[A-Za-z\\-]]+"

texts[, clean_text := str_replace_all(clean_text, pattern, ' ')]

# texts[, clean_text := gsub('[[:digit:]]+', ' ', clean_text)]

# texts[, clean_text := gsub('ˇ', ' ', clean_text)]


# str_replace('This is a test-to see if word-\nbreaking happens', '( [:alnum:]+\\-)(\\n)([:alnum:]+)', '\\1\\3')

# str_replace_all('( [A-Z][A-Z]+)([a-z]+) ', '\\1 \\2 ')

# text_cleaning <- function(text_f){
#   text_f <- text_f %>% str_replace_all("- ", ' ') %>%
#     str_replace_all("(^| )-", ' ') %>%
#     str_replace_all("\\.-", ' .') %>%
#     str_replace_all("-\\. ", '. ') %>%
#     str_replace_all(",+-", ', ') %>%
#     str_replace_all("-,+", ', ')  %>%
#     str_replace_all("- ", ' ')  %>%
#     str_replace_all(" -", ' ') %>%
#     str_replace_all("(^| )-", ' ') %>%
#     str_replace_all('( [A-Z][A-Z]+)([a-z]+) ', '\\1 \\2 ') %>%
#     str_replace_all(' +', ' ')
#   return(text_f)
#     
# }
# 
# texts[, clean_text := text_cleaning(clean_text)]
# 
# texts[, clean_text := str_replace(clean_text, 'i\\.e\\.', ' ')]
# 
# texts[, clean_text := text_cleaning(clean_text)]
# 
# texts[, clean_text := str_replace(clean_text, 'i\\.e\\.', ' ')]
# 
# texts[, clean_text := text_cleaning(clean_text)]
# 
# texts[, clean_text := str_replace(clean_text, 'u\\.s\\.', 'U.S.')]

# ab-\n\nnormal

#####################################
stop_base <-setdiff(stopwords("en", source = "marimo"), c('no', 'not', 'less', 'little', 'nor', 'cannot'))

stopwords_list <- c('also', 'table', 'figure',  as.character(as.roman(1:100)))

toks_reg <- tokens(texts[, clean_text], remove_punct = TRUE,
                   remove_symbols = TRUE, remove_numbers = TRUE,
                   remove_url = TRUE,
                   padding = TRUE) %>% 
  tokens_remove(stop_base,
                padding = TRUE) %>%
  tokens_remove(stopwords_list, case_insensitive = TRUE, padding = TRUE) %>%
  tokens_select(min_nchar = 3,
                padding = TRUE)

dfmat_papers <- dfm(toks_reg, tolower = FALSE)

tstat_freq <- textstat_frequency(dfmat_papers)

# View(tstat_freq[tstat_freq$frequency > 1, ])

# pattern <- "[^[A-Za-z\\-]]+"
# keys <- unique(as.character(tokens_select(toks_reg, pattern, valuetype = "regex")))
# keys
# vals <- stringr::str_replace_all(keys, pattern, ("")) 
# vals

# toks_reg <- toks_reg %>% tokens_replace(keys, vals)


pattern <- "^(-|_)+([:alnum:]+)(-|_)*$"
keys <- unique(as.character(tokens_select(toks_reg, pattern, valuetype = "regex")))
keys
vals <- stringr::str_replace_all(keys, pattern, ("\\2")) 
vals
clean_toks_reg <- toks_reg %>% tokens_replace(keys, vals)

pattern <-  "^(-|_)*([:alnum:]+)(-|_)+$"
keys <- unique(as.character(tokens_select(clean_toks_reg, pattern, valuetype = "regex")))
keys
vals <- stringr::str_replace_all(keys, pattern, ("\\2")) 
vals
clean_toks_reg <- clean_toks_reg %>% tokens_replace(keys, vals)


pattern <- "^\\W+"
keys <- unique(as.character(tokens_select(clean_toks_reg, pattern, valuetype = "regex")))
keys
vals <- stringr::str_replace_all(keys, pattern, ("")) 
vals
clean_toks_reg <- clean_toks_reg %>% tokens_replace(keys, vals)

pattern <- "w{3,}.*"
keys <- unique(as.character(tokens_select(clean_toks_reg, pattern, valuetype = "regex")))
keys
vals <- stringr::str_replace_all(keys, pattern, ("")) 
vals
clean_toks_reg <- clean_toks_reg %>% tokens_replace(keys, vals)

dfmat_papers <- dfm(clean_toks_reg, tolower = FALSE)

tstat_freq <- textstat_frequency(dfmat_papers)

# View(tstat_freq[tstat_freq$frequency > 1, ])

kw_comp <- kwic(toks_reg, pattern = "*aheadearnings", window = 10)

head(kw_comp, 10)

kw_comp <- kwic(texts[1, text], pattern = "*aheadearnings", window = 20)

head(kw_comp, 10)

clean_toks_reg_strip <- clean_toks_reg %>% tokens_select(min_nchar = 3,
              padding = FALSE)

kw_comp <- kwic(clean_toks_reg_strip, pattern = "aheadearnings", window = 5)

head(kw_comp, 10)

texts_lemma <- tokens_replace(clean_toks_reg_strip,
                              pattern = lexicon::hash_lemmas$token,
                              replacement = lexicon::hash_lemmas$lemma) %>%
  tokens_remove(stopwords_list, padding = FALSE)

kw_comp <- kwic(clean_toks_reg_strip, pattern = "cfoip", window = 5)

head(kw_comp, 10)

f.pos.bigrams <- function(text){
  return(
  spacy_parse(text, lemma = TRUE,
              entity = FALSE, nounphrase = FALSE))
}
# 
colls4 <- clean_toks_reg_strip  %>% textstat_collocations(min_count = 100, size = 4,
                                                          tolower = FALSE) %>%
  filter(z > 1) %>%
  filter(str_detect(.$collocation, '^[A-Z][A-z]+ [A-Z][A-z]+ [A-Z][A-z]+')) %>%
  filter(str_detect(.$collocation, 'Journal')) 
  
toks_comp4 <- clean_toks_reg_strip %>% tokens_compound(pattern = colls4,
                                          case_insensitive = TRUE) %>%
  tokens_select(min_nchar = 3,  padding = FALSE)

colls3 <- toks_comp4  %>% textstat_collocations(min_count = 20, size = 3,
                                                    tolower = FALSE) %>%
  filter(str_detect(.$collocation, '[A-Z][A-z]+ [A-Z][A-z]+ [A-Z][A-z]+'))  %>%
  filter(str_detect(.$collocation, 'Journal|Review|Association')) %>%
  filter(!str_detect(.$collocation,
                     'Table|Fama|Returns|Daniel|French|Novy|Jegadeesh|Vol|Pedersen|Chordia|Richardson|Chan')) %>%
  filter(count > 45)

toks_comp3 <- toks_comp4 %>% tokens_compound(pattern = colls3,
                                                      case_insensitive = TRUE) %>%
  tokens_select(min_nchar = 3,  padding = FALSE)

colls2 <- toks_comp3  %>% textstat_collocations(min_count = 20, size = 2,
                                               tolower = FALSE) %>%
  filter(str_detect(.$collocation, '[A-Z][A-z]+ [A-Z][A-z]+')) %>%
  filter(str_detect(.$collocation, '^Journal|Review|Association|^Fama')) %>%
  filter(!str_detect(.$collocation, 'Table| Fama|Returns|_|Source|Authors|Ball|CAPM|Francis'))

toks_comp2 <- toks_comp3 %>% tokens_compound(pattern = colls2,
                                           case_insensitive = TRUE) %>%
  tokens_select(min_nchar = 3,  padding = FALSE)

colls2l <- toks_comp2  %>% textstat_collocations(min_count = 100, size = 2,
                                               tolower = FALSE) %>%  
 filter(str_detect(.$collocation, '[a-z]+ [a-z]+')) %>%
  filter(!str_detect(.$collocation,
 'Speci cally|Unpublished|Library|predict future|examine|formed|even|every|use|size|strategies|downloaded|paper|Table|parentheses|per|table|copy|returns portfolios|stocks high|based|Panel|price momentum|nnn|firms high|recent past')
 ) %>% filter(z > 50) 

toks_comp2l <- toks_comp2 %>% tokens_compound(pattern = colls2l,
                                             case_insensitive = TRUE) %>%
  tokens_select(min_nchar = 3,  padding = FALSE)

colls2lr <- toks_comp2l  %>% textstat_collocations(min_count = 25, size = 2,
                                                   tolower = TRUE) %>%  
  filter(str_detect(.$collocation, 'risk$| avers[a-z]*')) %>%
  filter(!str_detect(.$collocation, '_|macbeth|factor|measure|relative'  ))

toks_comp2lr <- toks_comp2l %>% tokens_compound(pattern = colls2lr,
                                              case_insensitive = TRUE) %>%
  tokens_select(min_nchar = 3,  padding = FALSE)

texts_lemma <- tokens_replace(toks_comp2lr,
                              pattern = lexicon::hash_lemmas$token,
                              replacement = lexicon::hash_lemmas$lemma) %>%
  tokens_remove(stopwords_list, padding = FALSE) %>%
  tokens_select(min_nchar = 3,  padding = FALSE) %>%
  tokens_tolower(keep_acronyms = FALSE)

texts_join_lemma <- lapply(texts_lemma, paste, collapse = ' ')

kw_comp <- kwic(texts_lemma, pattern = "cfoip", window = 2)

head(kw_comp, 20)

texts[, text_lemmatized := texts_join_lemma]

texts[, word_count := str_count(text_lemmatized, "\\w+")]

words_mispricing <- c('abnormal',
                      'anomal',
                      'behav',
                      'optimistic',
                      'pessimistic',
                      'sentiment',
                      'underreact',
                      'overreact',
                      'abnormal',
                      'failure',
                      "market( |_|-)failure",
                      'bias',
                      'overvalu',
                      'misvalu',
                      'undervalu',
                      'attention',
                      'underperformance',
                      'extrapolate',
                      'underestimate',
                      'misreaction',
                      'inefficiency',
                      'delay',
                      'suboptimal',
                      'mislead',
                      'overoptimism',
                      "fail( |_|-)reflect",
                      'arbitrage',
                      'factor unlikely',
                      'not? reward',
                      'little risky?',
                      'risk cannot',
                      'low [a-z]+_risk',
                      'unrelated [a-z]+_risk',
                      'liquid',
                      'slow (to )?react',
                      'slow (to )?incorporat',
                      'short(-| )sale cost'
                      ) %>% paste(collapse = '|')

texts[, misp_count := str_count(text_lemmatized, words_mispricing)]

texts[1, misp_count]

# constraint?

words_risks <- c('utility',
                 'maximize',
                 'minimize',
                 'optimize',
                 'premium',
                 'premia',
                 'premiums',
                 '(?<!lower )[a-z]+_risk',
                 '(?<!not )avers',
                 '(?<!not )rational',
                 'consum',
                 'marginal',
                 'equilibrium',
                 'sdf',
                 'investment-based',
                 '(?<!not )risky',
                 'theoretical') %>% paste(collapse = '|')

texts[, risk_count := str_count(text_lemmatized, words_risks)]

texts[1, risk_count]

texts[, misprice_risk_ratio := (misp_count + 1)/(risk_count + 1)]

subset_text <- texts[!is.na(year), .(file_names, new_file_name, Year = year, Authors = author, journal,
                                     signalname, word_count, misp_count, risk_count, misprice_risk_ratio)]

fwrite(subset_text, '../IntermediateText/TextAnalysis.csv')

fwrite(texts, '../IntermediateText/FullTextAnalysis.csv')


words_mispricing <- c('abnormal*',
                      'anomal*',
                      'behav*',
                      'optimistic',
                      'pessimistic',
                      'sentiment',
                      'underreact*',
                      'overreact*',
                      'failure',
                      "market( |_|-)failure",
                      'bias',
                      'overvalu*',
                      'misvalu*',
                      'undervalu*',
                      'attention',
                      'underperformance',
                      'extrapolate',
                      'underestimate',
                      'misreaction',
                      'inefficiency',
                      'delay',
                      'suboptimal',
                      'mislead*',
                      'overoptimism',
                      "fail( |_|-)reflect",
                      'arbitrage',
                      'factor unlikely',
                      'not? reward',
                      'little risky?',
                      'risk cannot',
                      'low [a-z]+_risk',
                      'unrelated [a-z]+_risk',
                      'liquid*',
                      'slow( |to)react',
                      'slow( |to)incorporat',
                      'short(-| )sale cost'
)

kw_anom <- kwic(texts_lemma, pattern = words_mispricing)

anom_list <-  kw_anom$keyword %>% unique()

words_risks <- c('utility',
                 'maximize',
                 'minimize',
                 'optimize',
                 'premium',
                 'premia',
                 'premiums',
                 '*risk',
                 'avers*',
                 'rational*',
                 'consum*',
                 'marginal',
                 'equilibrium',
                 'sdf',
                 'investment-based',
                 'risky',
                 'theoretical')

kw_r <- kwic(texts_lemma, pattern = words_risks)

risk_list <-  kw_r$keyword %>% unique()
