library(udpipe)
library(word2vec)
library(udpipe)
library(data.table)
library(quanteda)
library(dplyr)

paper_text <- fread('../IntermediateText/FullTextAnalysis.csv')

subset_df <- paper_text[,
                              text_lemmatized]
gc()
tic <- Sys.time()
model <- word2vec(x = subset_df, dim = 300, iter = 30, threads = 16)
toc <- Sys.time()
print(toc - tic)
emb   <- as.matrix(model)
# head(emb)
predict(model, c("mispricing"), type = "nearest", top_n = 10)

write.word2vec(model, '../IntermediateText/word2vecmodel.bin')


####################

model <- read.word2vec(
  '../IntermediateText/word2vecmodel.bin'
  , normalize = TRUE
)
vocab <- summary(model, type = "vocabulary")
emb <- predict(model, c("anomaly"), type = "nearest")
embeddings <- as.matrix(model)
emb_names <- rownames(embeddings)
lemmas_text <- paper_text[,text_lemmatized]


sentence_to_vec <- function(string, embeddings_f, embed_names){
  words <- unlist(strsplit(string, ' ', TRUE))
  # print(words)
  words <- intersect(words, embed_names)
  # print(words)
  vec <- embeddings_f[words, ] %>% colMeans()
  nvec <- norm(vec, type="2")
  if(nvec == 0){
    # print(string)
    return(NA)
  }
  vec <- vec/nvec
  return(vec)
}

vecs <- lapply(lemmas_text, sentence_to_vec, embeddings, emb_names)

paper_text[,
              anomaly_sim :=
                sapply(vecs, word2vec_similarity,
                       embeddings['mispricing', ] + embeddings['liquidity', ]   - 2*embeddings['risk', ],
                       type = 'cosine')]



fwrite(paper_text[, -'text_lemmatized'], '../IntermediateText/Word2VecResults.csv')

compact_df <- paper_text[, .(new_file_name, word_count, misp_count, risk_count, misprice_risk_ratio, anomaly_sim)]

fwrite(compact_df, '../IntermediateText/Word2VecResultsCompact.csv')

# View(compact_df)

subset_text <- fread('../IntermediateText/Word2VecResults.csv')  %>%  rename(Journal = journal) %>%
  # dplyr::select(Authors, Year, Journal, file_names)%>%
  mutate(Journal = gsub('^RF$', 'ROF', Journal)) %>%
  mutate(Journal = gsub('^TAR$', 'AR', Journal)) %>%
  mutate(Authors = gsub('et al.?|and |,', '', Authors)) %>%
  mutate(FirstAuthor = word(Authors)) %>%
  filter(Authors != 'Ang et al') %>%
  filter(Authors != 'Chen Jegadeesh Lakonishok')

signal_text <- fread('SignalsTheoryChecked.csv')

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
  dplyr::select(signalname, word_count,
                misp_count, risk_count, misprice_risk_ratio, anomaly_sim)

signal_text2 <- signal_text %>% merge(joined_final)

signal_text2[, risk_mispricing_ratio := 1/misprice_risk_ratio]

signal_text2 <- signal_text2 %>% relocate(signalname, Journal,
                                        Authors, Year, theory1, desc,
                                        misp_count, risk_count,
                                        misprice_risk_ratio, risk_mispricing_ratio,
                                        anomaly_sim, quote)


fwrite(signal_text2, 'TextClassification.csv')
