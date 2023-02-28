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
model <- word2vec(x = subset_df, dim = 300, iter = 5, threads = 16)
toc <- Sys.time()
print(toc - tic)
emb   <- as.matrix(model)
# head(emb)
predict(model, c("risk"), type = "nearest", top_n = 10)

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


paper_text[,
              risk_sim :=
                sapply(vecs, word2vec_similarity,
                       embeddings['risk', ] - embeddings['liquidity', ],
                       type = 'cosine')]

fwrite(paper_text, '../IntermediateText/Word2VecResults.csv')

compact_df <- paper_text[, .(new_file_name, word_count, misp_count, risk_count, misprice_risk_ratio, anomaly_sim, risk_sim)]

fwrite(compact_df, '../IntermediateText/Word2VecResultsCompact.csv')

View(compact_df)
