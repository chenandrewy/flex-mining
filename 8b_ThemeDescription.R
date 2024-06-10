# Setup  ----------------------------------------------
rm(list = ls())
source('0_Environment.R')
load('../Data/tmp_4e_DMThemes.RData')

# Describe spanning over time -----------------------------------
# Convert sampendlist to Date objects assuming all dates are the first of the month
# make list of samples
samplist <- czsum %>%
  distinct(sampstart, sampend) %>%
  arrange(sampstart, sampend)

# mark dm strats that are spanned by current pub or previous pub
sampendlist = samplist$sampend %>% sort() %>% unique()
# sampendlist <- as.Date(paste0(sampendlist, "-01"), format="%b %Y-%d")

# Initialize an empty data frame to store the results
tab_span2 <- data.table(sampend = character(),
                        n_dm_tg2 = integer(),
                        n_span = integer(),
                        n_unspan = integer(),
                        pct_unspan = numeric(),
                        stringsAsFactors = FALSE)

# Loop over each sampendlist
for (sampend_loop in sampendlist %>% as.character()) {
  print(sampend_loop)
  # Subset data up to the current sampend
  data_subset <- dmpred$matched[sampend <= sampend_loop, ]
  
  # Calculate the total number of strategies
  n_dm_tg2 <- nrow(unique(data_subset[, .(sweight, dmname)]))
  
  # Calculate the number of spanned strategies
  spanned_strategies <- unique(data_subset[spanned_ever == TRUE, .(sweight, dmname)])
  n_span <- nrow(spanned_strategies)
  
  # Calculate the number and percentage of unspanned strategies
  n_unspan <- n_dm_tg2 - n_span
  pct_unspan <- 100 * n_unspan / n_dm_tg2
  
  # Append the results to the tab_span data frame
  tab_span2 <- rbind(tab_span2, data.table(sampend = sampend_loop %>% as.character(),
                                           n_dm_tg2 = n_dm_tg2,
                                           n_span = n_span,
                                           n_unspan = n_unspan,
                                           pct_unspan = pct_unspan))
}

tab_span2

# convert to latex
library(xtable)
table_latex <- xtable(tab_span2, caption = "Spanning of Strategies Over Time", label = "tab:spanning")
# Set the table alignment and format
align(table_latex) <- "llllll"
digits(table_latex) <- c(0, 0, 0, 0, 0, 1)  # Adjust the number of digits for each column

# Print the LaTeX code
print(table_latex, type = "latex", include.rownames = FALSE, caption.placement = "top", 
      hline.after = seq(from = 0, to = nrow(tab_span2), by = 1))

# Functions for clustering ---------------------------------------

make_hierarchy = function(dm_select){
  # dmselect has columns (sweight, dmname)
  # careful, scales O(nrow^3)
  # with 1,700 signals, just 25 sec
  dmcodelist = dmselect %>% 
    mutate(dmcode = paste0(sweight, '|', dmname)) %>%
    pull(dmcode)
    
  # extract correlation matrix
  lookup = data.table(i = 1:nrow(cmat), name = rownames(cmat))
  rowlist = lookup %>% filter(name %in% dmcodelist) %>% pull(i)  
  cmatcur = cmat[rowlist, rowlist]

  ## make hierarchical cluster hc --------------------------
  print('making hcluster, can take a couple minutes')
  tic = Sys.time()

  # JKP use dist = 1-cor and Ward 1963 linkage citing Murtagh Legendre 2014
  # but the default complete method seems similar
  hc = dist(1-cmatcur) %>% hclust(method = 'ward.D')  
  dend = as.dendrogram(hc)
  toc = Sys.time()
  difftime(toc, tic, units = 'secs')

  hier = list(clust = hc, dend = dend)
  return(hier)

} # end make_cluster

make_cluster = function(hier, nclustermax = 20){
  # hier is the output of make_hierarchy 

  # find height that gives < nclustermax
  heights = floor(seq(1, floor(max(hier$clust$height)/2), length.out = 500)) %>% 
      unique() 
  ngrouplist = sapply(heights, function(h) length(unique(cutree(hier$clust, h = h))))
  hstar = heights[ngrouplist <= nclustermax][1]

  # cut tree 
  sub_grp = cutree(hier$clust, h=hstar)

  # create name_cluster data table
  name_cluster = data.table(dmcode = names(sub_grp), cluster = sub_grp) %>% 
    left_join(dm_linktable[,.(dmcode, shortdesc, desc)], by='dmcode') %>%
    left_join(
      dmpred$matched[floor(sampend) == ycur]  %>% 
      mutate(dmcode = paste0(sweight, '|', dmname)) %>% 
      group_by(dmcode) %>% summarize(mean_tstat = mean(tstat)) %>% ungroup() %>% 
      select(dmcode, mean_tstat)
      , by='dmcode') %>% 
    arrange(cluster, dmcode) 

  return(name_cluster)
} # end make_cluster

# Describe DM themes FF1993's sample ----------------------

# for now use the n_dm_max largest tstats for spanned / unspanned
n_dm_max = 200
sampcur = czsum[signalname=='BMdec'] %>% select(sampstart, sampend)

# make dt of dm predictors for sampcur
dm_ycur = dmpred$matched %>% 
  .[sampstart == sampcur$sampstart
  & sampend == sampcur$sampend
    , .(sweight, dmname, spanned_ever, tstat)] %>% 
  distinct(sweight, dmname, spanned_ever, tstat) %>% 
  mutate(id = paste0(sweight, '|', dmname))

dm_ycur = dm_ycur %>% 
  arrange(-abs(tstat)) %>%
  group_by(spanned_ever) %>% filter(row_number() <= n_dm_max) %>% 
  ungroup() %>% 
  setDT() 

# correlations --------------------------------


make_cmat = function(stratlist){
  # stratlist is a dt with columns (id ~ sweight|dmname)

  # set up cluster
  cl <- makePSOCKcluster(ncores)
  registerDoParallel(cl)
  setkey(dm_rets,  id, yearm) # sort
  ## Loop --------------------------------------------------
  dmcor = foreach(i = 1:(nrow(stratlist)-1), .combine = rbind
    , .packages = c("data.table", "tidyverse", "zoo")
    , .export = c('dm_rets')
    ) %dopar% {

    if (i %% 100 == 0) {
      print(paste0("correlation for ", i, " of ", nrow(stratlist)))  
    }

    stratcur <- stratlist[i, ]$id

    # add *signed* current return to dm_rets
    # currently sign is based on mean across all samples, potential look ahead bias
    # but it should be minor
    retcur <- dm_rets[id == stratcur, .(yearm, ret)]  
    retcur$retsigned = sign(mean(retcur$ret)) * retcur$ret
    dm_rets[retcur, retcur := i.retsigned, on = .(yearm)]

    # find list of strats that has not been examined yet
    stratneeded = stratlist[(i+1):nrow(stratlist)]

    # compute correlation
    cor_cur = dm_rets[
      id %in% stratneeded$id
      , .(cor = cor(ret, retcur, use = "pairwise"))
      , by = c('id')
    ] %>% 
      mutate(id2 = stratcur) %>%
      transmute(id1 = id, id2, cor)

    # clean up
    dm_rets[ , retcur := NULL]

    return(cor_cur)
  } # end dmcor = foreach 
  stopCluster(cl)
  difftime(Sys.time(), tic, units = 'secs')

  ## Make matrix form --------------------------------------

  # add symmetrical correlations
  symcor = dmcor[ , .(id2 = id1, id1= id2, cor)]
  dmcor = rbind(dmcor, symcor)

  # add self correlations
  templist = dmcor$id1 %>% unique()
  selfcor = data.table(id1 = templist, id2 = templist, cor = 1)
  dmcor = rbind(dmcor, selfcor)

  # reshape  into matrix
  cmat = dmcor %>% dcast(id1 ~ id2, value.var = 'cor')  
  temprowname = cmat$id1
  cmat = cmat[ , id1 := NULL]
  cmat = as.matrix(cmat)
  rownames(cmat) = temprowname

  # make alternative cmat with more descriptive names
  cmat2 = cmat
  temp = data.table(i = 1:nrow(cmat), name = rownames(cmat)) %>% 
      left_join(dm_linktable[,.(dmcode, desc)], by = c('name' = 'dmcode')) %>% 
      arrange(i)
  rownames(cmat2) = temp$desc
  colnames(cmat2) = temp$desc

  return(list(cmat = cmat, cmat2 = cmat2))

} # end make_cmat

temp = make_cmat(dm_ycur[spanned_ever == TRUE])

# xxx ------------------------------------------------
dmpred$matched %>% 
  .[floor(sampend) == ycur 
    , .(sweight, dmname, spanned_ever, tstat)] %>% 
  distinct(sweight, dmname, spanned_ever, tstat) 
    
# for 1990, there are 8000 unspanned, 4000 spanned
dm_ycur %>% group_by(spanned_ever) %>% summarize(n())



# find clusters for spanned
temphier = make_hierarchy(
    dm_ycur[spanned_ever == TRUE])
name_cluster = make_cluster(temphier, nclustermax = 20)

# find clusters for unspanned
temphier = make_hierarchy(
    dm_ycur[spanned_ever == FALSE])
name_cluster = make_cluster(temphier, nclustermax = 20)

# tbc: make a nice table describing strats, including examples
name_cluster %>% group_by(cluster) %>% summarize(nstrat = n()) %>% 
  left_join(
    name_cluster %>% group_by(cluster) %>% arrange(cluster, -abs(mean_tstat)) %>%
      slice(1) 
    , by = 'cluster'
  ) %>% 
  print(n = 20) %>% 
  transmute(cluster, nstrat, example_dm = dmcode, example_desc = desc, mean_tstat)

