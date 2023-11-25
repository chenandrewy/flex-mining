# PCA decomp for all data mined strategies

# Environment ------------------------
source('0_Environment.R')

DMname = '../Data/Processed/CZ-style-v6 LongShort.RData'

# read in DM strats
dm_rets <- readRDS(DMname)$ret
dm_info <- readRDS(DMname)$port_list

dm_rets <- dm_rets %>%
left_join(
    dm_info %>% select(portid, sweight),
    by = c("portid")
) %>%
transmute(
    sweight,
    dmname = signalid,
    yearm,
    ret,
    nstock
) %>%
setDT()

# settings and functions ------------------------

# function for computing PCA
compute_pca = function(sweightselect='ew',nstratmax=10000){

    # setup ---
    ret1 = copy(dm_rets)
    ret1 = ret1[sweight == sweightselect, .(yearm, dmname, ret)]
    ret1 = ret1[yearm >= yearm_min & yearm <= yearm_max]        

    # select a random sample for testing
    dmnamelist = unique(dm_rets$dmname)
    nstrat = min(nstratmax, length(dmnamelist))
    set.seed(123)
    nameselect = sample(dmnamelist, nstrat, replace = FALSE)
    ret2 = ret1[dmname %in% nameselect , .(yearm, dmname, ret)]

    # PCA ---
    # make wide matrix
    temp = dcast(ret2, yearm ~ dmname, value.var = 'ret') 
    retmat0 = as.matrix(temp[ , -1])
    rownames(retmat0) = temp$yearm

    # drop signals with missing values
    nmonthmissmax = 0
    signalmiss = colSums(is.na(retmat0))
    retmat = retmat0[ , signalmiss <= nmonthmissmax] 

    # drop months with missing values (redundant right now)
    # nstratmissmax = 0.1*nstrat
    # monthmiss = rowSums(is.na(retmat)) 
    # retmat = retmat[monthmiss <= nstratmissmax , ]

    # PCA
    A = (retmat - colMeans(retmat))/ sqrt(nrow(retmat))
    Asvd = svd(A)
    pcadat = tibble(n_pc = 1:length(Asvd$d) , eval = Asvd$d^2)  %>% 
        mutate(cum_pct_exp = cumsum(eval)/sum(eval)*100) %>% 
        mutate(nstrat = dim(retmat)[2])

        # # sanity check (this requires a lot more compute)
        # coveig = eigen(cov(retmat))
        # temp = cumsum(coveig$values)/sum(coveig$values)*100
        # temp %>% head()
        # pcadat$cum_pct_exp %>% head()
        
    return(pcadat)
} # end compute_pca 

# run function over cases ------------------------

# fix sample 
yearm_min = 1984
yearm_max = 2019

nstratmax = 40000
pca_ew = compute_pca(sweightselect='ew', nstratmax=nstratmax)
pca_vw = compute_pca(sweightselect='vw', nstratmax=nstratmax)

# make a nice table ------------------------
tab = pca_ew %>% transmute(n_pc, pct_exp_ew = cum_pct_exp) %>% 
    left_join(
        pca_vw %>% transmute(n_pc, pct_exp_vw = cum_pct_exp),
        by = c('n_pc')
    ) %>% 
    filter(n_pc %in% c(1, 5, seq(10, 100, 10))) %>% 
    mutate(
            n_pc = as.character(as.integer(n_pc))
            , pct_exp_ew = round(pct_exp_ew, 0)
            , pct_exp_vw = round(pct_exp_vw, 0)
    )  %>% 
    t() 

alignment = paste0('cl', rep('c', ncol(tab)-1) %>% paste(collapse = ''))

# initialize latex table
library(xtable)
xtable(tab, align = alignment) %>% 
    print(include.colnames = FALSE, floating = FALSE
        , booktabs = TRUE) %>% 
    cat(file = '../Results/DM_pca.tex')

# read in tex and edit
texline = readLines('../Results/DM_pca.tex')

texline = append(texline, paste0(
    ' & \\multicolumn{'
    , nchar(alignment)-2
    , '}{c}{Panel (b): PCA Explained Variance (\\%)} \\\\'
    ), after = 4)
texline = append(texline,  '\\midrule', after = 7)

texline = str_replace(texline, fixed('n\\_pc'), 'Number of PCs') %>% 
    str_replace(fixed('pct\\_exp\\_ew'), 'Equal-Weighted') %>%
    str_replace(fixed('pct\\_exp\\_vw'), 'Value-Weighted') 

writeLines(texline, '../Results/DM_pca.tex')

# copy to dropbox / overleaf
file.copy('../Results/DM_pca.tex'
    ,'D:/Dropbox/Apps/Overleaf/PeerReviewedTheory_Paper/exhibits/DM_pca.tex'
    , overwrite = TRUE)


# check stuff on console ------------------------

# check coverage 
nall = dm_rets$dmname %>% unique() %>% length()
pca_ew$nstrat[1]
pca_vw$nstrat[1]
1-pca_ew$nstrat[1]/nall


# check robustness to part of sample
yearm_min = 2003
yearm_max = 2019

nstratmax = 40000
pca_ew_alt = compute_pca(sweightselect='ew', nstratmax=nstratmax)
pca_vw_alt = compute_pca(sweightselect='vw', nstratmax=nstratmax)

pca_ew_alt %>% transmute(n_pc, pct_exp_ew = cum_pct_exp) %>% 
    left_join(
        pca_vw_alt %>% transmute(n_pc, pct_exp_vw = cum_pct_exp),
        by = c('n_pc')
    ) %>% 
    filter(n_pc %in% c(1, 5, seq(10, 100, 10))) %>% 
    mutate(
            n_pc = as.character(as.integer(n_pc))
            , pct_exp_ew = round(pct_exp_ew, 0)
            , pct_exp_vw = round(pct_exp_vw, 0)
    )  %>% 
    t() 

pca_ew_alt$nstrat[1]
1-pca_ew_alt$nstrat[1]/nall
