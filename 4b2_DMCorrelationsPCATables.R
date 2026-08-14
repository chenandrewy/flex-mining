# Render correlation-quantile and PCA tables from chapter-3 caches.
#
# How to run: source from 4_Exhibits.R with the working directory set to
#   flex-mining/.
# Inputs:  ../Data/Processed/dm_correlation_quantiles.RDS
#          ../Data/Processed/dm_pca_table.RDS
# Outputs: ../Results/quantilesCorDM.tex
#          ../Results/DM_pca.tex

source("0_Environment.R")

required_files <- c(
  "../Data/Processed/dm_correlation_quantiles.RDS",
  "../Data/Processed/dm_pca_table.RDS"
)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing chapter-3 correlation/PCA cache(s): ",
    paste(missing_files, collapse = ", "),
    ". Run 3_Precompute.R first."
  )
}

quantiles_cor_dm <- readRDS(required_files[1])
quantiles_cor_dm %>%
  mutate(Kind = ifelse(weight == "ew", "Equal-Weighted", "Value-Weighted")) %>%
  transmute(
    Kind,
    empty1 = NA_character_,
    Q1, Q5, Q10, Q25, Q50, Q75, Q90, Q95, Q99
  ) %>%
  arrange(Kind) %>%
  xtable::xtable(digits = c(0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2)) %>%
  print(
    include.rownames = FALSE,
    include.colnames = FALSE,
    hline.after = NULL,
    only.contents = TRUE,
    file = "../Results/quantilesCorDM.tex"
  )

pca_tab <- readRDS(required_files[2])
alignment <- paste0("cl", paste(rep("c", ncol(pca_tab) - 1), collapse = ""))

capture.output(
  print(
    xtable::xtable(pca_tab, align = alignment),
    include.colnames = FALSE,
    floating = FALSE,
    booktabs = TRUE
  ),
  file = "../Results/DM_pca.tex"
)

texline <- readLines("../Results/DM_pca.tex")
texline <- append(
  texline,
  paste0(
    " & \\multicolumn{", nchar(alignment) - 2,
    "}{c}{Panel (b): PCA Explained Variance (\\%)} \\\\"
  ),
  after = 4
)
texline <- append(texline, "\\midrule", after = 7)
texline <- texline %>%
  str_replace(fixed("n\\_pc"), "Number of PCs") %>%
  str_replace(fixed("pct\\_exp\\_ew"), "Equal-Weighted") %>%
  str_replace(fixed("pct\\_exp\\_vw"), "Value-Weighted")
writeLines(texline, "../Results/DM_pca.tex")
