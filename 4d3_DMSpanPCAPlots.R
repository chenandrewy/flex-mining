# Render the correlation- and PCA-spanning exhibits from chapter-3 caches.
#
# How to run: source from 4_Exhibits.R with the working directory set to
#   flex-mining/.
# Inputs:  ../Data/Processed/dm_span_analysis.RDS
# Outputs: ../Results/Fig_DM_unspan_match_t_g_PCA.pdf
#          ../Results/Fig_DM_unspan_match_t_g_cor.pdf

source("0_Environment.R")

cache_path <- "../Data/Processed/dm_span_analysis.RDS"
if (!file.exists(cache_path)) {
  stop("Missing chapter-3 spanning cache: ", cache_path, ". Run 3_Precompute.R first.")
}

span_analysis <- readRDS(cache_path)

ReturnPlotsWithDM4series(
  dt = span_analysis$ret_for_plotting_pca,
  basepath = "../Results/Fig_DM",
  suffix = "unspan_match_t_g_PCA",
  rollmonths = 60,
  colors = c(colors, "#7E2F8E"),
  labelmatch = FALSE,
  yl = 0,
  yh = 125,
  legendlabels = c(
    "Published",
    "Adj. R2 > 0.25",
    "Adj. R2 < 0.25 \n& t-stat > t pub",
    "Adj. R2 < 0.25 \n& t-stat <= t pub"
  ),
  legendpos = c(25, 30) / 100,
  fontsize = 48,
  yaxislab = "Trailing 5-Year Return (bps pm)",
  linesize = 2
)

ReturnPlotsWithDM4series(
  dt = span_analysis$ret_for_plotting_cor,
  basepath = "../Results/Fig_DM",
  suffix = "unspan_match_t_g_cor",
  rollmonths = 60,
  colors = c(colors, "#7E2F8E"),
  labelmatch = FALSE,
  yl = 0,
  yh = 125,
  legendlabels = c(
    "Published",
    "Cor > 0.50",
    "Cor < 0.50 \n& t-stat > t pub",
    "Cor < 0.50 \n& t-stat <= t pub"
  ),
  legendpos = c(25, 30) / 100,
  fontsize = 48,
  yaxislab = "Trailing 5-Year Return (bps pm)",
  linesize = 2
)
