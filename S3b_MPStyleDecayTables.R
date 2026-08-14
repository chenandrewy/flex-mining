# Render Section 3 MP-style decay tables from the cached regression models.
#
# How to run: normally run through S3_Learning.R with the working directory set to
#   flex-mining/.
# Inputs:  ../Data/Processed/mp_style_decay_models.RDS
# Outputs: ../Results/Table_MPStyleRegsMain.tex
#          ../Results/Table_MPStyleRegsMainUnscaled.tex
#          ../Results/Table_MPStyleRegsIndividualDM.tex

source("0_Environment.R")

cache_path <- "../Data/Processed/mp_style_decay_models.RDS"
if (!file.exists(cache_path)) {
  stop("Missing MP-style model cache: ", cache_path, ". Run S3a_MPStyleDecayModels.R first.")
}
models <- readRDS(cache_path)

write_mp_table <- function(model_list, headers, path) {
  fixest::etable(
    model_list,
    tex = TRUE,
    dict = models$etable_dict,
    style.tex = fixest::style.tex("aer"),
    digits = 3,
    digits.stats = "r3",
    signif.code = NA,
    depvar = FALSE,
    headers = headers,
    fitstat = ~ n + r2 + wr2,
    file = path
  )
}

main_headers <- c(
  "Predictor Return", "Predictor Return",
  "DM Matched Return", "DM Matched Return",
  "Pred - Matched Ret", "Pred - Matched Ret"
)
write_mp_table(
  models$main_scaled,
  main_headers,
  "../Results/Table_MPStyleRegsMain.tex"
)
write_mp_table(
  models$main_unscaled,
  main_headers,
  "../Results/Table_MPStyleRegsMainUnscaled.tex"
)
write_mp_table(
  models$individual_dm,
  c("Scaled returns", "Scaled returns", "Unscaled returns", "Unscaled returns"),
  "../Results/Table_MPStyleRegsIndividualDM.tex"
)
