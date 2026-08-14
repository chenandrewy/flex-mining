# Render Section 3 MP-style decay tables from the cached regression models.
#
# How to run: normally run through S3_Learning.R with the working directory set to
#   flex-mining/.
# Inputs:  ../Data/Processed/mp_style_decay_models.RDS
# Outputs: ../Results/Table_MPStyleRegsComponentsScaled.tex
#          ../Results/Table_MPStyleRegsComponentsUnscaled.tex
#          ../Results/Table_MPStyleRegsIndividualDM.tex
#          ../Results/Table_MPStyleRegsNoTimeFE.tex
#          ../Results/Table_MPStyleRegsTimeFE.tex

source("0_Environment.R")
source("helpers/mp_table_helpers.R")

# An override permits render-only validation without touching ../Results.
output_dir <- Sys.getenv("MP_TABLE_OUTPUT_DIR", unset = "../Results")
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

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
  file.path(output_dir, "Table_MPStyleRegsComponentsScaled.tex")
)
write_mp_table(
  models$main_unscaled,
  main_headers,
  file.path(output_dir, "Table_MPStyleRegsComponentsUnscaled.tex")
)
write_mp_table(
  models$individual_dm,
  c("Scaled returns", "Scaled returns", "Unscaled returns", "Unscaled returns"),
  file.path(output_dir, "Table_MPStyleRegsIndividualDM.tex")
)

# Manuscript Tables 3 and 4: combine the alternating no-time-FE/time-FE
# specifications from the scaled and unscaled model lists. Cached tolerance
# variants can use the same helper and suffix without changing the layout.
write_combined_mp_tables(
  models$main_scaled,
  models$main_unscaled,
  output_dir = output_dir
)
