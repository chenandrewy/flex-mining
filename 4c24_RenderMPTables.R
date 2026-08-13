# Re-render all six manuscript MP-style decay tables from saved fits --
# formatting tweaks without re-estimation. Run 4c6 (baseline) and 4c22
# (Tol30/Tol10) first to create the fit files; this script warns if the
# upstream data changed after the fits were saved (stale fits).
#
# Outputs (../risk-vs-rfs-sub/latex-risk-vs/exhibits/):
#   HandTable_MPStyleRegsMain.tex          HandTable_MPStyleRegsTimeFE.tex
#   HandTable_MPStyleRegs{Main,TimeFE}_{Tol30,Tol10}.tex

rm(list = ls())
source('helpers/mp_table_helpers.R')

exdir = '../risk-vs-rfs-sub/latex-risk-vs/exhibits/'

check_stale = function(fits, name) {
  files = fits$meta$input_files
  if (is.null(files))  # legacy fits saved before input paths were recorded
    files = c('../Data/Processed/ret_for_plot0.RDS', '../Data/Processed/plotdat0.RDS')
  newest_input = max(file.mtime(files), na.rm = TRUE)
  if (newest_input > fits$meta$generated)
    warning(name, ' fits are STALE: data regenerated ', newest_input,
            ' after fits saved ', fits$meta$generated,
            '. Rerun the estimation script.', immediate. = TRUE)
}

# Baseline (manuscript Tables 3-4)
fits = readRDS('../Data/Processed/mp_decay_fits_Baseline.RDS')
check_stale(fits, 'Baseline')
make_combined_table(fits$noFE, timeFE = FALSE, file = paste0(exdir, 'HandTable_MPStyleRegsMain.tex'))
make_combined_table(fits$FE,   timeFE = TRUE,  file = paste0(exdir, 'HandTable_MPStyleRegsTimeFE.tex'))

# Tolerance specs (DRAFT Tables 3b/4b)
for (nm in c('Tol30', 'Tol10')) {
  f = paste0('../Data/Processed/mp_decay_fits_', nm, '.RDS')
  if (!file.exists(f)) { cat('skip', nm, '(no saved fits)\n'); next }
  fits = readRDS(f)
  check_stale(fits, nm)
  make_combined_table(c(fits$fits_s[c(1, 3, 5)], fits$fits_u[c(1, 3, 5)]), timeFE = FALSE,
                      file = paste0(exdir, 'HandTable_MPStyleRegsMain_', nm, '.tex'))
  make_combined_table(c(fits$fits_s[c(2, 4, 6)], fits$fits_u[c(2, 4, 6)]), timeFE = TRUE,
                      file = paste0(exdir, 'HandTable_MPStyleRegsTimeFE_', nm, '.tex'))
}
