# Record the R version and package versions used by this workflow.
#
# How to run: from flex-mining/, `Rscript docs/record-r-environment.R`
# Inputs:  the repository's .R files, excluding CodeArchive/, and the installed
#          package library
# Outputs: prints R version and one "package version" line per package to
#          stdout. Writes nothing; paste the result into docs/environment.md.

files <- list.files(".", pattern = "[.]R$", recursive = TRUE)
files <- files[!grepl("^CodeArchive/", files)]

txt <- unlist(lapply(files, readLines, warn = FALSE))

pkgs <- c(
  regmatches(txt, gregexpr("(?<=library\\()[A-Za-z0-9._]+", txt, perl = TRUE)),
  regmatches(txt, gregexpr("(?<=require\\()[A-Za-z0-9._]+", txt, perl = TRUE)),
  regmatches(txt, gregexpr("[A-Za-z0-9._]+(?=::)", txt, perl = TRUE))
)
pkgs <- sort(unique(gsub("[\"']", "", unlist(pkgs))))
pkgs <- pkgs[pkgs %in% rownames(installed.packages())]

cat("R", as.character(getRversion()), "\n")
cat("repos:", getOption("repos")[["CRAN"]], "\n")
cat(length(pkgs), "packages\n\n")
for (p in pkgs) cat(sprintf("%s %s\n", p, as.character(packageVersion(p))))
