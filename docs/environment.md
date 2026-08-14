# R environment

Recorded 2026-08-13, when `renv` was removed from this repository. Package
provenance now lives here rather than in a lockfile.

## What produced the current results

- **R 4.5.3**
- Packages installed from the dated Posit P3M snapshot
  **`https://packagemanager.posit.co/cran/__linux__/noble/2026-07-15`**, plus
  `pcaMethods` from Bioconductor.
- Packages resolve from the R installation's default library paths. There is no
  project-local library.

`pcaMethods` is not on P3M/CRAN; install it from Bioconductor:

```r
if (!requireNamespace("BiocManager", quietly = TRUE)) install.packages("BiocManager")
BiocManager::install("pcaMethods")
```

The snapshot date is the pin: it fixes package versions the way a lockfile
would, and it also covers the system libraries a lockfile cannot (the
`systemfonts` build needs `libfontconfig-dev`, `libjpeg-dev`, and
`libtiff-dev`). In this project's sandbox both are set in
`.devcontainer/Dockerfile` (`ARG R_VERSION`, `ARG CRAN_SNAPSHOT`) and installed
by `.devcontainer/install-packages.sh`.

## Versions in use

Collected from the packages referenced by the workflow's R files, excluding
`CodeArchive/`.

```
arrow 24.0.0            fuzzyjoin 0.1.6.1       readxl 1.4.5
BiocManager 1.30.27     getPass 0.2.4           reshape2 1.4.4
data.table 1.17.8       ggplot2 3.5.2           roll 1.1.7
doParallel 1.0.17       ggrepel 0.9.6           RPostgres 1.4.8
dotenv 1.0.3            glue 1.8.0              sandwich 3.1.1
dplyr 1.1.4             googledrive 2.1.1       splitstackshape 1.4.8
extrafont 0.19          gridExtra 2.3           stopwords 2.3
fixest 0.14.2           haven 2.5.5             stringr 1.5.1
foreach 1.5.2           huxtable 5.6.0          strucchange 1.5.4
fst 0.9.8               janitor 2.2.1           tibble 3.2.1
                        kableExtra 1.4.0        tictoc 1.2.1
                        latex2exp 0.9.6         tidyr 1.3.1
                        lexicon 1.2.1           tidyverse 2.0.0
                        lmtest 0.9.40           udpipe 0.8.11
                        lubridate 1.9.4         word2vec 0.4.0
                        multcomp 1.4.28         writexl 1.5.4
                        pcaMethods 2.2.0        xtable 1.8.4
                        quanteda 4.3.1          zoo 1.8.14
                        RColorBrewer 1.1.3
                        readr 2.1.5
```

Regenerate this list with `docs/record-r-environment.R`.

## Why some versions matter

Reported standard errors and test statistics depend on package versions, not
just on the code. `fixest` drives the clustered SEs in the MP-style regression
tables, and `strucchange` drives the structural break tests. Record the version
alongside any results that get published.

## History

The repository used `renv` until 2026-08-13. The lockfile that remains in git
history, deleted in `67fcac8`, records **R 4.5.1 and 175 packages** and is
**not** the environment that produced current results; it was already stale when
it was removed. An updated snapshot taken on 2026-08-13, recording R 4.5.3,
Bioconductor 3.22, and 187 packages, was never committed and is lost. This file
supersedes both.
