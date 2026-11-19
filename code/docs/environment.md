# R environment

First recorded 2026-08-13, when `renv` was removed from this repository, and
re-recorded 2026-09-25 after a full rebuild of the Chapter 3 caches and all
exhibits. Package provenance now lives here rather than in a lockfile.

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
arrow 24.0.0                  gridExtra 2.3.1               roll 1.2.1
data.table 1.18.4             haven 2.5.5                   RPostgres 1.4.10
digest 0.6.39                 httr 1.4.8                    sandwich 3.1.2
doParallel 1.0.17             huxtable 5.8.0                splitstackshape 1.4.8.1
dotenv 1.0.3                  janitor 2.2.1                 stringr 1.6.0
dplyr 1.2.1                   kableExtra 1.4.1              strucchange 1.5.4
fixest 0.14.2                 latex2exp 0.9.8               tictoc 1.2.1
foreach 1.5.2                 lmtest 0.9.40                 tidyr 1.3.2
fst 0.9.8                     lubridate 1.9.5               withr 3.0.3
getPass 0.2.4                 OpenSourceAP.DownloadR 0.2.0  writexl 1.5.4
ggplot2 4.0.3                 pcaMethods 2.2.0              xtable 1.8.8
glue 1.8.1                    readr 2.2.0                   zoo 1.8.15
readxl 1.5.0
```

`OpenSourceAP.DownloadR` is the one exception to the snapshot: chapter 1 needs
0.2.0 (CRAN, 2026-10-26), the first version that reads the Chen-Zimmermann data
from Hugging Face instead of the retired Google Drive folders. The snapshot
only has 0.1.0, so install it from current CRAN with
`install.packages("OpenSourceAP.DownloadR")`. `googledrive` is no longer used.

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

The 2026-08-13 list (data.table 1.17.8, ggplot2 3.5.2, xtable 1.8.4, and so on)
came from an older package library, not from the 2026-07-15 snapshot it named.
Installing from that snapshot gives the versions above. The move to data.table
1.18 changed `frollapply()`: an all-`NA` window result now keeps the logical
type of `fill = NA`, which silently turned the Table 1 t-statistics into `TRUE`
until `3b_DataMiningSummary.R` and `3d_DMCorrelationsPCA.R` switched to
`fill = NA_real_`. After that fix, the rebuilt tables match the August ones.
