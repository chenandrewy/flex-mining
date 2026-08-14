# flex-mining
Code for replicating ["Peer-reviewed theory does not help predict the cross-section of stock returns."](https://arxiv.org/pdf/2212.10317.pdf)

Monthly returns of 30,000 long-short strategies can be found at [this Google Drive link](https://drive.google.com/drive/folders/1SZe_aF4ZNvK4ZRx2jQUE1j19KQvBaqWr).

The origins of predictability and quotes are found in [DataInput/SignalsTheoryChecked.csv](https://github.com/chenandrewy/flex-mining/blob/main/DataInput/SignalsTheoryChecked.csv)

This project uses the active R installation's default library paths and does not manage a project-local package environment.

Current results were produced with **R 4.5.3** and packages from the Posit P3M snapshot dated **2026-07-15**, plus `pcaMethods` from Bioconductor. See [docs/environment.md](docs/environment.md) for the package versions and how to regenerate the list.

## Pipeline

Run scripts from the repository root. `MAIN.R` exposes an independent switch
for each chapter:

1. `1_Download_and_Clean.R` acquires a new external-data vintage and cleans it.
2. `2_DataMining.R` constructs and matches the mined strategies; this takes
   roughly two hours.
3. `3_Precompute.R` builds reusable correlations, PCA results, summary data,
   and plot panels under `../Data/Processed`.
4. `4_ResearchVsDataMining.R` renders the introduction figure and Section 2
   exhibits.
5. `5_Learning.R` renders the Section 3 learning tables from cached regression
   models.
6. `6_Heterogeneity.R` renders the Section 4 heterogeneity exhibits.
7. `7_BestPredictors.R` renders the Section 4b predictor examples.
8. `8_Appendices.R` renders appendix-only exhibits.
9. `9_ExportDataToCsv.R` exports the mined-strategy data for sharing.

Chapters 4-8 treat processed data as read-only. For a formatting-only figure
or table change, run only the corresponding chapter. Chapter 1 overwrites
`../Data/Raw` with a new, non-recoverable WRDS/Google Drive vintage, so its
`MAIN.R` switch is off by default and should be enabled deliberately.
