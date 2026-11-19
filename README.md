# flex-mining
Code for replicating ["Does peer-reviewed research help predict stock returns?"](https://arxiv.org/abs/2212.10317).

Analysis is in `code/`.  This repo includes the latex paper (`paper`).

Monthly returns of 29,000 long-short strategies can be found at [this Google Drive link](https://drive.google.com/drive/folders/1SZe_aF4ZNvK4ZRx2jQUE1j19KQvBaqWr).

The origins of predictability and quotes are found in [code/DataInput/SignalsTheoryChecked.csv](code/DataInput/SignalsTheoryChecked.csv)

## `code/`

This project uses the active R installation's default library paths and does not manage a project-local package environment.

Current results were produced with **R 4.5.3** and packages installed from the Posit P3M snapshot dated **2026-07-15** (for example, data.table 1.18.4), plus `pcaMethods` from Bioconductor. See [code/docs/environment.md](code/docs/environment.md) for the package versions and how to regenerate the list.

### Pipeline

Run scripts from `code/`. `MAIN.R` exposes an independent switch
for each data stage and paper section:

1. `1_Download_and_Clean.R` acquires a new external-data vintage and cleans it.
2. `2_DataMining.R` constructs and matches the mined strategies; this takes
   roughly four hours.
3. `3_Precompute.R` builds reusable correlations, PCA results, summary data,
   and plot panels under `../Data/Processed`.
4. `S2_ResearchVsDataMining.R` renders the introduction figure and Section 2
   exhibits.
5. `S3_Learning.R` renders the Section 3 learning tables from cached regression
   models.
6. `S4_Quality.R` renders Section 4 journal-ranking and renowned-match exhibits.
7. `SA_Appendices.R` renders appendix exhibits, including SA11 spanning plots.
8. `9_ExportDataToCsv.R` exports the mined-strategy data for sharing.
9. `Appendices/TimeFERobustness/run.R` rebuilds the time-fixed-effects
   robustness appendix from its external inputs.

The precompute stage (`3_Precompute.R`) runs `3d_DMCorrelationsPCA.R` and
`3e_DMSpanPCA.R` after the other cache builders. The former feeds Section 2
Table 1 panel (b) through `S2c_DMCorrelationsPCATables.R`; the latter feeds the
appendix spanning plots.

Steps 4-8 share the single `exhibits` switch in `runStages`; the slow or
externally sourced stages keep their own. Any driver above can be run on its
own with `Rscript <driver>.R` while iterating on one exhibit.

The paper-section stages treat processed data as read-only. For a formatting-only
figure or table change, run only the corresponding section. Chapter 1 overwrites
`../Data/Raw` with a new, non-recoverable WRDS vintage (plus the pinned CZ release from Hugging Face), so its
`MAIN.R` switch is off by default and should be enabled deliberately.

The time-FE robustness stage is enabled in `config.R`. See
`code/Appendices/TimeFERobustness/README.md` for its pinned inputs, storage
layout, commands, and WRDS warning.
