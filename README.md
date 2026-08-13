# flex-mining
Code for replicating ["Peer-reviewed theory does not help predict the cross-section of stock returns."](https://arxiv.org/pdf/2212.10317.pdf)

Monthly returns of 30,000 long-short strategies can be found at [this Google Drive link](https://drive.google.com/drive/folders/1SZe_aF4ZNvK4ZRx2jQUE1j19KQvBaqWr).

The origins of predictability and quotes are found in [DataInput/SignalsTheoryChecked.csv](https://github.com/chenandrewy/flex-mining/blob/main/DataInput/SignalsTheoryChecked.csv)

This project uses the active R installation's default library paths and does not manage a project-local package environment.

Current results were produced with **R 4.5.3** and packages from the Posit P3M snapshot dated **2026-07-15**, plus `pcaMethods` from Bioconductor. See [docs/environment.md](docs/environment.md) for the package versions and how to regenerate the list.
