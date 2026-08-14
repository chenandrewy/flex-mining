# Libraries ====
library(data.table)
library(RPostgres)
library(haven)
library(getPass)
library(dplyr)
library(fst)
library(lubridate)
library(splitstackshape)
library(zoo)
library(tidyr)
library(tictoc)
library(readr)
library(stringr)
library(ggplot2)
library(gridExtra)
library(xtable)
library(lmtest)
library(roll)
library(sandwich)
library(huxtable)
library(janitor)
library(kableExtra)
library(strucchange) 
library(foreach)
library(latex2exp)

if("lme4" %in% (.packages())){
  detach("package:lme4", unload=TRUE) 
}
if("multcomp" %in% (.packages())){
  detach("package:multcomp", unload=TRUE) 
}
if("TH.data" %in% (.packages())){
  detach("package:TH.data", unload=TRUE) 
}
if("MASS" %in% (.packages())){
  detach("package:MASS", unload=TRUE) 
}



# Paths -------------------------------------------------------------------

# code assumes that working directory is the directory with the R scripts
# check that working directory is correct
if (!file.exists('0_Environment.R')){
  stop('error: 0_Environment.R not found.  Please set working directory to the folder with the script')
}

# create data folders (separate to avoid storage problems)
dir.create('../Data/', showWarnings = F)
dir.create('../Data/Raw/', showWarnings = F)
dir.create('../Data/Processed/', showWarnings = F)
dir.create('../Results', showWarnings = F)
dir.create('../Results/Extra/', showWarnings = F)


# Globals ====
options(stringsAsFactors = FALSE)

# Run choices (globalSettings, runStages) and the RNG seed live in config.R so
# they can be sourced standalone. local = TRUE keeps them in this file's frame,
# which is globalenv under source() and settings_env under the sys.source()
# chapter drivers.
source('config.R', local = TRUE)

# Yan-Zheng numerator and denominator names
# YZ list MKTCAP in Table B.1, which we call me_datadate  mkvalt is not available earlier in the data

compnames = list()
compnames$yz.numer = c("acchg", "aco", "acox", "act", "am", "ao", "aoloch", "aox", "ap", "apalch",
                       "aqc", "aqi", "aqs", "at", "bast", "caps", "capx", "capxv", "ceq", "ceql", "ceqt", "ch", "che", "chech",
                       "cld2", "cld3", "cld4", "cld5", "cogs", "cstk", "cstkcv", "cstke", "dc", "dclo", "dcom", "dcpstk",
                       "dcvsr", "dcvsub", "dcvt", "dd", "dd1", "dd2", "dd3", "dd4", "dd5", "dfs", "dfxa", "diladj", "dilavx",
                       "dlc", "dlcch", "dltis", "dlto", "dltp", "dltr", "dltt", "dm", "dn", "do", "donr", "dp", "dpact", "dpc",
                       "dpvieb", "dpvio", "dpvir", "drc", "ds", "dudd", "dv", "dvc", "dvp", "dvpa", "dvpibb", "dvt", "dxd2", "dxd3",
                       "dxd4", "dxd5", "ebit", "ebitda", "esopct", "esopdlt", "esopt", "esub", "esubc", "exre", "fatb", "fatc", "fate",
                       "fatl", "fatn", "fato", "fatp", "fiao", "fincf", "fopo", "fopox", "fopt", "fsrco", "fsrct", "fuseo", "fuset", "gdwl",
                       "gp", "ib", "ibadj", "ibc", "ibcom", "icapt", "idit", "intan", "intc", "intpn", "invch", "invfg", "invo", "invrm",
                       "invt", "invwip", "itcb", "itci", "ivaco", "ivaeq", "ivao", "ivch", "ivncf", "ivst", "ivstch", "lco", "lcox",
                       "lcoxdr", "lct", "lifr", "lo", "lt", "mib", "mii", "mrc1", "mrc2", "mrc3", "mrc4", "mrc5", "mrct", "msa", "ni",
                       "niadj", "nieci", "nopi", "nopio", "np", "oancf", "ob", "oiadp", "pi", "pidom", "pifo", "ppegt", "ppenb",
                       "ppenc", "ppenli", "ppenme", "ppennr", "ppeno", "ppent", "ppevbb", "ppeveb", "ppevo", "ppevr", "prstkc",
                       "pstk", "pstkc", "pstkl", "pstkn", "pstkr", "pstkrv", "rdip", "re", "rea", "reajo", "recch", "recco", "recd", "rect",
                       "recta", "rectr", "reuna", "sale", "seq", "siv", "spi", "sppe", "sppiv", "sstk", "tlcf", "tstk", "tstkc", "tstkp",
                       "txach", "txbco", "txc", "txdb", "txdba", "txdbca", "txdbcl", "txdc", "txdfed", "txdfo", "txdi", "txditc",
                       "txds", "txfed", "txfo", "txndb", "txndba", "txndbl", "txndbr", "txo", "txp", "txpd", "txr", "txs", "txt", "txw",
                       "wcap", "wcapc", "wcapch", "xacc", "xad", "xdepl", "xi", "xido", "xidoc", "xint", "xopr", "xpp", "xpr", "xrd", "xrent",
                       "xsga")

compnames$yz.denom <- c("at", "act",  "invt", "ppent", "lt", "lct", "dltt",
                        "ceq", "seq", "icapt", "sale", "cogs", "xsga", "emp", 'me_datadate')


# compnames$yz.denom_alt <- c("at", "act",  "invt", "ppent", "lt", "lct", "dltt",
#                             "ceq", "seq", "icapt", "sale", "cogs", "xsga", "emp", 'me')

# 63 denominators with at least 25% non-missing observations in 1963
# compnames$pos_in_1963 <- c("aco", "acox","act","ao","aox","at","caps","capx","capxv","ceq","ceql","ceqt","che","cogs",
#                           "cstk","dlc","dltt","dp","dpact","dvc","dvp","dvt","ebit","ebitda","gp","ib","ibadj","ibcom",
#                           "icapt","intan","invt","itci","ivaeq","ivao","lct","lo","lt","ni","nopi","nopio","np",
#                           "oiadp","pi","ppegt","ppent","pstkl","pstkrv","re","recco","rect","sale","seq","txdb",
#                           "txditc","txt","wcap","xint","xopr","xpr","xrent","xsga","emp", "me_datadate")

# compnames$all = unique(Reduce(c, compnames))

# nice colors
colors = c(rgb(0,0.4470,0.7410), # MATBLUE
           rgb(0.8500, 0.3250, 0.0980), # MATRED
           rgb(0.9290, 0.6940, 0.1250) # MATYELLOW
)


# Functions ---------------------------------------------------------------

# Reusable functions live in helpers/, sourced here so every script that
# sources 0_Environment.R gets them. local = TRUE keeps them in this frame
# (globalenv under source(); settings_env under the sys.source drivers).
# NOTE: the risk_adjusted_helpers_{tv,fs}.R files are intentionally NOT sourced
# here -- they define overlapping names and are sourced per-script instead.
for (.helper in c("utils", "mining", "matching", "stats", "plotting", "implied_category")) {
  source(file.path("helpers", paste0(.helper, ".R")), local = TRUE)
}
rm(.helper)

