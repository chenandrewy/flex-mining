# Setup -------------------------------------------------------------------
source('0_Environment.R')
# WRDS and Open Source Asset Pricing access is needed only here, so these are
# not loaded by 0_Environment.R.
library(RPostgres)
library(getPass)
library(OpenSourceAP.DownloadR)

# Versions before 0.2.0 read the retired Google Drive folders.
if (packageVersion('OpenSourceAP.DownloadR') < '0.2.0') {
  stop("OpenSourceAP.DownloadR >= 0.2.0 is required (Hugging Face downloads). ",
       "Run install.packages('OpenSourceAP.DownloadR').")
}

# Load credentials from .env file
if (file.exists('.env')) {
  library(dotenv)
  load_dot_env('.env')
}

# Check whether wrds connection already exists, if not, create a new one
if (!exists('wrds') || !dbIsValid(wrds)) {
  # Try to get credentials from environment variables first
  user <- Sys.getenv('WRDS_USERNAME')
  pass <- Sys.getenv('WRDS_PASSWORD')
  
  # If not in env, fall back to interactive prompt
  if (user == '' || pass == '') {
    user <- getPass('wrds username: ')
    pass <- getPass('wrds password: ')
  }
  
  wrds <- dbConnect(Postgres(), 
                    host='wrds-pgdata.wharton.upenn.edu',
                    port=9737,
                    sslmode='require',
                    dbname='wrds',
                    user=user,
                    password= pass)
}

numRowsToPull <- -1 # Set to -1 for all rows and to some positive value for testing

# Chen-Zimmermann release on Hugging Face:
# https://huggingface.co/datasets/OpenSourceAP/CrossSection
# Other releases: e.g. '2023' (August 2023), '2026_10'.
czRelease = '2024_10' # October 2024 CZ Data

openap = OpenAP$new(release_year = czRelease)

# Chen-Zimmermann data -------------------------------------------

## dl from Hugging Face ====

# original-paper portfolios (Portfolios/Full Sets OP/PredictorPortsFull)
openap$dl_port('op') %>%
  data.table::fwrite('../Data/Raw/PredictorPortsFull.csv')

# signal doc: fetch the raw csv so the original column names are kept
# (openap$dl_signal_doc() goes through read.csv, which renames e.g.
# `Predictability in OP` to Predictability.in.OP)
download.file(openap$get_url('signal_doc'), '../Data/Raw/SignalDoc.csv'
              , mode = 'wb')

## Cleaning 1 ====

# keep only clear and likely
signaldoc =  data.table::fread('../Data/Raw/SignalDoc.csv') %>% 
  as_tibble() %>% 
  rename(signalname = Acronym) %>% 
  mutate(
    pubdate = as.yearmon(paste0(Year, '-12'))
    , sampend = as.yearmon(paste0(SampleEndYear, '-12')) 
    , sampstart = as.yearmon(paste0(SampleStartYear, '-07'))  # ensures no weird early 1963 edge effects
  ) %>% 
  transmute(signalname, Authors, Year, pubdate, sampend, sampstart
            , OP_pred = `Predictability in OP`
            , sweight = tolower(`Stock Weight`)
            , Rep_Quality = `Signal Rep Quality`
            , Journal
            , LongDescription) %>% 
  filter(
    OP_pred %in% c('1_clear','2_likely')
  ) %>% 
  mutate(
    sweight = if_else(is.na(sweight), 'ew', sweight)
  )

# patch Ritter 1991 typo
# see https://github.com/OpenSourceAP/CrossSection/issues/142
badrow = which(signaldoc$signalname == 'AgeIPO')
signaldoc[badrow, ]$sampstart = as.yearmon('Jul 1975')
signaldoc[badrow, ]$sampend = as.yearmon('Dec 1987')

# make monthly ls returns with sample def
czret = data.table::fread("../Data/Raw/PredictorPortsFull.csv") %>% 
  as_tibble() %>% 
  filter(!is.na(ret), port == 'LS') %>%                                                           
  left_join(signaldoc) %>% 
  mutate(date = as.yearmon(date)) %>% 
  mutate(
    samptype = case_when(
      (date >= sampstart) & (date <= sampend) ~ 'insamp'
      , (date > sampend) & (date <= pubdate) ~ 'oos'  
      , (date > pubdate) ~ 'postpub'
      , TRUE ~ NA_character_
    )
  ) %>% 
  select(signalname, date, ret, samptype, sampstart, sampend, pubdate, Rep_Quality) %>% 
  filter(!is.na(samptype)) %>% 
  # Add event time
  mutate(eventDate = interval(sampend, date) %/% months(1))
  
# summary stats
czsum = czret %>%
  filter(samptype == 'insamp') %>% 
  group_by(signalname) %>%
  summarize(
    rbar = mean(ret)
    , tstat = mean(ret)/sd(ret)*sqrt(dplyr::n())
  ) %>% 
  # add post-samp obs
  left_join(
    czret %>% filter(date > sampend) %>% 
      group_by(signalname) %>% summarize(nobs_postsamp = n()) %>% ungroup
    , by = 'signalname'
  ) %>% 
  # deal with zero postsamp obs
  mutate(
    nobs_postsamp = if_else(is.na(nobs_postsamp), 0L, nobs_postsamp)
  ) %>% 
  left_join(signaldoc %>% select(signalname, sampstart, sampend, pubdate, sweight, Rep_Quality
                                 , Authors, Year, Journal, LongDescription)
            , by = 'signalname')

# add filtering info
czsum = czsum %>% 
  mutate(
    rbar_ok = rbar > 0.15, n_ok = nobs_postsamp >= 9*12
    , Keep = rbar_ok & n_ok
  )

## Cleaning 2 ====

# merge on sumstats and Keep
czret2 = czret %>% 
  left_join(
    czsum %>% select(signalname, rbar, tstat, Keep)
    , by = 'signalname'
  )

#  filter Keep
czret2 = czret2 %>% 
  filter(Keep) %>% 
  setDT()

## save to disk ====

# need RDS for yearmon format argh
# also conceptually, these two are distinct.

saveRDS(czsum, '../Data/Processed/czsum_allpredictors.RDS')
saveRDS(czret2, '../Data/Processed/czret_keeponly.RDS')

# CRSP -----------------------------------------------------------

# Follows in part: https://wrds-www.wharton.upenn.edu/pages/support/research-wrds/macros/wrds-macro-crspmerge/

crspm_raw <- dbSendQuery(conn = wrds, statement = 
                       "select a.permno, a.permco, a.date, a.ret, a.retx, a.vol, a.shrout, a.prc, a.cfacshr, a.bidlo, a.askhi,
                     b.shrcd, b.exchcd, b.siccd, b.ticker, b.shrcls,  -- from identifying info table
                     c.dlstcd, c.dlret                                -- from delistings table
                     from crsp.msf as a
                     left join crsp.msenames as b
                     on a.permno=b.permno
                     and b.namedt<=a.date
                     and a.date<=b.nameendt
                     left join crsp.msedelist as c
                     on a.permno=c.permno 
                     and date_trunc('month', a.date) = date_trunc('month', c.dlstdt)
                     "
) %>% 
  # Pull data
  dbFetch(n = numRowsToPull) %>%
  as_tibble()

crspm = crspm_raw %>% mutate(
  ret = 100*ret
  , dlret = 100*dlret
  , yearm = as.yearmon(date)
  , date = as.Date(date)
  , me = abs(prc) * shrout
)

# write to disk
saveRDS(crspm, '../Data/Raw/crspm.RData')

# Fama-French Factors ----------------------------------------------------

FamaFrenchFactors <- dbSendQuery(conn = wrds, statement =
                                  "SELECT date, mktrf, smb, hml, rf, umd
                                 FROM ff.factors_monthly"
) %>% 
  # Pull data
  dbFetch(n = -1) %>%
  as_tibble()

# RMW and CMA begin in July 1963, but their shorter history should not truncate
# the CAPM and FF4 factors. Keep them for five-factor appendix calculations by
# joining them onto the longer FF3-plus-momentum panel.
FamaFrenchFiveFactors <- dbSendQuery(conn = wrds, statement =
                                      "SELECT date, rmw, cma
                                     FROM ff.fivefactors_monthly"
) %>%
  dbFetch(n = -1) %>%
  as_tibble()

FamaFrenchFactors <- FamaFrenchFactors %>%
  left_join(FamaFrenchFiveFactors, by = "date")

# Convert returns to percent and format date
FamaFrenchFactors <- FamaFrenchFactors %>%
  mutate(mktrf = 100*mktrf,
         smb = 100*smb,
         hml = 100*hml,
         umd = 100*umd,
         rf = 100*rf,
         rmw = 100*rmw,
         cma = 100*cma,
         yearm = as.yearmon(date),
         date = as.Date(date)
         ) %>% 
  select(-date)

if (anyDuplicated(FamaFrenchFactors$yearm)) {
  stop("Fama-French factor months are not unique.")
}
ff4_complete <- complete.cases(
  FamaFrenchFactors[, c("mktrf", "smb", "hml", "umd")]
)
if (!any(ff4_complete) ||
    min(FamaFrenchFactors$yearm[ff4_complete]) > as.yearmon("Jan 1927")) {
  stop("The FF3-plus-momentum history does not extend back to January 1927.")
}

# write to disk 
saveRDS(FamaFrenchFactors, '../Data/Raw/FamaFrenchFactors.RData')

# Compustat ----------------------------------------------------------------

# Select variable names: use all Yan-Zheng numerators and denominators
tempnames = union(compnames$yz.numer, compnames$yz.denom) 
tempnames[tempnames == 'do'] = 'a.do' # fix weird bug where 'do' is not accepted by dbSendQuery
tempnames = tempnames[tempnames != 'me_datadate']  # remove crsp me
tempnames = paste(tempnames, collapse = ', ')

qstring = paste0(
  'SELECT  gvkey, datadate, '
  , tempnames
  , "  FROM comp.funda as a 
      WHERE a.consol = 'C'
      AND a.popsrc = 'D'
      AND a.datafmt = 'STD'
      AND a.curcd = 'USD'
      AND a.indfmt = 'INDL'"  
)  

CompustatAnnualRaw = dbSendQuery(conn = wrds, statement = qstring) %>% 
  dbFetch(n = numRowsToPull) %>% 
  as.data.table()

# Download link table
CCM_LinkingTable <- dbSendQuery(conn = wrds, statement = 
                                  "SELECT a.gvkey, a.conm, a.tic, a.cusip, a.cik, a.sic, a.naics, b.linkprim,
                        b.linktype, b.liid, b.lpermno, b.lpermco, b.linkdt, b.linkenddt
                        FROM comp.names as a 
                        INNER JOIN crsp.ccmxpf_lnkhist as b
                        ON a.gvkey = b.gvkey
                        WHERE b.linktype in ('LC', 'LU')
                        AND b.linkprim in ('P', 'C')
                        ORDER BY a.gvkey"
) %>% 
  # Pull data
  dbFetch(n = numRowsToPull) %>%
  as_tibble()

## Clean -------------------------------------------------------------------

# Add identifiers for merging
CompustatAnnual <- left_join(CompustatAnnualRaw, CCM_LinkingTable, by="gvkey")

# convert dates, rename
CompustatAnnual <- CompustatAnnual %>% 
  mutate(
    datayearm = as.yearmon(datadate)
    , datadate = as.Date(datadate)
  ) %>% 
  rename(
    permno = lpermno
  )

#Use only if datadate is within the validity period of the link
CompustatAnnual <- CompustatAnnual %>% 
  mutate(
    permno = if_else(linkdt <= datadate & (datadate <= linkenddt | is.na(linkenddt) == TRUE), permno, NA_real_)
  )

# merge on me that matches datadate (me_datadate)
crspm = readRDS('../Data/Raw/crspm.RData') %>% 
  transmute(permno, datayearm = yearm, me_datadate = me)

CompustatAnnual = CompustatAnnual %>% 
  left_join(crspm, by = c('permno','datayearm'))

# Save to disk 
# fst format doesn't save yearmon format
saveRDS(
  CompustatAnnual,'../Data/Raw/CompustatAnnual.RData'
)

# Valid denominators ------------------------------------------------------

# The valid-denominator metadata (1963 non-missing frequencies and the
# preferred denominator per variable pair) needs no WRDS connection and is used
# only by the mined universe, so it is derived in memory inside chapter 2
# (derive_valid_denoms in 2a_CompustatToLongshort.R) rather than written here.
