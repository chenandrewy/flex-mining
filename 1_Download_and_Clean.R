# Setup -------------------------------------------------------------------
source('0_Environment.R')
library(googledrive)

# Check whether wrds connection already exists, if not, create a new one
if (!exists('wrds') || !dbIsValid(wrds)) {
  user <- getPass('wrds username: ')
  pass <- getPass('wrds password: ')
  
  wrds <- dbConnect(Postgres(), 
                    host='wrds-pgdata.wharton.upenn.edu',
                    port=9737,
                    sslmode='require',
                    dbname='wrds',
                    user=user,
                    password= pass)
}

numRowsToPull <- -1 # Set to -1 for all rows and to some positive value for testing

# August 2023 CZ Data
url <- "https://drive.google.com/drive/folders/1EP6oEabyZRamveGNyzYU0u6qJ-N43Qfq" 

# trigger login
url %>% drive_ls()

# Chen-Zimmermann data -------------------------------------------

## dl from gdrive ====

SUBDIR = 'Full Sets OP'; 
FILENAME = 'PredictorPortsFull.csv'

url %>% drive_ls() %>%
  filter(name == "Portfolios") %>% drive_ls() %>% 
  filter(name == SUBDIR) %>% drive_ls() %>% 
  filter(name == FILENAME) %>% 
  drive_download(path = paste0("../Data/Raw/",FILENAME), overwrite = TRUE)

# signal doc 
url %>% drive_ls() %>% 
  filter(name == "SignalDoc.csv") %>% 
  drive_download(path = "../Data/Raw/SignalDoc.csv", overwrite = TRUE)

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
  mutate(eventDate = interval(sampend, date) %/% months(1)) %>% 
  # fix sign of dCPVolSpread: https://github.com/OpenSourceAP/CrossSection/issues/139
  mutate(
    ret = if_else(signalname == 'dCPVolSpread', -ret, ret)
  ) 

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
  left_join(signaldoc %>% select(signalname, sampstart, sampend, sweight, Rep_Quality
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
                                   "SELECT date, mktrf, smb, hml, rf, umd, rmw, cma 
                                 FROM ff.fivefactors_monthly"
) %>% 
  # Pull data
  dbFetch(n = -1) %>%
  as_tibble()

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

# Save data for valid denominators ------------------------------------------------

comp0 = readRDS('../Data/Raw/CompustatAnnual.RData') 

# check some basic stuff to console
comp0 %>% 
  transmute(gvkey, year = year(datadate), permno, me_datadate, at) %>% 
  group_by(year) %>% 
  summarize(
    npermno = sum(!is.na(permno))
    , nme = sum(!is.na(me_datadate))
    , nat = sum(!is.na(at))
    , ngvkey = sum(!is.na(gvkey))
  ) %>% 
  print(n=30)

# count obs
fobs_list = comp0 %>% 
  filter(year(datadate)==1963, !is.na(permno)) %>% 
  arrange(gvkey, datadate) %>% 
  group_by(gvkey) %>% 
  filter(row_number() == 1) %>% 
  ungroup() %>% 
  summarise(across(everything(), function(x) sum(!is.na(x) & x!=0)/length(x)) ) %>% 
  pivot_longer(cols = everything()) %>% 
  transmute(
    name, freq_obs_1963 = value
  )

# keep only accounting vars + crsp me
tempnames = union(compnames$yz.numer, compnames$yz.denom) 
fobs_list = fobs_list %>% 
  filter(name %in% tempnames) %>% 
  arrange(-freq_obs_1963)

fwrite(fobs_list, 'DataIntermediate/freq_obs_1963.csv')

# manual checks -----------------------------------------------------------

fobs_list %>% 
  filter(name == 'me_datadate')

fobs_list %>% 
  filter(freq_obs_1963 > globalSettings$denom_min_fobs)

namecheck = fobs_list %>% 
  filter(freq_obs_1963 > globalSettings$denom_min_fobs) %>% 
  pull(name) 

# setdiff(namecheck, compnames$pos_in_1963)
# setdiff(compnames$pos_in_1963, namecheck)


