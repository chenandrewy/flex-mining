# Setup -------------------------------------------------------------------

source('0_Environment.R')

user <- getPass('wrds username: ')
pass <- getPass('wrds password: ')

wrds <- dbConnect(Postgres(), 
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  sslmode='require',
                  dbname='wrds',
                  user=user,
                  password= pass)

numRowsToPull <- -1 # Set to -1 for all rows and to some positive value for testing




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
saveRDS(crspm, '../Data/Intermediate/crspm.RData')


# Fama-French Factors ----------------------------------------------------

FamaFrenchFactors <- dbSendQuery(conn = wrds, statement = 
                                   "SELECT date, mktrf, smb, hml, rf, umd 
                                 FROM ff.factors_monthly"
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
         yearm = as.yearmon(date),
         date = as.Date(date)
         ) %>% 
  select(-date)


# write to disk 
saveRDS(FamaFrenchFactors, '../Data/Intermediate/FamaFrenchFactors.RData')


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
crspm = readRDS('../Data/Intermediate/crspm.RData') %>% 
  transmute(permno, datayearm = yearm, me_datadate = me)

CompustatAnnual = CompustatAnnual %>% 
  left_join(crspm, by = c('permno','datayearm'))

# Save to disk ------------------------------------------------------------

# fst format doesn't save yearmon format
saveRDS(
  CompustatAnnual,'../Data/Intermediate/CompustatAnnual.RData'
)


