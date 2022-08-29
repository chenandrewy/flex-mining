
# Setup -------------------------------------------------------------------


rm(list = ls())
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




# Download ----------------------------------------------------------------

# Select variable names: use all Yan-Zheng numerators and denominators
tempnames = c(compnames$yz.denom, compnames$yz.numer) 
tempnames[tempnames == 'do'] = 'a.do' # fix weird bug where 'do' is not accepted by dbSendQuery
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


# Clean -------------------------------------------------------------------

# Add identifiers for merging
CompustatAnnual <- left_join(CompustatAnnualRaw, CCM_LinkingTable, by="gvkey")

#Use only if datadate is within the validity period of the link
CompustatAnnual <- CompustatAnnual %>% 
  filter(
    (linkdt <= datadate & (datadate <= linkenddt | is.na(linkenddt) == TRUE)) == TRUE
  )

# convert dates, rename
CompustatAnnual <- CompustatAnnual %>% 
  mutate(
    datayearm = as.yearmon(datadate)
    , datadate = as.Date(datadate)
  ) %>% 
  rename(
    permno = lpermno
  )


#Yan and Zheng (2017): To mitigate a backfilling bias, we require that a firm be listed on Compustat for two years
#before it is included in our sample (Fama and French 1993)
CompustatAnnual <- CompustatAnnual %>% arrange(gvkey, datadate)
CompustatAnnual <- CompustatAnnual %>% group_by(gvkey) %>% 
  mutate(year = year(datadate),
         year_min = min(year)) %>% 
  filter(year != year_min & year != year_min + 1) %>% 
  select(-year, -year_min) %>% 
  ungroup()



# Save to disk ------------------------------------------------------------

# fst format doesn't save yearmon format
saveRDS(
  CompustatAnnual,'../Data/Intermediate/CompustatAnnual.RData'
)


