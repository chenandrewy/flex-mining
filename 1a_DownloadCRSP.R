# SETUP ====

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



# CRSP DOWNLOAD ====
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


# CRSP CLEANING ==== 
# incorporate delisting return
# GHZ cite Johnson and Zhao (2007), Shumway and Warther (1999)
# but the way HXZ does this might be a bit better
crspm = crspm_raw %>%
  mutate(
    dlret = ifelse(
      is.na(dlret)
      & (dlstcd == 500 | (dlstcd >=520 & dlstcd <=584))
      & (exchcd == 1 | exchcd == 2)
      , -0.35
      , dlret
    )
    , dlret = ifelse(
      is.na(dlret)
      & (dlstcd == 500 | (dlstcd >=520 & dlstcd <=584))
      & (exchcd == 3)
      , -0.55
      , dlret
    )
    , dlret = ifelse(
      dlret < -1 & !is.na(dlret)
      , -1
      , dlret
    )
    , dlret = ifelse(
      is.na(dlret)
      , 0
      , dlret
    )
    , ret = (1+ret)*(1+dlret)-1
    , ret = ifelse(
      is.na(ret) & ( dlret != 0)
      , dlret
      , ret
    )
  )

# convert ret to pct, other formatting
crspm = crspm %>%
  mutate(
    ret = 100*ret
    , yearm = as.yearmon(date)
    , date = as.Date(date)
    , me = abs(prc) * shrout
  )


# write to disk
# fst format doesn't save yearmon
saveRDS(crspm, '../Data/Intermediate/crspm.RData')


# DOWNLOAD OTHER STUFF ====


## Download Fama and French factors ====

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

