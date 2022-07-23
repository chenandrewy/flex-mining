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

m_crsp <- dbSendQuery(conn = wrds, statement = 
                       "select a.permno, a.permco, a.date, a.ret, a.retx, a.vol, a.shrout, a.prc, a.cfacshr, a.bidlo, a.askhi,
                     b.shrcd, b.exchcd, b.siccd, b.ticker, b.shrcls,  b.comnam,  -- from identifying info table
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

# write to disk 
write_fst(
  m_crsp,'../Data/Intermediate/m_crsp_raw.fst'
)


rm(m_crsp)



# CRSP CLEANING ==== 
crspm = read_fst('../Data/Intermediate/m_crsp_raw.fst')

# incorporate delisting return
# GHZ cite Johnson and Zhao (2007), Shumway and Warther (1999)
# but the way HXZ does this might be a bit better
crspm = crspm %>%
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
    , ret = (1+ret)*(1+dlret)-1 # 2022 02 patched ret + dlret < -1 problem
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
    , date = as.Date(date)
    , me = abs(prc) * shrout
    , yyyymm = year(date) * 100 + month(date)    
  )

# keep around me and melag for sanity
templag <- crspm %>%
  select(permno, yyyymm, me) %>%
  mutate(
    yyyymm = yyyymm + 1,
    yyyymm = if_else(yyyymm %% 100 == 13, yyyymm + 100 - 12, yyyymm)
  ) %>%
  transmute(permno, yyyymm, melag = me)

## subset into two smaller datasets for cleanliness
gc()
crspmret <- crspm %>%
  select(permno, date, yyyymm, ret) %>%
  filter(!is.na(ret)) %>%
  left_join(templag, by = c("permno", "yyyymm")) %>%
  arrange(permno, yyyymm)
gc()
crspminfo <- crspm %>%
  select(permno, yyyymm, prc, exchcd, me, shrcd, ticker) %>%
  arrange(permno, yyyymm)

# add info for easy me quantile screens
tempcut <- crspminfo %>%
  filter(exchcd == 1) %>%
  group_by(yyyymm) %>%
  summarize(
    me_nyse10 = quantile(me, probs = 0.1, na.rm = T),
    me_nyse20 = quantile(me, probs = 0.2, na.rm = T)
  )
crspminfo <- crspminfo %>%
  left_join(tempcut, by = "yyyymm")


# write to disk
write_fst(crspmret, '../Data/Intermediate/crspmret.fst')
write_fst(crspminfo, '../Data/Intermediate/crspminfo.fst')
write_fst(crspm, '../Data/Intermediate/crspm.fst')

## clean up
rm(list=ls(pattern='crsp'))
rm(list=ls(pattern='temp'))


# DOWNLOAD OTHER STUFF ====

## Download CCM linking table ====


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

#rename variables

CCM_LinkingTable <- CCM_LinkingTable %>% rename(timeLinkStart_d = linkdt,
                                                timeLinkEnd_d = linkenddt,
                                                permno = lpermno)

# write to disk 
write_fst(
  CCM_LinkingTable,'../Data/Intermediate/CCM_LinkingTable.fst'
)

#clean up
rm(CCM_LinkingTable)



## Download Fama and French factors ====



FamaFrenchFactors <- dbSendQuery(conn = wrds, statement = 
                                   "SELECT date, mktrf, smb, hml, rf, umd 
                                 FROM ff.factors_monthly"
) %>% 
  # Pull data
  dbFetch(n = numRowsToPull) %>%
  as_tibble()

# Convert returns to percent and format date
FamaFrenchFactors <- FamaFrenchFactors %>%
  mutate(mktrf = 100*mktrf,
         smb = 100*smb,
         hml = 100*hml,
         umd = 100*umd,
         rf = 100*rf,
         date = as.Date(date),
         yyyymm = year(date) * 100 + month(date)) %>% 
  select(-date)


# write to disk 
write_fst(
  FamaFrenchFactors,'../Data/Intermediate/m_FamaFrenchFactors.fst'
)

rm(FamaFrenchFactors)
gc()





