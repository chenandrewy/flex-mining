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





# COMPUSTAT ANNUAL ====

# 240 accounting variables and 15 base variables taken from Appendix table in Yan and Zheng (2017)
CompustatAnnual <- dbSendQuery(conn = wrds, statement = 
                                 "SELECT  gvkey, datadate, conm, fyear, tic, cusip, naicsh, sich, 
                                 prcc_c, emp,
                          acchg, aco, acox, act, am, ao, aoloch, aox, ap, apalch, 
		                      aqc, aqi, aqs, at, bast, caps, capx, capxv, ceq, ceql, ceqt, ch, che, chech,
		                      cld2, cld3, cld4, cld5, cogs, cstk, cstkcv, cstke, dc, dclo, dcom, dcpstk, 
		                      dcvsr, dcvsub, dcvt, dd, dd1, dd2, dd3, dd4, dd5, dfs, dfxa, diladj, dilavx,
		                      dlc, dlcch, dltis, dlto, dltp, dltr, dltt, dm, dn, a.do, donr, dp, dpact, dpc,
		                      dpvieb, dpvio, dpvir, drc, ds, dudd, dv, dvc, dvp, dvpa, dvpibb, dvt, dxd2, dxd3,
		                      dxd4, dxd5, ebit, ebitda, esopct, esopdlt, esopt, esub, esubc, exre, fatb, fatc, fate,
		                      fatl, fatn, fato, fatp, fiao, fincf, fopo, fopox, fopt, fsrco, fsrct, fuseo, fuset, gdwl,
		                      gp, ib, ibadj, ibc, ibcom, icapt, idit, intan, intc, intpn, invch, invfg, invo, invrm, 
		                      invt, invwip, itcb, itci, ivaco, ivaeq, ivao, ivch, ivncf, ivst, ivstch, lco, lcox, 
		                      lcoxdr, lct, lifr, lo, lt, mib, mii, mrc1, mrc2, mrc3, mrc4, mrc5, mrct, msa, ni,
		                      niadj, nieci, nopi, nopio, np, oancf, ob, oiadp, pi, pidom, pifo, ppegt, ppenb,
		                      ppenc, ppenli, ppenme, ppennr, ppeno, ppent, ppevbb, ppeveb, ppevo, ppevr, prstkc,
		                      pstk, pstkc, pstkl, pstkn, pstkr, pstkrv, rdip, re, rea, reajo, recch, recco, recd, rect,
		                      recta, rectr, reuna, sale, seq, siv, spi, sppe, sppiv, sstk, tlcf, tstk, tstkc, tstkp, 
		                      txach, txbco, txc, txdb, txdba, txdbca, txdbcl, txdc, txdfed, txdfo, txdi, txditc, 
		                      txds, txfed, txfo, txndb, txndba, txndbl, txndbr, txo, txp, txpd, txr, txs, txt, txw,
		                      wcap, wcapc, wcapch, xacc, xad, xdepl, xi, xido, xidoc, xint, xopr, xpp, xpr, xrd, xrent,
		                      xsga
                        FROM comp.funda as a 
	                      WHERE a.consol = 'C'
	                      AND a.popsrc = 'D'
	                      AND a.datafmt = 'STD'
	                      AND a.curcd = 'USD'
	                      AND a.indfmt = 'INDL'"
) %>% 
  # Pull data
  dbFetch(n = numRowsToPull) %>%
  as_tibble()


# Add identifiers for merging
CCM_LinkingTable = read_fst('../Data/Intermediate/CCM_LinkingTable.fst') %>% 
  select(gvkey, permno, timeLinkStart_d, timeLinkEnd_d)

CompustatAnnual <- left_join(CompustatAnnual, CCM_LinkingTable, by="gvkey")

#Use only if datadate is within the validity period of the link

CompustatAnnual <- CompustatAnnual %>% filter((timeLinkStart_d <= datadate & (datadate <= timeLinkEnd_d | is.na(timeLinkEnd_d) == TRUE)) == TRUE)

# Create two versions: Annual and monthly (monthly makes matching to monthly CRSP easier)

#Annual version

#CompustatAnnual <- CompustatAnnual %>% select(-timeLinkStart_d, timeLinkEnd_d, linkprim, liid, linktype)
CompustatAnnual <- CompustatAnnual %>% mutate(datadate = as.Date(datadate),
                                                      time_avail_d = as.Date(datadate %m+% months(6)), #assuming 6 months reporting lag
                                                      time_avail_y = format(time_avail_d,  "%Y")) 

#time_avail_m = format(time_avail_d, "%Y-%m"),
#yyyymm = year(time_avail_d) * 100 + month(time_avail_d)

CompustatAnnual <- CompustatAnnual %>% arrange(gvkey, datadate)

#Yan and Zheng (2017): To mitigate a backfilling bias, we require that a firm be listed on Compustat for two years
#before it is included in our sample (Fama and French 1993)

CompustatAnnual <- CompustatAnnual %>% group_by(gvkey) %>% 
  mutate(year = year(datadate),
         year_min = min(year)) %>% 
  filter(year != year_min & year != year_min + 1) %>% 
  select(-year, -year_min) %>% 
  ungroup()


# write to disk 
write_fst(
  CompustatAnnual,'../Data/Intermediate/a_aCompustat.fst'
)

# MONTHLY VERSION OF COMPA ====

CompustatAnnual_monthly  <- tidyr::expand(CompustatAnnual, time_avail_y = unique(time_avail_y), month = 1:12) %>%
  left_join(CompustatAnnual, by = 'time_avail_y') %>% 
  arrange(gvkey, datadate) %>% 
  mutate(time_avail_d_m = as.Date(time_avail_d %m+% months(month-1)),
                                               yyyymm = year(time_avail_d_m) * 100 + month(time_avail_d_m)) %>% 
  select(-month)


CompustatAnnual_monthly <- CompustatAnnual_monthly %>% 
  group_by(gvkey, yyyymm) %>% 
  filter(datadate == max(datadate)) %>% 
  group_by(permno, yyyymm) %>% 
  filter(datadate == max(datadate)) %>% 
  ungroup()

# write to disk 
write_fst(
  CompustatAnnual_monthly,'../Data/Intermediate/m_aCompustat.fst'
)


# code for testing
# CompustatAnnual_monthly <- as.data.table(CompustatAnnual_monthly)
# setkey(CompustatAnnual_monthly, gvkey, yyyymm)
# CompustatAnnual_monthly[, dupvar := 1L*(.N > 1L), by=c("gvkey", "yyyymm")] #duplicates tag
# test <- CompustatAnnual_monthly %>% filter(dupvar>0)


rm(list=ls(pattern='Compustat'))
rm(list=ls(pattern='CCM'))
gc()


