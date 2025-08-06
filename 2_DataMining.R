# wrapper for generating baseline data mined strategies 

# 2a_CompustatToLongshort.R takes about 2-3 hours.  The rest is fast

# Environment -------------------------------------------------------------

source('0_Environment.R')


# Mining ------------------------------------------------------------------
source('2a_CompustatToLongshort.R') 
source('2b_MatchDataMinedToPub.R')
source('2c_TickerToLongshort.R')
source('2d_RiskAdjustDataMinedSignals.R')