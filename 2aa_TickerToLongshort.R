# script for attempting to replicate Harvey 2017 JF, when he says:

# "Here are the instructions that I gave my research assistant: (1) form ortfo-
# lios based on the first, second, and third letters of the ticker symbol ..."
# "There are 3,160 possible long-short portfolios based on the first three let-
# ters of the tickers."

# one way to interpret this is that the kth letter has 26-27 values (including blanks)
# so there are about 26 + 27 + 27 = possible long portfolios to be made form
# forming "portfolios based on the first, second, and third letters"
# Then there are about choose(80, 2) = 3,160 possible long-short portfolios

# Created 2023 09 Andrew

# Environment ---------------------------------------------------------------
rm(list = ls())
source("0_Environment.R")

# import crsp data
crsp0 <- readRDS("../Data/Raw/crspm.RData")
setDT(crsp0)

# Clean crsp  -------------------------------------------------

crsp <- copy(crsp0)

# keep only common stocks
crsp <- crsp[exchcd %in% c(1, 2, 3) & shrcd %in% c(10, 11, 12)]

# select key variables
crsp <- crsp[, .(permno, date, ret, ticker, me)]

# lag ticker and me
crsp <- crsp[order(permno, date)]
crsp[, `:=`(
    lag_tic = data.table::shift(ticker, 1L, type = "lag"),
    lag_me = data.table::shift(me, 1L, type = "lag")
),
by = .(permno)
]


# Make long portfolios --------------------------------
# "form portfolios based on the first, second, and third letters of the ticker symbol"
tic_kth_letter_port <- function(k) {
    # create portfolio assignments
    crsp2 <- copy(crsp)
    crsp2[!is.na(lag_tic), `:=`(port = paste0("tic", k, substr(lag_tic, k, k)))]

    # find EW and VW returns
    port <- crsp2[!is.na(ret) & !is.na(port) & !is.na(lag_me),
        .(
            ret_ew = mean(ret), ret_vw = weighted.mean(ret, lag_me),
            nstock = .N
        ),
        by = c("date", "port")
    ] %>%
        pivot_longer(
            cols = c("ret_ew", "ret_vw"), names_to = "sweight", values_to = "ret",
            names_prefix = "ret_"
        ) %>%
        setDT()

    return(port)
} # end tic_kth_letter_port

# apply function to k = 1,2,3
longport <- rbindlist(lapply(1:3, tic_kth_letter_port))


# Make long-short portfolios --------------------------------

# make a list of all possible long portfolios
long_list <- longport$port %>%
    unique() %>%
    sort()

# list of all choose 2 combinations of long portfolios
longshort_list <- combn(long_list, 2, simplify = FALSE)

longshort_list <- do.call(rbind.data.frame, t(longshort_list)) %>% setDT()
names(longshort_list) <- c("longport", "shortport")

# it's a bit confusing to call this signal_list, but you can think
# about it as the signal is 1 if tic1 = A and 0 if tic1 = B, etc.
# port_list refers to the stock weights
# more generally, we use port_list to refer to the quantile cutoffs
# but these signals are all binary
signal_list <- longshort_list %>%
    arrange(longport, shortport) %>%
    mutate(
        signalid = row_number(),
        signalname = paste0(longport, "_minus_", shortport)
    ) %>%
    select(signalid, signalname, longport, shortport)

port_list <- data.table(
    portid = 1:2, sweight = c("ew", "vw")
)

# make list of all compinations of signals and port_list
# this nomenclature sucks, sorry
ls_distinct <- expand_grid(
    signalid = signal_list$signalid, portid = port_list$portid
) %>%
    setDT() %>%
    left_join(signal_list, by = "signalid") %>%
    left_join(port_list, by = "portid")

# loop over each ls_distinct row
n_ls <- nrow(ls_distinct)
tempret <- list()
tic <- Sys.time()
for (i in 1:n_ls) {
    # load up settings
    setcur <- ls_distinct[i, ]

    # print progress
    if (i %% 100 == 0) {
        print(paste0("long-short port ", i, " of ", n_ls))
        print(setcur)
        print(paste0("time remaining = ", round((Sys.time() - tic) / i * (n_ls - i), 2)))
    }

    # get long portfolio returns
    longcur <- longport[sweight == setcur$sweight & port == setcur$longport] %>%
        transmute(date, ret_long = ret, nlong = nstock)

    # get short portfolio returns
    shortcur <- longport[sweight == setcur$sweight & port == setcur$shortport] %>%
        transmute(date, ret_short = ret, nshort = nstock)

    # merge long and short
    lscur <- merge(longcur, shortcur, by = "date", all = TRUE) %>%
        mutate(ret = ret_long - ret_short)

    # store
    tempret[[i]] <- lscur %>%
        mutate(signalid = setcur$signalid, portid = setcur$portid) %>%
        select(signalid, portid, date, ret, nlong, nshort)
} # end for i
lsret <- rbindlist(tempret) %>% setDT()

# save to disk -------------------------------------------------

ticdat = list(
    ret = lsret
    , signal_list = signal_list
    , port_list = port_list
    , name = 'ticker_Harvey2017JF'
)

saveRDS(ticdat, '../Data/Processed/ticker_Harvey2017JF.RDS')

# save to csv for sharing -------------------------------------------------

dir.create('../Data/Export/', showWarnings = F)

# save equal-weighted
portcur <- port_list[sweight == "ew"]
temp = lsret[portid == portcur$portid] %>% 
    left_join(signal_list[, .(signalid, signalname)], by = "signalid") %>% 
    select(signalname, date, ret, nlong, nshort)
fwrite(temp, '../Data/Export/ticker_Harvey2017JF_ew.csv', row.names = FALSE)


# save value-weighted
portcur <- port_list[sweight == "vw"]
temp = lsret[portid == portcur$portid] %>% 
    left_join(signal_list[, .(signalid, signalname)], by = "signalid") %>% 
    select(signalname, date, ret, nlong, nshort)
fwrite(temp, '../Data/Export/ticker_Harvey2017JF_vw.csv', row.names = FALSE)    

# check -------------------------------------------------
portcur <- port_list[sweight == "ew"]

ls_sum <- lsret[portid == portcur$portid & year(date) >= 1963 &
    !is.na(ret)] %>%
    group_by(signalid) %>%
    summarize(
        rbar = mean(ret), vol = sd(ret), nmonth = n(), tstat = rbar / vol * sqrt(nmonth)
    )

ls_sum

# compare to standard normal
Femp = ecdf(ls_sum$tstat)

tlist = seq(-4, 4, by = 0.5)

plotme = tibble(
    tleft = tlist[-length(tlist)]
    , tright = tlist[-1]
    , tmid = (tleft + tright) / 2
    , Pr_emp = Femp(tright) - Femp(tleft)
    , Pr_norm = pnorm(tright) - pnorm(tleft)
) %>% 
pivot_longer(cols = c("Pr_emp", "Pr_norm"), names_to = "type", values_to = "Pr") 

plt = ggplot(plotme, aes(x = tmid, y = Pr, color = type)) +
    geom_line() +
    geom_point() +
    theme_bw() +
    labs(x = "t-stat", y = "Pr(t-stat)") +
    scale_color_manual(values = c("black", "red")) 

ggsave('temp.pdf', scale = 0.5)

