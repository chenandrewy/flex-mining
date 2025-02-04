# 4zz-structural-break.r
# Created 2024 10: Motivation:

# Theme from the NBER Summer Institute: What if the economic ideas do help predict returns, but publicizing them makes them, in the end, similar to data mining?

# My response is generally: we can't rule that out. But we prefer the story that the decline in the published returns is due to improvements in IT circa 2004 (Chordia Subra Tong 2014), rather than the publicization of the predictor via academia (Mclean and Pontiff). The data can't really separate these two since the publication dates also cluster around 2004.

# Setup ========================
rm(list = ls())
source("0_Environment.r")

DMname <- paste0(
    "../Data/Processed/",
    globalSettings$dataVersion,
    " LongShort.RData"
)

## load data ====

# published ret
czsum <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
    filter(Keep) %>%
    setDT()

czcat <- fread("DataInput/SignalsTheoryChecked.csv") %>%
    mutate(pubdate = as.yearmon(paste0(Year, "-12"))) %>%
    select(signalname, pubdate, theory) 

czret <- readRDS("../Data/Processed/czret_keeponly.RDS") %>%
    left_join(czcat, by = "signalname") %>% 
    filter(date >= as.yearmon('1964-01'))
# Compute all breakdates ========================

# compute break dates
breakdat0 <- foreach(
    signalcur = unique(czret$signalname),
    .packages = c("strucchange", "data.table"),
    .combine = rbind
) %do% {
    # Display progress
    print(paste0("Computing break date for signal: ", signalcur))
    retcur = czret[signalname == signalcur]

    # estimate
    est <- breakpoints(
        formula = ret ~ 1,
        data = retcur,
        breaks = 1
    )    

    # output
    opt_nbreak = if_else(is.na(est$breakpoints[1]), 0, 1)
    date_break = retcur$date[summary(est)$breakpoints[1]]

    datout = data.table(
        signalname = signalcur,
        opt_nbreak = opt_nbreak,
        date_break = date_break,
        ret_pre = mean(retcur[date <= date_break]$ret),
        ret_post = mean(retcur[date > date_break]$ret),
        BIC_0 = summary(est)$RSS['BIC', '0'],
        BIC_1 = summary(est)$RSS['BIC', '1'],
        RSS_0 = summary(est)$RSS['RSS', '0'],
        RSS_1 = summary(est)$RSS['RSS', '1']
    )
    return(datout)

} # end foreach

breakdat <- breakdat0 %>%
    mutate(diff_ret = ret_post - ret_pre) %>%
    merge(czsum %>% transmute(signalname, sampend)) %>% 
    merge(czcat %>% transmute(signalname, theory)) %>% 
    mutate(break_vs_sampend = date_break - sampend)

# Table: description of split dates and returns ========================

# set up
# NASDAQ decimation is April 2001, easy to introduce
# Using this split date means no pre-split data for DelDRC (Prakash and Sinha, CAR)
# which, incidentally, is explicitly about the effects of SEC rule SAB 101 (from year 2000)
# about governing software firms. 

# But NASDAQ decimation does not line up with other exhibits, which have been
# using 2004-12 as the "post-IT" date, following Chen and Velikov 2023.

tech_date_user = as.yearmon('2004-12')

tempret = czret %>%
    filter(date >= sampstart) %>%
    select(signalname, date, ret, sampend) %>% 
    merge(breakdat %>% select(signalname, date_break)) %>% 
    mutate(it_date = tech_date_user ) 

# compute summary for each split type
tabdat <- foreach(split_type = c('sampend', 'date_break', 'it_date'), .combine = rbind) %do% {

    # remove DelDRC due to its special post-IT sample
    if (split_type == 'it_date'){
        tempret2 = tempret %>% filter(signalname != 'DelDRC')
    } else {
        tempret2 = tempret
    }

    # compute summary
    tempret2 %>% 
        mutate(split_date = .data[[split_type]]) %>%
        mutate(samptype = if_else(date <= split_date, 'pre', 'post')) %>%
        group_by(signalname, samptype) %>%
        summarize(ret = mean(ret), split_date = mean(split_date), .groups = 'drop') %>% 
        pivot_wider(names_from = samptype, values_from = ret) %>% 
        mutate(diff_ret = post - pre) %>% 
        summarize(
            split_type = split_type,
            split_date = mean(split_date),
            pre = mean(pre)*100, 
            post = mean(post)*100, 
            pct_decay = mean(diff_ret < 0)*100
        ) 
} %>% 
    mutate(split_type = factor(
        split_type,
        levels = c('sampend', 'it_date', 'date_break'),
        labels = c('1. Paper\'s Sample Ends',
                   '2. High Speed Internet',
                   '3. Data-Driven Break')
    )) %>% 
    arrange(split_type) %>% 
    print()

# make latex
tabdat %>% 
    kable('latex', booktabs = T, linesep='', escape=F, digits=0) %>% 
    cat(file='../Results/temp.tex')

# make it beautiful
tex = readLines('../Results/temp.tex')
mcol = function(x) paste0('\\multicolumn{1}{c}{', x, '}')
tex[2] = "\\begin{tabular}{lcccc}"
tex[4] = paste0(
    mcol('Event'), ' & ',
    mcol('Mean'), ' & ',
    '\\multicolumn{2}{c}{Return (bps p.m.)} & ',
    mcol('\\% of Signals'), ' \\\\ ',
    ' & ',
    mcol('Date'), ' & ',
    mcol('Before'), ' & ',
    mcol('After'), ' & ',
    mcol('w/ Decay'), ' \\\\ '
)

tex %>% print()
writeLines(tex, '../Results/samp_split_summary.tex')


# Scatterplot ========================

# I guess this should be both clearer and more precise

# set up (optional for filtering, grouping)
breakdat2 = breakdat

# plot and save to pdf
set.seed(123)
tempnoise = rnorm(nrow(breakdat2), 0, 0.5) # noise makes it easier to see the overlapping points
(
    breakdat2 %>%
    mutate(date_break = as.numeric(date_break),
           sampend = as.numeric(sampend)+tempnoise) %>%
    ggplot(aes(x = sampend, y = date_break)) +
    geom_point(size = 2, color = 'gray30') +
    # Horizontal Dec 2004 line
    geom_hline(yintercept = tech_date_user, linetype = 'dashed') +
    annotate("text", x = 1960, y = tech_date_user + 1.5, 
             hjust = 0,
             label = '2. High Speed Internet',
             size = 5) +

    theme_light(base_size = 18) +
    theme(
          legend.title = element_blank(),
          legend.position = c(80,10)/100,
          legend.box = "horizontal",
          legend.background = element_rect(
            fill = "white", color = 'black'),
          legend.text = element_text(size = 12),
          legend.margin = margin(t = 5, r = 5, b = 5, l = 5) 
          ) +
    labs(x = '1. Paper\'s Sample Ends', y = '3. Data-Driven Break') +
    coord_cartesian(xlim = c(1960, 2020), ylim = c(1960, 2020))
) %>% 
    ggsave(filename = '../Results/break_vs_sampend.pdf', width = 10, height = 8, device = cairo_pdf, scale = 0.8)


# Extra Plots ========================

if (FALSE){
# Function to create the plot
plot_date_diff <- function(breakdat2, date_alt_str = '2004-12') {

    datediff <- breakdat2 %>%
        mutate(
            date_it = as.yearmon(date_alt_str),
            diff_date = as.numeric(date_break - sampend),
            diff_it = as.numeric(date_break - date_it)
        ) %>%
        select(signalname, sampend, date_break, diff_date, diff_it)

    # plot the date differences
    plot <- datediff %>%
        pivot_longer(cols = c(diff_date, diff_it), names_to = "date_type", values_to = "diff") %>%
        mutate(date_type = factor(date_type, levels = c("diff_date", "diff_it"),
                                  labels = c("Publication", "Info Tech"))) %>%
        ggplot(aes(x = diff, fill = date_type)) +
        geom_histogram(binwidth = 2.5, alpha = 0.5, position = "identity") +
        scale_fill_manual(values = c(colors[1], colors[2]), 
                          labels = c('Sample End', 
                          date_alt_str),
                          name = "Conjectured break") +
        theme_minimal(base_size = 24) +
        theme(legend.position = c(2,9)/10) +
        scale_x_continuous(breaks = seq(-100, 100, 5)) +
        labs(x = 'Empirical break date - conjectured break (years)')
        

    return(plot)
}

## Plot all and breaks = 1 ====
#   This is the key plot! 
#   Plot 1 shows that sample ends can be very far from the break dates.
#   The conjecture that the breaks are all at 2004-12 seems to be a better fit.
plot1 <- plot_date_diff(breakdat[diff_ret < 0]) +
    ggtitle("All published signals with post-break decay")
plot2 <- plot_date_diff(breakdat[opt_nbreak == 1 & diff_ret < 0]) +
    ggtitle("Optimal breaks = 1 with post-break decay")

ggsave(plot1, filename = '../Results/deleteme_plot1.pdf', width = 10, height = 8, device = cairo_pdf, scale = 1.2)
ggsave(plot2, filename = '../Results/deleteme_plot2.pdf', width = 10, height = 8, device = cairo_pdf, scale = 1.2)

## Simpler plot ========================
# the problem here is that you can't see the signal-level difference between sampend date and break date
# both distributions look very similar in aggregate, but i fyou "condition" on the signal, there's a difference.
breakdat2 = breakdat[diff_ret < 0]

ref_date = as.yearmon('2004-12')

p1 = breakdat2 %>% 
    select(signalname, date_break, sampend) %>% 
    pivot_longer(cols = c(date_break, sampend)) %>% 
    mutate(name = factor(name,
                         levels = c('date_break', 'sampend'),
                         labels = c('Estimated Break Date', 'Sample End Date'))) %>%
    ggplot(aes(x = value, fill = name)) +
    # histogram
    geom_histogram(binwidth = 2.5, alpha = 0.5, position = "dodge") +
    scale_fill_manual(values = c(colors[1], colors[2]), name = "Date") +
    # vertical line for Dec 2004
    geom_vline(xintercept = ref_date, linetype = 'solid') +
    annotate("text", x = ref_date, y = 35, 
    label = as.character(ref_date), vjust = 1.5, angle = 90,
             size = 10) +
    theme_minimal(base_size = 24) +
    theme(legend.position = c(2,9)/10,
          legend.title = element_blank()) +
    labs(x = '', y = 'Number of Predictors')

ggsave(p1, filename = '../Results/deleteme_sampend.pdf', width = 10, height = 8, device = cairo_pdf, scale = 1.2)

} # end if (FALSE)