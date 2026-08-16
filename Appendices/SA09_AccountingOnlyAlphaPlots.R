# Appendix accounting-only CAPM and FF4 alpha-decay plots.
#
# How to run: normally run through SA_Appendices.R.
# Inputs: risk_adjusted_dm_benchmarks.RDS, published-signal documentation, and
#   DataInput/SignalsTheoryChecked.csv
# Outputs: ../Results/Fig_DM_{CAPM,FF4}_tv_AccountingOnly_CalendarSE.pdf

rm(list = ls())
pdf(NULL)
source("0_Environment.R")

risk_path <- "../Data/Processed/risk_adjusted_dm_benchmarks.RDS"
if (!file.exists(risk_path)) {
  stop("Missing risk_adjusted_dm_benchmarks.RDS; run 3_Precompute.R first.")
}
risk <- readRDS(risk_path)
if (!identical(risk$metadata$schema_version, 2L)) {
  stop("Accounting-only alpha plots require risk benchmark schema version 2.")
}

incl_signals <- restrictInclSignals(
  restrictType = globalSettings$restrictType,
  topT = globalSettings$topT
)
accounting_signals <- readRDS("../Data/Processed/czsum_allpredictors.RDS") %>%
  left_join(
    fread("../Data/Raw/SignalDoc.csv") %>%
      transmute(
        Acronym, Cat.Data, Cat.Form,
        Def = tolower(`Detailed Definition`)
      ),
    by = c("signalname" = "Acronym")
  ) %>%
  filter(signalname %in% incl_signals, Cat.Data == "Accounting") %>%
  mutate(
    drop = grepl("quarter", Def) |
      grepl("analyst|meanest|earningssurprise", paste(tolower(signalname), Def)) |
      Cat.Form == "discrete" |
      signalname %in% c("ShareIss1Y", "ShareIss5Y")
  ) %>%
  filter(!drop) %>%
  pull(signalname)
theory <- fread("DataInput/SignalsTheoryChecked.csv") %>%
  select(signalname, theory)

render_model <- function(model, label, output_file) {
  plot_data <- risk[[model]]$panel %>%
    filter(pubname %in% accounting_signals) %>%
    left_join(theory, by = c("pubname" = "signalname")) %>%
    transmute(
      pubname, eventDate, calendarDate,
      ret = published_return, matchRet = dm_return, theory
    )
  if (n_distinct(plot_data$pubname) == 0L) {
    stop("No accounting predictors remain in the ", label, " benchmark.")
  }
  plot <- ReturnPlotsWithDM_std_errors_indicators(
    dt = plot_data,
    basepath = "../Results/Fig_DM",
    suffix = paste0(label, "_AccountingOnly_CalendarSE"),
    rollmonths = 60,
    colors = colors,
    labelmatch = FALSE,
    yl = 0, yh = 125, xl = -360, xh = 300,
    legendlabels = c(
      paste0("Pub Compustat Annual (", label, " Alpha)"),
      paste0("Data-Mined for |t|>2.0 (", label, " Alpha)"),
      "N/A"
    ),
    legendpos = c(42, 20) / 100,
    fontsize = 28,
    yaxislab = paste0("Trailing 5-Year ", label, " Alpha (bps pm)"),
    linesize = 1.5
  ) + theme(
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.3),
    legend.margin = margin(-0.7, 0.5, 0.5, 0.5, "cm"),
    legend.position = c(44, 15) / 100,
    legend.spacing.y = unit(0.2, "cm")
  ) + guides(color = guide_legend(byrow = TRUE))
  ggsave(output_file, plot, width = 10, height = 8)
  message("Wrote ", output_file)
}

render_model(
  "capm", "CAPM", "../Results/Fig_DM_CAPM_tv_AccountingOnly_CalendarSE.pdf"
)
render_model(
  "ff4", "FF4", "../Results/Fig_DM_FF4_tv_AccountingOnly_CalendarSE.pdf"
)
