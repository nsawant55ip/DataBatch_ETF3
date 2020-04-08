clear
set more off
set mem 500m

capture log using "C:\DataBatch_ETF_NewProject\Logs\make_idx_univ.log", text replace
insheet using C:\DataBatch_ETF_NewProject\Output\US_ETF_IMP_M.csv, clear names
sort mqaid dateoffset
by mqaid: keep if _n==_N
drop if histtick  == ""
*replace tick=histtick if tick=="Null"
gen delisted = 1 if tick == "Null"
replace tick = histtick if tick == "Null"
keep mqaid tick delisted
duplicates drop mqaid tick, force
replace tick = tick+" US Equity"
sort mqaid
outsheet using C:\DataBatch_ETF_NewProject\Output\idx_univ_with_hist.csv, comma replace
drop if delisted == 1
drop delisted
outsheet using C:\DataBatch_ETF_NewProject\Output\idx_univ.csv, comma replace


capture log close
