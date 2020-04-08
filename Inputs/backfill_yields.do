clear
set more off
set mem 500m

capture log using "C:\\DataBatch_ETF_NewProject\\Logs\\backfill_yields.log", text replace
insheet using C:\DataBatch_ETF_NewProject\Output\mqa_yields.csv, clear
gen date = date(dates,"mdy")
gen month = month(date)
gen year = year(date)
gen day = day(date)
sort monthnumber day
bys monthnumber : keep if _n == _N
drop month year day date
sort monthnumber
save C:\DataBatch_ETF_NewProject\Output\temp1.dta
insheet using C:\DataBatch_ETF_NewProject\Inputs\fred_yields.csv, clear
sort monthnumber
merge monthnumber using C:\DataBatch_ETF_NewProject\Output\temp1.dta, update replace
drop _merge monthnumber 
erase C:\DataBatch_ETF_NewProject\Output\temp1.dta
outsheet using C:\DataBatch_ETF_NewProject\Output\yields.csv, comma replace

capture log close