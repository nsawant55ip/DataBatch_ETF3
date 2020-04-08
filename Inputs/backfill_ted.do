clear
set more off
set mem 500m

capture log using "C:\\DataBatch_ETF_NewProject\\Logs\\backfill_ted.log", text replace

insheet using C:\DataBatch_ETF_NewProject\Output\bbg_etfs_daily.csv, clear
keep date pted 
rename date dates
gen date = date(dates,"mdy")
sort date
drop dates
save C:\DataBatch_ETF_NewProject\Output\temp2.dta, replace

insheet using C:\DataBatch_ETF_NewProject\Inputs\TEDRATE.csv, clear
rename date dates
gen date = date(dates, "ymd")
drop dates
rename tedrate pted
replace pted = pted*100
sort date
merge date using C:\DataBatch_ETF_NewProject\Output\temp2.dta, update replace
drop _merge
order date pted
sort date 
format date %dN/D/CY
outsheet using C:\DataBatch_ETF_NewProject\Output\tedrate_daily.csv, comma replace

carryforward pted, replace
gen month = month(date)
gen year = year(date)
gen day = day(date)
gen monthnumber = (year - 1980)*12 + month
sort monthnumber day
bys monthnumber : keep if _n == _N
sort monthnumber
drop day year month  
format date %dN/D/CY
erase C:\DataBatch_ETF_NewProject\Output\temp2.dta
order date monthnumber pted
outsheet using C:\DataBatch_ETF_NewProject\Output\tedrate.csv, comma replace
capture log close
