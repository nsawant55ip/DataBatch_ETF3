# Set up code
rm(list=ls(all=TRUE))
gc()


setwd("C:/DataBatch_ETF_NewProject/Output")

url_list = c("yld_3m"  = "http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_3M&type=csv",
             "yld_1y"  = "http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_1Y&type=csv",
             "yld_2y"  = "http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y&type=csv",
             "yld_5y"  = "http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y&type=csv",
             "yld_7y"  = "http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_7Y&type=csv",
             "yld_10y" = "http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y&type=csv",
             "yld_20y" = "http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_20Y&type=csv",
             "yld_30y" = "http://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=165.YC.B.U2.EUR.4F.G_N_A.SV_C_YM.SR_30Y&type=csv" )

for (url in names(url_list)){
    download.file(url_list[url], destfile = paste("Euro_yields_",url, ".csv", sep = ""), mode = "wb", quiet = T)
}

for (url in names(url_list)){
    temp_table <- read.csv(paste("Euro_yields_",url, ".csv", sep = ""), skip = 5, header = FALSE, stringsAsFactors = F)
    colnames(temp_table) = c("dates", url)
    temp_table$dates = as.Date(temp_table$dates)
    temp_table[,2] = temp_table[,2]/100
    if (! exists("euro_yields_dat")){
        euro_yields_dat = temp_table
    }else{
        euro_yields_dat = merge(euro_yields_dat, temp_table, by = "dates", all = T)
    }
}

euro_yields_dat = euro_yields_dat[order(euro_yields_dat$dates),]
write.csv(euro_yields_dat, "yields_eafe.csv", row.names = F)

for (url in names(url_list)){
    file.remove(paste("Euro_yields_",url, ".csv", sep = ""))
}
