library(RODBC)
dbconn<- odbcDriverConnect('driver={SQL Server};server=ainmg1-vwsql02;database=qai;trusted_connection=true')
df<-sqlQuery(dbconn,"SELECT DISTINCT S.ID as mqaid, PC.Ticker, S.Sedol, S.ISIN,S.CUSIP,S.NAME
FROM SECMSTRX S
	JOIN
		SECMAPX M
			ON S.SECCODE = M.SECCODE
			AND M.VENTYPE = 1 
			AND M.EXCHANGE = 1
			AND S.TYPE_= 1
			AND S.Sedol  IS NOT NULL 
			AND S.Isin IS NOT NULL 
			AND S.Cusip IS NOT NULL 
	JOIN
		PRC.PRCINFO I
			ON I.CODE = M.VENCODE
			AND I.SECTYPE = 'F'  
	JOIN
	    PRC.PrcScChg PC
			ON PC.Code=I.Code
            AND PC.Ticker IS NOT NULL")
write.csv(df,"C:/DataBatch_ETF_NewProject/Output/focusV2_SecurityTableDetails_ETF.csv",row.names=F)
close(dbconn)
