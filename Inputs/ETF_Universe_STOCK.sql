---This is version 3. Latest copy
----last updated: 1/29/2018
----1/29/2018 :  addition of cusip & vencode to the output
----4/28/2017 updates: renaming Tick & SEDOL to tick & sedol respectively. Shishir demands
----5/8/2017: addition of  CUSIP IN ('06742A75','09348R10','09348R20','09348R30','09348R40') 
---Originally Developed By: Saudamini, Devesh
---major changes made: addition of static list of ID`s from ada.dbo.ETFMonthlyPrice ( By Avinash)


SET NOCOUNT ON;

USE qai;

DECLARE @STARTDATE AS SMALLDATETIME;

SET @STARTDATE = '12/31/1998'; -- 98


--####################PART-1 as is
SELECT  S.ID, S.SECCODE, NULL AS CODE, I.TYPECODE AS SECTYPE, P.MARKETDATE AS MARKETDATE, S.CUSIP, S.ISIN ,S.NAME
INTO #DS_UNIV
FROM SECMSTRX S 
	JOIN SECMAPX M 
		ON S.SECCODE = M.SECCODE  
		AND S.TYPE_ = 1  
		AND M.VENTYPE = 33
	JOIN DS2CTRYQTINFO I 
		ON I.INFOCODE = M.VENCODE
		AND I.REGION = 'US' 
			AND (I.TYPECODE = 'ET' OR I.TYPECODE = 'ETN' OR I.TYPECODE = 'ETF')-- FOR ETF IN DS-- FOR ETF IN DS
	JOIN
		DS2PRIMQTPRC P
			ON P.INFOCODE = I.INFOCODE
			AND P.MARKETDATE = (
								SELECT
									MAX(MARKETDATE)
								FROM
									DS2PRIMQTPRC
								WHERE
									INFOCODE = I.INFOCODE
								);

								

SELECT	S.ID, S.SECCODE, M.VENCODE AS IDC_CODE, P.DATE_ AS MARKETDATE, S.CUSIP, S.ISIN, S.NAME
INTO #IDC_UNIV
FROM SECMSTRX S
	JOIN
		SECMAPX M
			ON S.SECCODE = M.SECCODE
			AND M.VENTYPE = 1 
			AND M.EXCHANGE = 1
			AND S.TYPE_= 1
	LEFT JOIN
		PRC.PRCINFO I
			ON I.CODE = M.VENCODE
			AND I.SECTYPE = 'F'   -- FOR ETF IN IDC
	JOIN
		PRC.PRCDLY P
			ON P.CODE = I.CODE
			AND P.DATE_= (
							SELECT
								MAX(DATE_)
							FROM
								PRC.PRCDLY
							WHERE
								CODE = I.CODE
						);

WITH IDC AS
(
SELECT IDC.ID, IDC.SECCODE, IDC.IDC_CODE AS VCODE, Q.TYPECODE  SECTYPE, IDC.MARKETDATE, IDC.CUSIP , IDC.ISIN, IDC.NAME
	FROM #IDC_UNIV IDC
		LEFT JOIN SECMAPX X
			ON IDC.SECCODE = X.SECCODE
			AND X.VENTYPE = 33 -- DATSTREAM VENDOR TYPE ID is 33.
		LEFT JOIN DS2CTRYQTINFO Q
			ON X.VENCODE = Q.INFOCODE -- WE WANT TO FILTER if SECTYPE is 'ET' in the future
UNION 
SELECT * 
	FROM #DS_UNIV
)
SELECT
	ID, SECCODE, VCODE AS VENCODE, SECTYPE, MARKETDATE, CUSIP, ISIN, NAME
INTO
	#RSUSUNIVERSE_B
FROM
	IDC
WHERE
	MARKETDATE >= '1999-01-01'
		AND SECTYPE = 'ET' OR SECTYPE IS NULL OR SECTYPE = 'ETN' OR SECTYPE = 'ETF' 
  -- AND SecCode = '11022237'
ORDER  BY ID;

DROP TABLE #DS_UNIV;
DROP TABLE #IDC_UNIV;


SELECT
	DISTINCT
	RSU.ID AS [ID],
	RSU.VENCODE,
	RSU.CUSIP,
	RSU.ISIN,
	RSU.NAME
INTO
	#RS_US_UNIV_STOCK1
FROM
	#RSUSUNIVERSE_B RSU
LEFT JOIN
	PRC.PRCINFO I
		ON RSU.VENCODE = I.CODE
LEFT JOIN
	PRC.PRCSCCHG HIST_CHG
		ON RSU.VENCODE = HIST_CHG.CODE
		AND
		HIST_CHG.STARTDATE >= '1/1/1999'
		-- AND HIST_CHG.EXCHANGE IN ('A', 'B', 'F', 'E', 'S')
LEFT JOIN
	PRC.PRCSCCHG CHG_EXCH_START
		ON RSU.VENCODE = CHG_EXCH_START.CODE
		AND '1/1/1999' BETWEEN CHG_EXCH_START.STARTDATE AND ISNULL(CHG_EXCH_START.ENDDATE, '2079-06-05')
		-- AND CHG_EXCH_START.EXCHANGE IN ('A', 'B', 'F', 'E','S')
 WHERE
	HIST_CHG.EXCHANGE IS NOT NULL
	OR
	CHG_EXCH_START.EXCHANGE IS NOT NULL
ORDER BY
	RSU.ID;


DROP TABLE #RSUSUniverse_B;
--select * FROM #RS_US_UNIV_STOCK1

--################################PART-2
--inclusion of static ID`s

SELECT ID,VENCODE,CUSIP,ISIN,NAME INTO #RS_US_UNIV_STOCK1static from ada.dbo.ETFMonthlyPrice
-- #RSUSUniverse_Bstatic;

--#############################################PART-3################################################
--combining usual output with static output
--insert into #tblALLMQAIDS values(46428852)
--select * FROM  #RS_US_UNIV_STOCK1
--select * from #RS_US_UNIV_STOCK1static
select * into #tblALLMQAIDS from #RS_US_UNIV_STOCK1
UNION 
select * from #RS_US_UNIV_STOCK1static;
--WHERE  #RS_US_UNIV_STOCK1.ID NOT IN #RS_US_UNIV_STOCK1static.ID 

--select * FROM #tblALLMQAIDS  
--WHERE  ID IN ('06742A75','09348R10','09348R20','09348R30','09348R40')
--select * FROM #RS_US_UNIV_STOCK1static
--select * FROM #RS_US_UNIV_STOCK1
--select * FROM #tblALLMQAIDS
--drop table #tblALLMQAIDS
--order by ID;

--select * from #tblALLMQAIDS where ID='26922E10'
--select * from #RS_US_UNIV_STOCK1static where ID='26922E10'

DROP TABLE #RS_US_UNIV_STOCK1;
DROP TABLE #RS_US_UNIV_STOCK1static;
--####################################################PART-4#############################################
--final output

WITH CTE 
AS
(
SELECT
	DISTINCT
	D.DATE_,
	S.VenCode,
	S.ISIN,
	S.NAME,
	HIST.Ticker AS histtick,
	DATEDIFF(M, '1980-01-01', D.Date_ ) + 1 As MonthNumber,
	D.CODE,
	ID,
	FORMAT(D.DATE_, 'yyyyMMdd') AS DateOffSet,
	I.Ticker,
	CE.GVKEY,	
	CASE
		WHEN D.Date_ = D2.Date_
			THEN 1
		ELSE 0
	END DFlag,
	CASE
		WHEN ISNULL(CSI.SICH, CSI_I.SICH)  IS NULL OR ISNULL(CSI.SICH, CSI_I.SICH) = -99999
			THEN ISNULL(CSV.SIC, 0)
		ELSE
			ISNULL(CSI.SICH, CSI_I.SICH)
	END AS Dnum,
	I.CurrEx As exch,
	CHG_EXCH_START.Exchange AS Hexch,
	A.Factor as Splitfactor,
	ROUND(H.SHARES / (1000000*A.FACTOR), 5) AS SharesOut,
	I.Sedol,

	SUM(
        CASE 
            WHEN D.VOLUME > 0 THEN D.VOLUME 
        ELSE 
            0 
        END 
        / A.FACTOR ) OVER (PARTITION BY S.ID, Month(D.Date_), YEAR(D.Date_)) AS Volume,

	Round(
		(
			(
				(
					cast(D2.TotRet as decimal(20,8))
					-
					cast(D3.TotRet as decimal(20,8))
				) / cast(D3.TotRet as decimal(20,8))
			) * 100
		), 5
	) As MonRet
	, D2.Close_
	, D2.Close_ * A.Factor AS ADJ,
	d2.totret,
    S.cusip
FROM
	#tblALLMQAIDS S
JOIN
	PRC.PRCINFO I      
		ON I.CODE = S.VENCODE
JOIN
	PRC.PRCDLY D
		ON I.CODE = D.CODE
		-- AND D.VOLUME > 0
LEFT JOIN
	PRC.PRCSHR H
		ON H.CODE = D.CODE
		AND H.DATE_= ( 
						SELECT
							MAX(DATE_)
						FROM
							PRC.PRCSHR
						WHERE
							CODE = H.CODE
							AND DATE_ <= D.DATE_
					 )
JOIN PRC.PRCADJ A 
		ON A.CODE = D.CODE
		AND D.DATE_ BETWEEN A.STARTDATE AND ISNULL(A.ENDDATE, '2076-01-01')
        AND A.ADJTYPE = 1
LEFT JOIN
	PRC.PRCVOL V
		ON V.CODE = I.CODE
		AND V.DATE_ = (
						SELECT
								MAX(DATE_)
						FROM
							PRC.PRCVOL
						WHERE
							CODE = V.CODE
							AND DATE_ <= D.DATE_
					  )
		AND V.BlockVol <> -99999
		AND V.BLOCKVOL IS NOT NULL
LEFT JOIN
	PRC.PRCDLY D2
		ON I.CODE = D2.CODE
		AND D2.Date_ = ( SELECT MAX(Date_) FROM PRC.PRCDLY WHERE Code = D2.Code AND Date_ <= EOMONTH(D.Date_) AND TotRet IS NOT NULL)
LEFT JOIN
	PRC.PRCDLY D3
		ON I.CODE = D3.CODE
		AND D3.Date_ = ( SELECT MAX(Date_) FROM PRC.PRCDLY WHERE Code = D3.Code AND Date_ <= EOMONTH(DATEADD(M, -1, D.Date_)) AND TotRet IS NOT NULL)
LEFT JOIN
	CSVSecurity CE
		ON CE.CUSIP = S.Cusip
		AND EXCNTRY = 'USA'
LEFT JOIN
	CSVCompany CSV
		ON CE.GVKEY = CSV.GVKEY
LEFT JOIN
	CSCoIndustry CSI
		ON CE.GVKEY = CSI.GVKEY
		AND CSI.DATADATE = (
								SELECT
									MAX(DATADATE)
								FROM
									CSCoIndustry
								WHERE
									GVKEY = CSI.GVKEY
									AND DATADATE <= D.Date_
						   )
	AND CSV.COSTAT = 'A'
LEFT JOIN
	CSICoIndustry CSI_I
		ON CE.GVKEY = CSI_I.GVKEY
		AND CSI_I.DATADATE = (
								SELECT
									MAX(DATADATE)
								FROM
									CSICoIndustry
								WHERE
									GVKEY = CSI_I.GVKEY
									AND DATADATE <= D.Date_
						   )
	AND CSV.COSTAT = 'I'
-- SPMI
LEFT JOIN
	CSSecMth SPMI
		ON CE.GVKEY = SPMI.GVKEY
		AND CE.IID = SPMI.IID
		AND SPMI.DATADATE = (
								SELECT
									MAX(DATADATE)
								FROM
									CSSecMth
								WHERE
									GVKEY = SPMI.GVKEY
									AND IID = SPMI.IID
									AND DATADATE <= D.Date_
							)
		AND CSV.COSTAT = 'A'
LEFT JOIN
	CSISecMth SPMI_I
		ON CE.GVKEY = SPMI_I.GVKEY
		AND CE.IID = SPMI_I.IID
		AND SPMI_I.DATADATE = (
								SELECT
									MAX(DATADATE)
								FROM
									CSISecMth
								WHERE
									GVKEY = SPMI_I.GVKEY
									AND IID = SPMI_I.IID
									AND DATADATE <= D.Date_
							)
		AND CSV.COSTAT = 'I'
LEFT JOIN
	Prc.PrcScChg CHG_EXCH_START
		ON S.VenCode = CHG_EXCH_START.Code
		AND D2.Date_ BETWEEN CHG_EXCH_START.StartDate AND ISNULL(CHG_EXCH_START.EndDate, '2079-06-05')
		-- AND CHG_EXCH_START.Exchange IN ('A', 'B', 'F', 'E', 'S')
LEFT JOIN
	Prc.PrcScChg CHG_EXCH_START_PM
		ON S.VenCode = CHG_EXCH_START_PM.Code
		AND D3.Date_ BETWEEN CHG_EXCH_START_PM.StartDate AND ISNULL(CHG_EXCH_START_PM.EndDate, '2079-06-05')
		-- AND CHG_EXCH_START_PM.Exchange IN ('A', 'B', 'F', 'E', 'S')
LEFT JOIN prc.PrcScChg HIST
	ON HIST.CODE = S.VENCODE
	AND D.date_ BETWEEN HIST.STARTDATE AND ISNULL(HIST.ENDDATE, '2079-06-05')
		
WHERE
	D.Date_ >= @STARTDATE

	AND
		(
			CHG_EXCH_START.Exchange IS NOT NULL
			OR
			CHG_EXCH_START_PM.Exchange IS NOT NULL
		)

	--AND ID ='06738G40'
)
SELECT
	FORMAT(DATE_ , 'MM/dd/yyyy') AS Dates,
	MonthNumber AS monthnumber,
	DateOffSet AS dateoffset,
	ID AS mqaid,
	Dnum AS dnum,
	GVKEY AS gvkey,
	ISIN AS isin,
	histtick AS histtick,
	NAME AS name,
	exch AS Exch,
	Hexch AS HExch,
	cast(TotRet as decimal(20,4)) as totret,
	CAST(Close_ AS DECIMAL(20, 5)) AS monthendclose_unadj,
	CAST(ADJ AS DECIMAL(20, 5)) AS monthendclose,
	Splitfactor AS splitfactor,
	CAST(SharesOut AS DECIMAL(30, 5)) AS SharesOut,
	CAST(Volume AS DECIMAL(30, 5)) AS Volume,
	CASE
		WHEN LAG(Hexch) OVER (PARTITION BY ID order by MonthNumber) IS NULL
			THEN NULL
		ELSE
			CAST(MonRet AS DECIMAL(30, 5))
		END AS monreturn,
	ISNULL(Ticker,'Null') AS tick,
    Sedol 
      + RIGHT (10 - (RIGHT (CASE WHEN ASCII(LEFT(SEDOL,1)) > 64 THEN (ASCII(LEFT(SEDOL,1)) - 55 ) *1 ELSE LEFT(SEDOL,1) * 1 END 
      +     CASE WHEN ASCII(SUBSTRING(SEDOL,2,1)) > 64 THEN (ASCII(SUBSTRING(SEDOL,2,1)) - 55 ) *3 ELSE SUBSTRING(SEDOL,2,1) * 3 END 
      +     CASE WHEN ASCII(SUBSTRING(SEDOL,3,1)) > 64 THEN (ASCII(SUBSTRING(SEDOL,3,1)) - 55 ) *1 ELSE SUBSTRING(SEDOL,3,1) * 1 END
      +     CASE WHEN ASCII(SUBSTRING(SEDOL,4,1)) > 64 THEN (ASCII(SUBSTRING(SEDOL,4,1)) - 55 ) *7 ELSE SUBSTRING(SEDOL,4,1) * 7 END
      +     CASE WHEN ASCII(SUBSTRING(SEDOL,5,1)) > 64 THEN (ASCII(SUBSTRING(SEDOL,5,1)) - 55 ) *3 ELSE SUBSTRING(SEDOL,5,1) * 3 END
      +     CASE WHEN ASCII(RIGHT(SEDOL,1)) > 64 THEN (ASCII(RIGHT(SEDOL,1)) - 55 ) *9 ELSE RIGHT(SEDOL,1) * 9 END,1)),1)
    AS sedol,
    cusip AS cusip,
    vencode as vencode
INTO 
	#US_TEMP_TABLE
FROM
	CTE
WHERE
	DFlag = 1
	-- and ID ='06739F10'
ORDER BY
	ID, DATE_;

    
-- Fix that works. We do not want NULL monret for mqaids trading before 229.
SELECT * FROM #US_TEMP_TABLE WHERE monthnumber >= 229
DROP TABLE #US_TEMP_TABLE;
DROP TABLE #tblALLMQAIDS;




----FOLLOWING CODE CAN BE USED TO FILL STATIC TABLE WHENEVER DEEMED NECESSARY

----query to fill static table
----Code to be used to add static ETF`s latest has been to include CUSIP IN ('06742A75','09348R10','09348R20','09348R30','09348R40') 
----check if its getting fetched from DS (ET & ETN filetrs are off)
--SELECT S.ID, S.SECCODE, NULL AS CODE, I.TYPECODE AS SECTYPE, P.MARKETDATE AS MARKETDATE, S.CUSIP, S.ISIN ,S.NAME,M.VENCODE
--INTO #DS_UNIVstatic
--FROM SECMSTRX S 
--	JOIN SECMAPX M 
--		ON S.SECCODE = M.SECCODE  
--		AND S.TYPE_ = 1  
--		AND M.VENTYPE = 33
--	JOIN DS2CTRYQTINFO I 
--		ON I.INFOCODE = M.VENCODE
--		AND I.REGION = 'US' 
--			--AND (I.TYPECODE = 'ET' OR I.TYPECODE = 'ETN')-- FOR ETF IN DS-- FOR ETF IN DS
--	JOIN
--		DS2PRIMQTPRC P
--			ON P.INFOCODE = I.INFOCODE
--			AND P.MARKETDATE = (
--								SELECT
--									MAX(MARKETDATE)
--								FROM
--									DS2PRIMQTPRC
--								WHERE
--									INFOCODE = I.INFOCODE
--								)
--								where CUSIP IN ('06742A75','09348R10','09348R20','09348R30','09348R40') 

----check if its getting fecthed from IDC
--select	s.id, s.seccode, m.vencode as idc_code, p.date_ as marketdate, s.cusip, s.isin, s.name
--into #idc_etfstatic
--from secmstrx s
--	join
--		secmapx m
--			on s.seccode = m.seccode
--			and m.ventype = 1 
--			and m.exchange = 1
--			and s.type_= 1
--	left join
--		prc.prcinfo i
--			on i.code = m.vencode
--			and i.sectype = 'f'   -- for etf in idc
--	join
--		prc.prcdly p
--			on p.code = i.code
--			and p.date_= (
--							select
--								max(date_)
--							from
--								prc.prcdly
--							where
--								code = i.code
--						)
--					where S.CUSIP IN ('06742A75','09348R10','09348R20','09348R30','09348R40') 
					
									
--select * from ada.dbo.ETFMonthlyPrice
--select * from #IDC_ETFstatic
--select * from #DS_UNIVstatic


----FINAL query to insert static ETFs

--Insert into ada.dbo.ETFMonthlyPrice
--select ID,SECCODE,'ETF',CUSIP,ISIN,NAME,VENCODE from #DS_UNIVstatic

--select * from  ada.dbo.ETFMonthlyPrice



