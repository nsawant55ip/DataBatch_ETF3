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

								

SELECT	S.ID, S.SECCODE, M.VENCODE, P.DATE_, S.CUSIP, S.ISIN, S.NAME
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
			AND I.SECTYPE = 'F'   -- FOR ETF and traded CEF IN IDC
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

-- Select items that are not in DS Universe as the ETF stock query considers the DS Universe as the ETF Universe
-- whereas IDC treats CEF and ETF in the same category. 
select * into #RSUSUniverse_B
from #IDC_UNIV where id NOT IN (select id from #DS_UNIV)


DROP TABLE #DS_UNIV;
DROP TABLE #IDC_UNIV;


SELECT
	DISTINCT
	RSU.Id AS [ID],
	RSU.VenCode,
	RSU.Cusip,
	RSU.ISIN,
	RSU.Name
INTO
	#RS_US_UNIV_STOCK1
FROM
	#RSUSUniverse_B RSU
JOIN
	PRC.PrcInfo I
		On RSU.VenCode = I.Code
LEFT JOIN
	PrcCode2 CTypeDesc
		ON ASCII(I.SecType) = CTypeDesc.Code AND Type_ = 1
LEFT JOIN
	Prc.PrcScChg HIST_CHG
		ON RSU.VenCode = HIST_CHG.Code
		AND
		HIST_CHG.StartDate >= '1/1/1995'
		AND HIST_CHG.Exchange IN ('A', 'B', 'F')
LEFT JOIN
	Prc.PrcScChg CHG_EXCH_START
		ON RSU.VenCode = CHG_EXCH_START.Code
		AND '1/1/1995' BETWEEN CHG_EXCH_START.StartDate AND ISNULL(CHG_EXCH_START.EndDate, '2079-06-05')
		AND CHG_EXCH_START.Exchange IN ('A', 'B', 'F')
 WHERE
	HIST_CHG.Exchange IS NOT NULL
	OR
	CHG_EXCH_START.Exchange IS NOT NULL
ORDER BY
	RSU.Id;

DROP TABLE #RSUSUniverse_B;

WITH CTE 
AS
(
SELECT
	DISTINCT
	D.DATE_,
	S.VenCode,
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
	ROUND(H.SHARES / (1000000*A.FACTOR), 6) AS SharesOut,
	I.Sedol,

	SUM(
        CASE 
            WHEN D.VOLUME > 0 THEN D.VOLUME 
        ELSE 
            0 
        END 
        / A.FACTOR ) OVER (PARTITION BY S.ID, Month(D.Date_), YEAR(D.Date_)) AS Volume,
	SUM(V.BLOCKVOL / A.FACTOR) OVER (PARTITION BY S.ID, Month(D.Date_), YEAR(D.Date_)) AS BlockVol,

	Round(
		(
			(
				(
					D2.TotRet
					-
					D3.TotRet
				) / D3.TotRet
			) * 100
		), 6
	) As MonRet
	, D2.Close_
	, D2.Close_ * A.Factor AS ADJ,
	CASE
		WHEN ISNULL(SPMI.SPMIM, SPMI_I.SPMIM) = 92
			THEN 1
		WHEN ISNULL(SPMI.SPMIM, SPMI_I.SPMIM) = 91
			THEN 0
		WHEN ISNULL(SPMI.SPMIM, SPMI_I.SPMIM) = 10
			THEN -1
		ELSE
				NULL
	END AS ffsnpsizeindic,
	S.Cusip As Cusip,
	S.ISIN As isin,
	S.Name As name,
    S.ID as nyse_tick
FROM
	#RS_US_UNIV_STOCK1 S
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
		AND CHG_EXCH_START.Exchange IN ('A', 'B', 'F')
LEFT JOIN
	Prc.PrcScChg CHG_EXCH_START_PM
		ON S.VenCode = CHG_EXCH_START_PM.Code
		AND D3.Date_ BETWEEN CHG_EXCH_START_PM.StartDate AND ISNULL(CHG_EXCH_START_PM.EndDate, '2079-06-05')
		AND CHG_EXCH_START_PM.Exchange IN ('A', 'B', 'F')
WHERE
	D.Date_ >= @StartDate
	AND
		(
			CHG_EXCH_START.Exchange IS NOT NULL
			OR
			CHG_EXCH_START_PM.Exchange IS NOT NULL
		)
)
SELECT
	FORMAT(DATE_ , 'MM/dd/yyyy') AS Dates,
	MonthNumber AS startmonth,
	DateOffSet AS dateoffset,
	ID AS mqaid,
	Dnum AS dnum,
	GVKEY AS gvkey,
	exch AS Exch,
	Hexch AS HExch,
	ROUND(Close_ , 6) AS monthendclose_unadj,
	ROUND(ADJ, 6) AS monthendclose,
	ROUND(SharesOut , 6) AS SharesOut,
	ROUND(Volume , 6) AS Volume,
	CASE
		WHEN LAG(Hexch) OVER (PARTITION BY ID order by MonthNumber) IS NULL
			THEN NULL
		ELSE
			ROUND(MonRet, 6)
		END AS monreturn,
	ISNULL(Ticker,'Null') AS Tick,
    Sedol 
      + RIGHT (10 - (RIGHT (CASE WHEN ASCII(LEFT(SEDOL,1)) > 64 THEN (ASCII(LEFT(SEDOL,1)) - 55 ) *1 ELSE LEFT(SEDOL,1) * 1 END 
      +     CASE WHEN ASCII(SUBSTRING(SEDOL,2,1)) > 64 THEN (ASCII(SUBSTRING(SEDOL,2,1)) - 55 ) *3 ELSE SUBSTRING(SEDOL,2,1) * 3 END 
      +     CASE WHEN ASCII(SUBSTRING(SEDOL,3,1)) > 64 THEN (ASCII(SUBSTRING(SEDOL,3,1)) - 55 ) *1 ELSE SUBSTRING(SEDOL,3,1) * 1 END
      +     CASE WHEN ASCII(SUBSTRING(SEDOL,4,1)) > 64 THEN (ASCII(SUBSTRING(SEDOL,4,1)) - 55 ) *7 ELSE SUBSTRING(SEDOL,4,1) * 7 END
      +     CASE WHEN ASCII(SUBSTRING(SEDOL,5,1)) > 64 THEN (ASCII(SUBSTRING(SEDOL,5,1)) - 55 ) *3 ELSE SUBSTRING(SEDOL,5,1) * 3 END
      +     CASE WHEN ASCII(RIGHT(SEDOL,1)) > 64 THEN (ASCII(RIGHT(SEDOL,1)) - 55 ) *9 ELSE RIGHT(SEDOL,1) * 9 END,1)),1)
    AS SEDOL,
	BlockVol AS BlockVol,
	ffsnpsizeindic,
	VenCode As Vencode,
	Cusip As Cusip,
	isin As isin,
	name As name,
    nyse_tick as NYSE_TICK
INTO 
	#US_TEMP_TABLE
FROM
	CTE
WHERE
	DFlag = 1
ORDER BY
	ID, DATE_;

    
-- Fix that works. We do not want NULL monret for mqaids trading before 229.
SELECT * FROM #US_TEMP_TABLE WHERE startmonth >= 229;


DROP TABLE #RS_US_UNIV_STOCK1;
DROP TABLE #US_TEMP_TABLE;
