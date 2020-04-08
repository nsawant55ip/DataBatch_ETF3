---This is version 2. Latest copy
---last updated: 5/09/2017
---Originally Developed By: Saudamini, Devesh
---5/09/2017: CUSIP ID`s('06742A75','09348R10','09348R20','09348R30','09348R40') added to the static table ada.dbo.ETFDailyPrice 
---Latest changes made: addition of static list of ID`s from ada.dbo.ETFDailyPrice ( By Avinash)


SET NOCOUNT ON;
use qai;
DECLARE @StartDateUS AS SMALLDATETIME;

DECLARE @Holiday NVARCHAR(200);
DECLARE @StartDate DATE;

SET @StartDateUS ='12/31/1998';
SET @Holiday = 'US_EXCH_HOLIDAY'; -- Unused Variable


--To select the past 3 trading days
SELECT DISTINCT DATE_ INTO #DTE FROM PRC.PRCDLY order by DATE_ DESC
SELECT top 20 DATE_ INTO #T3 FROM #DTE order by DATE_ DESC
SELECT @StartDate =  MIN(DATE_) FROM #T3 
DROP TABLE #DTE
DROP TABLE #T3
print @StartDate


---###########################################################PART-1####################################################
--selecting securities universe as usual

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
		AND (I.TYPECODE = 'ET' OR I.TYPECODE = 'ETN' OR I.TYPECODE = 'ETF') -- OR I.TYPECODE = 'PREF')-- FOR ETF IN DS
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
			AND I.SECTYPE = 'F' -- OR I.SECTYPE = 'I')   -- FOR ETF IN IDC
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
	#RSUSUniverse_B
FROM
	IDC
WHERE
	MARKETDATE >=  '1997-01-01'
	AND (SECTYPE = 'ET' OR SECTYPE IS NULL OR SECTYPE = 'ETN' OR  SECTYPE = 'ETF')  --OR  SECTYPE = 'PREF'

ORDER  BY ID;

DROP TABLE #DS_UNIV;
DROP TABLE #IDC_UNIV;

SELECT
	DISTINCT
	RSU.Id AS [ID],
	--RSU.SecCode,
	RSU.VenCode,
	RSU.CUSIP,
	RSU.ISIN,
	RSU.NAME
INTO
	#RS_US_UNIV_STOCK1
FROM
	#RSUSUniverse_B RSU
JOIN
	PRC.PrcInfo I
		On RSU.VenCode = I.Code
LEFT JOIN
	Prc.PrcScChg HIST_CHG
		ON RSU.VenCode = HIST_CHG.Code
		AND
		HIST_CHG.StartDate >=  @StartDateUS
		AND HIST_CHG.Exchange IN ('A', 'B', 'F','E','S')
LEFT JOIN
	Prc.PrcScChg CHG_EXCH_START
		ON RSU.VenCode = CHG_EXCH_START.Code
		AND @StartDateUS BETWEEN CHG_EXCH_START.StartDate AND ISNULL(CHG_EXCH_START.EndDate, '2079-06-05')
		AND CHG_EXCH_START.Exchange IN ('A', 'B', 'F','E','S')
 WHERE
	HIST_CHG.Exchange IS NOT NULL
	OR
	CHG_EXCH_START.Exchange IS NOT NULL
ORDER BY
	RSU.Id;
DROP TABLE #RSUSUniverse_B;


--###############################################################PART-2###############################################
SELECT ID,VENCODE,CUSIP,ISIN,NAME INTO #RS_US_UNIV_STOCK1static from ada.dbo.ETFDailyPrice;
---############################################################PART-3######################################################################
--combining usual output with static output
--INSERT INTO Ada.dbo.dpricingToBeAddedMQAIDS(mqaid,tick)values('46428852','-');

select * into #tblALLStocks from #RS_US_UNIV_STOCK1
UNION 
select * from #RS_US_UNIV_STOCK1static;

--select * FROM #tblALLStocks  
--WHERE  ID IN ('15350117','46428852','67066Y50','26922E10','26922E20','26922E30','26922E40')

DROP TABLE #RS_US_UNIV_STOCK1;
DROP TABLE #RS_US_UNIV_STOCK1static;

--###########################################################PART-4########################################################
--nothing changed in this part.

WITH CTE_D AS
(
SELECT  D.Date_ AS Date_, ROW_NUMBER() OVER (ORDER By DATE_ DESC) AS RN
                FROM            SDEXCHINFO_V E
                JOIN            SDDATES_V D
                ON      D.ExchCode = D.ExchCode
                AND     (E.ExchCode = 16 OR E.ExchCode = 439 OR E.ExchCode = 657 OR E.ExchCode = 1463 OR E.ExchCode = 381)
                JOIN            SDINFO_V I
                ON      I.CODE = D.CODE
                AND     D.Code = 121
                WHERE   Date_ Not IN (SELECT D.DATE_
                                                FROM            SDEXCHINFO_V E
                                                JOIN            SDDATES_V D
                                                        ON      E.EXCHCODE = D.EXCHCODE
                                                        AND     E.EXCHCODE IN (SELECT DISTINCT EXCHCODE FROM SDDATES_V)         -- THIS LIMITS THE DATA TO WHAT TQA SOURCES FROM THE VENDOR
                                                        --AND     E.EXCHCODE = 16
														AND   (E.ExchCode = 16 OR E.ExchCode = 439 OR E.ExchCode = 657 OR E.ExchCode = 1463 OR E.ExchCode = 381)    --NYSE Market                                                              -- USE SPECIFIC EXCHCODE TO FILTER BY EXCHANGE
                                                JOIN            SDINFO_V I
                                                        ON      I.CODE = D.CODE
                                                        AND     I.CODE != 289)                                                                  -- GETTING RID OF WEEKENDS
						AND Date_ < FORMAT(GETDATE(), 'MM/dd/yyyy')
)
--SELECT @StartDate = Date_ FROM CTE_D  WHERE RN = 5

SELECT
     --   S.ID, S.SecCode, S.VenCode, Min(D.DATE_) AS FirstDate
		   S.ID,  S.VenCode, Min(D.DATE_) AS FirstDate
INTO
	#RSFirstDate_A
FROM
        #tblALLStocks S
JOIN
        PRC.PRCINFO I
                ON I.CODE = S.VENCODE
JOIN
        PRC.PRCDLY D
                ON I.CODE = D.CODE
                --AND D.VOLUME IS NOT NULL
JOIN
        PRC.PRCADJ A
                ON A.CODE = D.CODE
                AND D.DATE_ BETWEEN A.STARTDATE AND ISNULL(A.ENDDATE, '2079-06-05')
        AND A.ADJTYPE = 1
GROUP BY
	--S.ID, S.SecCode, S.VenCode
		S.ID,  S.VenCode
ORDER BY
	--S.ID, S.SecCode, S.VenCode
	S.ID,  S.VenCode

--select * from #RSFirstDate_A



-- Update First Date if less than '1/1/1997'
UPDATE
	#RSFirstDate_A
SET
	FirstDate = '1/1/1997'
WHERE
	FirstDate < '1/1/1997';


----------------OUTPUT matches that of MONTHLY query



WITH CTE_ADJ AS
(
SELECT
		D1.Close_ * A.FACTOR AS AdjustedFactor,
		D1.Close_ AS UnAdjustedFactor,
		A.Factor AS SplitFactor,
		S.Id,
		--S.SecCode,
		S.VenCode,
		S.FirstDate
FROM
		#RSFirstDate_A S
JOIN
	PRC.PRCINFO I      
		ON I.CODE = S.VENCODE
LEFT JOIN
	PRC.PRCDLY D1
		ON D1.Code = I.Code
		AND D1.Date_ = (
							SELECT 
								MAX(Date_)
							FROM
								PRC.PRCDLY
							WHERE
								Code = D1.Code
							AND
								Date_ <= @StartDate
						)
LEFT JOIN
	PRC.PRCADJ A 
		ON A.CODE = I.CODE
		AND D1.Date_ BETWEEN A.STARTDATE AND ISNULL(A.ENDDATE, '2079-06-05')
		AND A.ADJTYPE = 1
)
SELECT
	Set1.*,
	CASE
		WHEN((Set1.AdjustedFactor <> Set1.UnAdjustedFactor) OR Set1.SplitFactor <> 1)
		THEN
			Set1.FirstDate
	ELSE
		@StartDate
	END AS BeginDate
INTO
	#RSAdj_A
FROM
	CTE_ADJ Set1
ORDER BY ID;

DROP TABLE #RSFirstDate_A;

WITH CTE 
AS
(
	SELECT
		D.DATE_  ,
		S.Id AS MQAID,
		D.CODE,
		I.CurrEx AS exch,
		CHG.Exchange hexch,
		Round(D4.Close_ * A.FACTOR, 5) AS [Close],
		D1.TotRet,
		D3.TotRet AS PreviousTotRet,

		(ISNULL(D.VOLUME, D2.Volume) /A.FACTOR) AS Volume,
		Round((H.Shares / 1000000) / A.FACTOR , 5) AS Shares,
		A.Factor
	FROM
		#RSAdj_A S
	LEFT JOIN
		PRC.PRCINFO I      
			ON I.CODE = S.VENCODE
	LEFT JOIN
		PRC.PRCDLY D
			ON I.CODE = D.CODE
			AND D.DATE_ >= S.BeginDate
	LEFT JOIN
		PRC.PRCDLY D1
			ON I.CODE = D1.CODE
			AND D1.Date_ = (
								SELECT
									MAX(Date_)
								FROM
									PRC.PRCDLY
								WHERE
									Code = D1.Code
									AND Date_ <= D.Date_
									--AND D.TotRet IS NOT NULL
							)
	LEFT JOIN
		PRC.PRCDLY D2
			ON I.CODE = D2.CODE
			AND D2.Date_ = (
								SELECT
									MAX(Date_)
								FROM
									PRC.PRCDLY
								WHERE
									Code = D2.Code
									AND Date_ <= D.Date_
									--AND Volume IS NOT NULL
							)
	LEFT JOIN
		PRC.PRCDLY D3
			ON I.CODE = D3.CODE
			AND D3.Date_ = (
								SELECT
									MAX(Date_)
								FROM
									PRC.PRCDLY
								WHERE
									Code = D3.Code
									AND Date_ < D.Date_
									--AND D.TotRet IS NOT NULL
							)
	LEFT JOIN
		PRC.PRCDLY D4
			ON I.CODE = D4.CODE
			AND D4.Date_ = (
								SELECT
									MAX(Date_)
								FROM
									PRC.PRCDLY
								WHERE
									Code = D4.Code
									AND Date_ <= D.Date_
									--AND Close_ IS NOT NULL
							)
	LEFT JOIN
			PRC.PRCSHR H
			ON H.CODE = D.CODE
			AND H.DATE_=
				(
					SELECT
						MAX(DATE_)
					FROM
						PRC.PRCSHR
					WHERE
						CODE = H.CODE
						AND DATE_ <= D.DATE_
				)
	JOIN
		PRC.PRCADJ A 
			ON A.CODE = D.CODE
			AND D.DATE_ BETWEEN A.STARTDATE AND ISNULL(A.ENDDATE, '2079-06-05')
			AND A.ADJTYPE = 1
	LEFT JOIN
		PRC.PRCSCCHG CHG
			ON CHG.CODE = D.CODE
			AND D.DATE_ BETWEEN CHG.STARTDATE AND ISNULL(CHG.ENDDATE, '2079-06-05')

--WHERE
--ID ='02072L30'
)
SELECT DISTINCT
	FORMAT(DATE_, 'MM/dd/yyyy') AS [Date],
	MQAID,
	exch,
	hexch,
	totret,
	cast([close] AS DECIMAL(20,5)) AS [CLOSE],
	CAST(
		(
			(
				(
					cast(Set1.TotRet as decimal(20,8))
					-
					cast(Set1.PreviousTotRet as decimal(20,8))
				) / cast(Set1.PreviousTotRet as decimal(20,8))
			) * 100
		) AS DECIMAL(20,5)
	) As ret,
	cast(volume as decimal(20,5)) as volume,
	CAST(Shares AS DECIMAL(20,5)) AS shrout
FROM
	CTE Set1
ORDER BY
	MQAID, [Date]

DROP TABLE #RSAdj_A;




----
----FOLLOWING CODE CAN BE USED TO FILL STATIC TABLE WHENEVER DEEMED NECESSARY

----query to fill static table

--select * from  ada.dbo.ETFDailyPrice 
--select * from  ada.dbo.ETFMonthlyPrice
--where CUSIP IN ('06742A75','09348R10','09348R20','09348R30','09348R40')