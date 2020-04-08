---this is version 3. latest copy
---last updated: 3/22/2017
---Original Developers:
---Latest updates: Here instead of static table we are directly creating a temp table of NEW ETS and then using union to avoid duplicates

SET NOCOUNT ON;
DECLARE @StartDateUS AS SMALLDATETIME;

DECLARE @SecTable TABLE
(
	[ID] VARCHAR(31),
	SecCode INT,
	VenCode INT
)

SET @StartDateUS = '1/1/1995';

-- Get US Universe MQA IDs
WITH IDC AS(
SELECT
	S.ID,
	S.SecCode,
	M.VenCode,
	P.Date_
FROM
	SECMSTRX S
JOIN
	SECMAPX M
		ON S.SECCODE = M.SECCODE
		AND M.VENTYPE = 1
		AND M.EXCHANGE = 1
		AND S.TYPE_= 1
JOIN
	PRC.PRCINFO I
		ON I.CODE = M.VENCODE
		AND I.SECTYPE IN ('F')
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
					)
)

SELECT
	Id, SecCode, Vencode
INTO
	#RSUSUniverse_H
FROM
	IDC
WHERE
	DATE_ >= @StartDateUS
ORDER  BY Id;


INSERT INTO @SecTable(ID, SecCode, VenCode)
SELECT
	DISTINCT
	RSU.Id AS [ID],
	RSU.SecCode,
	RSU.VenCode
FROM
	#RSUSUniverse_H RSU
JOIN
	PRC.PrcInfo I
		On RSU.VenCode = I.Code
LEFT JOIN
	Prc.PrcScChg HIST_CHG
		ON RSU.VenCode = HIST_CHG.Code
		AND
		HIST_CHG.StartDate >= @StartDateUS
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

DROP TABLE #RSUSUniverse_H;

---Use following query to ADD NEW ETF/ID`s
SELECT
	S.Id,
	S.SecCode,
	M.VenCode
INTO #tempstatic
FROM
	SecMstrX S
JOIN
	SecMapX M
		ON S.SecCode = M.SecCode
		AND VenType = 1
		AND Exchange = 1
WHERE
	S.Id IN (
	'SPY', 'QQQ', 'IWM', 'MDY', 'XLE', 'XLU', 'XLF', 'OIH', 'XLB', '46428719', 'GLD', 'PPH',
	 'DIA', '46428723', 'EWZ', 'RTH', '86330E74', '86330e64', 'SMH', '46428743', '91232N10', 
	'46428745', 'XLY', 'XLP', 'XLV', 'XLI', '86330E78', 'XLK', 'IYR', '46428851', '73936T55',
	'46428Q10', '46428722','26922E10','26922E20','26922E30','26922E40','46428852','67066Y50');

--where CUSIP IN ('06742A75','09348R10','09348R20','09348R30','09348R40')

SELECT * into #secListfinal from @SecTable 
UNION select * from #tempstatic;


SELECT
        S.ID, S.SecCode, S.VenCode, MAX(D.DATE_) AS EndDate, DATEADD(M, -3, MAX(D.Date_)) AS StartDate
INTO
	#RSLastDate_B
FROM
    #secListfinal S
JOIN
    PRC.PRCINFO I
        ON I.CODE = S.VENCODE
JOIN
    PRC.PRCDLY D
        ON I.CODE = D.CODE
        AND D.VOLUME IS NOT NULL
JOIN
    PRC.PRCADJ A
        ON A.CODE = D.CODE
        AND D.DATE_ BETWEEN A.STARTDATE AND A.ENDDATE
        AND A.ADJTYPE = 1
-- WHERE S.ID = 'SPY'
GROUP BY
	S.ID, S.SecCode, S.VenCode
ORDER BY
	S.ID, S.SecCode, S.VenCode;

WITH CTE
AS
(
SELECT
		D.DATE_ AS [Date],
		S.ID AS MQAID,
		I.Ticker,
		-- ISNULL(D.Volume, D1.Volume) AS Volume,
		S.StartDate,
		S.EndDate,
		AVG(ISNULL(D.Volume, D1.Volume)/ A.Factor) OVER (PARTITION BY S.ID)  AS ADV3M
	FROM
		#RSLastDate_B S
	JOIN
		PRC.PRCINFO I
			ON I.CODE = S.VENCODE
	JOIN
		PRC.PRCDLY D
			ON I.CODE = D.CODE
			AND D.VOLUME IS NOT NULL
            AND D.VOLUME >= 0
			AND D.DATE_ >= S.StartDate
	LEFT JOIN
			PRC.PRCDLY D1
			ON D1.CODE = D.CODE
			AND D1.DATE_=
				(
					SELECT
						MAX(DATE_)
					FROM
						PRC.PRCDLY
					WHERE
						CODE = D.CODE
						AND DATE_ < D.DATE_
				)
			AND D.VOLUME IS NOT NULL
            AND D.VOLUME >= 0
	JOIN
		PRC.PRCADJ A 
			ON A.CODE = D.CODE
			AND D.DATE_ BETWEEN A.STARTDATE AND ISNULL(A.ENDDATE, '2079-06-05')
			AND A.ADJTYPE = 1
)
SELECT
	FORMAT(EndDate, 'MM/dd/yyyy') AS [Date],
	MQAID as mqaid,
	Ticker,
	Round(ADV3m,0) as ADV3m
	--Round(ADV3M,3) AS ADV3m3,
	--Round(ADV3M,0) AS ADV3m0,
	--ADV3M as ADV3masis
FROM
	CTE Set1
GROUP BY
	MQAID, EndDate, Ticker, ADV3M
ORDER BY
	EndDate

DROP TABLE #RSLastDate_B;
DROP TABLE #secListfinal;
DROP TABLE #tempstatic;