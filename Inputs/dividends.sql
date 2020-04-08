--This version 8. Latest copy
--Last updated: 5/09/2017
--Originally Developed By: Contractor, Saudamini, Devesh, 
--3/20/2017: addition of static list ( ada.dbo.HistoricalDividends ), calculating divamount & close prices separately (Avinash)
--5/09/2017: CUSIP IN ('06742A75','09348R10','09348R20','09348R30','09348R40') id`s added to static table
--WHAT IS THIS QUERY DOING ?
--for the universal set of securities, we are calculating the monthly div-amount and close prices.
---the close price is the price on the max(date) price for that month 
---the div_amount is sum(over) all div_amount for that month




SET NOCOUNT ON;
use qai;

DECLARE @STARTDATE AS SMALLDATETIME;

SET @STARTDATE = '1/1/1997';

--part1
--#####################################################################################################################
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
			AND (I.TYPECODE = 'ET' OR I.TYPECODE = 'ETN') -- OR I.TYPECODE='PREF')-- FOR ETF IN DS-- FOR ETF IN DS
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
			AND I.SECTYPE = 'F' -- OR I.SECTYPE = 'I' OR I.SECTYPE='X' OR I.SECTYPE='P')
			--AND I.SECTYPE = 'F'   -- FOR ETF IN IDC
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
		AND (SECTYPE = 'ET' OR SECTYPE IS NULL OR SECTYPE = 'ETN') -- OR  SECTYPE = 'PREF') 
 --AND ID = '00162Q74'
ORDER  BY ID;

DROP TABLE #DS_UNIV;
DROP TABLE #IDC_UNIV;

--getting the distinct mqaids
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
		AND HIST_CHG.EXCHANGE IN ('A', 'B', 'F', 'E', 'S') --,'%')
LEFT JOIN
	PRC.PRCSCCHG CHG_EXCH_START
		ON RSU.VENCODE = CHG_EXCH_START.CODE
		AND '1/1/1999' BETWEEN CHG_EXCH_START.STARTDATE AND ISNULL(CHG_EXCH_START.ENDDATE, '2079-06-05')
		AND CHG_EXCH_START.EXCHANGE IN ('A', 'B', 'F', 'E','S') --,'%')
 WHERE
	HIST_CHG.EXCHANGE IS NOT NULL
	OR
	CHG_EXCH_START.EXCHANGE IS NOT NULL
ORDER BY
	RSU.ID;

DROP TABLE #RSUSUniverse_B;


--#####################################################################################################################
--part2-STATIC MQAID LIST
SELECT ID,VENCODE,CUSIP,ISIN,NAME INTO #RS_US_UNIV_STOCK1static from  ada.dbo.HistoricalDividends

--######################################################################################################
--part3
--combining usual output with static output
--insert into #tblALLMQAIDS values(46428852)

select * into #tblALLMQAIDS from #RS_US_UNIV_STOCK1
UNION 
select * from #RS_US_UNIV_STOCK1static;
--WHERE  #RS_US_UNIV_STOCK1.ID NOT IN #RS_US_UNIV_STOCK1static.ID 


--select * FROM #RS_US_UNIV_STOCK1static
----select * FROM #RS_US_UNIV_STOCK1
--select * FROM #tblALLMQAIDS
--WHERE ID IN ('73935X22','46428852','67066Y50','26922E10','26922E20','26922E30','26922E40')
--order by ID;

--######################################################################################################

--WITH MTH_DATES AS ( -- FETCH DATE RANGE TO HELP FIND MONTH ENDS
--      SELECT * FROM (  
--      SELECT DATE_ , LEAD(DATE_,1) OVER (ORDER BY DATE_) AS NXTDT  
--      FROM SDDATES_V A WHERE A.CODE='283'  
--      ) T 
--)
---for close-price , the factor is corresponding to the value where 
--P.date(max within exdate & monthed) falls between start & end date 
 
SELECT 
    S.ID AS mqaid,
    I.TICKER AS tick,
    FORMAT(D.EXDATE, 'MM/dd/yyyy') AS divExDate,
	FORMAT(D.RECDATE, 'MM/dd/yyyy') AS Recdate,
    FORMAT(D.ANNDATE, 'MM/dd/yyyy') AS Anndate,
    --ROUND(D.RATE*A.FACTOR,5) AS DIdiv_amountV_AMT,
    ROUND(P.CLOSE_*A.FACTOR,5) AS monthclose,
	FORMAT(P.Date_, 'MM/dd/yyyy') AS maxpdatetobewithinrange,
	FORMAT(A.StartDate, 'MM/dd/yyyy') AS Astartdt,
	FORMAT(A.EndDate, 'MM/dd/yyyy') AS Aenddt,
	P.Close_ as MyClose,
	A.FACTOR as MyFactor,
	A.*,
	D.RECDATE as recdatecopy,
	D.ANNDATE as anndatecopy
INTO #tblALLMQAIDSclose
FROM #tblALLMQAIDS S 
   LEFT JOIN PRC.PRCINFO I 
        ON I.CODE = S.VENCODE
   LEFT JOIN PRC.PRCDIV D 
        ON D.CODE = I.CODE
		AND D.DivType = 1
   LEFT JOIN PRC.PRCDLY P 
        ON P.CODE = I.CODE
       	AND P.DATE_ = (SELECT MAX(DATE_) FROM PRC.PRCDLY WHERE CODE = I.CODE AND DATE_ <= EOMONTH(D.EXDATE))  
   LEFT JOIN PRC.PRCADJ A 
        ON A.CODE = D.CODE 
        AND A.ADJTYPE = 1
		AND P.Date_ BETWEEN A.STARTDATE AND ISNULL(A.ENDDATE,GETDATE())   -- IDC Time Series
	--WHERE S.ID IN ('73935X22','46428852','67066Y50','26922E10','26922E20','26922E30','26922E40')
	ORDER BY S.ID;


---for div-amount, the factor is corresponding the ExDate BETWEEN A.STARTDATE AND ISNULL(A.ENDDATE,GETDATE()) 
SELECT 
    S.ID AS mqaid,
    I.TICKER AS tick,
    FORMAT(D.EXDATE, 'MM/dd/yyyy') AS divExDate,
	FORMAT(D.RECDATE, 'MM/dd/yyyy') AS Recdate,
    FORMAT(D.ANNDATE, 'MM/dd/yyyy') AS Anndate,
    ROUND(D.RATE*A.FACTOR,5) AS DIdiv_amountV_AMT,
	EOMONTH(D.EXDATE) as divexmonthenddate,
    FORMAT(A.StartDate, 'MM/dd/yyyy') AS Astartdt,
	FORMAT(A.EndDate, 'MM/dd/yyyy') AS Aenddt,
	A.FACTOR as MyFactor,
	A.*
INTO #tblALLMQAIDSdivamount
FROM #tblALLMQAIDS S 
   LEFT JOIN PRC.PRCINFO I 
        ON I.CODE = S.VENCODE
   LEFT JOIN PRC.PRCDIV D 
        ON D.CODE = I.CODE
		AND D.DivType = 1
   LEFT JOIN PRC.PRCADJ A 
        ON A.CODE = D.CODE 
        AND A.ADJTYPE = 1
		AND D.ExDate BETWEEN A.STARTDATE AND ISNULL(A.ENDDATE,GETDATE())   -- IDC Time Series
		--WHERE S.ID IN ('73935X22','46428852','67066Y50','26922E10','26922E20','26922E30','26922E40')
	ORDER BY S.ID;

--############################################################################################################################
--select * from #tblALLMQAIDSclose  where mqaid='00162Q67'
--select * from #tblALLMQAIDSdivamount  where mqaid='00162Q67'
----sum(div_amount) for same date wherever possible and retain max(monthclose value)
Select 
c.mqaid ,
c.tick ,
c.divExDate ,
MAX(c.recdatecopy) over (partition by mqaid,divExDate) as Recdate,
MAX(c.anndatecopy) over (partition by mqaid,divExDate) as Anndate,
--SUM(DIdiv_amountV_AMT) over (partition by mqaid,divExDate) as div_amount,
MAX(c.monthclose) over (partition by mqaid,divExDate) as monthclose
INTO #tblALLMQAIDS3
FROm #tblALLMQAIDSclose C
--WHERE C.mqaid IN ('73935X22','46428852','67066Y50','26922E10','26922E20','26922E30','26922E40')


----sum & max operations above, duplicates the record wherever similar mqaid and divexdate occur. we just select distinct records here.
select distinct * into #tblALLMQAIDS31 from #tblALLMQAIDS3;

--select * from #tblALLMQAIDS31 WHERE mqaid IN ('73935X22','46428852','67066Y50','26922E10','26922E20','26922E30','26922E40')
----sum(div_amount) for same date wherever possible and retain max(monthclose value)
Select 
mqaid,
tick,
divExDate,
--MAX(Recdate) as Recdate,
--MAX(Anndate) as Anndate,
SUM(DIdiv_amountV_AMT) over (partition by mqaid,divExDate) as div_amount
INTO #tblALLMQAIDS4
FROm #tblALLMQAIDSdivamount

----sum & max operations above, duplicates the record wherever similar mqaid and divexdate occur. we just select distinct records here.
select distinct * into #tblALLMQAIDS41 from #tblALLMQAIDS4;

--select * from #tblALLMQAIDS31 WHERE mqaid IN ('73935X22','46428852','67066Y50','26922E10','26922E20','26922E30','26922E40')
--select * from #tblALLMQAIDS41 WHERE mqaid IN ('73935X22','46428852','67066Y50','26922E10','26922E20','26922E30','26922E40')
--select * from #tblALLMQAIDS41 WHERE mqaid IN ('73935X22','46428852','67066Y50','26922E10','26922E20','26922E30','26922E40')
--

--############################################################################################################################
--final output
select 
C.mqaid,
C.tick,
C.divExDate,
format(C.Recdate,'MM/dd/yyyy') as Recdate,
format(C.Anndate,'MM/dd/yyyy') as Anndate,
D.div_amount,
C.monthclose
from
#tblALLMQAIDS31  C 
LEFT JOIN #tblALLMQAIDS41 D
ON C.mqaid = D.mqaid
AND  C.divExDate=D.divExDate
--WHERE C.mqaid IN ('73935X22','46428852','67066Y50','26922E10','26922E20','26922E30','26922E40')
order by C.divExDate; 


--filter out records where max(date from prcdly between EOMONTH(exdate)) is between startdate and enddate
----select * INTO #tblALLMQAIDS3 FROM #tblALLMQAIDS2 where maxdatewithinrangeselected  between StartDate AND EndDate;
--select * INTO #tblALLMQAIDS3 FROM #tblALLMQAIDS2 where divExDate between StartDate AND EndDate;

----sum(div_amount) for same date wherever possible and retain max(monthclose value)
--Select 
--mqaid,
--tick,
--divExDate,
--SUM(DIdiv_amountV_AMT) over (partition by mqaid,divExDate) as div_amount,
--MAX(monthclose) over (partition by mqaid,divExDate) as monthclose,
--MAX(MyRate) over (partition by mqaid,divExDate) as myrate,
--MAX(MyFactor) over (partition by mqaid,divExDate) as myfactor
--INTO #tblALLMQAIDS4
--FROm #tblALLMQAIDS3;

----sum & max operations above, duplicates the record wherever similar mqaid and divexdate occur. we just select distinct records here.
--select distinct * into #tblALLMQAIDS5 from #tblALLMQAIDS4;


--select * from #tblALLMQAIDS5;

----select * from #tblALLMQAIDS2 where mqaid ='00439V10' order by divExDate    
----select * FROM #tblALLMQAIDS5 where mqaid ='00439V10'   

 -- ('25459Y20','25459W29','WMH')  order by divExDate --in ( 'WMH','TTH') 
      --in ( 'WMH','TTH')  
DROP TABLE #RS_US_UNIV_STOCK1static;
DROP TABLE #RS_US_UNIV_STOCK1;
--drop table #tblforamount1
drop table #tblALLMQAIDS;
drop table #tblALLMQAIDSclose;
drop table #tblALLMQAIDSdivamount;
drop table #tblALLMQAIDS3
drop table #tblALLMQAIDS31;
drop table #tblallmqaids4;
drop table #tblallmqaids41;


 
-------following query can be used to fill 
--INSERT INTO ada.dbo.HistoricalDividends
--select * from  ada.dbo.ETFMonthlyPrice
--where CUSIP IN ('06742A75','09348R10','09348R20','09348R30','09348R40')

--select * from  ada.dbo.HistoricalDividends;