--Yields Daily Version 2. This is latst version
--Last Updated=1/1/2018
---code to carry forward last value as -is till the trade-date if no value found implemented.
--Originally Developed By: Avinash
---WHAT IS THIS QUERY DOING ?
---Purpose is to fetch daily Close price & TotRet (which is based on close price) for SPOT category only.
---In cases where we have no prices or totret available on a specific trading date, previous values are pulled down with the help of using cursors.---Category-1-IDNAMES:'@MSCIS167', '@UTDKI43' 
---Category:('TU_FUT0', 'TU_FUT1', 'FV_FUT0', 'FV_FUT1', 'TY_FUT0', 'TY_FUT1', 'ED_FUT0', 'ED_FUT1', 'CL_FUT0')


SET NOCOUNT ON;

DECLARE @MarketQAId NVARCHAR(31);
DECLARE @StartDate DATE;
DECLARE @PriorTradingDate DATE;

SET @MarketQAId = 'SPX_IDX';
SET @StartDate = '04/01/1972' --m/d/yyyy

--DROP TABLE #RS5

DECLARE @TradingDates TABLE
(
	Date_ SMALLDATETIME NOT NULL
);


--extract market trading dates
INSERT INTO @TradingDates
SELECT
	Date_
FROM
	IdxDaily D
JOIN
	IdxInfo I
    ON D.Code = I.Code
WHERE
	I.Ticker = 'SPX_IDX'
	AND Date_ >= @StartDate;

----finding last rading date from the given startdate
SELECT
	@PriorTradingDate = Max(Date_)
FROM
	IdxDaily D
JOIN
	IdxInfo I
    ON D.Code = I.Code
WHERE
	I.Ticker = 'SPX_IDX'
	AND Date_ < @StartDate;

--following table will be used everywhere
SELECT * INTO #Tradedates FROM @TradingDates;


DECLARE @FinalOutput TABLE
(
	Date_ SMALLDATETIME NOT NULL,
	MonthNumber INT NOT NULL,
	YLD_3M VARCHAR(31) NULL DEFAULT '.', YLD_6M VARCHAR(31) NULL DEFAULT '.',
	YLD_1Y VARCHAR(31) NULL DEFAULT '.', YLD_2Y VARCHAR(31) NULL DEFAULT '.',
	YLD_3Y VARCHAR(31) NULL DEFAULT '.', YLD_5Y VARCHAR(31) NULL DEFAULT '.',
	YLD_7Y VARCHAR(31) NULL DEFAULT '.', YLD_10Y VARCHAR(31) NULL DEFAULT '.',
	YLD_30Y VARCHAR(31) NULL DEFAULT '.' 

);

INSERT INTO @FinalOutput
(
	Date_,
	MonthNumber
)
SELECT
	Date_ AS Date_,
	DATEDIFF(M, '1980-01-01', Date_ ) + 1 As MonthNumber
FROM
	@TradingDates D;

---extract spot trading values 
--transaction data
SELECT
	I.Ticker AS ID,
	D.Date_ AS Date_,
	D.Close_ AS [Close],
	LAG(D.Close_) OVER (PARTITION BY I.Ticker ORDER BY I.Ticker, D.Date_) AS PriorClose,
	(D.Close_ / 100) / 12 AS MTotRetA
INTO 
	#RS5
FROM
	SPOTINFO I      
LEFT JOIN
	SPOTDLY D
		ON D.CODE = I.CODE AND D.Date_ >= @PriorTradingDate
	WHERE I.Ticker IN ('BY_SPOT', '6Y_SPOT', '1Y_SPOT', '2Y_SPOT', '3Y_SPOT', '5Y_SPOT', '7Y_SPOT',
				'NY_SPOT', 'UY_SPOT');


--SELECT * FROM #RS5


---
DECLARE @MyFinalTable TABLE
(
	ID NVARCHAR(31),
	Date_ SMALLDATETIME NOT NULL,
	MyClose NVARCHAR(31)

);

--variables to store intermediate results
DECLARE @MyID NVARCHAR(31);
DECLARE @prevDate DATE;
DECLARE @currDate DATE;
DECLARE @nextDate DATE;
DECLARE @prevClose NVARCHAR(31); 
DECLARE @currClose NVARCHAR(31);
DECLARE @nextClose NVARCHAR(31);
DECLARE @RSCursor Cursor;
 

--Declare @rowcount int;
--set @rowcount=(SELECT COUNT(DATE_) FROM #RS5 WHERE ID='BY_SPOT')
--DECLARE @i int;
--DECLARE @next int;

--RS5 has the data which we are trying to expand over each day. It is currnetly only for month or so

--ID='BY_SPOT' '6Y_SPOT' '1Y_SPOT'  '2Y_SPOT'  '3Y_SPOT'  '5Y_SPOT'  '7Y_SPOT'  'NY_SPOT'  'UY_SPOT' 
--for above list of ID`s we run the following loop each time to pull down the previous close values
declare @tblidnames table(idtickers varchar(100));
insert into @tblidnames values('BY_SPOT'),('6Y_SPOT'), ('1Y_SPOT'),('2Y_SPOT'),('3Y_SPOT'),('5Y_SPOT'),('7Y_SPOT'),('NY_SPOT'),('UY_SPOT')
declare @curridname as nvarchar(31);
DECLARE @idcursor Cursor;

BEGIN 
	--looping through each IDNAME
	SET @idcursor=CURSOR FOR SELECT idtickers FROM @tblidnames
	OPEN @idcursor
	FETCH NEXT from @idcursor into @curridname
	WHILE @@FETCH_STATUS = 0
		BEGIN 
			----do all your activities herre with  here

				SET  @MyID=@curridname
				BEGIN
				----looping through each record for the current IDNAME
				SET @RSCursor = CURSOR FOR SELECT ID,DATE_,[Close] 
					FROM #RS5 
					WHERE ID=@MyID 
					ORDER BY DATE_ 

					OPEN @RSCursor
					--go to next record
					FETCH NEXT from @RSCursor into @MyID,@currDate,@currClose
						WHILE @@FETCH_STATUS = 0
						BEGIN 
							---adding date between curr and nextdate and pulling down values from current-date record
							FETCH NEXT from @RSCursor into  @MyID,@nextDate,@nextClose ----next date is here
								WHILE(@currDate < @nextDate)
									BEGIN
										INSERT INTO @MyFinalTable(ID,Date_,MyClose)  
										VALUES(@MyID,@currDate,@currClose)	

										SET @currDate=DATEADD(day,1,@currDate)
									END


								--Print 'Next loop...'
								SET @currDate=@nextDate
								SET @currClose=@nextClose

						END
								INSERT INTO @MyFinalTable(ID,Date_,MyClose)  
										VALUES(@MyID,@currDate,@currClose)	

					CLOSE @RSCursor;
					DEALLOCATE @RSCursor;
				END;

			--PRINT 'done with' + @curridname

			FETCH NEXT from @idcursor into @curridname
			
		END
	CLOSE @idcursor;
	DEALLOCATE @idcursor;

END;

 
SELECT * INTO #RSmyfinaltable FROM  @MyFinalTable;

-- YLD_3M -- BY_SPOT
UPDATE @FinalOutput
SET YLD_3M = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)),'.')
FROM
	@FinalOutput T1
LEFT JOIN #RSmyfinaltable T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'BY_SPOT'

-- YLD_6M -- 6Y_SPOT
UPDATE @FinalOutput
SET
	YLD_6M = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
LEFT JOIN #RSmyfinaltable T2 ON T2.Date_ = T1.Date_ AND T2.Id = '6Y_SPOT'

-- YLD_1Y -- 1Y_SPOT
UPDATE @FinalOutput
SET
	YLD_1Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
LEFT JOIN #RSmyfinaltable T2 ON T2.Date_ = T1.Date_ AND T2.Id = '1Y_SPOT'

-- YLD_2Y -- 2Y_SPOT
UPDATE @FinalOutput
SET
	YLD_2Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
LEFT JOIN #RSmyfinaltable T2 ON T2.Date_ = T1.Date_ AND T2.Id = '2Y_SPOT'

-- YLD_3Y -- 3Y_SPOT
UPDATE @FinalOutput
SET
	YLD_3Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
LEFT JOIN #RSmyfinaltable T2 ON T2.Date_ = T1.Date_ AND T2.Id = '3Y_SPOT'

-- YLD_5Y -- 5Y_SPOT
UPDATE @FinalOutput
SET
	YLD_5Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
LEFT JOIN #RSmyfinaltable T2 ON T2.Date_ = T1.Date_ AND T2.Id = '5Y_SPOT'

-- YLD_7Y -- 7Y_SPOT
UPDATE @FinalOutput
SET
	YLD_7Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
LEFT JOIN #RSmyfinaltable T2 ON T2.Date_ = T1.Date_ AND T2.Id = '7Y_SPOT'

-- YLD_10Y -- NY_SPOT
UPDATE @FinalOutput
SET
	YLD_10Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
LEFT JOIN #RSmyfinaltable T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'NY_SPOT'

-- YLD_30Y -- UY_SPOT
UPDATE @FinalOutput
SET
	YLD_30Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
LEFT JOIN #RSmyfinaltable T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'UY_SPOT'



--following changes added on 1/1/2018. This is to just carry forward the last values to the latest trade-date. 
--this had to be done because for some cases the values for spot was "." in the finaloutput & for some it had value. ETF bastched failed
--the following code just carries forward the last value till the trade-date value as -is for all in case of absence
DECLARE @valtocopy VARCHAR(31);
DECLARE @nonemptyvalue_lastdate DATE;
DECLARE @lastdate DATE;


-- YLD_3M -- BY_SPOT
SET	@lastdate = (select max(Date_) from @FinalOutput where YLD_3M = '.');
SET	@nonemptyvalue_lastdate = (select max(Date_) from @FinalOutput where YLD_3M <>'.');
SET @valtocopy=(select YLD_3M from @FinalOutput where Date_= @nonemptyvalue_lastdate);
UPDATE @FinalOutput
SET
	YLD_3M = @valtocopy
FROM
	@FinalOutput 
WHERE YLD_3M='.' AND  Date_ > @nonemptyvalue_lastdate;

-- YLD_6M -- 6Y_SPOT
SET	@lastdate = (select max(Date_) from @FinalOutput where YLD_6M = '.');
SET	@nonemptyvalue_lastdate = (select max(Date_) from @FinalOutput where YLD_6M <>'.');
SET @valtocopy=(select YLD_6M from @FinalOutput where Date_= @nonemptyvalue_lastdate);
UPDATE @FinalOutput
SET
	YLD_6M = @valtocopy
FROM
	@FinalOutput 
WHERE YLD_6M='.' AND  Date_ > @nonemptyvalue_lastdate;

-- YLD_1Y -- 1Y_SPOT
SET	@lastdate = (select max(Date_) from @FinalOutput where YLD_1Y = '.');
SET	@nonemptyvalue_lastdate = (select max(Date_) from @FinalOutput where YLD_1Y <>'.');
SET @valtocopy=(select YLD_1Y from @FinalOutput where Date_= @nonemptyvalue_lastdate);
UPDATE @FinalOutput
SET
	YLD_1Y = @valtocopy
FROM
	@FinalOutput 
WHERE YLD_1Y='.' AND  Date_ > @nonemptyvalue_lastdate;

-- YLD_2Y -- 2Y_SPOT
SET	@lastdate = (select max(Date_) from @FinalOutput where YLD_2Y = '.');
SET	@nonemptyvalue_lastdate = (select max(Date_) from @FinalOutput where YLD_2Y <>'.');
SET @valtocopy=(select YLD_2Y from @FinalOutput where Date_= @nonemptyvalue_lastdate);
UPDATE @FinalOutput
SET
	YLD_2Y = @valtocopy
FROM
	@FinalOutput 
WHERE YLD_2Y='.' AND  Date_ > @nonemptyvalue_lastdate;



-- YLD_3Y -- 3Y_SPOT
SET	@lastdate = (select max(Date_) from @FinalOutput where YLD_3Y = '.');
SET	@nonemptyvalue_lastdate = (select max(Date_) from @FinalOutput where YLD_3Y <>'.');
SET @valtocopy=(select YLD_3Y from @FinalOutput where Date_= @nonemptyvalue_lastdate);
UPDATE @FinalOutput
SET
	YLD_3Y = @valtocopy
FROM
	@FinalOutput 
WHERE YLD_3Y='.' AND  Date_ > @nonemptyvalue_lastdate;


-- YLD_5Y -- 5Y_SPOT
SET	@lastdate = (select max(Date_) from @FinalOutput where YLD_5Y = '.');
SET	@nonemptyvalue_lastdate = (select max(Date_) from @FinalOutput where YLD_5Y <>'.');
SET @valtocopy=(select YLD_5Y from @FinalOutput where Date_= @nonemptyvalue_lastdate);
UPDATE @FinalOutput
SET
	YLD_5Y = @valtocopy
FROM
	@FinalOutput 
WHERE YLD_5Y='.' AND  Date_ > @nonemptyvalue_lastdate;


-- YLD_7Y -- 7Y_SPOT
SET	@lastdate = (select max(Date_) from @FinalOutput where YLD_7Y = '.');
SET	@nonemptyvalue_lastdate = (select max(Date_) from @FinalOutput where YLD_7Y <>'.');
SET @valtocopy=(select YLD_7Y from @FinalOutput where Date_= @nonemptyvalue_lastdate);
UPDATE @FinalOutput
SET
	YLD_7Y = @valtocopy
FROM
	@FinalOutput 
WHERE YLD_7Y='.' AND  Date_ > @nonemptyvalue_lastdate;


-- YLD_10Y -- NY_SPOT
SET	@lastdate = (select max(Date_) from @FinalOutput where YLD_10Y = '.');
SET	@nonemptyvalue_lastdate = (select max(Date_) from @FinalOutput where YLD_10Y <>'.');
SET @valtocopy=(select YLD_10Y from @FinalOutput where Date_= @nonemptyvalue_lastdate);
UPDATE @FinalOutput
SET
	YLD_10Y = @valtocopy
FROM
	@FinalOutput 
WHERE YLD_10Y='.' AND  Date_ > @nonemptyvalue_lastdate;

-- YLD_30Y -- UY_SPOT
UPDATE @FinalOutput
SET	@lastdate = (select max(Date_) from @FinalOutput where YLD_30Y = '.');
SET	@nonemptyvalue_lastdate = (select max(Date_) from @FinalOutput where YLD_30Y <>'.');
SET @valtocopy=(select YLD_10Y from @FinalOutput where Date_= @nonemptyvalue_lastdate);
UPDATE @FinalOutput
SET
	YLD_30Y = @valtocopy
FROM
	@FinalOutput 
WHERE YLD_30Y='.' AND  Date_ > @nonemptyvalue_lastdate;

---changes end here


SELECT FORMAT(Date_, 'MM/dd/yyyy') AS dates,
	   YLD_3M as yld_3m,
	   YLD_6M as yld_6m,
	   YLD_1Y as yld_1y,
	   YLD_2Y as yld_2y,
	   YLD_3Y as yld_3y,
	   YLD_5Y as yld_5y,
	   YLD_7Y as yld_7y,
	   YLD_10Y as yld_10y,
	   YLD_30Y as yld_30y,
	   MonthNumber as monthnumber
    INTO  #RS7
    FROM @FinalOutput;
    
--ALTER TABLE #RS7 DROP COLUMN Date_;
SELECT * FROM #RS7
drop table #Tradedates
--SELECT * FROM #RS5
DROP TABLE #RS5;
DROP TABLE #RS7;
DROP TABLE #RSmyfinaltable;

--We will expand #RS5 table. RS5 seems to capture only monthly [Close] value for ID. All missing dates betwen months we will create and fill it up with the previous value 
--table to store above results 
