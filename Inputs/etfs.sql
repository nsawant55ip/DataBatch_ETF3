---ETF MONTHLY VERSION 7--Latest Version
---Last updated: 5/23/2017 : 5th May onwards subscription to IDC pricing tables was lost. We decided to use datastream in lace of IDC wherever possible.
---IDNAMES:'@MSCIS167', '@UTDKI43'  were affected. we could find the prices for '@UTDKI43' from DS tables but not for '@MSCIS167 (this will be fetched from bbg)
---Originally Developed By: Contractor, Saudamini, Devesh,
---latest changes made: pulling down prices and totret using cursor (Avinash)
---NOTE: commenting out: oih/poih,pph/ppph,rth/prth,smh/psmh,mib30/pmib30
---WHAT IS THIS QUERY DOING ?
---It fetches Monthly Close price & TotRet for following categories of securities.We are manually pulling down previous close & totRet values (wherever available) using cursors.
---Category-1-IDNAMES:'@MSCIS167', '@UTDKI43' 
---Category-2-IDNAMES:'QQQ', 'SPY', 'MDY', 'IWM', 'XLU', 'XLF', 'XLE', 'OIH', 'PPH',
----	'GLD', 'RTH', '86330E74', 'XLB', '86330e64', 'EWZ', '46428723', '46428719', 'DIA',
----	'91232N10', '46428743', '46428745', 'SMH', 'XLK', 'IYR', '46428851', '73936T55', '46428Q10', '46428722',
----	'XLY', 'XLP', 'XLV', 'XLI', '86330E78', '25459Y56','25459Y61','25459W25','25459W23','22542D53',
----    '22542D57','22542D54','22542D58','25459W29','25459W28','IWD','IWF','86330E62','46432F39','73937B77'

---Category-3-IDNAMES: #RS3 ('SPX_IDX', 'RUA_IDX', 'RUA_D_IDX', 'RUT_IDX', 'RUI_IDX', 'RUI_D_IDX', 'DJX_IDX')
---Category-4-IDNAMES: #RS2 ('VIX_IDX', 'RVXK_IDX')
---Category-5-IDNAMES: #RS6 ('TU_FUT0', 'TU_FUT1', 'FV_FUT0', 'FV_FUT1', 'TY_FUT0', 'TY_FUT1', 'ED_FUT0', 'ED_FUT1', 'CL_FUT0')
---Category-6-IDNAME: 'HUI'

SET NOCOUNT ON;

DECLARE @MarketQAId NVARCHAR(31);
DECLARE @StartDate DATE;
DECLARE @PriorTradingDate DATE;

SET @MarketQAId = 'SPX_IDX';
SET @StartDate = '1/1/1980'


DECLARE @TradingDates TABLE
(
	Date_ SMALLDATETIME NOT NULL
);

--picking up the dates on which trading happened for SPX_IDX
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

----finding last trading date from the given startdate
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

--final table to be filled with values of close&return for each ticker/ID against each date on which trading happened(SPX_IDX traded dates)
DECLARE @FinalOutput TABLE
(
	Date_ SMALLDATETIME NOT NULL,
	MonthNumber INT NOT NULL,
	QQQ VARCHAR(31) NULL DEFAULT '.', PQQQ VARCHAR(31) NULL DEFAULT '.',
	SPY VARCHAR(31) NULL DEFAULT '.', PSPY VARCHAR(31) NULL DEFAULT '.',
	MDY VARCHAR(31) NULL DEFAULT '.', PMDY VARCHAR(31) NULL DEFAULT '.',
	VIX VARCHAR(31) NULL DEFAULT '.', RVIX VARCHAR(31) NULL DEFAULT '.',
	XLU VARCHAR(31) NULL DEFAULT '.', PXLU VARCHAR(31) NULL DEFAULT '.',
	XLF VARCHAR(31) NULL DEFAULT '.', PXLF VARCHAR(31) NULL DEFAULT '.',
	XLE VARCHAR(31) NULL DEFAULT '.', PXLE VARCHAR(31) NULL DEFAULT '.',
	--OIH VARCHAR(31) NULL DEFAULT '.', POIH VARCHAR(31) NULL DEFAULT '.',
	--PPH VARCHAR(31) NULL DEFAULT '.', PPPH VARCHAR(31) NULL DEFAULT '.',
	--RTH VARCHAR(31) NULL DEFAULT '.', PRTH VARCHAR(31) NULL DEFAULT '.',
	GLD VARCHAR(31) NULL DEFAULT '.', PGLD VARCHAR(31) NULL DEFAULT '.',
	XHB VARCHAR(31) NULL DEFAULT '.', PXHB VARCHAR(31) NULL DEFAULT '.',
	XLB VARCHAR(31) NULL DEFAULT '.', PXLB VARCHAR(31) NULL DEFAULT '.',
	IWM VARCHAR(31) NULL DEFAULT '.', PIWM VARCHAR(31) NULL DEFAULT '.',
	XME VARCHAR(31) NULL DEFAULT '.', PXME VARCHAR(31) NULL DEFAULT '.',
	EEM VARCHAR(31) NULL DEFAULT '.', PEEM VARCHAR(31) NULL DEFAULT '.',
	EWZ VARCHAR(31) NULL DEFAULT '.', PEWZ VARCHAR(31) NULL DEFAULT '.',
	IYT VARCHAR(31) NULL DEFAULT '.', PIYT VARCHAR(31) NULL DEFAULT '.',
	DIA VARCHAR(31) NULL DEFAULT '.', PDIA VARCHAR(31) NULL DEFAULT '.',
	USO VARCHAR(31) NULL DEFAULT '.', PUSO VARCHAR(31) NULL DEFAULT '.',
	TBILL3 VARCHAR(31) NULL DEFAULT '.', 
	TBILL VARCHAR(31) NULL DEFAULT '.',
	TU VARCHAR(31) NULL DEFAULT '.', PTU VARCHAR(31) NULL DEFAULT '.',
	TU_B VARCHAR(31) NULL DEFAULT '.', PTU_B VARCHAR(31) NULL DEFAULT '.',
	FV5 VARCHAR(31) NULL DEFAULT '.', PFV5 VARCHAR(31) NULL DEFAULT '.',
	FV5_B VARCHAR(31) NULL DEFAULT '.', PFV5_B VARCHAR(31) NULL DEFAULT '.',
	TY10 VARCHAR(31) NULL DEFAULT '.', PTY10 VARCHAR(31) NULL DEFAULT '.',
	TY10_B VARCHAR(31) NULL DEFAULT '.', PTY10_B VARCHAR(31) NULL DEFAULT '.',
	[ED] VARCHAR(31) NULL DEFAULT '.', PED VARCHAR(31) NULL DEFAULT '.',
	ED_B VARCHAR(31) NULL DEFAULT '.', PED_B VARCHAR(31) NULL DEFAULT '.',
	TLT VARCHAR(31) NULL DEFAULT '.', PTLT VARCHAR(31) NULL DEFAULT '.',
	SHY VARCHAR(31) NULL DEFAULT '.', PSHY VARCHAR(31) NULL DEFAULT '.',
	--SMH VARCHAR(31) NULL DEFAULT '.', PSMH VARCHAR(31) NULL DEFAULT '.',
	CL VARCHAR(31) NULL DEFAULT '.', PCL VARCHAR(31) NULL DEFAULT '.',
	XLY VARCHAR(31) NULL DEFAULT '.', PXLY VARCHAR(31) NULL DEFAULT '.',
	XLP VARCHAR(31) NULL DEFAULT '.', PXLP VARCHAR(31) NULL DEFAULT '.',
	MXWO VARCHAR(31) NULL DEFAULT '.',PMXWO VARCHAR(31) NULL DEFAULT '.',
	XLV VARCHAR(31) NULL DEFAULT '.', PXLV VARCHAR(31) NULL DEFAULT '.',
	FTSE100 VARCHAR(31) NULL DEFAULT '.',PFTSE100 VARCHAR(31) NULL DEFAULT '.',
	DAX VARCHAR(31) NULL DEFAULT '.', PDAX VARCHAR(31) NULL DEFAULT '.',
	CAC40 VARCHAR(31) NULL DEFAULT '.', PCAC40 VARCHAR(31) NULL DEFAULT '.',
	--MIB30 VARCHAR(31) NULL DEFAULT '.', PMIB30 VARCHAR(31) NULL DEFAULT '.',
	SX5E VARCHAR(31) NULL DEFAULT '.', PSX5E VARCHAR(31) NULL DEFAULT '.',
	GC_SPOT VARCHAR(31) NULL DEFAULT '.', PGC_SPOT VARCHAR(31) NULL DEFAULT '.',
	PL_SPOT VARCHAR(31) NULL DEFAULT '.', PPL_SPOT VARCHAR(31) NULL DEFAULT '.',
	SI_SPOT VARCHAR(31) NULL DEFAULT '.', PSI_SPOT VARCHAR(31) NULL DEFAULT '.',
	YLD_3M VARCHAR(31) NULL DEFAULT '.', YLD_6M VARCHAR(31) NULL DEFAULT '.',
	YLD_1Y VARCHAR(31) NULL DEFAULT '.', YLD_2Y VARCHAR(31) NULL DEFAULT '.',
	YLD_3Y VARCHAR(31) NULL DEFAULT '.', YLD_5Y VARCHAR(31) NULL DEFAULT '.',
	YLD_10Y VARCHAR(31) NULL DEFAULT '.',
	XLI VARCHAR(31) NULL DEFAULT '.', PXLI VARCHAR(31) NULL DEFAULT '.',
	KIE VARCHAR(31) NULL DEFAULT '.', PKIE VARCHAR(31) NULL DEFAULT '.',
	SPX VARCHAR(31) NULL DEFAULT '.', PSPX VARCHAR(31) NULL DEFAULT '.',
	XLK VARCHAR(31) NULL DEFAULT '.', PXLK VARCHAR(31) NULL DEFAULT '.',
	IYR VARCHAR(31) NULL DEFAULT '.', PIYR VARCHAR(31) NULL DEFAULT '.',
	RUA VARCHAR(31) NULL DEFAULT '.', PRUA VARCHAR(31) NULL DEFAULT '.',
	RUAT VARCHAR(31) NULL DEFAULT '.', PRUAT VARCHAR(31) NULL DEFAULT '.',
	RUT VARCHAR(31) NULL DEFAULT '.', PRUT VARCHAR(31) NULL DEFAULT '.',
	RUI VARCHAR(31) NULL DEFAULT '.', PRUI VARCHAR(31) NULL DEFAULT '.',
	RUIT VARCHAR(31) NULL DEFAULT '.', PRUIT VARCHAR(31) NULL DEFAULT '.',
	HYG VARCHAR(31) NULL DEFAULT '.', PHYG VARCHAR(31) NULL DEFAULT '.',
	PHB VARCHAR(31) NULL DEFAULT '.', PPHB VARCHAR(31) NULL DEFAULT '.',
	SLV VARCHAR(31) NULL DEFAULT '.', PSLV VARCHAR(31) NULL DEFAULT '.',
	DJX VARCHAR(31) NULL DEFAULT '.', PDJX VARCHAR(31) NULL DEFAULT '.',
	AGG VARCHAR(31) NULL DEFAULT '.', PAGG VARCHAR(31) NULL DEFAULT '.',
    JDST VARCHAR(31) NULL DEFAULT '.', PJDST VARCHAR(31) NULL DEFAULT '.',
    JNUG VARCHAR(31) NULL DEFAULT '.', PJNUG VARCHAR(31) NULL DEFAULT '.',
    NUGT VARCHAR(31) NULL DEFAULT '.', PNUGT VARCHAR(31) NULL DEFAULT '.',
    DUST VARCHAR(31) NULL DEFAULT '.', PDUST VARCHAR(31) NULL DEFAULT '.',
    DGAZ VARCHAR(31) NULL DEFAULT '.', PDGAZ VARCHAR(31) NULL DEFAULT '.',
    UGAZ VARCHAR(31) NULL DEFAULT '.', PUGAZ VARCHAR(31) NULL DEFAULT '.',
    DWTI VARCHAR(31) NULL DEFAULT '.', PDWTI VARCHAR(31) NULL DEFAULT '.',
    UWTI VARCHAR(31) NULL DEFAULT '.', PUWTI VARCHAR(31) NULL DEFAULT '.',
    RUSL VARCHAR(31) NULL DEFAULT '.', PRUSL VARCHAR(31) NULL DEFAULT '.',
    RUSS VARCHAR(31) NULL DEFAULT '.', PRUSS VARCHAR(31) NULL DEFAULT '.',
    IWD VARCHAR(31) NULL DEFAULT '.', PIWD VARCHAR(31) NULL DEFAULT '.',
    IWF VARCHAR(31) NULL DEFAULT '.', PIWF VARCHAR(31) NULL DEFAULT '.',
    XOP VARCHAR(31) NULL DEFAULT '.', PXOP VARCHAR(31) NULL DEFAULT '.',
	 SPLV VARCHAR(31) NULL DEFAULT '.', PSPLV VARCHAR(31) NULL DEFAULT '.',
	 MTUM VARCHAR(31) NULL DEFAULT '.', PMTUM VARCHAR(31) NULL DEFAULT '.',
	 HUI VARCHAR(31) NULL DEFAULT '.', PHUI VARCHAR(31) NULL DEFAULT '.'
	  
);

--insert trading dates and monthnumbers in the finaloutput corresponding to which close & return values for tickers is to be gathered 
INSERT INTO @FinalOutput
(
	Date_,
	MonthNumber
)
SELECT
	DISTINCT
	D1.Date_ AS Date_,
	DATEDIFF(M, '1980-01-01', D1.Date_ ) + 1 As MonthNumber
FROM
	#Tradedates D
LEFT JOIN
	#Tradedates D1
	ON D1.Date_ = ( SELECT MAX(Date_) FROM #Tradedates WHERE Date_ <= EOMONTH(D.Date_));



---SOME COMMONLY USED TABLES
--for selecing speicifc securities only 
DECLARE @SecurityTable TABLE
(
	[Id] VARCHAR(31) NOT NULL,
	[Code] INT NOT NULL

);

--user defined tables to store intermediate date values between 2-dates
----IDNAMES:'QQQ', 'SPY', 'MDY', 'IWM', 'XLU', 'XLF', 'XLE', 'OIH', 'PPH',
----	'GLD', 'RTH', '86330E74', 'XLB', '86330e64', 'EWZ', '46428723', '46428719', 'DIA',
----	'91232N10', '46428743', '46428745', 'SMH', 'XLK', 'IYR', '46428851', '73936T55', '46428Q10', '46428722',
----	'XLY', 'XLP', 'XLV', 'XLI', '86330E78', '25459Y56','25459Y61','25459W25','25459W23','22542D53',
----    '22542D57','22542D54','22542D58','25459W29','25459W28','IWD','IWF','86330E62','46432F39','73937B77'
DECLARE @MtblPulledDownValues4 TABLE  
(
	ID NVARCHAR(31),
	Date_ SMALLDATETIME NOT NULL,
	MyClose decimal(28,7),
	MyMonthEndDate DATE,
	MyMonthEndClose decimal(28,7),
	MyMonthEndTotRet decimal(28,7),
	MyPriorMonthEndDate DATE,
	MyPriorMonthEndClose decimal(28,7),
	MyPriorMonthEndTotRet decimal(28,7),
	MyMTotRet decimal(28,7) ---calculated--there is no actual TotRet vendor value. So this is calculated based on Close values Y/X - 1 
);


------IDNAMES: #HUI
DECLARE @MtblPulledDownValues7 TABLE  
(
	ID NVARCHAR(31),
	Date_ SMALLDATETIME NOT NULL,
	MyClose decimal(28,7),
	MyPriorMonthEndClose decimal(28,7),
	MyMTotRet decimal(28,7) ---calculated--there is no actual TotRet vendor value. So this is calculated based on Close values Y/X - 1 
);



------IDNAMES: #RS2 ('VIX_IDX', 'RVXK_IDX')
DECLARE @MtblPulledDownValues6 TABLE  
(
	ID NVARCHAR(31),
	Date_ SMALLDATETIME NOT NULL,
	MyClose decimal(28,7),
	MyMonthEndDate DATE,
	MyMonthEndClose decimal(28,7),
	MyMTotRet decimal(28,7) ---calculated--there is no actual TotRet vendor value. So this is calculated based on Close values Y/X - 1 
);



-----IDNAMES=('BY_SPOT'),('6Y_SPOT'),('1Y_SPOT'),('2Y_SPOT'),('3Y_SPOT'),('5Y_SPOT'),('NY_SPOT')
--('TB_SPOT'),('DF_SPOT'),('MX_SPOT'),('MI_SPOT'),('FX_SPOT'),('GC_SPOT'),('PL_SPOT'),('SI_SPOT')
DECLARE @MtblPulledDownValues2 TABLE  
(
	ID NVARCHAR(31),
	Date_ SMALLDATETIME NOT NULL,
	MyClose decimal(28,7),
	MyMonthEndDate DATE,
	MyMonthEndClose decimal(28,7),
	MyPriorMonthEndDate DATE,
	MyPriorMonthEndClose decimal(28,7),
	MyMTotRetA decimal(28,7),
	MyMTotRet decimal(28,7) ---calculated--there is no actual TotRet vendor value. So this is calculated based on Close values Y/X - 1 
);


--IDNAMES:('SPX_IDX', 'RUA_IDX', 'RUA_D_IDX', 'RUT_IDX', 'RUI_IDX', 'RUI_D_IDX', 'DJX_IDX')
--IDNAMES: #RS6 ('TU_FUT0', 'TU_FUT1', 'FV_FUT0', 'FV_FUT1', 'TY_FUT0', 'TY_FUT1', 'ED_FUT0', 'ED_FUT1', 'CL_FUT0')
DECLARE @MtblPulledDownValues1 TABLE  
(
	ID NVARCHAR(31),
	Date_ SMALLDATETIME NOT NULL,
	MyClose decimal(28,7),
	MyMonthEndDate DATE,
	MyMonthEndClose decimal(28,7),
	MyPriorMonthEndDate DATE,
	MyPriorMonthEndClose decimal(28,7),
	MyMTotRet decimal(28,7) ---calculated--there is no actual TotRet vendor value. So this is calculated based on Close values Y/X - 1 
);


--IDNAMES:'@MSCIS167', '@UTDKI43' 
DECLARE @MtblPulledDownValues3 TABLE  
(
	ID NVARCHAR(31),
	Date_ SMALLDATETIME NOT NULL,
	MyClose decimal(28,7),
	MyMonthEndDate DATE,
	MyMonthEndTotRet decimal(28,7),
	MyPriorMonthEndDate DATE,
	MyPriorMonthEndTotRet decimal(28,7),
	MyMTotRet decimal(28,7) ---calculated--there is no actual TotRet vendor value. So this is calculated based on Close values Y/X - 1 
);


DECLARE @tblPulledDownValues11 TABLE  
(
	ID NVARCHAR(31),
	Date_ SMALLDATETIME NOT NULL,
	MyClose decimal(28,7),
	MyMTotRet decimal(28,7)
);

--variables to store intermediate results during pull down operations
DECLARE @MyID NVARCHAR(31);
DECLARE @prevDate DATE;
DECLARE @currDate DATE;
DECLARE @nextDate DATE;
DECLARE @prevClose decimal(28,7); 
DECLARE @currClose decimal(28,7);
DECLARE @nextClose decimal(28,7);
DECLARE @prevTotRet decimal(28,7); 
DECLARE @currTotRet decimal(28,7);
DECLARE @nextTotRet decimal(28,7);
DECLARE @prevMTotRetA decimal(28,7); 
DECLARE @currMTotRetA decimal(28,7);
DECLARE @nextMTotRetA decimal(28,7);

--MonthEndDate
--MonthEndClose
DECLARE @prevMonthEndDate DATE;
DECLARE @currMonthEndDate DATE;
DECLARE @nextMonthEndDate DATE;
DECLARE @prevMonthEndClose decimal(28,7);
DECLARE @currMonthEndClose decimal(28,7);
DECLARE @nextMonthEndClose decimal(28,7);
DECLARE @prevMonthEndTotRet decimal(28,7);
DECLARE @currMonthEndTotRet decimal(28,7);
DECLARE @nextMonthEndTotRet decimal(28,7);


DECLARE @prevPriorMonthEndDate DATE;
DECLARE @currPriorMonthEndDate DATE;
DECLARE @nextPriorMonthEndDate DATE;
DECLARE @prevPriorMonthEndClose decimal(28,7);
DECLARE @currPriorMonthEndClose decimal(28,7);
DECLARE @nextPriorMonthEndClose decimal(28,7);
DECLARE @prevPriorMonthEndTotRet decimal(28,7);
DECLARE @currPriorMonthEndTotRet decimal(28,7);
DECLARE @nextPriorMonthEndTotRet decimal(28,7);



DECLARE @calMyMTotRet decimal(28,7)--(31); -- as nvarchar(31)
DECLARE @divisor decimal(28,7);
DECLARE @RSCursor Cursor;

--table to store IDnames for which pull-down in to be implemented
declare @tblidnames table(idtickers varchar(100));

--cursor variables
declare @curridname as nvarchar(31);
DECLARE @idcursor Cursor;


------############################################################################################################################
-------IDNAMES: '@UTDKI43' [ ('@MSCIS167') will be feteched from bbg table)]

SELECT
	'@UTDKI43' as ID,
	DT.Date_ AS Date_,
	IndexData.PI_ AS [Close],
	--ISNULL(IndexData.RI, ID2.RI) As RI
	--ID3.PI_ AS PPI_,
	(IndexData.PI_ - ID2.PI_) / ISNULL(ID2.PI_,1) AS MyMTotRet
INTO
	#RS1
FROM
	@FinalOutput DT
LEFT JOIN
	Ds2IndexData IndexData
		ON DT.Date_ = IndexData.ValueDate
		AND IndexData.ValueDate >= @StartDate
LEFT JOIN 
    Ds2IndexData ID2
		ON ID2.DSIndexCode='36888'
		AND ID2.ValueDate=(select max(ValueDate) from DS2IndexData where DS2IndexData.DSIndexCode='36888' AND valuedate <= EOMONTH(DT.Date_))
WHERE IndexData.DSIndexCode ='36888' -- 'FTSE100';
ORDER BY DT.Date_


--pull down values for intermediate days above
--clean up previous list
DELETE FROM @tblidnames;
--insert new IDNAMES
insert into @tblidnames values('@UTDKI43');


BEGIN 
	--select all the IDNAMES
	SET @idcursor=CURSOR FOR SELECT idtickers FROM @tblidnames
	OPEN @idcursor
	FETCH NEXT from @idcursor into @curridname
	WHILE @@FETCH_STATUS = 0
		BEGIN 
			----for each IDNAME
				SET  @MyID=@curridname
				SET @prevClose=NULL
				BEGIN
				----extract all records for current IDNAME 
				SET @RSCursor = CURSOR FOR SELECT Date_,[Close],MyMTotRet
					FROM #RS1
					ORDER BY Date_ 

					--looping through each record from the above set
					OPEN @RSCursor
					--go to next record
					FETCH NEXT from @RSCursor into @currDate,@currClose,@currMTotRetA --as is values pick up
						WHILE @@FETCH_STATUS = 0
						BEGIN
								FETCH NEXT from @RSCursor into @nextDate,@nextClose,@nextMTotRetA ---as is next values picked up

									WHILE(@currDate < @nextDate) ---for in-between dates JUST pull down previous values
									BEGIN
										SET @divisor = ( 
														CASE 
															WHEN @prevClose=0 OR @prevClose IS NULL THEN 1
															ELSE 
																 @prevClose
														END
											)
										
										SET @calMyMTotRet = (@currClose - @prevClose)/@divisor 									
									
										--insert record @@tblPulledDownValues3 table
										INSERT INTO @tblPulledDownValues11(ID,Date_,MyClose,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@calMyMTotRet)
										
										SET @currDate=DATEADD(day,1,@currDate) --incrementing the currdate to next
										---during pull down MTotRet needs to be calculated. so the curr val become prev 
										--in-between values will be previous ones
										SET @prevClose=@currClose

																		
									END

								SET @prevClose=@currClose
								--SET @prevMTotRetA=@currMTotRetA
								----Print 'Next loop...'
								SET @currDate=@nextDate
								SET @currClose=@nextClose
								--SET @currMTotRetA=@nextMTotRetA

						


						END
						--for last value
						
										SET @divisor = ( 
														CASE 
															WHEN @prevClose=0 OR @prevClose IS NULL THEN 1
															ELSE 
																 @prevClose
														END
											)
										
										SET @calMyMTotRet = (@currClose - @prevClose)/@divisor 									
									
										--insert record @tblPulledDownValues11 table
										INSERT INTO @tblPulledDownValues11(ID,Date_,MyClose,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@calMyMTotRet)
					
					CLOSE @RSCursor;
					DEALLOCATE @RSCursor;
				END;

			--PRINT 'done with' + @curridname

			FETCH NEXT from @idcursor into @curridname
			
		END
	CLOSE @idcursor;
	DEALLOCATE @idcursor;

END;

--store all these trading dates & pulled down values for missing dates records in tempvariable; 
SELECT * INTO #temptblPulledDownValues11 FROM  @tblPulledDownValues11;

-- FTSE FTSE100
UPDATE @FinalOutput
SET
	FTSE100 = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PFTSE100 = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
LEFT JOIN #temptblPulledDownValues11 T2 ON T2.Date_ = T1.Date_ AND T2.ID = '@UTDKI43'


--Select * FROM #RS1
DROP TABLE #RS1;
--Select * FROM #temptblPulledDownValues1
DROP TABLE #temptblPulledDownValues11;
DELETE FROM @MtblPulledDownValues1;
  


--------##############################################################################################
------IDNAMES=('BY_SPOT'),('6Y_SPOT'),('1Y_SPOT'),('2Y_SPOT'),('3Y_SPOT'),('5Y_SPOT'),('NY_SPOT')
------('TB_SPOT'),('DF_SPOT'),('MX_SPOT'),('MI_SPOT'),('FX_SPOT'),('GC_SPOT'),('PL_SPOT'),('SI_SPOT') #RS5

SELECT
	I.Ticker AS ID,
	D.Date_ AS Date_,
	D.Close_ AS [Close],
	D.Date_ AS MonthEndDate,
	D.Close_ AS MonthEndClose,

	D3.Date_ AS PriorMonthEndDate,
	D3.Close_ AS PriorMonthEndClose,

	--(D.Close_ - D3.Close_) / D3.Close_ AS MTotRet, ---calculated field
	(D.Close_ / 100) / 12 AS MTotRetA

INTO #RS2

FROM
	SPOTINFO I      
LEFT JOIN
	SPOTDLY D
		ON D.CODE = I.CODE

LEFT JOIN
	SPOTDLY D3
		ON D3.CODE = I.CODE
		AND D3.Date_ =
			(
				SELECT
					MAX(DATE_)
				FROM
					SPOTDLY
				WHERE
					CODE = I.CODE
					AND DATE_ <= EOMONTH(DATEADD(M, -1, D.Date_))
			)
WHERE
	I.Ticker IN ('TB_SPOT', '2Y_SPOT', 'DF_SPOT', 'MX_SPOT', 'MI_SPOT', 'FX_SPOT', 
				'BY_SPOT', '6Y_SPOT', '1Y_SPOT', '3Y_SPOT', '5Y_SPOT', 'NY_SPOT',
				'GC_SPOT', 'PL_SPOT', 'SI_SPOT')

	--AND
	--D.Date_ IN (Select Date_ From @TradingDates);


------code for pulling down values 2/21/2017
DELETE FROM @tblidnames;
insert into @tblidnames values('BY_SPOT'),('6Y_SPOT'),('1Y_SPOT'),('2Y_SPOT'),('3Y_SPOT'),('5Y_SPOT'),('NY_SPOT'),
('DF_SPOT'),('MX_SPOT'),('TB_SPOT'),('MI_SPOT'),('FX_SPOT'),('GC_SPOT'),('PL_SPOT'),('SI_SPOT');

--set the default previous close value

BEGIN 
	--select all the IDNAMES
	SET @idcursor=CURSOR FOR SELECT idtickers FROM @tblidnames
	OPEN @idcursor
	FETCH NEXT from @idcursor into @curridname
	WHILE @@FETCH_STATUS = 0
		BEGIN 
			----for each IDNAME
				SET  @MyID=@curridname
				SET @prevClose=NULL
				BEGIN
				----extract all records for current IDNAME 
				SET @RSCursor = CURSOR FOR SELECT ID,Date_,[Close],MonthEndDate,MonthEndClose,PriorMonthEndDate,PriorMonthEndClose,MTotRetA
					FROM #RS2
					WHERE ID=@MyID 
					ORDER BY Date_ 

					--looping through each record from the above set
					OPEN @RSCursor
					--go to next record
					FETCH NEXT from @RSCursor into @MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate,@currPriorMonthEndClose,@currMTotRetA --as is values pick up
						WHILE @@FETCH_STATUS = 0
						BEGIN 
							
							---adding date between curr and nextdate and pulling down values from current-date record
							FETCH NEXT from @RSCursor into @MyID,@nextDate,@nextClose,@nextMonthEndDate,@nextMonthEndClose,@nextPriorMonthEndDate,@nextPriorMonthEndClose,@nextMTotRetA---as is next values picked up
						
				
								WHILE(@currDate < @nextDate) ---for in-between dates JUST pull down previous values
					
									BEGIN
										SET @divisor = ( 
														CASE 
															WHEN @currPriorMonthEndClose=0 OR @currPriorMonthEndClose IS NULL THEN 1
															ELSE 
																 @currPriorMonthEndClose
														END
											)
										
										SET @calMyMTotRet = (@currMonthEndClose - @currPriorMonthEndClose)/@divisor 									
									
										--insert record @tblPulledDownValues2 table
									 	INSERT INTO @MtblPulledDownValues2(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyPriorMonthEndDate,MyPriorMonthEndClose,MyMTotRetA,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate,@currPriorMonthEndClose,@currMTotRetA,@calMyMTotRet)
										
										SET @currDate=DATEADD(day,1,@currDate) --incrementing the currdate to next
										---during pull down MTotRet needs to be calculated. so the curr val become prev 
										--in-between values will be previous ones
										--SET @prevClose=@currClose
										--set @prevMonthEndDate= @currMonthEndDate
										--set @prevMonthEndClose = @currMonthEndClose
										--set @prevPriorMonthEndDate = @currPriorMonthEndDate
										--set @prevPriorMonthEndClose = @currPriorMonthEndClose

																		
									END

								--SET @prevClose=@currClose
								----SET @prevMTotRetA=@currMTotRetA
								------Print 'Next loop...'
								--SET @currDate=@nextDate
								--SET @currClose=@nextClose
								--SET @currMTotRetA=@nextMTotRetA

										--SET @prevClose=@currClose
										SET @currDate=@nextDate
										SET @currClose=@nextClose
										set @currMonthEndDate=@nextMonthEndDate
										set @currMonthEndClose= @nextMonthEndClose
										set @currPriorMonthEndDate=  @nextPriorMonthEndDate
										set @currPriorMonthEndClose = @nextPriorMonthEndClose
										SET @currMTotRetA=@nextMTotRetA
						


						END
						--for last value
						
									SET @divisor = ( 
														CASE 
															WHEN @currPriorMonthEndClose=0 OR @currPriorMonthEndClose IS NULL THEN 1
															ELSE 
																 @currPriorMonthEndClose
														END
											)
										
										SET @calMyMTotRet = (@currMonthEndClose - @currPriorMonthEndClose)/@divisor 									
									
										--insert record @tblPulledDownValues2 table
									 	INSERT INTO @MtblPulledDownValues2(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyPriorMonthEndDate,MyPriorMonthEndClose,MyMTotRetA,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate,@currPriorMonthEndClose,@currMTotRetA,@calMyMTotRet)
										
					
					CLOSE @RSCursor;
					DEALLOCATE @RSCursor;
				
				END;

			--PRINT 'done with' + @curridname

			FETCH NEXT from @idcursor into @curridname
			
		END
	CLOSE @idcursor;
	DEALLOCATE @idcursor;

END;

--store all these trading dates & pulled down values for missing dates records in tempvariable; 
SELECT * INTO #temptblPulledDownValues2 FROM  @MtblPulledDownValues2;


  


---update final output table
---- TB_SPOT
UPDATE @FinalOutput
SET
	TBILL3 = ISNULL(CAST(Round(T2.MyMTotRetA, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'TB_SPOT'


-- 2Y_SPOT
UPDATE @FinalOutput
SET
	TBILL = ISNULL(CAST(Round(T2.MyMTotRetA, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '2Y_SPOT'


-- DAX -- DF_SPOT
UPDATE @FinalOutput
SET
	DAX = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PDAX = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'DF_SPOT'

-- CAC40 -- MX_SPOT
UPDATE @FinalOutput
SET
	CAC40 = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PCAC40 = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'MX_SPOT'

---- MIB30 -- MI_SPOT
--UPDATE @FinalOutput
--SET
--	MIB30 = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
--	PMIB30 = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
--FROM
--	@FinalOutput T1
--  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'MI_SPOT'

-- SX5E -- FX_SPOT
UPDATE @FinalOutput
SET
	SX5E = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PSX5E = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'FX_SPOT'


-- YLD_3M -- BY_SPOT
UPDATE @FinalOutput
SET
	YLD_3M = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'BY_SPOT'

-- YLD_6M -- 6Y_SPOT
UPDATE @FinalOutput
SET
	YLD_6M = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '6Y_SPOT'

-- YLD_1Y -- 1Y_SPOT
UPDATE @FinalOutput
SET
	YLD_1Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '1Y_SPOT'

-- YLD_2Y -- 2Y_SPOT
UPDATE @FinalOutput
SET
	YLD_2Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '2Y_SPOT'

-- YLD_3Y -- 3Y_SPOT
UPDATE @FinalOutput
SET
	YLD_3Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '3Y_SPOT'

-- YLD_5Y -- 5Y_SPOT
UPDATE @FinalOutput
SET
	YLD_5Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '5Y_SPOT'

-- YLD_10Y -- NY_SPOT
UPDATE @FinalOutput
SET
	YLD_10Y = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'NY_SPOT'



-- GC_SPOT
UPDATE @FinalOutput
SET
	GC_SPOT = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PGC_SPOT = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'GC_SPOT'

-- PL_SPOT
UPDATE @FinalOutput
SET
	PL_SPOT = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PPL_SPOT = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'PL_SPOT'

-- SI_SPOT
UPDATE @FinalOutput
SET
	SI_SPOT = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PSI_SPOT = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues2 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'SI_SPOT'


--select * FROM #RS2
DROP TABLE #RS2;
--select * FROM #temptblPulledDownValues2
DROP TABLE #temptblPulledDownValues2;
DELETE FROM @MtblPulledDownValues2;

------########################################################################################
--------IDNAMES #RS1
--------'QQQ', 'SPY', 'MDY', 'IWM', 'XLU', 'XLF', 'XLE', 'OIH', 'PPH',
--------	'GLD', 'RTH', '86330E74', 'XLB', '86330e64', 'EWZ', '46428723', '46428719', 'DIA',
--------	'91232N10', '46428743', '46428745', 'SMH', 'XLK', 'IYR', '46428851', '73936T55', '46428Q10', '46428722',
--------	'XLY', 'XLP', 'XLV', 'XLI', '86330E78', '25459Y56','25459Y61','25459W25','25459W23','22542D53',
--------    '22542D57','22542D54','22542D58','25459W29','25459W28','IWD','IWF','86330E62','46432F39','73937B77'

--lets get list of securities and their codes
INSERT INTO @SecurityTable (Id, Code)
SELECT
	S.Id,
	M.VenCode As Code

FROM
	SecMstrX S
JOIN
	SecMapX M
		ON S.SecCode = M.SecCode
		AND VenType = 1
		AND Exchange = 1
WHERE
	S.Id IN (
	'QQQ', 'SPY', 'MDY', 'IWM', 'XLU', 'XLF', 'XLE', 'OIH', 'PPH',
	'GLD', 'RTH', '86330E74', 'XLB', '86330e64', 'EWZ', '46428723', '46428719', 'DIA',
	'91232N10', '46428743', '46428745', 'SMH', 'XLK', 'IYR', '46428851', '73936T55', '46428Q10', '46428722',
	'XLY', 'XLP', 'XLV', 'XLI', '86330E78', '25459Y56','25459Y61','25459W25','25459W23','22542D53',
    '22542D57','22542D54','22542D58','25459W29','25459W28','IWD','IWF','86330E62','46432F39','73937B77');


----transaction records
SELECT
	ID,
	D.DATE_  As [Date_],
	D.Close_* A.FACTOR AS [Close],

	D2.Date_ AS MonthEndDate,
	D2.Close_* A.FACTOR AS MonthEndClose,
	D2.TotRet AS MonthEndTotRet,

	D3.Date_ AS PriorMonthDate,
	D3.Close_* A.FACTOR PriorMonthCLose,
	D3.TotRet PriorTotRet,
		

	--(D2.TotRet - D3.TotRet) / D3.TotRet As MTotRet,

	CASE WHEN
		D.DATE_ = D2.Date_
			THEN 1
	ELSE
		0
	END AS Flag
INTO #RS3
FROM
	@SecurityTable S
JOIN
	PRC.PRCINFO I      
		ON I.CODE = S.CODE
LEFT JOIN
	PRC.PRCDLY D
		ON I.CODE = D.CODE
JOIN
	PRC.PRCADJ A 
		ON A.CODE = D.CODE
		AND D.DATE_ BETWEEN A.STARTDATE AND ISNULL(A.ENDDATE, '2079-06-05')
		AND A.ADJTYPE = 1
LEFT JOIN
	PRC.PRCDLY D2
		ON I.CODE = D2.CODE
		AND D2.Date_ =
			(
				SELECT
					MAX(DATE_)
				FROM
					PRC.PRCDLY
				WHERE
					CODE = D2.CODE
					AND DATE_< = EOMONTH(D.Date_)
                    --AND DATE_ IN (Select Date_ From @TradingDates)
			)
LEFT JOIN
	PRC.PRCDLY D3
		ON I.CODE = D3.CODE
		AND D3.Date_ =
			(
				SELECT
					MAX(DATE_)
				FROM
					PRC.PRCDLY
				WHERE
					CODE = D3.CODE
					AND DATE_ <= EOMONTH(DATEADD(M, -1, D.Date_))
			)
WHERE
	D.Date_ IN (Select Date_ From @TradingDates)




  
--clean up previous list
DELETE FROM @tblidnames;
insert into @tblidnames values('QQQ'),('SPY'),('MDY'),('IWM'),('XLU'),('XLF'),('XLE'),('OIH'),('PPH'),
	('GLD'), ('RTH'),('86330E74'),('XLB'),('86330e64'),('EWZ'),('46428723'),('46428719'),('DIA'),
	('91232N10'),('46428743'),('46428745'),('SMH'),('XLK'),('IYR'),('46428851'),('73936T55'),('46428Q10'),('46428722'),
	('XLY'),('XLP'),('XLV'),('XLI'),('86330E78'),('25459Y56'),('25459Y61'),('25459W25'),('25459W23'),('22542D53'),
    ('22542D57'),('22542D54'),('22542D58'),('25459W29'),('25459W28'),('IWD'),('IWF'),('86330E62'),('46432F39'),('73937B77');



--pull down begins here
BEGIN 
	--select all the IDNAMES
	SET @idcursor=CURSOR FOR SELECT idtickers FROM @tblidnames
	OPEN @idcursor
	FETCH NEXT from @idcursor into @curridname
	WHILE @@FETCH_STATUS = 0
		BEGIN 
			----for each IDNAME
				SET  @MyID=@curridname
				SET @prevClose=NULL
				BEGIN
				----extract all records for current IDNAME 
				SET @RSCursor = CURSOR FOR SELECT ID,Date_,[Close],MonthEndDate,MonthEndClose,MonthEndTotRet,PriorMonthDate,PriorMonthClose,PriorTotRet
					FROM #RS3
					WHERE ID=@MyID 
					ORDER BY Date_ 

					--looping through each record from the above set
					OPEN @RSCursor
					--go to next record
					FETCH NEXT from @RSCursor into @MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currMonthEndTotRet,@currPriorMonthEndDate,@currPriorMonthEndClose,@currPriorMonthEndTotRet--as is values pick up
						WHILE @@FETCH_STATUS = 0
						BEGIN 
							
							---adding date between curr and nextdate and pulling down values from current-date record
							FETCH NEXT from @RSCursor into @MyID,@nextDate,@nextClose,@nextMonthEndDate,@nextMonthEndClose,@nextMonthEndTotRet,@nextPriorMonthEndDate,@nextPriorMonthEndClose,@nextPriorMonthEndTotRet--as is next values picked up
						
				
								WHILE(@currDate < @nextDate) ---for in-between dates JUST pull down previous values
					
									BEGIN
												SET @divisor = ( 
														CASE 
															WHEN @currPriorMonthEndTotRet=0 OR @currPriorMonthEndTotRet IS NULL THEN 1
															ELSE 
																 @currPriorMonthEndTotRet
														END
											)
										
										SET @calMyMTotRet = (@currMonthEndTotRet - @currPriorMonthEndTotRet)/@divisor									
									
										--insert record @tblPulledDownValues2 table

									 	INSERT INTO @MtblPulledDownValues4(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyMonthEndTotRet,MyPriorMonthEndDate,MyPriorMonthEndClose,MyPriorMonthEndTotRet,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currMonthEndTotRet,@currPriorMonthEndDate,@currPriorMonthEndClose,@currPriorMonthEndTotRet,@calMyMTotRet)
										
										SET @currDate=DATEADD(day,1,@currDate) --incrementing the currdate to next
										---during pull down MTotRet needs to be calculated. so the curr val become prev 
										--in-between values will be previous ones
										--SET @prevClose=@currClose
										--set @prevMonthEndDate= @currMonthEndDate
										--set @prevMonthEndClose = @currMonthEndClose
										--set @prevPriorMonthEndDate = @currPriorMonthEndDate
										--set @prevPriorMonthEndClose = @currPriorMonthEndClose

																		
									END

								--SET @prevClose=@currClose
								----SET @prevMTotRetA=@currMTotRetA
								------Print 'Next loop...'
								--SET @currDate=@nextDate
								--SET @currClose=@nextClose
								--SET @currMTotRetA=@nextMTotRetA

										--SET @prevClose=@currClose
										SET @currDate=@nextDate
										SET @currClose=@nextClose
								 							 
										set @currMonthEndDate= @nextMonthEndDate
										set @currMonthEndClose= @nextMonthEndClose
										set @currMonthEndTotRet= @nextMonthEndTotRet

										set @currPriorMonthEndDate = @nextPriorMonthEnddate
										set @currPriorMonthEndClose = @nextPriorMonthEndClose
										set @currPriorMonthEndTotRet = @nextPriorMonthEndTotRet
						


						END
						--for last value
						
											SET @divisor = ( 
														CASE 
															WHEN @currPriorMonthEndTotRet=0 OR @currPriorMonthEndTotRet IS NULL THEN 1
															ELSE 
																 @currPriorMonthEndTotRet
														END
											)
										
										SET @calMyMTotRet = (@currMonthEndTotRet - @currPriorMonthEndTotRet)/@divisor									
									
										--insert record @tblPulledDownValues2 table

									 	INSERT INTO @MtblPulledDownValues4(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyMonthEndTotRet,MyPriorMonthEndDate,MyPriorMonthEndClose,MyPriorMonthEndTotRet,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currMonthEndTotRet,@currPriorMonthEndDate,@currPriorMonthEndClose,@currPriorMonthEndTotRet,@calMyMTotRet)
										
					DEALLOCATE @RSCursor;
				
				END;

			--PRINT 'done with' + @curridname

			FETCH NEXT from @idcursor into @curridname
			
		END
	CLOSE @idcursor;
	DEALLOCATE @idcursor;

END;

--store all these trading dates & pulled down values for missing dates records in tempvariable; 
SELECT * INTO #temptblPulledDownValues3 FROM  @MtblPulledDownValues4;




--update final table 
-- QQQ
UPDATE @FinalOutput
SET
	QQQ = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PQQQ = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'QQQ'

-- SPY
UPDATE @FinalOutput
SET
	SPY = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PSPY = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'SPY'

-- MDY
UPDATE @FinalOutput
SET
	MDY = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PMDY = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'MDY'

-- XLU
UPDATE @FinalOutput
SET
	XLU = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXLU = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'XLU'

-- XLF
UPDATE @FinalOutput
SET
	XLF = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXLF = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'XLF'

-- XLE
UPDATE @FinalOutput
SET
	XLE = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXLE = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'XLE'

---- OIH
--UPDATE @FinalOutput
--SET
--	OIH = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
--	POIH = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
--FROM
--	@FinalOutput T1
--  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'OIH'

---- PPH
--UPDATE @FinalOutput
--SET
--	PPH = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
--	PPPH = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
--FROM
--	@FinalOutput T1
--  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'PPH'

---- RTH
--UPDATE @FinalOutput
--SET
--	RTH = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
--	PRTH = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
--FROM
--	@FinalOutput T1
--  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'RTH'

-- GLD
UPDATE @FinalOutput
SET
	GLD = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PGLD = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'GLD'

-- XHB -- 86330E74
UPDATE @FinalOutput
SET
	XHB = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXHB = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '86330E74'

-- XLB
UPDATE @FinalOutput
SET
	XLB = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXLB = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'XLB'

-- IWM
UPDATE @FinalOutput
SET
	IWM = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PIWM = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'IWM'

-- XME -- 86330e64
UPDATE @FinalOutput
SET
	XME = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXME = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '86330e64'

-- EEM -- 46428723
UPDATE @FinalOutput
SET
	EEM = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PEEM = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '46428723'

-- EWZ
UPDATE @FinalOutput
SET
	EWZ = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PEWZ = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'EWZ'

-- IYT -- 46428719
UPDATE @FinalOutput
SET
	IYT = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PIYT = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '46428719'

-- DIA
UPDATE @FinalOutput
SET
	DIA = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PDIA = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'DIA'

-- USO -- 91232N10
UPDATE @FinalOutput
SET
	USO = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PUSO = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '91232N10'

-- TLT -- 46428743
UPDATE @FinalOutput
SET
	TLT = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PTLT = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '46428743'

-- SHY -- 46428745
UPDATE @FinalOutput
SET
	SHY = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PSHY = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '46428745'

---- SMH
--UPDATE @FinalOutput
--SET
--	SMH = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
--	PSMH = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
--FROM
--	@FinalOutput T1
--  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'SMH'

-- XLK
UPDATE @FinalOutput
SET
	XLK = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXLK = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'XLK'

-- IYR
UPDATE @FinalOutput
SET
	IYR = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PIYR = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'IYR'

-- HYG -- 46428851
UPDATE @FinalOutput
SET
	HYG = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PHYG = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '46428851'

-- PHB -- 73936T55
UPDATE @FinalOutput
SET
	PHB = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PPHB = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '73936T55'

-- SLV -- 46428Q10
UPDATE @FinalOutput
SET
	SLV = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PSLV = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '46428Q10'

-- AGG -- 46428722
UPDATE @FinalOutput
SET
	AGG = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PAGG = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '46428722'

-- XLY
UPDATE @FinalOutput
SET
	XLY = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXLY = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'XLY'

-- XLP
UPDATE @FinalOutput
SET
	XLP = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXLP = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'XLP'

-- XLV
UPDATE @FinalOutput
SET
	XLV = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXLV = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'XLV'

-- XLI
UPDATE @FinalOutput
SET
	XLI = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXLI = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'XLI'

-- KIE -- 86330E78
UPDATE @FinalOutput
SET
	KIE = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PKIE = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '86330E78'

--JDST -- 25459Y56
UPDATE @FinalOutput
SET
	JDST = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PJDST = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '25459Y56'

--JNUG -- 25459Y61
UPDATE @FinalOutput
SET
	JNUG = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PJNUG = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '25459Y61'

--NUGT -- 25459W25
UPDATE @FinalOutput
SET
	NUGT = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PNUGT = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '25459W25'

--DUST -- 25459W23
UPDATE @FinalOutput
SET
	DUST = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PDUST = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '25459W23'

--DGAZ -- 22542D53
UPDATE @FinalOutput
SET
	DGAZ = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PDGAZ = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '22542D53'

--UGAZ -- 22542D57
UPDATE @FinalOutput
SET
	UGAZ = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PUGAZ = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '22542D57'

--DWTI -- 22542D54
UPDATE @FinalOutput
SET
	DWTI = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PDWTI = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '22542D54'

--UWTI -- 22542D58
UPDATE @FinalOutput
SET
	UWTI = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PUWTI = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '22542D58'

--RUSL -- 25459W29
UPDATE @FinalOutput
SET
	RUSL = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PRUSL = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '25459W29'

--RUSS -- 25459W28
UPDATE @FinalOutput
SET
	RUSS = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PRUSS = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '25459W28'

--IWD 
UPDATE @FinalOutput
SET
	IWD = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PIWD = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'IWD'

--IWF 
UPDATE @FinalOutput
SET
	IWF = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PIWF = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'IWF'

--XOP -- 86330E62 
UPDATE @FinalOutput
SET
	XOP = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PXOP = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '86330E62'

--MTUM 
UPDATE @FinalOutput
SET
	MTUM = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PMTUM = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '46432F39'

--SPLV 
UPDATE @FinalOutput
SET
	SPLV = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PSPLV = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues3 T2 ON T2.Date_ = T1.Date_ AND T2.Id = '73937B77'



--SELECT * FROM  #RS3 
DROP TABLE #RS3;
--select * FROM #temptblPulledDownValues3 
DROP TABLE #temptblPulledDownValues3;
DELETE FROM @MtblPulledDownValues4;




------#############################################################################################################
------IDNAMES: ('TU_FUT0', 'TU_FUT1', 'FV_FUT0', 'FV_FUT1', 'TY_FUT0', 'TY_FUT1', 'ED_FUT0', 'ED_FUT1', 'CL_FUT0')

SELECT
	S.Ticker, DX.InfoCode, DX.Date_, DX.Close_
INTO
	#CMD
FROM
	CMDaily D
JOIN
	CMDaily DX
		ON DX.InfoCode = D.InfoCode
		AND DX.Date_ = D.Date_
		AND DX.[Contract] = D.[Contract]
		AND DX.[Contract] = (
								SELECT
									MIN([Contract])
								FROM
									CMDaily
								WHERE
									Infocode = DX.InfoCode
									AND Date_ = DX.Date_
							)
JOIN
	CMSeries S ON S.InfoCode = D.InfoCode AND S.Ticker IN
	('TU_FUT0', 'TU_FUT1', 'FV_FUT0', 'FV_FUT1', 'TY_FUT0', 'TY_FUT1', 'ED_FUT0', 'ED_FUT1', 'CL_FUT0');
--WHERE
		--D.DATE_ BETWEEN DateAdd(M, -1, @StartDate) AND @EndDate;

SELECT
	D.Ticker AS ID,
	D.Date_ AS Date_,
	D.Close_ AS [Close],
	D2.Date_ AS MonthEndDate,
	D2.Close_ AS MonthEndClose,

	D3.Date_ AS PriorMonthEndDate,
	D3.Close_ AS PriorMonthEndClose,

	--(D2.Close_ - D3.Close_) / D3.Close_ AS MTotRet,

	CASE WHEN
		D.DATE_ = D2.Date_
			THEN 1
	ELSE
		0
	END AS Flag
INTO
	#RS4
FROM
	#CMD D
JOIN
	#CMD D2
		ON D2.InfoCode = D.InfoCode
		AND D2.Date_ =
			(
				SELECT
					MAX(DATE_)
				FROM
					#CMD
				WHERE
					InfoCode = D.InfoCode
					AND DATE_ <= EOMONTH(D.Date_)
                    --AND DATE_ IN (Select Date_ From @TradingDates)
			)
LEFT JOIN
	#CMD D3
		ON D3.InfoCode = D.InfoCode
		AND D3.Date_ =
			(
				SELECT
					MAX(DATE_)
				FROM
					#CMD
				WHERE
					InfoCode = D.InfoCode
					AND DATE_ <= EOMONTH(DATEADD(M, -1, D.Date_))
			)
WHERE
	D.Date_ IN (SELECT DATE_ FROM @TradingDates);


DELETE FROM @tblidnames;
insert into @tblidnames values('TU_FUT0'),('TU_FUT1'),('FV_FUT0'),('FV_FUT1'),('TY_FUT0'),('TY_FUT1'), 
('ED_FUT0'),('ED_FUT1'),('CL_FUT0');


--pull down begins here
BEGIN 
	--select all the IDNAMES
	SET @idcursor=CURSOR FOR SELECT idtickers FROM @tblidnames
	OPEN @idcursor
	FETCH NEXT from @idcursor into @curridname
	WHILE @@FETCH_STATUS = 0
		BEGIN 
			----for each IDNAME
				SET  @MyID=@curridname
				SET @prevClose=NULL
				BEGIN
				----extract all records for current IDNAME 
				SET @RSCursor = CURSOR FOR SELECT ID,Date_,[Close],MonthEndDate,MonthEndClose,PriorMonthEndDate,PriorMonthEndClose
					FROM #RS4
					WHERE ID=@MyID 
					ORDER BY Date_ 

					--looping through each record from the above set
					OPEN @RSCursor
					--go to next record
					FETCH NEXT from @RSCursor into @MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate,@currPriorMonthEndClose--as is values pick up
						WHILE @@FETCH_STATUS = 0
						BEGIN 
							
							---adding date between curr and nextdate and pulling down values from current-date record
							FETCH NEXT from @RSCursor into @MyID,@nextDate,@nextClose,@nextMonthEndDate,@nextMonthEndClose,@nextPriorMonthEndDate,@nextPriorMonthEndClose--as is next values picked up
						
				
								WHILE(@currDate < @nextDate) ---for in-between dates JUST pull down previous values
					
									BEGIN
												SET @divisor = ( 
														CASE 
															WHEN @currPriorMonthEndClose=0 OR @currPriorMonthEndClose IS NULL THEN 1
															ELSE 
																 @currPriorMonthEndClose
														END
											)
										
										SET @calMyMTotRet = (@currMonthEndClose - @currPriorMonthEndClose)/@divisor 									
									
										--insert record @tblPulledDownValues2 table

									 	INSERT INTO @MtblPulledDownValues1(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyPriorMonthEndDate,MyPriorMonthEndClose,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate,@currPriorMonthEndClose,@calMyMTotRet)
										
										SET @currDate=DATEADD(day,1,@currDate) --incrementing the currdate to next
										---during pull down MTotRet needs to be calculated. so the curr val become prev 
										--in-between values will be previous ones
										--SET @prevClose=@currClose
										--set @prevMonthEndDate= @currMonthEndDate
										--set @prevMonthEndClose = @currMonthEndClose
										--set @prevPriorMonthEndDate = @currPriorMonthEndDate
										--set @prevPriorMonthEndClose = @currPriorMonthEndClose

																		
									END

								--SET @prevClose=@currClose
								----SET @prevMTotRetA=@currMTotRetA
								------Print 'Next loop...'
								--SET @currDate=@nextDate
								--SET @currClose=@nextClose
								--SET @currMTotRetA=@nextMTotRetA

										--SET @prevClose=@currClose
										SET @currDate=@nextDate
										SET @currClose=@nextClose
								 							 
										set @currMonthEndDate= @nextMonthEndDate
										set @currMonthEndClose= @nextMonthEndClose

										set @currPriorMonthEndDate = @nextPriorMonthEndDate
										set @currPriorMonthEndClose = @nextPriorMonthEndClose
						


						END
						--for last value
						
										SET @divisor = ( 
														CASE 
															WHEN @currPriorMonthEndClose=0 OR @currPriorMonthEndClose IS NULL THEN 1
															ELSE 
																 @currPriorMonthEndClose
														END
											)
										
										SET @calMyMTotRet = (@currMonthEndClose - @currPriorMonthEndClose)/@divisor 									
									
										--insert record @tblPulledDownValues2 table

									 	INSERT INTO @MtblPulledDownValues1(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyPriorMonthEndDate,MyPriorMonthEndClose,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate,@currPriorMonthEndClose,@calMyMTotRet)
										
										
					
					CLOSE @RSCursor;
					DEALLOCATE @RSCursor;
				
				END;

			--PRINT 'done with' + @curridname

			FETCH NEXT from @idcursor into @curridname
			
		END
	CLOSE @idcursor;
	DEALLOCATE @idcursor;

END;

--store all these trading dates & pulled down values for missing dates records in tempvariable; 
SELECT * INTO #temptblPulledDownValues4 FROM  @MtblPulledDownValues1;

--update final table
-- TU -- TU_FUT0
UPDATE @FinalOutput
SET
	TU = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PTU = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues4 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'TU_FUT0'

-- TU_B -- TU_FUT1
UPDATE @FinalOutput
SET
	TU_B = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PTU_B = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues4 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'TU_FUT1'

-- FV5 -- FV_FUT0
UPDATE @FinalOutput
SET
	FV5 = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PFV5 = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues4 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'FV_FUT0'

-- FV5_B -- FV_FUT1
UPDATE @FinalOutput
SET
	FV5_B = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PFV5_B = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues4 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'FV_FUT1'

-- TY10 -- TY_FUT0
UPDATE @FinalOutput
SET
	TY10 = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PTY10 = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues4 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'TY_FUT0'

-- TY10_B -- TY_FUT1
UPDATE @FinalOutput
SET
	TY10_B = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PTY10_B = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues4 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'TY_FUT1'

-- ED -- ED_FUT0
UPDATE @FinalOutput
SET
	[ED] = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PED = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues4 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'ED_FUT0'

-- ED_B -- ED_FUT1
UPDATE @FinalOutput
SET
	ED_B = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PED_B = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues4 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'ED_FUT1'

-- CL -- CL_FUT0
UPDATE @FinalOutput
SET
	CL = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PCL = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues4 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'CL_FUT0'


DROP TABLE #CMD;
--SELECT * FROM #RS4
DROP TABLE #RS4;
--SELECT * FROM #temptblPulledDownValues4;
DROP TABLE #temptblPulledDownValues4;
DELETE FROM @MtblPulledDownValues1;



------########################################################################################################
------IDNAMES: #RS2 ('VIX_IDX', 'RVXK_IDX')

SELECT
	I.Ticker AS ID,
	D.Date_ AS Date_,
	D.Close_ As [Close], --extra
	D2.Date_ AS MonthEndDate,
	D2.Close_ MonthEndClose,
	CASE WHEN
		D.DATE_ = D2.Date_
			THEN 1
	ELSE
		0
	END AS Flag
INTO
	#RS5
FROM
	IdxInfo I      
LEFT JOIN
	IdxDaily D
		ON D.CODE = I.CODE
LEFT JOIN
	IdxDaily D2
		ON D2.CODE = I.CODE
		AND D2.Date_ =
			(
				SELECT
					MAX(DATE_)
				FROM
					IdxDaily
				WHERE
					CODE = I.CODE
					AND DATE_ <= EOMONTH(D.Date_)
                    --AND DATE_ IN (Select Date_ From @TradingDates)
			)
WHERE
	I.Ticker IN ('VIX_IDX', 'RVXK_IDX') 
	AND
	D.Date_ IN (Select Date_ From @TradingDates)



--code for pulling down values 
DELETE FROM @tblidnames;
insert into @tblidnames values('VIX_IDX'),('RVXK_IDX');


--pull down begins here
BEGIN 
	--select all the IDNAMES
	SET @idcursor=CURSOR FOR SELECT idtickers FROM @tblidnames
	OPEN @idcursor
	FETCH NEXT from @idcursor into @curridname
	WHILE @@FETCH_STATUS = 0
		BEGIN 
			----for each IDNAME
				SET  @MyID=@curridname
				SET @prevClose=NULL
				BEGIN
				----extract all records for current IDNAME 
				SET @RSCursor = CURSOR FOR SELECT ID,Date_,[Close],MonthEndDate,MonthEndClose
					FROM #RS5
					WHERE ID=@MyID 
					ORDER BY Date_ 

					--looping through each record from the above set
					OPEN @RSCursor
					--go to next record
					FETCH NEXT from @RSCursor into @MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose--as is values pick up
						WHILE @@FETCH_STATUS = 0
						BEGIN 
							
							---adding date between curr and nextdate and pulling down values from current-date record
							FETCH NEXT from @RSCursor into @MyID,@nextDate,@nextClose,@nextMonthEndDate,@nextMonthEndClose--as is next values picked up
						
				
								WHILE(@currDate < @nextDate) ---for in-between dates JUST pull down previous values
					
									BEGIN
												
										SET @calMyMTotRet = NULL								
									
										--insert record @tblPulledDownValues2 table

									 	INSERT INTO @MtblPulledDownValues6(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@calMyMTotRet)
										
										SET @currDate=DATEADD(day,1,@currDate) --incrementing the currdate to next
										---during pull down MTotRet needs to be calculated. so the curr val become prev 
										--in-between values will be previous ones
										--SET @prevClose=@currClose
										--set @prevMonthEndDate= @currMonthEndDate
										--set @prevMonthEndClose = @currMonthEndClose
										--set @prevPriorMonthEndDate = @currPriorMonthEndDate
										--set @prevPriorMonthEndClose = @currPriorMonthEndClose

																		
									END

								--SET @prevClose=@currClose
								----SET @prevMTotRetA=@currMTotRetA
								------Print 'Next loop...'
								--SET @currDate=@nextDate
								--SET @currClose=@nextClose
								--SET @currMTotRetA=@nextMTotRetA

										--SET @prevClose=@currClose
										SET @currDate=@nextDate
										SET @currClose=@nextClose
								 							 
										set @currMonthEndDate= @nextMonthEndDate
										set @currMonthEndClose= @nextMonthEndClose

								
						


						END
						--for last value
						
										SET @calMyMTotRet = NULL								
									
										--insert record @tblPulledDownValues2 table

									 	INSERT INTO @MtblPulledDownValues6(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@calMyMTotRet)
										
					
					CLOSE @RSCursor;
					DEALLOCATE @RSCursor;
				
				END;

			--PRINT 'done with' + @curridname

			FETCH NEXT from @idcursor into @curridname
			
		END
	CLOSE @idcursor;
	DEALLOCATE @idcursor;

END;

--store all these trading dates & pulled down values for missing dates records in tempvariable; 
SELECT * INTO #temptblPulledDownValues5 FROM  @MtblPulledDownValues6;


--Final updates
-- VIX_IDX
UPDATE @FinalOutput
SET
	VIX = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues5 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'VIX_IDX'

-- RVXK_IDX
UPDATE @FinalOutput
SET
	RVIX = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues5 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'RVXK_IDX'


--SELECT * FROM #RS5
DROP TABLE #RS5;
--SELECT * FROM #temptblPulledDownValues5;
DROP TABLE #temptblPulledDownValues5;
DELETE FROM @MtblPulledDownValues6;






--------######################################################################################################################
--------IDNAMES: #RS3 ('SPX_IDX', 'RUA_IDX', 'RUA_D_IDX', 'RUT_IDX', 'RUI_IDX', 'RUI_D_IDX', 'DJX_IDX')
SELECT
	I.Ticker AS ID,
	D.Date_ AS Date_,
	D.Close_ AS [Close],
	D2.Date_ AS MonthEndDate,
	D2.Close_ AS MonthEndClose,

	D3.Date_ AS PriorMonthEndDate,
	D3.Close_ AS PriorMonthEndClose,

	--(D2.Close_ - D3.Close_) / D3.Close_ As MTotRet,

	CASE WHEN
		D.DATE_ = D2.Date_
			THEN 1
	ELSE
		0
	END AS Flag
INTO #RS6
FROM
	IdxInfo I      
LEFT JOIN
	IdxDaily D
		ON D.CODE = I.CODE
LEFT JOIN
	IdxDaily D2
		ON D2.CODE = I.CODE
		AND D2.Date_ =
			(
				SELECT
					MAX(DATE_)
				FROM
					IdxDaily
				WHERE
					CODE = I.CODE
					AND DATE_ <= EOMONTH(D.Date_)
                    --AND DATE_ IN (Select Date_ From @TradingDates)
			)
LEFT JOIN
	IdxDaily D3
		ON D3.CODE = I.CODE
		AND D3.Date_ =
			(
				SELECT
					MAX(DATE_)
				FROM
					IdxDaily
				WHERE
					CODE = I.CODE
					AND DATE_ <= EOMONTH(DATEADD(M, -1, D.Date_))
			)
WHERE
	I.Ticker IN ('SPX_IDX', 'RUA_IDX', 'RUA_D_IDX', 'RUT_IDX', 'RUI_IDX', 'RUI_D_IDX', 'DJX_IDX')
	AND
	D.Date_ IN (Select Date_ From @TradingDates);


   

--pull down values for intermediate days above
--clean up previous list
---code for pulling down values 
DELETE FROM @tblidnames;
insert into @tblidnames values('SPX_IDX'),('RUA_IDX'),('RUA_D_IDX'),('RUT_IDX'),('RUI_IDX'),('RUI_D_IDX'),('DJX_IDX');



--pull down begins here
BEGIN 
	--select all the IDNAMES
	SET @idcursor=CURSOR FOR SELECT idtickers FROM @tblidnames
	OPEN @idcursor
	FETCH NEXT from @idcursor into @curridname
	WHILE @@FETCH_STATUS = 0
		BEGIN 
			----for each IDNAME
				SET  @MyID=@curridname
				SET @prevClose=NULL
				BEGIN
				----extract all records for current IDNAME 
				SET @RSCursor = CURSOR FOR SELECT ID,Date_,[Close],MonthEndDate,MonthEndClose,PriorMonthEndDate,PriorMonthEndClose
					FROM #RS6
					WHERE ID=@MyID 
					ORDER BY Date_ 

					--looping through each record from the above set
					OPEN @RSCursor
					--go to next record
					FETCH NEXT from @RSCursor into @MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate ,@currPriorMonthEndClose--as is values pick up
						WHILE @@FETCH_STATUS = 0
						BEGIN 
							
							---adding date between curr and nextdate and pulling down values from current-date record
							FETCH NEXT from @RSCursor into @MyID,@nextDate,@nextClose,@nextMonthEndDate,@nextMonthEndClose,@nextPriorMonthEndDate,@nextPriorMonthEndClose--as is next values picked up
						
				
								WHILE(@currDate < @nextDate) ---for in-between dates JUST pull down previous values
					
									BEGIN
												SET @divisor = ( 
														CASE 
															WHEN @currPriorMonthEndClose=0 OR @currPriorMonthEndClose IS NULL THEN 1
															ELSE 
																 @currPriorMonthEndClose
														END
											)
										
										SET @calMyMTotRet = (@currMonthEndClose - @currPriorMonthEndClose)/@divisor 									
									
										--insert record @tblPulledDownValues2 table

									 	INSERT INTO @MtblPulledDownValues1(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyPriorMonthEndDate,MyPriorMonthEndClose,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate,@currPriorMonthEndClose,@calMyMTotRet)
										
										SET @currDate=DATEADD(day,1,@currDate) --incrementing the currdate to next
										---during pull down MTotRet needs to be calculated. so the curr val become prev 
										--in-between values will be previous ones
										--SET @prevClose=@currClose
										--set @prevMonthEndDate= @currMonthEndDate
										--set @prevMonthEndClose = @currMonthEndClose
										--set @prevPriorMonthEndDate = @currPriorMonthEndDate
										--set @prevPriorMonthEndClose = @currPriorMonthEndClose

																		
									END

								--SET @prevClose=@currClose
								----SET @prevMTotRetA=@currMTotRetA
								------Print 'Next loop...'
								--SET @currDate=@nextDate
								--SET @currClose=@nextClose
								--SET @currMTotRetA=@nextMTotRetA

										--SET @prevClose=@currClose
										SET @currDate=@nextDate
										SET @currClose=@nextClose
								 							 
										set @currMonthEndDate= @nextMonthEndDate
										set @currMonthEndClose= @nextMonthEndClose

										set @currPriorMonthEndDate = @nextPriorMonthEndDate
										set @currPriorMonthEndClose = @nextPriorMonthEndClose
						


						END
						--for last value
						
										SET @divisor = ( 
														CASE 
															WHEN @currPriorMonthEndClose=0 OR @currPriorMonthEndClose IS NULL THEN 1
															ELSE 
																 @currPriorMonthEndClose
														END
											)
										
										SET @calMyMTotRet = (@currMonthEndClose - @currPriorMonthEndClose)/@divisor 									
									
										--insert record @tblPulledDownValues2 table

									 	INSERT INTO @MtblPulledDownValues1(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyPriorMonthEndDate,MyPriorMonthEndClose,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate,@currPriorMonthEndClose,@calMyMTotRet)
										
										
					
					CLOSE @RSCursor;
					DEALLOCATE @RSCursor;
				
				END;

			--PRINT 'done with' + @curridname

			FETCH NEXT from @idcursor into @curridname
			
		END
	CLOSE @idcursor;
	DEALLOCATE @idcursor;

END;

--store all these trading dates & pulled down values for missing dates records in tempvariable; 
SELECT * INTO #temptblPulledDownValues6 FROM  @MtblPulledDownValues1;




--final updates
-- SPX_IDX
UPDATE @FinalOutput
SET
	SPX = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PSPX = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues6 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'SPX_IDX'

-- RUA_IDX
UPDATE @FinalOutput
SET
	RUA = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PRUA = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues6 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'RUA_IDX'

-- RUA_D_IDX
UPDATE @FinalOutput
SET
	RUAT = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PRUAT = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues6 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'RUA_D_IDX'

-- RUT_IDX
UPDATE @FinalOutput
SET
	RUT = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PRUT = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues6 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'RUT_IDX'

-- RUI_IDX
UPDATE @FinalOutput
SET
	RUI = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PRUI = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues6 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'RUI_IDX'

-- RUI_D_IDX
UPDATE @FinalOutput
SET
	RUIT = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PRUIT = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues6 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'RUI_D_IDX'

-- DJX_IDX
UPDATE @FinalOutput
SET
	DJX = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PDJX = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValues6 T2 ON T2.Date_ = T1.Date_ AND T2.Id = 'DJX_IDX'

--Select * FROM #RS6
DROP TABLE #RS6;
--Select * FROM #temptblPulledDownValues6
DROP TABLE #temptblPulledDownValues6;
DELETE FROM @MtblPulledDownValues1;


------############################################################################################################################
------IDNAMES: #HUI

SELECT
		 DD.DATE_, 
		 DD.CLOSE_ AS [CLOSE],
		 --LAG(CLOSE_) OVER (PARTITION BY DD.CODE ORDER BY DD.CODE, DATE_) AS PRIORCLOSE
		 DD2.Date_ AS MonthEndDate,
		 DD2.Close_ AS MonthEndClose,
		 DD3.Date_ AS PriorMonthEndDate,
		 DD3.Close_ AS PriorMonthEndClose,
		 CASE WHEN
				DD.DATE_ = DD2.Date_
					THEN 1
		ELSE
				0
		END AS Flag
INTO #HUI
FROM SECMSTRX AA
JOIN SECMAPX BB
    ON AA.SECCODE = BB.SECCODE
    AND BB.VENTYPE = 1
JOIN PRC.PRCINFO CC
    ON CC.CODE = BB.VENCODE
JOIN PRC.PRCIDX DD
    ON DD.CODE = CC.CODE
LEFT JOIN
	PRC.PRCIDX  DD2
		ON CC.CODE = DD2.CODE
		AND DD2.Date_ =
			(
				SELECT
					MAX(DATE_)
				FROM
					PRC.PRCIDX
				WHERE
					CODE = DD2.CODE
					AND DATE_< = EOMONTH(DD.Date_)
                    --AND DATE_ IN (Select Date_ From @TradingDates)
			)
LEFT JOIN
	PRC.PRCIDX DD3
		ON CC.CODE = DD3.CODE
		AND DD3.Date_ =
			(
				SELECT
					MAX(DATE_)
				FROM
					PRC.PRCIDX
				WHERE
					CODE = DD3.CODE
					AND DATE_ <= EOMONTH(DATEADD(M, -1, DD.Date_))
			)
WHERE
	DD.Date_ IN (Select Date_ From @TradingDates) AND 
	AA.NAME LIKE 'NYSE ARCA GOLD BUGS EQUAL-$ WEIGHT'
    AND AA.TYPE_ = 1




--code for pulling down values 
DELETE FROM @tblidnames;


SET  @MyID='HUI';
BEGIN	
SET @RSCursor = CURSOR FOR SELECT Date_,[Close],MonthEndDate,MonthEndClose,PriorMonthEndDate,PriorMonthEndClose
					FROM #HUI
					ORDER BY Date_ 

					--looping through each record from the above set
					OPEN @RSCursor
					--go to next record
					FETCH NEXT from @RSCursor into @currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate ,@currPriorMonthEndClose--as is values pick up
						WHILE @@FETCH_STATUS = 0
						BEGIN 
							
							---adding date between curr and nextdate and pulling down values from current-date record
							FETCH NEXT from @RSCursor into @nextDate,@nextClose,@nextMonthEndDate,@nextMonthEndClose,@nextPriorMonthEndDate,@nextPriorMonthEndClose--as is next values picked up
								
								WHILE(@currDate < @nextDate) ---for in-between dates JUST pull down previous values
					
									BEGIN
												SET @divisor = ( 
														CASE 
															WHEN @currPriorMonthEndClose=0 OR @currPriorMonthEndClose IS NULL THEN 1
															ELSE 
																 @currPriorMonthEndClose
														END
											)
										
										SET @calMyMTotRet = (@currClose - @currPriorMonthEndClose)/@divisor 									
									
										--insert record @tblPulledDownValues2 table

									 	INSERT INTO @MtblPulledDownValues1(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyPriorMonthEndDate,MyPriorMonthEndClose,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate,@currPriorMonthEndClose,@calMyMTotRet)
										
										SET @currDate=DATEADD(day,1,@currDate) --incrementing the currdate to next
										---during pull down MTotRet needs to be calculated. so the curr val become prev 
										--in-between values will be previous ones
										--SET @prevClose=@currClose
										--set @prevMonthEndDate= @currMonthEndDate
										--set @prevMonthEndClose = @currMonthEndClose
										--set @prevPriorMonthEndDate = @currPriorMonthEndDate
										--set @prevPriorMonthEndClose = @currPriorMonthEndClose

																		
									END

								--SET @prevClose=@currClose
								----SET @prevMTotRetA=@currMTotRetA
								------Print 'Next loop...'
								--SET @currDate=@nextDate
								--SET @currClose=@nextClose
								--SET @currMTotRetA=@nextMTotRetA

										--SET @prevClose=@currClose
										SET @currDate=@nextDate
										SET @currClose=@nextClose
								 							 
										set @currMonthEndDate= @nextMonthEndDate
										set @currMonthEndClose= @nextMonthEndClose

										set @currPriorMonthEndDate = @nextPriorMonthEndDate
										set @currPriorMonthEndClose = @nextPriorMonthEndClose
						


						END
						--for last value
						
										SET @divisor = ( 
														CASE 
															WHEN @currPriorMonthEndClose=0 OR @currPriorMonthEndClose IS NULL THEN 1
															ELSE 
																 @currPriorMonthEndClose
														END
											)
										
										SET @calMyMTotRet = (@currClose - @currPriorMonthEndClose)/@divisor 									
									
										--insert record @tblPulledDownValues2 table

									 	INSERT INTO @MtblPulledDownValues1(ID,Date_,MyClose,MyMonthEndDate,MyMonthEndClose,MyPriorMonthEndDate,MyPriorMonthEndClose,MyMTotRet)  
										VALUES(@MyID,@currDate,@currClose,@currMonthEndDate,@currMonthEndClose,@currPriorMonthEndDate,@currPriorMonthEndClose,@calMyMTotRet)
										
										
					
					CLOSE @RSCursor;
					DEALLOCATE @RSCursor;
				
	END;

		


----store all these trading dates & pulled down values for missing dates records in tempvariable;  
SELECT * INTO #temptblPulledDownValueshui FROM  @MtblPulledDownValues1;


--final updates
--HUI 
UPDATE @FinalOutput
SET
	--HUI = ISNULL(CAST(Round((T2.[Close] - T2.PRIORCLOSE) / T2.PRIORCLOSE, 5) As Varchar(31)), '.'),
	HUI = ISNULL(CAST(Round(T2.MyMTotRet, 5) As Varchar(31)), '.'),
	PHUI = ISNULL(CAST(Round(T2.MyClose, 5) As Varchar(31)), '.')
FROM
	@FinalOutput T1
  JOIN #temptblPulledDownValueshui T2 ON T2.Date_ = T1.Date_ 


--SELECT * FROM #HUI
DROP TABLE #HUI;
--SELECT * FROM #temptblPulledDownValueshui;
DROP TABLE #temptblPulledDownValueshui;
DELETE FROM @MtblPulledDownValues7;

------------######################################################################################################################################


SELECT FORMAT(Date_, 'MM/dd/yyyy') AS [Date], * 
    INTO  #RS7
    FROM @FinalOutput;
    
ALTER TABLE #RS7 DROP COLUMN Date_;

SELECT * FROM #RS7
Order by MonthNumber;
DROP TABLE #RS7;
DROP TABLE #Tradedates;
--SELECT * FROM #HUI





