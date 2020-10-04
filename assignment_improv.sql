-- ***********************************************************
-- Create the Schema Share_prediction
-- ***********************************************************

CREATE SCHEMA Share_prediction;

-- ***********************************************************
-- Create the website_visits table
-- ***********************************************************
DROP TABLE IF EXISTS `website_visits` ;

CREATE TABLE `website_visits` (
  `ID` int(11) NOT NULL,
  `CUSTOMER_NAME` varchar(255) DEFAULT NULL,
  `CUSTOMER_TYPE` int(11) DEFAULT NULL,
  `DATE_STARTED` datetime DEFAULT NULL,
  `DURATION` double DEFAULT NULL,
  `GENDER` varchar(45) DEFAULT NULL,
  `AGE` int(11) DEFAULT NULL,
  `SALARY` int(11) DEFAULT NULL,
  `REVIEW_DURATION` double DEFAULT NULL,
  `RELATED_DURATION` double DEFAULT NULL,
  `PURCHASED` int(11) DEFAULT 0,
  `IS_MALE` int(11) DEFAULT NULL,
  `IS_FEMALE` int(11) DEFAULT NULL,
  `VIEWED_REVIEW` int(11) DEFAULT NULL,
  `VIEWED_RELATED` int(11) DEFAULT NULL,
  `AGE_RANGE` varchar(255) DEFAULT NULL,
  `SALARY_RANGE` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


-- checking the TABLE after creatiON
SELECT * FROM bajajauto;
SELECT * FROM eichermotors;
SELECT * FROM heromotocorp;
SELECT * FROM infosys;
SELECT * FROM tcs;
SELECT * FROM tvsmotors;

-- checking the data type of the TABLE
DESCRIBE bajajauto;
DESCRIBE eichermotors;
DESCRIBE heromotocorp;
DESCRIBE infosys;
DESCRIBE tcs;
DESCRIBE tvsmotors;

-- changing the format before changing it to date type
UPDATE bajajauto
SET date = STR_TO_DATE(`date`,'%d-%M-%Y');

UPDATE eichermotors
SET date = STR_TO_DATE(`date`,'%d-%M-%Y');

UPDATE heromotocorp
SET date = STR_TO_DATE(`date`,'%d-%M-%Y');

UPDATE infosys
SET date = STR_TO_DATE(`date`,'%d-%M-%Y');

UPDATE tcs
SET date = STR_TO_DATE(`date`,'%d-%M-%Y');

UPDATE tvsmotors
SET date = STR_TO_DATE(`date`,'%d-%M-%Y');


-- changing the date column to  data type FROM text type for all the TABLEs .
ALTER TABLE bajajauto
MODIFY `date` date;

ALTER TABLE eichermotors
MODIFY `date` date;

ALTER TABLE heromotocorp
MODIFY `date` date;

ALTER TABLE infosys
MODIFY `date` date;

ALTER TABLE tcs
MODIFY `date` date;

ALTER TABLE tvsmotors
MODIFY `date` date;

--------------

-- Creating a procedure for creating tables 

DELIMITER $$
DROP PROCEDURE IF EXISTS TAB_CREATE;

CREATE PROCEDURE TAB_CREATE (IN tabname varchar(30), IN sourcetab varchar(30))
BEGIN
	SET @droptable = CONCAT ("DROP TABLE IF EXISTS ", tabname);
	PREPARE deletetb FROM @droptable;
	EXECUTE deletetb ; 
	DEALLOCATE PREPARE deletetb ;


	SET @createtable = CONCAT("CREATE TABLE ", tabname, " AS SELECT date , `Close Price`,
						avg(`close price`) OVER( ORDER BY date ROWS between 19 PRECEDING  AND CURRENT ROW ) AS `20 Day MA`,
						avg(`close price`) OVER( ORDER BY date ROWS between 49 PRECEDING  AND CURRENT ROW) AS `50 Day MA`
						FROM " ,sourcetab);

	PREPARE createtb FROM @createtable ;
	EXECUTE createtb; 
	DEALLOCATE PREPARE createtb ;

END $$


----------------

-- 1. Creating  a new TABLE named 'bajaj1' containing the date, close price, 20 Day MA AND 50 Day MA. 
-- This needs to be done for all 6 stocks


-- creating the TABLE bajaj1 with the columns date,close price,20 Day MA, 50 Day MA

CALL TAB_CREATE('bajaj1','bajajauto')

SELECT * FROM bajaj1 ORDER BY date;


-- creating the TABLE eicher1 with the columns date,close price,20 Day MA, 50 Day MA

CALL TAB_CREATE('eicher1','eichermotors')

SELECT * FROM eicher1 ORDER BY date;



-- creating the TABLE hero1 with the columns date,close price,20 Day MA, 50 Day MA

CALL TAB_CREATE('hero1','heromotocorp')

SELECT * FROM hero1 ORDER BY date;

-- creating the TABLE infosys with the columns date,close price,20 Day MA, 50 Day MA

CALL TAB_CREATE('infosys1','infosys')

SELECT * FROM infosys1 ORDER BY date;

-- creating the TABLE tcs with the columns date,close price,20 Day MA, 50 Day MA

CALL TAB_CREATE('tcs1','tcs')

SELECT * FROM tcs1 ORDER BY date;

-- creating the TABLE tvs1 with the columns date,close price,20 Day MA, 50 Day MA

CALL TAB_CREATE('tvs1','tvsmotors')

SELECT * FROM tvs1 ORDER BY date;

-- 2.Creating a Master TABLE Containing the date and close price of all the six stocks. (Column header for the price is the name of the stock)


-- Creating the mASter TABLE cONtaining the date AND close price of all the six stocks. 

CREATE TABLE master AS
SELECT b.date,
b.`close price` AS Bajaj,
t.`close price` AS TCS,
tv.`close price` AS TVS,
i.`close price` AS Infosys, 
e.`close price` AS Eicher, 
h.`close price` AS HERO 
FROM bajaj1 b INNER JOIN tcs1 t ON b.date = t.date
INNER JOIN tvs1 tv ON b.date = tv.date
INNER JOIN infosys1 i ON b.date = i.date
INNER JOIN eicher1 e ON b.date = e.date
INNER JOIN hero1 h ON b.date = h.date;

SELECT * FROM master;


-- Creating a procedure which adds a column diff_20ma_50ma which is the difference to 20 day moving average AND 50 day MA
-- and another column staus which tell what is the signal for that day 
--This is to generate buy AND sell signal for a particular day AND AS majority of the values are hold 
-- i have UPDATEd default value AS hold


DELIMITER $$
DROP PROCEDURE IF EXISTS TRIGGER_ ;

CREATE PROCEDURE TRIGGER_ (IN tabname varchar(30))
BEGIN
	SET @altertab = CONCAT("ALTER TABLE ", tabname, " add diff_20ma_50ma float(8,2),add `status` varchar(10) default 'hold' ");

	PREPARE altertb FROM @altertab ;
	EXECUTE altertb; 
	DEALLOCATE PREPARE altertb ;

	SET @updatetab1 = CONCAT("UPDATE ", tabname, " SET diff_20ma_50ma =`20 Day MA` - `50 Day MA` ");

	PREPARE updatetb1 FROM @updatetab1 ;
	EXECUTE updatetb1; 
	DEALLOCATE PREPARE updatetb1 ;

	SET @updatetab2 = CONCAT("UPDATE ", tabname, " INNER JOIN 
 							(SELECT `date`,
							CASE
	 						WHEN  (diff_20ma_50ma* lag(diff_20ma_50ma) OVER(ORDER BY date) <0) AND (`20 Day MA`>`50 Day MA`)  THEN 'BUY'
	 						WHEN (diff_20ma_50ma* lag(diff_20ma_50ma) OVER(ORDER BY date) <0)  AND (`20 Day MA`<`50 Day MA`) THEN  'SELL' 
     						ELSE 'hold'
							END AS `status1`
							FROM  ",tabname,") a
						ON ",tabname, ".`date` =a.`date`"
						, "SET ", tabname,".`status` = a.status1");

	PREPARE updatetb2 FROM @updatetab2 ;
	EXECUTE updatetb2; 
	DEALLOCATE PREPARE updatetb2 ;

END $$



-- Adding a column diff_20ma_50ma which is the difference to 20 day moving average AND 50 day MA

-- Adding another column staus which tell what is the signal for that day 
-- This is to generate buy AND sell signal for a particular day AND AS majority of the values are hold 
-- i have UPDATEd default value AS hold
call TRIGGER_('bajaj1');


-- Doing this operatiON for all the other TABLEs
call TRIGGER_('hero1');
call TRIGGER_('eicher1');
call TRIGGER_('tvs1');
call TRIGGER_('tcs1');
call TRIGGER_('infosys1');

--3.Using the TABLE CREATEd in Part(1) to generate buy AND sell signal. Storing this in another TABLE named 'bajaj2'. 
-- Doing the same for all the shares
CREATE TABLE bajaj2 AS
	SELECT date,`close price`,status AS 'Signal'
	FROM bajaj1;


CREATE TABLE eicher2 AS
	SELECT date,`close price`,status AS 'Signal'
	FROM eicher1;

CREATE TABLE hero2 AS
	SELECT date,`close price`,status AS 'Signal'
	FROM hero1;

CREATE TABLE infosys2 AS
	SELECT date,`close price`,status AS 'Signal'
	FROM infosys1;

CREATE TABLE tcs2 AS
	SELECT date,`close price`,status AS 'Signal'
	FROM tcs1;

CREATE TABLE tvs2 AS
	SELECT date,`close price`,status AS 'Signal'
	FROM tvs1;




-- AS there is no reliable signal for the first 50 days of our data , SETting them AS NA

-- for that i am creating a procedure which we can call for all the shares.


DELIMITER ^^
DROP PROCEDURE IF EXISTS UPD_NA ;

CREATE PROCEDURE UPD_NA (IN tabname1 varchar(30))
BEGIN
	SET @updatetbna = CONCAT("UPDATE ",tabname1, " INNER JOIN 
							( SELECT * FROM ",tabname1," ORDER BY `date` limit 50) a
								ON ",tabname1,".`date` =a.`date`"
                                ," SET ",tabname1,".signal = 'NA'");

	PREPARE updatetabna FROM @updatetbna ;
	EXECUTE updatetabna; 
	DEALLOCATE PREPARE updatetabna ;
END ^^


-- Updating all the tables
call UPD_NA('bajaj2');
call UPD_NA('eicher2');
call UPD_NA('hero2');
call UPD_NA('tcs2');
call UPD_NA('tvs2');
call UPD_NA('infosys2');


-- 4.Creating a PROCEDURE, that takes the date and table AS input AND returns the signal for that particular day (Buy/Sell/Hold) for the particular stock.
 
DELIMITER ^^
DROP PROCEDURE IF EXISTS PRINT_STATUS ;
CREATE PROCEDURE PRINT_STATUS(IN d varchar(10),IN tab_name varchar(20))

 BEGIN
	select
		if(tab_name='bajaj2' , (select `signal` from bajaj2 where `date`=d),
 		if (tab_name='eicher2' , (select `signal` from eicher2 where `date`=d),
		if (tab_name='hero2' , (select `signal` from hero2 where `date`=d),
 		if (tab_name='tcs2' , (select `signal` from tcs2 where `date`=d),
 		if (tab_name='tvs2' , (select `signal` from tvs2 where `date`=d),
 		 (select `signal` from infosys2 where `date`=d)))))) AS `signal`;
 	
 END ^^

-- This is how i input date in the functiON AND retrive the signal to buy or sell ON a date.

call PRINT_STATUS('2015-03-30','eicher2');