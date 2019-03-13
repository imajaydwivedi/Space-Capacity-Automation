
--select d.name, d.recovery_model_desc, d.log_reuse_wait_desc from sys.databases d where d.name = 'Extract_Calculated'
--select * FROM master.sys.dm_db_log_space_usage; 

declare @_loopSQLText varchar(max);
DECLARE @T_DbccShrinkFile_Raw TABLE (ID INT IDENTITY(1,1), output varchar(4000));
DECLARE @T_DbccShrinkFile TABLE ( [DBID] int, FileID int, CurrentSize int, MinimumSize int, UsedPages int, EstimatedPages int );
DECLARE @_sqlcmdCommand   VARCHAR(1000);
DECLARE @c_DBName VARCHAR(255);
-- Variables for Extracting Data from Raw output of DBCC SHRINKFILE Command (@t_DbccShrinkFile_Raw)
Declare @_DbccShrinkFile_RawText varchar(4000);
Declare @_num varchar(10) = '%[0-9.,]%';
Declare @_oth varchar(10) = '%[^0-9.,]%';
--if object_id('tempdb..#t_DbccShrinkFile_Raw') is not null
--	drop table #t_DbccShrinkFile_Raw;
--create table #t_DbccShrinkFile_Raw (ID INT IDENTITY(1,1), output varchar(2000));


--Declare @txt varchar(4000) 
--Declare @num varchar(10) = '%[0-9.,]%'
--Declare @oth varchar(10) = '%[^0-9.,]%'
SET @c_DBName = 'Extract_Calculated'
set @_loopSQLText = 'DBCC SHRINKFILE (N''Extract_Calculated_log'' , 0, TRUNCATEONLY);'
SET    @_sqlcmdCommand = 'sqlcmd -S "'+@@servername+'" -d "'+@c_DBName+'" -Q "'+@_loopSQLText+'"'

--PRINT       @sqlcmdCommand
--INSERT @t_DbccShrinkFile
--exec (@_loopSQLText)

INSERT @T_DbccShrinkFile_Raw
EXEC   master..xp_cmdshell @_sqlcmdCommand;

select * from @T_DbccShrinkFile_Raw WHERE ID <= 3;

SELECT	@_DbccShrinkFile_RawText = output
FROM	@T_DbccShrinkFile_Raw
WHERE	ID = 3;

Set @_DbccShrinkFile_RawText+='X'

IF OBJECT_ID('tempdb..#T_DbccShrinkFile_LineSplit') IS NOT NULL
	DROP TABLE #T_DbccShrinkFile_LineSplit;
;WITH T_DbccShrinkFile_LineSplit AS 
(
	SELECT	1 AS i,
			Substring(@_DbccShrinkFile_RawText,Patindex(@_num,@_DbccShrinkFile_RawText),patindex(@_oth,Substring(@_DbccShrinkFile_RawText,Patindex(@_num,@_DbccShrinkFile_RawText),4000))-1) AS num, 
			substring(@_DbccShrinkFile_RawText,Patindex(@_num,@_DbccShrinkFile_RawText)+patindex(@_oth,Substring(@_DbccShrinkFile_RawText,Patindex(@_num,@_DbccShrinkFile_RawText),4000))-1,4000) AS txt
	--
	UNION ALL
	--
	SELECT	i+1,
			Substring(txt,Patindex(@_num,txt),patindex(@_oth,Substring(txt,Patindex(@_num,txt),4000))-1), 
			substring(txt,Patindex(@_num,txt)+patindex(@_oth,Substring(txt,Patindex(@_num,txt),4000))-1,4000)
	FROM	T_DbccShrinkFile_LineSplit
	WHERE	txt like '%[0-9]%'
)
SELECT	IND=i, NUM=CAST( num  AS INT)
INTO	#T_DbccShrinkFile_LineSplit
FROM	T_DbccShrinkFile_LineSplit
WHERE	num like '%[0-9]%';

SELECT * FROM #T_DbccShrinkFile_LineSplit;

INSERT INTO @T_DbccShrinkFile
([DBID],FileID,CurrentSize, MinimumSize,UsedPages,EstimatedPages)
SELECT	*
FROM (
		SELECT IND,NUM
		FROM #T_DbccShrinkFile_LineSplit
	 ) AS Base
PIVOT (
		SUM(NUM)
		FOR IND IN ([1], [2], [3],[4], [5], [6])
	 ) PIV;

select * from @T_DbccShrinkFile;