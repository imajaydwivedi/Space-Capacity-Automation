/*
	Ajay Dwivedi
	Query to find DBs with inappropriate Data File Size.sql
*/

-- Query to find out databases with % free space
DECLARE @volume VARCHAR(20) = 'F:\';
DECLARE @freeSpaceThreshold_Percentage int = 25;
DECLARE @DBFiles TABLE
(
	[DbName] [varchar](500),
	[FileName] [varchar](500),
	[growth_Pages] int,
	[AutoGrowth] [varchar](50) null,
	[physical_name] varchar(1000),
	[CurrentSizeMB] [numeric](17, 6),
	[FreeSpaceMB] [numeric](18, 6),
	[SpaceUsed] [numeric] (18,6),
	[type_desc] [varchar](60),
	[is_percent_growth] [bit],
	[% space used] [numeric] (18,2)
);

INSERT @DBFiles
( DbName, FileName, growth_Pages, physical_name, CurrentSizeMB, FreeSpaceMB, SpaceUsed, type_desc, is_percent_growth, [% space used] )
EXEC sp_MSforeachdb '
			USE [?];
			SELECT	DB_NAME() AS DbName,
					name AS FileName,
					growth as [growth_Pages],
					physical_name,
					size/128.0 AS CurrentSizeMB,
					size/128.0 -CAST(FILEPROPERTY(name,''SpaceUsed'') AS INT)/128.0 AS FreeSpaceMB,
					CAST(FILEPROPERTY(name,''SpaceUsed'') AS INT)/128.0 AS [SpaceUsed],
					type_desc,
					is_percent_growth,
					((CAST(FILEPROPERTY(name,''SpaceUsed'') AS INT)/128.0) * 100.0) / (size/128.0) AS [% space used]
			FROM sys.database_files;
			';

;WITH T1 AS
(
	select case when [% space used] < (100-@freeSpaceThreshold_Percentage) and FreeSpaceMB > 5120 and (FreeSpaceMB - 5120) > 2048 then 'YES' else '' end as [Consider]
			,DbName, FileName, 
			case when [growth_Pages] = 0 then '0'
				when ([growth_Pages] * 8 / 1024 / 1024) >= 1 then cast(([growth_Pages] * 8 / 1024 / 1024) as varchar(20)) + ' gb'
				when ([growth_Pages] * 8 / 1024) >= 1 then cast(([growth_Pages] * 8 / 1024) as varchar(20)) + ' mb'
				else cast(([growth_Pages] * 8) as varchar(20)) + ' kb'
				end as AutoGrowth,
			physical_name, CurrentSizeMB/1024 as CurrentSizeGB, FreeSpaceMB/1024 as FreeSpaceGB, [% space used]
			,CASE WHEN [% space used] < (100-@freeSpaceThreshold_Percentage) and FreeSpaceMB > 5120 and (FreeSpaceMB - 5120) > 2048 THEN '
	USE '+QUOTENAME(DbName)+';
	DBCC SHRINKFILE (N'''+FileName+''' , '+CAST(CAST(CurrentSizeMB-FreeSpaceMB+2150 AS bigint) AS VARCHAR(20))+');
	'				ELSE '' END AS TSQLCode
			,CASE	WHEN CHARINDEX(' gb',AutoGrowth) <> 0 THEN CAST(LEFT(AutoGrowth,CHARINDEX(' gb',AutoGrowth)) AS INT) * 1024
					WHEN CHARINDEX(' mb',AutoGrowth) <> 0 THEN CAST(LEFT(AutoGrowth,CHARINDEX(' mb',AutoGrowth)) AS INT) 					
				ELSE NULL
				END 				
				AS AutoGrowth_MB
	from @DBFiles
	where physical_name like (@volume+'%')
)
SELECT	Consider, DbName, FileName, AutoGrowth, physical_name, CurrentSizeGB, FreeSpaceGB, [% space used], 
		AutoGrowth_MB,
		TSQLCode
		,CASE	WHEN Consider = 'YES' AND (AutoGrowth_MB IS NULL OR AutoGrowth_MB < 512)
				THEN '
ALTER DATABASE ['+DbName+'] MODIFY FILE ( NAME = N'''+[FileName]+''', FILEGROWTH = 1024MB );' 
				ELSE NULL 
			END
				AS TSQLCode_setAutoGrowth
		
FROM	T1
ORDER BY [Consider] desc;

--exec sp_helpdb 'SAM3'