--	Created By:	Ajay Dwivedi
--	Purpose:	Script to Find [percent_free] space and Shrink Files using loop.
				-- Also, not considering [sp_msForEachDB] since its is not compatible with databases with compatability of 80.
--	Inputs:		2 (Mount Point & Threshold)			[CTRL][SHIFT][M]

SET NOCOUNT ON;

DECLARE @Path VARCHAR(50) -- Input 01: Mount Point with \ in last
		,@FreeSpaceThresholdInPercent FLOAT = 5.0 -- Input 02: Threshold in %. Say, generate shrink command for files to free 10% space on volume
		,@Generate_TRUNCATEONLY INT = 0

--	Declare variables for Cursor
DECLARE @db_name NVARCHAR(100)
		,@SQLString NVARCHAR(max)
		,@verbose TINYINT = 0;

DECLARE @volume_mount_point VARCHAR(256),
		@MountPoint_SizeInGB FLOAT,
		@MountPoint_FreeSpaceInGB FLOAT,
		@MountPoint_PercentFreeSpace FLOAT,
		@MountPoint_SizeInMB FLOAT,
		@MountPoint_FreeSpaceInMB FLOAT,
		@rowCounter INT = 1,
		@rowCounts INT = 0,
		@tsql_script VARCHAR(4000),
		@spaceToBeReleasedAfterShrinking_SizeInMB NUMERIC(20,2);

--	Create table for storing free space
IF OBJECT_ID('tempdb..#FileSpace') IS NOT NULL
	DROP TABLE #FileSpace;
CREATE TABLE #FileSpace 
(
	databaseName sysname, name sysname, physical_name varchar(max), isLogFile tinyint, File_SizeInMB float,  
	File_FreeSpaceInMB float, volume_mount_point varchar(256), MountPoint_SizeInMB float, MountPoint_FreeSpaceInMB float, 
	MountPoint_SizeInGB float, MountPoint_FreeSpaceInGB float, [MountPoint_PercentFreeSpace] as ((MountPoint_FreeSpaceInMB/MountPoint_SizeInMB)*100)
);

DECLARE database_cursor CURSOR FOR 
		SELECT	db.name
		FROM	sys.databases	AS db
		WHERE	db.state_desc = 'ONLINE'
		--AND		db.name NOT IN ('master','tempdb','model','msdb')
		AND		db.compatibility_level > 80
		AND		DB.NAME NOT IN ('UHT_IDD','UHT_IDD_Auth')
		
OPEN database_cursor
FETCH NEXT FROM database_cursor INTO @db_name;

WHILE @@FETCH_STATUS = 0 
BEGIN 
     SET @SQLString = '
USE ['+@db_name+'];
select	DB_NAME() as databaseName, f.name, f.physical_name, FILEPROPERTY(name,''IsLogFile'') as isLogFile, f.size/128.0 as File_SizeInMB, f.size/128.0 - CAST(FILEPROPERTY(f.name, ''SpaceUsed'') AS int)/128.0 AS File_FreeSpaceInMB
		,s.volume_mount_point, s.total_bytes/1024.0/1024.0 as MountPoint_SizeInMB, s.available_bytes/1024.0/1024.0 AS MountPoint_FreeSpaceInMB
		,s.total_bytes/1024.0/1024.0/1024.0 as MountPoint_SizeInGB, s.available_bytes/1024.0/1024.0/1024.0 AS MountPoint_FreeSpaceInGB
from	sys.database_files f
cross apply
		sys.dm_os_volume_stats(DB_ID(), f.file_id) s';

	--	Find free space for files
	BEGIN TRY
		INSERT INTO #FileSpace
			(databaseName, name, physical_name, isLogFile, File_SizeInMB, File_FreeSpaceInMB, volume_mount_point, 
				MountPoint_SizeInMB, MountPoint_FreeSpaceInMB, MountPoint_SizeInGB, MountPoint_FreeSpaceInGB)
		EXEC	(@SQLString);

			 FETCH NEXT FROM database_cursor INTO @db_name;
	END TRY
	BEGIN CATCH
		--PRINT	'';
	END CATCH

	IF @verbose = 1
	BEGIN
		SELECT	'SELECT * FROM #FileSpace' AS RunningQuery
				,*
		FROM	#FileSpace
	END
END
CLOSE database_cursor 
DEALLOCATE database_cursor 

SELECT	@volume_mount_point=s.volume_mount_point, @MountPoint_SizeInGB=s.MountPoint_SizeInGB, @MountPoint_FreeSpaceInGB=s.MountPoint_FreeSpaceInGB, 
		@MountPoint_PercentFreeSpace=s.MountPoint_PercentFreeSpace, @MountPoint_SizeInMB=s.MountPoint_SizeInMB, @MountPoint_FreeSpaceInMB=s.MountPoint_FreeSpaceInMB
FROM	#FileSpace s
--WHERE	s.volume_mount_point LIKE @Path+'%'
GROUP BY s.volume_mount_point, s.MountPoint_SizeInGB, s.MountPoint_FreeSpaceInGB, s.MountPoint_PercentFreeSpace
		,s.MountPoint_SizeInMB, s.MountPoint_FreeSpaceInMB;

SET @spaceToBeReleasedAfterShrinking_SizeInMB =	--(required space - free space)
												(((@FreeSpaceThresholdInPercent * @MountPoint_SizeInMB) / 100) - @MountPoint_FreeSpaceInMB);

PRINT	'/*	**************** Analyzing Mount Point for path '''+ISNULL(@Path,'')+''' **************************
	Total Size = '+cast(@MountPoint_SizeInMB as varchar(20))+ ' MB = '+cast(@MountPoint_SizeInGB as varchar(20))+ ' GB
	Available Space = '+cast(@MountPoint_FreeSpaceInMB as varchar(20))+ ' MB = '+cast(@MountPoint_FreeSpaceInGB as varchar(20))+ ' GB
	% Free Space = '+cast(@MountPoint_PercentFreeSpace as varchar(20))+ '
	Space to free (threshold) = '+cast(@FreeSpaceThresholdInPercent as varchar(10))+'%
	Extra Space to release with shrinking = '+ -- (required space - free space)
								
									(case when @spaceToBeReleasedAfterShrinking_SizeInMB <= 0 then 'Enough free space is already there. No shrinking required.' 
									else	(case when @spaceToBeReleasedAfterShrinking_SizeInMB >= 1024 
													then CAST(@spaceToBeReleasedAfterShrinking_SizeInMB / 1024 AS VARCHAR(22)) + ' GB'
												ELSE CAST(@spaceToBeReleasedAfterShrinking_SizeInMB AS VARCHAR(22)) + ' MB'
											end)
									END) +'

NOTE: Files would not shrink below initial size. So below values are only estimation. Please re-run the script to refresh this result each time.
	  Also, databases of compatibility of 80 are not supported by this script.
*/
';

--	Create intermediate table for Running Total feature (as UNBOUND PRECEEDING is not supported below SQL 2012)
IF OBJECT_ID('tempdb..#FileSpace_Having5gbOrMore_onPath') IS NOT NULL
	DROP TABLE #FileSpace_Having5gbOrMore_onPath
SELECT	databaseName, name, physical_name, isLogFile, File_SizeInMB, 
		File_FreeSpaceInMB - (1024*5) AS File_FreeSpaceInMB, -- Leave 5 gb space on data/log file
		volume_mount_point, MountPoint_SizeInMB, MountPoint_FreeSpaceInMB, MountPoint_SizeInGB, MountPoint_FreeSpaceInGB, MountPoint_PercentFreeSpace
		,RowID = ROW_NUMBER()OVER(ORDER BY File_FreeSpaceInMB DESC, File_SizeInMB ASC)
INTO	#FileSpace_Having5gbOrMore_onPath
FROM	#FileSpace as s
WHERE	File_FreeSpaceInMB > (1024*5) -- considers files whose free space is greater than 5 gb
	--AND	s.volume_mount_point LIKE @Path+'%'
ORDER BY File_FreeSpaceInMB DESC, File_SizeInMB ASC;

IF @verbose = 1
BEGIN
	SELECT	'SELECT * FROM #FileSpace_Having5gbOrMore_onPath' AS RunningQuery
			,*
	FROM	#FileSpace_Having5gbOrMore_onPath
	ORDER BY File_FreeSpaceInMB DESC;
END

--	Intermediate table for shrinking file upto @spaceToBeReleasedAfterShrinking_SizeInMB
IF OBJECT_ID('tempdb..#FileSpace_ShrinkTheseFilesOnly') IS NOT NULL
	DROP TABLE #FileSpace_ShrinkTheseFilesOnly
SELECT	databaseName, name, physical_name, isLogFile, File_SizeInMB, 
		[File_FreeSpaceInMB] =	CASE	WHEN File_FreeSpace_TillNow <= @spaceToBeReleasedAfterShrinking_SizeInMB
										THEN s1.File_FreeSpaceInMB
										ELSE @spaceToBeReleasedAfterShrinking_SizeInMB - (File_FreeSpace_TillNow-s1.File_FreeSpaceInMB)
										END
		,[File_FreeSpaceInMB _Old] = File_FreeSpaceInMB, File_FreeSpace_TillNow, [@spaceToBeReleasedAfterShrinking_SizeInMB] = @spaceToBeReleasedAfterShrinking_SizeInMB,
		[TillNow <= @ToBeFree] = case when File_FreeSpace_TillNow <= @spaceToBeReleasedAfterShrinking_SizeInMB then 'yes' else 'no' end,
		[TillNow - @ToBeFree < FreeSpace] = case when ((File_FreeSpace_TillNow - @spaceToBeReleasedAfterShrinking_SizeInMB) < File_FreeSpaceInMB) then 'yes' else 'no' end,
		volume_mount_point, MountPoint_SizeInMB, MountPoint_FreeSpaceInMB, MountPoint_SizeInGB, 
		MountPoint_FreeSpaceInGB, MountPoint_PercentFreeSpace, RowID
INTO	#FileSpace_ShrinkTheseFilesOnly
FROM	#FileSpace_Having5gbOrMore_onPath as s1
CROSS APPLY
	(
		SELECT	SUM(s2.File_FreeSpaceInMB) AS File_FreeSpace_TillNow
		FROM	#FileSpace_Having5gbOrMore_onPath AS s2
		WHERE	s2.RowID <= s1.RowID
	) as s2
WHERE	File_FreeSpace_TillNow <= @spaceToBeReleasedAfterShrinking_SizeInMB
	OR	((File_FreeSpace_TillNow - @spaceToBeReleasedAfterShrinking_SizeInMB) < File_FreeSpaceInMB)

IF @verbose = 1
BEGIN
	SELECT	'SELECT * FROM #FileSpace_ShrinkTheseFilesOnly' AS RunningQuery
			,*
	FROM	#FileSpace_ShrinkTheseFilesOnly
END

/*
--	Display Shrink command
IF @Generate_TRUNCATEONLY = 1
BEGIN
	IF OBJECT_ID('tempdb..#FileSpace_TruncateOnly') IS NOT NULL
		DROP TABLE #FileSpace_TruncateOnly;
	SELECT ROW_NUMBER()OVER(ORDER BY databaseName, name ) AS ID,
	'
	USE ['+databaseName+']
	GO
	DBCC SHRINKFILE (N'''+name+''' , 0, TRUNCATEONLY)
	GO
	' AS tsql_script
	INTO #FileSpace_TruncateOnly
	FROM #FileSpace_Final as s
	ORDER BY File_FreeSpaceInMB DESC;

	SET @rowCounts = (SELECT COUNT(*) FROM #FileSpace_TruncateOnly);
	WHILE(@rowCounter <= @rowCounts)
	BEGIN

		SET @tsql_script = (SELECT tsql_script FROM #FileSpace_TruncateOnly t WHERE t.ID = @rowCounter);
		PRINT @tsql_script;

		SET @rowCounter = @rowCounter + 1;
	END
END
ELSE
*/
BEGIN
	IF OBJECT_ID('tempdb..#FileSpace_ShrinkFile') IS NOT NULL
		DROP TABLE #FileSpace_ShrinkFile;

	;WITH FileSpace_Final AS
	(
		SELECT s1.databaseName, s1.name, s1.File_SizeInMB, s1.File_FreeSpaceInMB, RowID, CEILING(File_FreeSpaceInMB / 10240.0) as TotalChunks, 1 as LoopCounter FROM #FileSpace_ShrinkTheseFilesOnly as s1
		--
		UNION ALL
		--
		SELECT s1.databaseName, s1.name, s1.File_SizeInMB, s1.File_FreeSpaceInMB, s1.RowID
				,s2.TotalChunks as TotalChunks, s2.LoopCounter + 1 as LoopCounter
		FROM #FileSpace_ShrinkTheseFilesOnly as s1
		INNER JOIN
			FileSpace_Final as s2
		ON	s1.databaseName = s2.databaseName
		AND	s1.name = s2.name
		AND	s2.LoopCounter < s2.TotalChunks
	)
	SELECT	ROW_NUMBER()OVER(ORDER BY File_FreeSpaceInMB DESC, LoopCounter ) AS ID,
	CASE WHEN TotalChunks = 1 THEN  
		'
USE ['+databaseName+']
GO
	PRINT	''Shrinking file '+QUOTENAME(name)+' of '+QUOTENAME(databaseName)+'.''
DBCC SHRINKFILE (N'''+name+''' , '+cast(convert(numeric,(File_SizeInMB-File_FreeSpaceInMB) ) as varchar(50))+')
	WITH NO_INFOMSGS 
GO
--	Space freed on ['+databaseName+'] = '+cast(File_FreeSpaceInMB as varchar(50))+' MB
--	Total Space freed = '+CAST( File_FreeSpaceInMB AS VARCHAR(20))+' MB
		'
		WHEN LoopCounter <> TotalChunks
		THEN '
USE ['+databaseName+']
GO
	PRINT	''Shrink file '+QUOTENAME(name)+' of '+QUOTENAME(databaseName)+' for '+CAST(LoopCounter AS VARCHAR)+' of '+CAST(TotalChunks AS VARCHAR)+' times.''
DBCC SHRINKFILE (N'''+name+''' , '+cast(convert(numeric,(File_SizeInMB-(10240 * LoopCounter)) ) as varchar(50))+')
	WITH NO_INFOMSGS 
GO
--	Space freed on ['+databaseName+'] = 10 GB
		' 
		ELSE '
USE ['+databaseName+']
GO
	PRINT	''Shrink file '+QUOTENAME(name)+' of '+QUOTENAME(databaseName)+' for '+CAST(LoopCounter AS VARCHAR)+' of '+CAST(TotalChunks AS VARCHAR)+' times.''
DBCC SHRINKFILE (N'''+name+''' , '+cast(convert(numeric, File_SizeInMB- File_FreeSpaceInMB ) as varchar(50))+')
	WITH NO_INFOMSGS 
GO
--	Space freed on ['+databaseName+'] = '+cast(( File_FreeSpaceInMB-((TotalChunks-1)*10240) ) as varchar(50))+' MB
--	Total Space freed with all Shrink operations for the database = '+CAST( File_FreeSpaceInMB/1024 AS VARCHAR(20))+' GB
		' 
	END		AS tsql_script
	INTO	#FileSpace_ShrinkFile
	FROM	FileSpace_Final as s
	ORDER BY File_FreeSpaceInMB DESC, LoopCounter;

	SET @rowCounts = (SELECT COUNT(*) FROM #FileSpace_ShrinkFile);
	WHILE(@rowCounter <= @rowCounts)
	BEGIN

		SET @tsql_script = (SELECT tsql_script FROM #FileSpace_ShrinkFile t WHERE t.ID = @rowCounter);
		PRINT @tsql_script;

		SET @rowCounter = @rowCounter + 1;
	END
END
GO
