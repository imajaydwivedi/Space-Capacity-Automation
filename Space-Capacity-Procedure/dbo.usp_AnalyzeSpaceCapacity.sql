USE DBA;
IF OBJECT_ID('dbo.usp_AnalyzeSpaceCapacity') IS NULL
  EXEC ('CREATE PROCEDURE dbo.usp_AnalyzeSpaceCapacity AS RETURN 0;')
GO
--	EXEC [dbo].[usp_AnalyzeSpaceCapacity] @help = 1
/*	
DECLARE	@_errorOccurred BIT; 
EXEC @_errorOccurred = [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = 'E:\Data6\' ,@oldVolume = 'E:\Data5\' 
														,@forceExecute = 1 ; 
SELECT CASE WHEN @_errorOccurred = 1 THEN 'fail' ELSE 'pass' END AS [Pass/Fail];
*/
ALTER PROCEDURE [dbo].[usp_AnalyzeSpaceCapacity]
	@getInfo BIT = 0, @getLogInfo BIT = 0, @volumeInfo BIT = 0, @help BIT = 0, @addDataFiles BIT = 0, @addLogFiles BIT = 0, @optimizeLogFiles BIT = 0, @restrictDataFileGrowth BIT = 0, @restrictLogFileGrowth BIT = 0, @generateCapacityException BIT = 0, @generateLogBackup BIT = 0, @unrestrictFileGrowth BIT = 0, @removeCapacityException BIT = 0, @UpdateMountPointSecurity BIT = 0, @restrictMountPointGrowth BIT = 0, @expandTempDBSize BIT = 0, @getVolumeSpaceConsumers BIT = 0,
	@newVolume VARCHAR(200) = NULL, @oldVolume VARCHAR(200) = NULL, @mountPointGrowthRestrictionPercent TINYINT = 79, @tempDBMountPointPercent TINYINT = NULL, @tempDbMaxSizeThresholdInGB INT = NULL, @DBs2Consider VARCHAR(1000) = NULL, @mountPointFreeSpaceThreshold_GB INT = 60, @vlfCountThreshold INT = 500
	,@verbose BIT = 0 ,@testAllOptions BIT = 0 ,@forceExecute BIT = 0 ,@allowMultiVolumeUnrestrictedFiles BIT = 0 ,@output4IdealScenario BIT = 0, @handleXPCmdShell BIT = 0, @sortBySize BIT = 0
AS
BEGIN
	/*
		Created By:		Ajay Dwivedi
		Updated on:		13-Mar-2019
		Current Ver:	3.7 - Fixed Below Issues as per MileStone v3.7 - Release of Apr 2019
						Issue# 03) @optimizeLogFiles - Regrow Log files by Reducing High VLF Count
		Purpose:		This procedure can be used to generate automatic TSQL code for working with ESCs like 'DBSEP1234- Data- Create and Restrict Database File Names' type.
	*/

	SET NOCOUNT ON;
	SET ANSI_WARNINGS OFF;
	
	IF @verbose = 1
		PRINT	'Declaring Local Variables';

	--	Declare table for Error Handling
	IF OBJECT_ID('tempdb..#ErrorMessages') IS NOT NULL
		DROP TABLE #ErrorMessages;
	CREATE TABLE #ErrorMessages
	(
		ErrorID INT IDENTITY(1,1),
		ErrorCategory VARCHAR(50), -- 'Compilation Error', 'Runtime Time', 'ALTER DATABASE Error'
		DBName varchar (255) NULL,
		[FileName] varchar (255) NULL,
		ErrorDetails TEXT NOT NULL,
		TSQLCode TEXT NULL
	);

	--	Declare table for Successful/Failure Output
	DECLARE @OutputMessages TABLE
	(
		MessageID INT IDENTITY(1,1),
		Status VARCHAR(15),
		Category VARCHAR(100), -- 'Compilation Error', 'Runtime Time', 'ALTER DATABASE Error', 'Add Data File', 'Add Log File'
		DBName varchar (255) NULL,
		[FileGroup] varchar (255) NULL,
		[FileName] varchar (255) NULL,
		MessageDetails TEXT NOT NULL,
		TSQLCode TEXT NULL
	);

	--	Declare variable to check if any error occurred
	DECLARE	@_errorOccurred BIT 
	SET @_errorOccurred = 0;

	DECLARE @_configurationValue_CmdShell TINYINT;
	DECLARE @_powershellCMD VARCHAR(2000);
	DECLARE	@_newVolume VARCHAR(200),
			@_addFileSQLText VARCHAR(MAX)
			,@_isServerPartOfMirroring TINYINT
			,@_mirroringPartner VARCHAR(50)
			,@_principalDatabaseCounts_Mirroring SMALLINT
			,@_mirrorDatabaseCounts_Mirroring SMALLINT
			,@_nonAccessibleDatabasesCounts SMALLINT
			,@_nonAccessibleDatabases VARCHAR(MAX)
			,@_mirrorDatabases VARCHAR(MAX)
			,@_principalDatabases VARCHAR(MAX)
			,@_nonAddedDataFilesDatabases VARCHAR(MAX)
			,@_nonAddedDataFilesDatabasesCounts SMALLINT
			,@_nonAddedLogFilesDatabases VARCHAR(MAX)
			,@_nonAddedLogFilesDatabasesCounts SMALLINT
			,@_databasesWithMultipleDataFiles VARCHAR(MAX)
			,@_databasesWithMultipleDataFilesCounts SMALLINT
			,@_totalSpace_OldVolume_GB DECIMAL(20,2)
			,@_freeSpace_OldVolume_Percent TINYINT
			,@_freeSpace_OldVolume_GB DECIMAL(20,2)
			,@_errorMSG VARCHAR(2000)
			,@_loopCounter SMALLINT
			,@_loopCounts SMALLINT
			,@_loopSQLText VARCHAR(MAX)
			,@_loopSQLText_BackupLog VARCHAR(MAX)
			,@_loopSQLText_DbccShrinkFile VARCHAR(MAX)
			,@_loopSQLText_AlterDbModifySize VARCHAR(MAX)
			,@_loopSQLText_AlterDbAutoGrowth VARCHAR(MAX)
			,@_dbName varchar (255)
			,@_fileGroup varchar (255)
			,@_name varchar (255)
			,@_newName varchar (255)
			,@_oldVolumesSpecified VARCHAR(200) -- Store comma separated volume names for @oldVolume
			,@_capacityExceptionSQLText VARCHAR(MAX)
			,@_svrName VARCHAR(255)
			,@_sqlGetMountPointVolumes VARCHAR(400)
			,@_sqlGetInfo VARCHAR(4000)
			,@_commaSeparatedMountPointVolumes VARCHAR(2000)
			,@_LogOrData VARCHAR(5)
			,@_Total_Files_Size_MB DECIMAL(20,2)
			,@_Total_Files_SpaceUsed_MB DECIMAL(20,2)
			,@_Space_That_Can_Be_Freed_MB DECIMAL(20,2)
			,@_Weightage_Sum DECIMAL(20,2)
			,@_Space_To_Add_to_Files_MB DECIMAL(20,2)
			,@_productVersion VARCHAR(20)
			,@_SpaceToBeFreed_MB DECIMAL(20,2)
			--,@_helpText VARCHAR(MAX)
			,@_sqlText NVARCHAR(4000) -- Can be used for any dynamic queries
			,@_procSTMT_Being_Executed VARCHAR(2000)
			,@_dbaMaintDatabase varchar(255);

	DECLARE @_current_ID BIGINT, 
			@_current_line varchar(2000), 
			@_current_PipeCounts INT,
			@_previous_ID BIGINT, 
			@_previous_line varchar(2000), 
			@_previoust_PipeCounts INT,
			@_next_ID BIGINT, 
			@_next_line varchar(2000), 
			@_next_PipeCounts INT;
	DECLARE @_counter INT

	DECLARE	@_logicalCores TINYINT
			,@_fileCounts TINYINT
			,@_maxFileNO TINYINT
			,@_counts_of_Files_To_Be_Created SMALLINT
			,@_jobTimeThreshold_in_Hrs INT;

	/*	There are many bugs with system stored procedure [dbo].[sp_MSforeachdb].
		So, creating my own procedure to be used later in code
		http://sqlblog.com/blogs/aaron_bertrand/archive/2010/02/08/bad-habits-to-kick-relying-on-undocumented-behavior.aspx
	*/
	IF OBJECT_ID('dbo.ForEachDB_MyWay') IS NULL
	EXEC ('	CREATE PROCEDURE dbo.ForEachDB_MyWay
				@cmd            NVARCHAR(MAX),
				@name_pattern   NVARCHAR(257) = ''%'',
				@recovery_model NVARCHAR(60) = NULL
			AS
			BEGIN
				--	http://sqlblog.com/blogs/aaron_bertrand/archive/2010/02/08/bad-habits-to-kick-relying-on-undocumented-behavior.aspx
				--	Code developed by Aaron Bertrand
				SET NOCOUNT ON;

				DECLARE
					@sql NVARCHAR(MAX),
					@db  NVARCHAR(257);

				DECLARE c_dbs CURSOR LOCAL FORWARD_ONLY STATIC READ_ONLY 
					FOR
						SELECT QUOTENAME([name])
							FROM sys.databases
							WHERE (@recovery_model IS NULL OR (recovery_model_desc = @recovery_model))
							AND [name] LIKE @name_pattern
							AND [state] = 0
							AND [is_read_only] = 0
						ORDER BY [name];

				OPEN c_dbs;
    
				FETCH NEXT FROM c_dbs INTO @db;

				WHILE @@FETCH_STATUS <> -1
				BEGIN
					SET @sql = REPLACE(@cmd, ''?'', @db);
					BEGIN TRY
						EXEC(@sql);
					END TRY
					BEGIN CATCH
						-- I''ll leave more advanced error handling as an exercise:
						PRINT ERROR_MESSAGE();
					END CATCH

					FETCH NEXT FROM c_dbs INTO @db;
				END

				CLOSE c_dbs;
				DEALLOCATE c_dbs;
			END'
		);

	IF @verbose=1
		PRINT	'Initiating local variables';

	SET	@_addFileSQLText = ''
	SET	@_isServerPartOfMirroring = 1
	SET	@_principalDatabaseCounts_Mirroring = 0
	SET	@_mirrorDatabaseCounts_Mirroring = 0
	SET	@_nonAddedDataFilesDatabasesCounts = 0
	SET	@_nonAddedLogFilesDatabasesCounts = 0
	SET	@_databasesWithMultipleDataFilesCounts = 0
	SET	@_loopCounter = 0
	SET	@_loopCounts = 0
	SET	@_svrName = @@SERVERNAME
	SET	@_productVersion = (SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(20)) AS PVersion);
	SET @_counts_of_Files_To_Be_Created = 0;
	SET @_jobTimeThreshold_in_Hrs = NULL; -- Set threshold hours to 18 here
	SELECT @_oldVolumesSpecified = CASE WHEN (@oldVolume IS NOT NULL) AND (CHARINDEX(',',@oldVolume)<>0) THEN @oldVolume ELSE NULL END;
	SET @_dbaMaintDatabase = 'sqldba';

	IF @verbose=1 
		PRINT	'Declaring Table Variables';

	DECLARE @output TABLE (ID BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY, line varchar(2000), PipeCounts AS LEN(line) - LEN(REPLACE(line,'|','')));
	DECLARE @T_Files_Final_Add TABLE (ID INT IDENTITY(1,1), TSQL_AddFile VARCHAR(2000),DBName varchar (255), [fileGroup] varchar (255), name varchar (255), _name varchar (255));
	DECLARE @T_LogFiles_Final_Add TABLE (ID INT IDENTITY(1,1), TSQL_AddFile VARCHAR(2000),DBName varchar (255), name varchar (255), _name varchar (255));
	DECLARE @T_Files_Final_Restrict TABLE (ID INT IDENTITY(1,1), TSQL_RestrictFileGrowth VARCHAR(2000),DBName varchar (255), name varchar (255), _name varchar (255));
	DECLARE @T_Files_Final_AddUnrestrict TABLE (ID INT IDENTITY(1,1), TSQL_AddFile VARCHAR(2000),DBName varchar (255), name varchar (255), _name varchar (255) NULL);
	DECLARE @T_Files_Final_AddUnrestrictLogFiles TABLE (ID INT IDENTITY(1,1), TSQL_AddFile VARCHAR(2000),DBName varchar (255), name varchar (255), _name varchar (255) NULL);
	DECLARE @T_Files_ReSizeTempDB TABLE (ID INT IDENTITY(1,1), TSQL_ResizeTempDB_Files VARCHAR(2000));
	DECLARE @T_Files_restrictMountPointGrowth TABLE (ID INT IDENTITY(1,1), TSQL_restrictMountPointGrowth VARCHAR(2000));
	DECLARE @T_Files_Remove TABLE (ID INT IDENTITY(1,1), TSQL_EmptyFile VARCHAR(2000), TSQL_RemoveFile VARCHAR(2000), name varchar (255), Volume VARCHAR(255));

	DECLARE @mountPointVolumes TABLE ( Volume VARCHAR(200), [Label] VARCHAR(100) NULL, [capacity(MB)] DECIMAL(20,2), [freespace(MB)] DECIMAL(20,2) ,VolumeName VARCHAR(50), [capacity(GB)]  DECIMAL(20,2), [freespace(GB)]  DECIMAL(20,2), [freespace(%)]  DECIMAL(20,2) );
	DECLARE @filegroups TABLE ([DBName] [varchar](255), [name] [varchar](255), [data_space_id] smallint, [type_desc] [varchar](100) );
	DECLARE @Databases TABLE (ID INT IDENTITY(1,1), DBName VARCHAR(200));
	DECLARE	@DatabasesBySize TABLE (DBName varchar (255), database_id SMALLINT, [Size (GB)] DECIMAL(20,2));
	DECLARE	@T_DatabasesNotAccessible TABLE (database_id SMALLINT, DBName varchar (255));
	DECLARE @filterDatabaseNames TABLE (DBName varchar (255), Category AS (CASE WHEN LEFT(DBName,1) = '-' THEN 'NOT IN' ELSE 'IN' END), _DBName AS RIGHT(DBName,LEN(DBName)-1)); -- This table will be used if multiple Database names are supplied in @DBsToConsider parameter
	DECLARE @oldVolumeNames TABLE (ID INT IDENTITY(1,1), oldVolume VARCHAR(20)); -- This table will be used if multiple volumes are supplied in @oldVolume parameter
	DECLARE @DBFiles TABLE
	(
		[DbName] [varchar](500),
		[FileName] [varchar](500),
		[data_space_id] int NULL, --FileGroup id
		[physical_name] varchar(1000),
		[CurrentSizeMB] [numeric](17, 6),
		[FreeSpaceMB] [numeric](18, 6),
		[SpaceUsed] [numeric] (20,0), -- File used space in MB
		[type_desc] [varchar](60),
		[growth] [int],
		[is_percent_growth] [bit],
		[% space used] [numeric] (18,2)
	);

	IF @verbose=1 
		PRINT	'Creating temp table #T_Files_Derived';
	IF OBJECT_ID('tempdb..#T_Files_Derived') IS NOT NULL
		TRUNCATE TABLE #T_Files_Derived;
	CREATE TABLE #T_Files_Derived
	(
		[dbName] [nvarchar](128) NULL,
		[database_id] [int] NULL,
		[file_id] [int] NULL,
		[type_desc] [nvarchar](60) NULL,
		[data_space_id] [int] NULL, -- filegroup id
		[name] [varchar](255) NULL,
		[physical_name] [nvarchar](260) NULL,
		[size] [int] NULL,	-- file size from sys.master_files
		[max_size] [int] NULL, -- max_size value from sys.master_files
		[growth] [int] NULL, --	growth value from sys.master_files
		[is_percent_growth] [bit] NULL,
		[fileGroup] [varchar](255) NULL,
		[FileIDRankPerFileGroup] [bigint] NULL,
		[isExistingOn_NewVolume] [int] NULL,
		[isExisting_UnrestrictedGrowth_on_OtherVolume] [int] NULL,
		--[Category] [varchar](10) NULL,
		[Size (GB)] [decimal](20, 2) NULL, -- database size from @DatabasesBySize
		[_name] [nvarchar](4000) NULL,
		[_physical_name] [nvarchar](4000) NULL,
		[TotalSize_All_DataFiles_MB]  [decimal](20, 2) NULL, -- sum total of used space for all data files of database
		[TotalSize_All_LogFiles_MB]  [decimal](20, 2) NULL, -- sum total of current size for all log files of database
		[_initialSize] [varchar](10) NULL, -- initial size of data/log file like 8000MB, 256MB
		[_autoGrowth] [varchar](10) NULL, -- auto growth size of data/log file to be created like 8000MB, 10%
		[maxfileSize_oldVolumes_MB] [decimal](20, 0) NULL, -- max size of data/log file for particular combination of Database & FileGroup
		[TSQL_AddFile] [varchar](2000) NULL,
		[TSQL_RestrictFileGrowth] [varchar](2000) NULL,
		[TSQL_UnRestrictFileGrowth] [varchar](2000) NULL
	);
	DECLARE @tempDBFiles TABLE
	(
		[fileNo] INT IDENTITY(1,1),
		[DBName] [varchar](255) NULL,
		[FileId] TINYINT,
		[LogicalName] [varchar](255) NOT NULL,
		[physical_name] [nvarchar](260) NOT NULL,
		[FileSize_MB] [numeric](18, 6) NULL,
		[Volume] [varchar](200) NULL,
		[VolumeName] [varchar](20) NULL,
		[VolumeSize_MB] [decimal](20, 2) NULL
		,[isToBeDeleted] BIT DEFAULT 0
	);


	IF OBJECT_ID('tempdb..#stage') IS NOT NULL
		DROP TABLE #stage;
	CREATE TABLE #stage([RecoveryUnitId] INT, [file_id] INT,[file_size] BIGINT,[start_offset] BIGINT,[f_seq_no] BIGINT,[status] BIGINT,[parity] BIGINT,[create_lsn] NUMERIC(38));
	IF OBJECT_ID('tempdb..#LogInfoByFile') IS NOT NULL
		DROP TABLE #LogInfoByFile;
	CREATE TABLE #LogInfoByFile (DBName VARCHAR(200), FileId INT, VLFCount INT);
	
	IF OBJECT_ID('tempdb..#runningAgentJobs') IS NOT NULL -- Used to find if any backup job is running.
		DROP TABLE #runningAgentJobs;

	--	Table to be used in @getVolumeSpaceConsumers functionality
	IF OBJECT_ID('tempdb..#VolumeFiles') IS NOT NULL -- Get all the files on @oldVolume
		DROP TABLE #VolumeFiles;
	CREATE TABLE #VolumeFiles
	(
		[Name] [varchar](255) NOT NULL,
		[ParentPathID] INT NULL,
		[ParentPath] [varchar](255) NULL,
		[SizeBytes] BIGINT NULL,
		[Size] AS (CASE	WHEN	[SizeBytes]/1024.0/1024/1024 > 1.0 
						THEN	CAST(CAST([SizeBytes]/1024.0/1024/1024 AS DECIMAL(20,2)) AS VARCHAR(21)) + ' gb'
						WHEN	[SizeBytes]/1024.0/1024 > 1.0 
						THEN	CAST(CAST([SizeBytes]/1024.0/1024 AS DECIMAL(20,2)) AS VARCHAR(21)) + ' mb'
						WHEN	[SizeBytes]/1024.0 > 1.0 
						THEN	CAST(CAST([SizeBytes]/1024.0 AS DECIMAL(20,2)) AS VARCHAR(21)) + ' kb'
						ELSE	CAST(CAST([SizeBytes] AS DECIMAL(20,2)) AS VARCHAR(21)) + ' bytes'
						END),
		[Owner] [varchar](100) NULL,
		[CreationTime] DATETIME NULL,
		[LastAccessTime] DATETIME NULL,
		[LastWriteTime] DATETIME NULL,
		[IsFile] BIT NULL DEFAULT 1
	);
	IF OBJECT_ID('tempdb..#VolumeFolders') IS NOT NULL -- Get all the files on @oldVolume
		DROP TABLE #VolumeFolders;
	CREATE TABLE #VolumeFolders
	(
		[PathID] INT NULL,
		[Name] [varchar](255) NOT NULL,
		[ParentPathID] [varchar](255) NULL,
		[SizeBytes] BIGINT NULL,
		[Size] AS (CASE	WHEN	[SizeBytes]/1024.0/1024/1024 > 1.0 
						THEN	CAST(CAST([SizeBytes]/1024.0/1024/1024 AS DECIMAL(20,2)) AS VARCHAR(21)) + ' gb'
						WHEN	[SizeBytes]/1024.0/1024 > 1.0 
						THEN	CAST(CAST([SizeBytes]/1024.0/1024 AS DECIMAL(20,2)) AS VARCHAR(21)) + ' mb'
						WHEN	[SizeBytes]/1024.0 > 1.0 
						THEN	CAST(CAST([SizeBytes]/1024.0 AS DECIMAL(20,2)) AS VARCHAR(21)) + ' kb'
						ELSE	CAST(CAST([SizeBytes] AS DECIMAL(20,2)) AS VARCHAR(21)) + ' bytes'
						END),
		[TotalChildItems] INT NULL,
		[Owner] [varchar](100) NULL,
		[CreationTime] DATETIME NULL,
		[LastAccessTime] DATETIME NULL,
		[LastWriteTime] DATETIME NULL,
		[IsFolder] BIT NULL DEFAULT 1
	);

	-- Variables for @optimizeLogFiles
	DECLARE @T_DbccShrinkFile_Raw TABLE (ID INT IDENTITY(1,1), output varchar(4000));
	DECLARE @T_DbccShrinkFile TABLE ( [DBID] int, FileID int, CurrentSize BIGINT, MinimumSize BIGINT, UsedPages BIGINT, EstimatedPages BIGINT );
	DECLARE @_sqlcmdCommand   VARCHAR(1000);
	DECLARE @_DbccShrinkFile_RawText VARCHAR(4000); -- for Extracting Data from Raw output of DBCC SHRINKFILE Command (@t_DbccShrinkFile_Raw)
	DECLARE @_num VARCHAR(10) = '%[0-9.,]%';
	DECLARE @_oth VARCHAR(10) = '%[^0-9.,]%';
	DECLARE @_spaceReleasedBySHRINKFILE_MB BIGINT;
	DECLARE @_DbSizeAfterShrink_MB BIGINT;

	IF OBJECT_ID('tempdb..#T_DbccShrinkFile_LineSplit') IS NOT NULL
		DROP TABLE #T_DbccShrinkFile_LineSplit;
	CREATE TABLE #T_DbccShrinkFile_LineSplit(IND SMALLINT, NUM BIGINT)
	
	BEGIN TRY	-- Try Catch for executable blocks that may throw error
		IF @verbose = 1
			PRINT	'	This is starting point of outermost Try/Catch Block	';

		IF @help = 1
			GOTO HELP_GOTO_BOOKMARK;

		--	============================================================================
			--	Begin:	Validations 
		--	============================================================================
		IF @verbose=1 
			PRINT	'
/*	******************** BEGIN: Validations *****************************/';

		IF	(@help=1 OR @volumeInfo=1 OR @addDataFiles=1 OR @addLogFiles=1 OR @optimizeLogFiles=1 OR @restrictDataFileGrowth=1 OR @restrictLogFileGrowth=1 OR @generateCapacityException=1 OR @unrestrictFileGrowth=1 OR @removeCapacityException=1 OR @UpdateMountPointSecurity=1 OR @restrictMountPointGrowth=1 OR @expandTempDBSize=1 OR @optimizeLogFiles=1 OR @getVolumeSpaceConsumers=1)
		BEGIN	
			SET	@getInfo = 0;
			SET @getLogInfo = 0;
		END
		ELSE 
		BEGIN
			IF (@getLogInfo=0)
				SET	@getInfo = 1;
		END

		IF @verbose=1 
			PRINT	'	Evaluation value of @_LogOrData variable';
		IF (@addDataFiles=1 OR @restrictDataFileGrowth=1 OR @getInfo=1)
			SET @_LogOrData = 'Data';
		ELSE IF (@restrictLogFileGrowth=1 OR @addLogFiles=1 OR @getLogInfo=1 or @optimizeLogFiles=1)
			SET @_LogOrData = 'Log';
		ELSE IF @oldVolume IS NOT NULL AND EXISTS (SELECT * FROM sys.master_files as mf WHERE mf.physical_name LIKE (@oldVolume+'%') AND type_desc = 'ROWS')
			SET @_LogOrData = 'Data';
		ELSE
			SET @_LogOrData = 'Log';

		--	Set Final Size thresholds for TempDb
		IF (@expandTempDBSize = 1)
		BEGIN
			IF @verbose=1 
				PRINT	'	Evaluation value of @tempDbMaxSizeThresholdInGB and @tempDBMountPointPercent variables';

			-- If both parameter are provided, throw error.
			IF (@tempDbMaxSizeThresholdInGB IS NOT NULL AND @tempDBMountPointPercent IS NOT NULL)
			BEGIN
				SET @_errorMSG = 'Kindly provide only one parameter. Either @tempDbMaxSizeThresholdInGB or @tempDBMountPointPercent.';
				IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
					EXECUTE sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
				ELSE
					EXECUTE sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
			END
			-- If both parameters are NULL, use @tempDbMaxSizeThresholdInGB with default 
			IF (@tempDbMaxSizeThresholdInGB IS NULL AND @tempDBMountPointPercent IS NULL)
			BEGIN 
				SET @_errorMSG = '	/*	Value for neither @tempDbMaxSizeThresholdInGB or @tempDBMountPointPercent is provided. So, proceeding with @tempDbMaxSizeThresholdInGB = ';
				SET @tempDbMaxSizeThresholdInGB = 16;
				IF @forceExecute = 0
				BEGIN
					SET @_errorMSG =	@_errorMSG + CAST(	@tempDbMaxSizeThresholdInGB AS VARCHAR(5)) + '
		*/';	
					PRINT @_errorMSG;
				END
			END
		END
		IF @verbose = 1
			PRINT '	Evaluating value for @_procSTMT_Being_Executed';
		SET @_procSTMT_Being_Executed = 'EXEC [dbo].[usp_AnalyzeSpaceCapacity] ';
		IF @getInfo = 1
			SET @_procSTMT_Being_Executed = @_procSTMT_Being_Executed + ' @getInfo = 1';
		ELSE IF @getLogInfo = 1
			SET @_procSTMT_Being_Executed = @_procSTMT_Being_Executed + ' @getLogInfo = 1';
		ELSE IF  @optimizeLogFiles = 1
			SET @_procSTMT_Being_Executed = @_procSTMT_Being_Executed + ' @optimizeLogFiles = 1';
		ELSE IF @help = 1
			SET @_procSTMT_Being_Executed = @_procSTMT_Being_Executed + ' @help = 1';
		ELSE IF @addDataFiles = 1
			SET @_procSTMT_Being_Executed = @_procSTMT_Being_Executed + ' @addDataFiles = 1 ' + ',@newVolume = '+QUOTENAME(@newVolume,'''')+' ,@oldVolume = '+QUOTENAME(@oldVolume,'''') + (CASE WHEN @DBs2Consider IS NOT NULL THEN ' ,@DBs2Consider = '+QUOTENAME(@DBs2Consider,'''') ELSE '' END) + (CASE WHEN @forceExecute = 1 THEN ' ,@forceExecute = 1' ELSE '' END)+ ';';
		ELSE IF @addLogFiles = 1
			SET @_procSTMT_Being_Executed = @_procSTMT_Being_Executed + ' @addLogFiles = 1 ' + ',@newVolume = '+QUOTENAME(@newVolume,'''')+' ,@oldVolume = '+QUOTENAME(@oldVolume,'''') + (CASE WHEN @DBs2Consider IS NOT NULL THEN ' ,@DBs2Consider = '+QUOTENAME(@DBs2Consider,'''') ELSE '' END) + (CASE WHEN @forceExecute = 1 THEN ' ,@forceExecute = 1' ELSE '' END)+ ';';
		ELSE IF @restrictDataFileGrowth = 1
			SET @_procSTMT_Being_Executed = @_procSTMT_Being_Executed + ' @restrictDataFileGrowth = 1 ' + ' ,@oldVolume = '+QUOTENAME(@oldVolume,'''') + (CASE WHEN @DBs2Consider IS NOT NULL THEN ' ,@DBs2Consider = '+QUOTENAME(@DBs2Consider,'''') ELSE '' END) + (CASE WHEN @forceExecute = 1 THEN ' ,@forceExecute = 1' ELSE '' END)+ ';';
		ELSE IF @restrictLogFileGrowth = 1
			SET @_procSTMT_Being_Executed = @_procSTMT_Being_Executed + ' @restrictLogFileGrowth = 1 ' + ' ,@oldVolume = '+QUOTENAME(@oldVolume,'''') + (CASE WHEN @DBs2Consider IS NOT NULL THEN ' ,@DBs2Consider = '+QUOTENAME(@DBs2Consider,'''') ELSE '' END) + (CASE WHEN @forceExecute = 1 THEN ' ,@forceExecute = 1' ELSE '' END)+ ';';

		IF @verbose = 1
			PRINT '	Value of @_procSTMT_Being_Executed = '+CHAR(10)+CHAR(10)+@_procSTMT_Being_Executed+CHAR(10);

		--	Check if valid parameter is selected for procedure
		IF (COALESCE(@getInfo,@help,@addDataFiles,@addLogFiles,@restrictDataFileGrowth,@restrictLogFileGrowth,@generateCapacityException,@unrestrictFileGrowth
		,@restrictMountPointGrowth,@expandTempDBSize,-999) = -999)
		BEGIN
			SET @_errorMSG = 'Procedure does not accept NULL for parameter values.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXECUTE sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXECUTE sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END

		--	Check if valid parameter is selected for procedure
		IF (@help = 1 AND (@addDataFiles=1 OR @addLogFiles=1 OR @restrictDataFileGrowth=1 OR @restrictLogFileGrowth=1 OR @generateCapacityException=1 OR @unrestrictFileGrowth=1 OR @removeCapacityException=1 OR @expandTempDBSize=1 ))
		BEGIN
			SET @_errorMSG = '@help=1 is incompatible with any other parameters.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END

		--	Check if valid parameter is selected for procedure
		IF (@generateCapacityException = 1 AND (@addDataFiles=1 OR @restrictDataFileGrowth=1 OR @unrestrictFileGrowth=1 OR @help=1 OR @removeCapacityException=1))
		BEGIN
			SET @_errorMSG = '@generateCapacityException=1 is incompatible with any other parameters.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END

		--	Check if valid parameter is selected for procedure
		IF (@unrestrictFileGrowth = 1 AND (@addDataFiles=1 OR @restrictDataFileGrowth=1 OR @generateCapacityException=1 OR @help=1 OR @removeCapacityException=1))
		BEGIN
			SET @_errorMSG = '@unrestrictFileGrowth=1 is incompatible with any other parameters.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END

		--	Check if valid parameter is selected for procedure
		IF (@removeCapacityException = 1 AND (@addDataFiles=1 OR @restrictDataFileGrowth=1 OR @generateCapacityException=1 OR @help=1 OR @unrestrictFileGrowth=1))
		BEGIN
			SET @_errorMSG = '@removeCapacityException=1 is incompatible with any other parameters.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END

		--	Check if valid parameter is selected for procedure
		IF ( (@addDataFiles=1 OR @addLogFiles=1) AND (@newVolume IS NULL OR @oldVolume IS NULL))
		BEGIN
			SET @_errorMSG = '@oldVolume & @newVolume parameters must be specified with '+(CASE WHEN @addDataFiles=1 THEN '@addDataFiles' ELSE '@addLogFiles' END)+' = 1 parameter.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END

		--	Check if valid parameter is selected for procedure
		IF ( (@restrictDataFileGrowth=1 OR @restrictLogFileGrowth=1 OR @restrictMountPointGrowth=1 OR @getVolumeSpaceConsumers=1) AND (@oldVolume IS NULL))
		BEGIN
			SET @_errorMSG = '@oldVolume parameters must be specified with '+(CASE WHEN @getVolumeSpaceConsumers=1 THEN '@getVolumeSpaceConsumers' WHEN @restrictDataFileGrowth=1 THEN '@restrictDataFileGrowth' WHEN @restrictLogFileGrowth=1 THEN '@restrictLogFileGrowth' ELSE '@restrictMountPointGrowth' END)+' = 1 parameter.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END

		IF @verbose=1 
			PRINT	'/*	******************** END: Validations *****************************/
';
		--	============================================================================
			--	End:	Validations 
		--	============================================================================

		--	============================================================================
			--	Begin:	Common Code 
		--	----------------------------------------------------------------------------
			/*	Get data for below tables:-
			1) @mountPointVolumes - Get all volume details like total size, free space etc
			2) @filegroups - Get details of DatabaseName, filegroup name, and type_desc
			3) @DBFiles - Get data/log file usage details along with DbName, FileName, data_space_id
			4) @DatabasesBySize - Get Database size details
			*/
		BEGIN	-- Begin block of Common Code

			IF @verbose=1 
				PRINT	'
/*	******************** BEGIN: Common Code *****************************/';

			-- Jump to @getVolumeSpaceConsumers code
			IF @getVolumeSpaceConsumers = 1
				GOTO getVolumeSpaceConsumers_GOTO_BOOKMARK;

			-- Check if more than one volume has been mentioned in @oldVolume parameter
			IF @_oldVolumesSpecified IS NOT NULL -- proceed if more than one volume specified 
			BEGIN
				
				IF @verbose = 1
					PRINT	'	Following old volumes are specified:- '+@oldVolume;
		
				WITH t1(Volume,AllVolumeNames) AS 
				(
					SELECT	CAST(LEFT(@oldVolume, CHARINDEX(',',@oldVolume+',')-1) AS VARCHAR(500)) as Volume,
							STUFF(@oldVolume, 1, CHARINDEX(',',@oldVolume+','), '') as AllVolumeNames
					--
					UNION ALL
					--
					SELECT	CAST(LEFT(AllVolumeNames, CHARINDEX(',',AllVolumeNames+',')-1) AS VARChAR(500)) AS Volume,
							STUFF(AllVolumeNames, 1, CHARINDEX(',',AllVolumeNames+','), '')  as AllVolumeNames
					FROM t1
					WHERE AllVolumeNames > ''	
				)
				INSERT @oldVolumeNames
				SELECT CASE WHEN RIGHT(RTRIM(LTRIM(Volume)),1) <> '\' THEN Volume+'\' ELSE Volume END FROM t1 ORDER BY Volume ASC;

				IF @verbose = 1
				BEGIN
					PRINT	' SELECT * FROM @oldVolumeNames;';
					SELECT 'SELECT * FROM @oldVolumeNames' AS RunningQuery, * FROM @oldVolumeNames;
				END

			END
			ELSE
			BEGIN
				IF @verbose=1 
					PRINT	'	Adding Backslash at the end for @oldVolume & @newVolume';
				SELECT	@oldVolume = CASE WHEN RIGHT(RTRIM(LTRIM(@oldVolume)),1) <> '\' THEN @oldVolume+'\' ELSE @oldVolume END;
			END
			SELECT	@newVolume = CASE WHEN RIGHT(RTRIM(LTRIM(@newVolume)),1) <> '\' THEN @newVolume+'\' ELSE @newVolume END;

			IF @_oldVolumesSpecified IS NOT NULL -- proceed if more than one volume specified 
			BEGIN
				SET @_counter = (SELECT MAX(ID) FROM @oldVolumeNames);

				WHILE (@_counter > 1)
				BEGIN
					SELECT  @oldVolume =  oldVolume FROM @oldVolumeNames WHERE ID = @_counter;

					IF @addDataFiles = 1
					BEGIN
						IF @forceExecute <> 1
							PRINT 'Calling another instance of Space Capacity Procedure with @oldVolume = '+QUOTENAME(@oldVolume,'''')+'.'+CHAR(10);
						EXECUTE sp_executesql
	N'EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = @v_newVolume ,@oldVolume = @v_oldVolume ,@forceExecute = @v_forceExecute ,@verbose = @v_verbose;', N'@v_newVolume VARCHAR(20), @v_oldVolume VARCHAR(20), @_v_forceExecute TINYINT, @v_verbose TINYINT', @v_newVolume = @newVolume, @v_oldVolume = @oldVolume, @v_forceExecute = @forceExecute, @v_verbose = @verbose;
						
						IF @forceExecute <> 1
							PRINT '	Executed completed for this instance.';
					END
					ELSE IF @restrictDataFileGrowth = 1
					BEGIN
						IF @forceExecute <> 1
							PRINT 'Calling another instance of Space Capacity Procedure with @oldVolume = '+QUOTENAME(@oldVolume,'''')+'.'+CHAR(10);
						EXECUTE sp_executesql
	N'EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictDataFileGrowth = 1 ,@oldVolume = @v_oldVolume ,@forceExecute = @v_forceExecute ,@verbose = @v_verbose;', N'@v_oldVolume VARCHAR(20), @v_forceExecute TINYINT, @v_verbose TINYINT', @v_oldVolume = @oldVolume, @v_forceExecute = @forceExecute, @v_verbose = @verbose;
						
						IF @forceExecute <> 1
							PRINT '	Execution completed for this instance.';
					END

					SET @_counter = @_counter - 1;
				END
			END
			
			--	set value of @oldVolume for just one volume/drive. The parent procedure will be using this value only.
			SELECT	@oldVolume = oldVolume
			FROM	@oldVolumeNames
			WHERE	ID = 1;

			-- Check is specific databases have been mentioned
			IF @DBs2Consider IS NOT NULL
			BEGIN
				IF @verbose = 1
					PRINT	'	Following databases are specified:- '+@DBs2Consider;
		
				WITH t1(DBName,DBs) AS 
				(
					SELECT	CAST(LEFT(@DBs2Consider, CHARINDEX(',',@DBs2Consider+',')-1) AS VARCHAR(500)) as DBName,
							STUFF(@DBs2Consider, 1, CHARINDEX(',',@DBs2Consider+','), '') as DBs
					--
					UNION ALL
					--
					SELECT	CAST(LEFT(DBs, CHARINDEX(',',DBs+',')-1) AS VARChAR(500)) AS DBName,
							STUFF(DBs, 1, CHARINDEX(',',DBs+','), '')  as DBs
					FROM t1
					WHERE DBs > ''	
				)
				INSERT @filterDatabaseNames
				SELECT LTRIM(RTRIM(DBName)) FROM t1;

				IF @verbose = 1
					SELECT 'select * from @filterDatabaseNames' AS Query, * FROM @filterDatabaseNames;
			END

			--	Check if xp_cmdshell has to be enabled
			IF (@handleXPCmdShell = 1 AND @optimizeLogFiles <> 1)
			BEGIN
				select @_configurationValue_CmdShell = CAST(value AS TINYINT) from sys.configurations as c where c.name = 'xp_cmdshell';

				IF @verbose = 1
				BEGIN
					IF (@_configurationValue_CmdShell = 0)
						PRINT	'	xp_cmdshell is in disabled mode. Proceeding to enable it..
		It will be disabled again after execution of this procedure.';
				END

				--	enable cmdshell if it is otherwise
				IF @_configurationValue_CmdShell = 0
				BEGIN
					-- To allow advanced options to be changed.  
					EXEC sp_configure 'show advanced options', 1;  
					-- To update the currently configured value for advanced options.  
					RECONFIGURE; 
					-- To enable the feature.  
					EXEC sp_configure 'xp_cmdshell', 1;  
					-- To update the currently configured value for this feature.  
					RECONFIGURE;
				END
			END

			--	Begin: Get Data & Log Mount Point Volumes
			SET @_powershellCMD =  'powershell.exe -c "Get-WmiObject -ComputerName ' + QUOTENAME(@@servername,'''') + ' -Class Win32_Volume -Filter ''DriveType = 3'' | select name,Label,capacity,freespace | foreach{$_.name+''|''+$_.Label+''|''+$_.capacity/1048576+''|''+$_.freespace/1048576}"';
			/*
			SET @_powershellCMD =  'powershell.exe -c "Get-WmiObject -ComputerName ' + QUOTENAME(@@servername,'''') + ' -Class Win32_Volume -Filter ''DriveType = 3'' | select name,Label,capacity,freespace | foreach{$_.name+''|''+$_.Label+''|''+$_.capacity/1048576+''%''+$_.freespace/1048576+''*''}"';
			*/

			-- Clear previous output
			DELETE @output;

			IF @verbose = 1
			BEGIN
				PRINT	'	Executing xp_cmdshell command:-
		'+@_powershellCMD;
			END

			--inserting disk name, Label, total space and free space value in to temporary table
			IF @optimizeLogFiles <> 1
			BEGIN
				INSERT @output
				EXEC xp_cmdshell @_powershellCMD;

				IF @verbose = 1
				BEGIN
					PRINT	'	SELECT * FROM @output';
					SELECT 'SELECT * FROM @output' AS RunningQuery,* FROM @output;
				END
			

				--	Check if some volume exists in @mountPointVolumes
				IF EXISTS (SELECT * FROM @output WHERE line LIKE '''powershell.exe'' is not recognized as an internal or external command%') 
				BEGIN
					SELECT	@_errorMSG = 'You are using '+i.SQLVersionBuild+' on '+i.WindowsVersionBuild+'. PowerShell is not found on this server.'
					FROM  (
							SELECT	SERVERPROPERTY('ServerName') AS [SQLServerName]
									, SERVERPROPERTY('ProductVersion') AS [SQLProductVersion]
									, SERVERPROPERTY('ProductMajorVersion') AS [ProductMajorVersion]
									, SERVERPROPERTY('ProductMinorVersion') AS [ProductMinorVersion]
									, SERVERPROPERTY('ProductBuild') AS [ProductBuild]
									, CASE LEFT(CONVERT(VARCHAR, SERVERPROPERTY('ProductVersion')),4) 
									   WHEN '8.00' THEN 'SQL Server 2000'
									   WHEN '9.00' THEN 'SQL Server 2005'
									   WHEN '10.0' THEN 'SQL Server 2008'
									   WHEN '10.5' THEN 'SQL Server 2008 R2'
									   WHEN '11.0' THEN 'SQL Server 2012'
									   WHEN '12.0' THEN 'SQL Server 2014'
									   ELSE 'SQL Server 2016+'
									  END AS [SQLVersionBuild]
									, SERVERPROPERTY('ProductLevel') AS [SQLServicePack]
									, SERVERPROPERTY('Edition') AS [SQLEdition]
									, RIGHT(SUBSTRING(@@VERSION, CHARINDEX('Windows NT', @@VERSION), 14), 3) as [WindowsVersionNumber]
									, CASE RIGHT(SUBSTRING(@@VERSION, CHARINDEX('Windows NT', @@VERSION), 14), 3)
									   WHEN '5.0' THEN 'Windows 2000'
									   WHEN '5.1' THEN 'Windows XP'
									   WHEN '5.2' THEN 'Windows Server 2003/2003 R2'
									   WHEN '6.0' THEN 'Windows Server 2008/Windows Vista'
									   WHEN '6.1' THEN 'Windows Server 2008 R2/Windows 7'
									   WHEN '6.2' THEN 'Windows Server 2012/Windows 8'
									   ELSE 'Windows 2012 R2+'
									  END AS [WindowsVersionBuild]
						  ) AS i;
					--SET @_errorMSG = 'Volume configuration is not per standard. Kindly perform the activity manually.';
			
					IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
						EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
					ELSE
						EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
				END

				IF @verbose=1 
					PRINT	'	Executing code to find Data/Log Mount Point Volumes';
				;WITH t_RawData AS
				(
					SELECT	ID = 1, 
							line, 
							expression = left(line,CHARINDEX('|',line)-1), 
							searchExpression = SUBSTRING ( line , CHARINDEX('|',line)+1, LEN(line)+1 ), 
							delimitorPosition = CHARINDEX('|',SUBSTRING ( line , CHARINDEX('|',line)+1, LEN(line)+1 ))



					FROM	@output
					WHERE	line like '[A-Z][:]%'
							--line like 'C:\%'
					-- 
					UNION all
					--
					SELECT	ID = ID + 1, 
							line, 
							expression = CASE WHEN delimitorPosition = 0 THEN searchExpression ELSE left(searchExpression,delimitorPosition-1) END, 
							searchExpression = CASE WHEN delimitorPosition = 0 THEN NULL ELSE SUBSTRING(searchExpression,delimitorPosition+1,len(searchExpression)+1) END, 
							delimitorPosition = CASE WHEN delimitorPosition = 0 THEN -1 ELSE CHARINDEX('|',SUBSTRING(searchExpression,delimitorPosition+1,len(searchExpression)+1)) END
					FROM	t_RawData
					WHERE	delimitorPosition >= 0
				)
				,T_Volumes AS 
				(
					SELECT	line, [Volume],[Label], [capacity(MB)],[freespace(MB)]
					FROM (
							SELECT	line, 
									[Column] =	CASE	ID
														WHEN 1
														THEN 'Volume'
														WHEN 2
														THEN 'Label'
														WHEN 3
														THEN 'capacity(MB)'
														WHEN 4
														THEN 'freespace(MB)'
														ELSE NULL
														END,
									[Value] = expression
							FROM	t_RawData
							) as up
					PIVOT (MAX([Value]) FOR [Column] IN ([Volume],[Label], [capacity(MB)],[freespace(MB)])) as pvt
					--ORDER BY LINE
				)
				INSERT INTO @mountPointVolumes
				(Volume, [Label], [capacity(MB)], [freespace(MB)] ,VolumeName, [capacity(GB)], [freespace(GB)], [freespace(%)])
				SELECT	Volume
						,[Label]
						,[capacity(MB)] = CAST([capacity(MB)] AS numeric(20,2))
						,[freespace(MB)] = CAST([freespace(MB)] AS numeric(20,2)) 


						,[Label] as VolumeName
						,CAST((CAST([capacity(MB)] AS numeric(20,2))/1024.0) AS DECIMAL(20,2)) AS [capacity(GB)]
						,CAST((CAST([freespace(MB)] AS numeric(20,2))/1024.0) AS DECIMAL(20,2)) AS [freespace(GB)]
						,CAST((CAST([freespace(MB)] AS numeric(20,2))*100.0)/[capacity(MB)] AS DECIMAL(20,2)) AS [freespace(%)]
				FROM	T_Volumes v
				WHERE	v.Volume LIKE '[A-Z]:\Data\'
					OR	v.Volume LIKE '[A-Z]:\Data[0-9]\'
					OR	v.Volume LIKE '[A-Z]:\Data[0-9][0-9]\'
					OR	v.Volume LIKE '[A-Z]:\Logs\'
					OR	v.Volume LIKE '[A-Z]:\Logs[0-9]\'
					OR	v.Volume LIKE '[A-Z]:\Logs[0-9][0-9]\'
					OR	v.Volume LIKE '[A-Z]:\tempdb\'
					OR	v.Volume LIKE '[A-Z]:\tempdb[0-9]\'
					OR	v.Volume LIKE '[A-Z]:\tempdb[0-9][0-9]\'
					OR	EXISTS (SELECT * FROM sys.master_files as mf WHERE mf.physical_name LIKE (Volume+'%'))
					OR	(@volumeInfo = 1)
					OR	@newVolume IS NOT NULL;

				IF @verbose=1
				BEGIN
					PRINT	'	Values populated for @mountPointVolumes';
					PRINT	'	SELECT * FROM @mountPointVolumes;'
					SELECT 'SELECT * FROM @mountPointVolumes;' AS RunningQuery,* FROM @mountPointVolumes;
				END

				--	Check if some volume exists in @mountPointVolumes
				IF NOT EXISTS (SELECT * FROM @mountPointVolumes v ) 
				BEGIN
					SET @_errorMSG = 'Volume configuration is not per standard. Kindly perform the activity manually.';
			
					IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
						EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
					ELSE
						EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
				END

				--	if @newVolume value provided is like 'F:\Data\' where as the drive/volume is 'F:\' only	
				IF NOT EXISTS (SELECT * FROM @mountPointVolumes v WHERE v.Volume = @newVolume)
				BEGIN
					-- Save the new volume path provided by user. This will be used in ALTER DATABASE ADD FILE script
					SET @_newVolume = @newVolume;
				
					-- Re-set @newVolume with actual volume means 'F:\' drive in above case
					SELECT	@newVolume = v2.Volume
					FROM  (	SELECT MAX(LEN(v.Volume)) AS Max_Volume_Length FROM @mountPointVolumes as v WHERE @_newVolume LIKE (v.Volume+'%') ) as v1
					INNER JOIN
							(	SELECT v.Volume FROM @mountPointVolumes as v WHERE @_newVolume LIKE (v.Volume+'%') ) as v2
						ON	LEN(v2.Volume) = v1.Max_Volume_Length
				END
				ELSE
				BEGIN
					SET @_newVolume = @newVolume;
				END

				--	Perform free space Validation based on table @mountPointVolumes
				IF NOT EXISTS (SELECT * FROM @mountPointVolumes v WHERE v.Volume = @newVolume AND v.[freespace(%)] >= 20) AND (@addDataFiles=1 OR @addLogFiles=1) 
				BEGIN
					IF NOT EXISTS (SELECT * FROM @mountPointVolumes v WHERE @newVolume LIKE (v.Volume+'%'))
						SET @_errorMSG = 'Kindly specify correct value for @newVolume as provided mount point volume '+QUOTENAME(@newVolume,'''')+' does not exist';
					ELSE
						SET @_errorMSG = 'Available free space on @newVolume='+QUOTENAME(@newVolume,'''')+' is less than 20 percent.';
			
					IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
						EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
					ELSE
						EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
				END
			END -- End block for @optimizeLogFiles = 1

			--	Running jobs
			/*
			;WITH T_Last_Job_Status AS
			(
				SELECT	JobName, Status, RunDate, [Duration HH:MM]
					,ROW_NUMBER()OVER(PARTITION BY JobName, Status ORDER BY RunDate DESC, [Duration HH:MM] DESC) AS RowID
				FROM	(
							SELECT      [JobName]   = JOB.name,
										[Status]    = CASE WHEN HIST.run_status = 0 THEN 'Failed'
										WHEN HIST.run_status = 1 THEN 'Succeeded'
										WHEN HIST.run_status = 2 THEN 'Retry'
										WHEN HIST.run_status = 3 THEN 'Canceled'
										END,
										[RunDate]   = msdb.dbo.agent_datetime(run_date, run_time),
										[Duration HH:MM]  = CAST(((run_duration/10000*3600 + (run_duration/100)%100*60 + run_duration%100 + 31 ) / 60)/60 AS VARCHAR(2))
															+ ':' + CAST(((run_duration/10000*3600 + (run_duration/100)%100*60 + run_duration%100 + 31 ) / 60)%60 AS VARCHAR(2))
							FROM        msdb.dbo.sysjobs JOB
							INNER JOIN  msdb.dbo.sysjobhistory HIST ON HIST.job_id = JOB.job_id
							WHERE    JOB.name IN ('DBA - Backup All Databases')
								AND	 HIST.step_id = 0
						) as t
			)
			,T_Last_Job_Status_2 AS
			(
				SELECT	JobName, [Succeeded], [Canceled], [Failed], [Retry]
				FROM  (	
						SELECT JobName, Status, [Duration HH:MM] FROM T_Last_Job_Status WHERE RowID = 1
					  ) AS up
				PIVOT (MAX([Duration HH:MM]) FOR [Status] IN (Succeeded, Canceled, Failed, Retry)) AS pvt
			)
			SELECT	@@SERVERNAME as [InstanceName],
				--ja.job_id,
				j.name AS job_name,
				Js.step_name,
				ja.start_execution_date as StartTime, 
				CAST(DATEDIFF(HH,ja.start_execution_date,GETDATE()) AS VARCHAR(2))+':'+CAST((DATEDIFF(MINUTE,ja.start_execution_date,GETDATE())%60) AS VARCHAR(2)) AS [ElapsedTime(HH:MM)],    
				ISNULL(last_executed_step_id,0)+1 AS current_executed_step_id,
				COALESCE(cte.Succeeded, cte.Canceled, cte.Failed, cte.Retry) as [TimeTakenLastTime(HH:MM)]
				,BlockedSPID
				,bs.session_id as Blocking_Session_ID, bs.DBName, bs.status, bs.percent_complete, bs.running_time, bs.wait_type, bs.program_name, bs.host_name, bs.login_name, CONVERT(VARCHAR(1000), bs.sql_handle, 2) as [sql_handle]
			INTO	#runningAgentJobs
			FROM msdb.dbo.sysjobactivity ja 
			LEFT JOIN msdb.dbo.sysjobhistory jh 
				ON ja.job_history_id = jh.instance_id
			JOIN msdb.dbo.sysjobs j 
			ON ja.job_id = j.job_id
			JOIN msdb.dbo.sysjobsteps js
				ON ja.job_id = js.job_id
				AND ISNULL(ja.last_executed_step_id,0)+1 = js.step_id
			LEFT JOIN
				T_Last_Job_Status_2 AS cte
				ON cte.JobName = j.name
			LEFT JOIN
				(
					--	Query to find what's is running on server
					SELECT '"spid" :: ' +CAST(s2.session_id AS VARCHAR(3)) + ' | "DBName" :: '+ s2.DBName +' | "Status" :: '+ s2.status + ' | "% Completed" :: '+ CAST(S2.percent_complete AS VARCHAR(5)) +' | "RunningTime(HH:MM:SS)" :: '+ S2.running_time +' | "WaitType" :: ' + S2.wait_type + ' | "program_name" :: '+ S2.program_name + ' | "host_name" :: '+ S2.host_name + ' | "login_Name" :: '+ S2.login_name + ' | "sql_handle" :: ' + CONVERT(VARCHAR(1000), s2.sql_handle, 2) as BlockedSPID
							,s2.session_id, s2.DBName, s2.status, S2.percent_complete, S2.running_time, S2.wait_type, S2.program_name, S2.host_name, S2.login_name, CONVERT(VARCHAR(1000), s2.sql_handle, 2) as [sql_handle]
							,s.program_name as job_program_name
					FROM sys.dm_exec_sessions AS s
					INNER JOIN sys.dm_exec_requests AS r ON r.session_id = s.session_id
					INNER JOIN 
						(
							-- Fetch details of blocking session id
							SELECT	si.session_id
									,DB_NAME(COALESCE(ri.database_id,dbid)) as DBName
									,COALESCE(ri.STATUS,LTRIM(RTRIM(sp.status))) as [STATUS]
									,COALESCE(ri.percent_complete,'') AS percent_complete
									,COALESCE(CAST(((DATEDIFF(s,start_time,GetDate()))/3600) as varchar) + ':'
										+ CAST((DATEDIFF(s,start_time,GetDate())%3600)/60 as varchar) + ':'
										+ CAST((DATEDIFF(s,start_time,GetDate())%60) as varchar) + ':'
										,CAST(DATEDIFF(hh,last_batch,GETDATE()) AS VARCHAR) + ':' + CAST(DATEDIFF(mi,last_batch,GETDATE())%60 AS VARCHAR)+':' + CAST(DATEDIFF(ss,last_batch,GETDATE())%3600 AS VARCHAR))  as running_time
									,COALESCE(CAST((estimated_completion_time/3600000) as varchar) + ' hour(s), '
												  + CAST((estimated_completion_time %3600000)/60000  as varchar) + 'min, '
												  + CAST((estimated_completion_time %60000)/1000  as varchar) + ' sec','')  as est_time_to_go
									,dateadd(second,estimated_completion_time/1000, getdate())  as est_completion_time 
									,COALESCE(ri.blocking_session_id,sp.blocked) as 'blocked by'
									,COALESCE(ri.wait_type,LTRIM(RTRIM(sp.lastwaittype))) as wait_type
									,COALESCE(ri.sql_handle, sp.sql_handle) as [sql_handle]
									,si.login_name
									,si.host_name
									,si.program_name
								FROM sys.dm_exec_sessions AS si
								LEFT JOIN sys.dm_exec_requests AS ri ON ri.session_id = si.session_id
								LEFT JOIN sys.sysprocesses AS sp ON sp.spid = si.session_id
						) AS s2
						ON		s2.session_id = r.blocking_session_id
					-- Agent job session is represented by outer query.
				) AS bs
				ON	master.dbo.fn_varbintohexstr(convert(varbinary(16), j.job_id)) COLLATE Latin1_General_CI_AI = substring(replace(bs.job_program_name, 'SQLAgent - TSQL JobStep (Job ', ''), 1, 34)
			WHERE ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
			AND start_execution_date is not null
			AND stop_execution_date is null
			AND j.name IN ('DBA - Backup All Databases')
			AND	(	@_jobTimeThreshold_in_Hrs IS NULL
				OR	DATEDIFF(HH,ja.start_execution_date,GETDATE()) >= @_jobTimeThreshold_in_Hrs);

			IF @verbose = 1
			BEGIN
				SELECT	J.*, Q.*
				FROM	(	SELECT	'SELECT * FROM #runningAgentJobs;' AS RunningQuery	) AS Q
				CROSS JOIN
						#runningAgentJobs AS J;
			END

			IF @addLogFiles = 1 OR @addDataFiles = 1 -- If user want to run ALTER DATABASE scripts, then check for running backups
			BEGIN
				IF EXISTS (SELECT * FROM #runningAgentJobs)
				BEGIN
					SET @_errorMSG = 'Backup job is running. So kindly create/restrict files later.';
			
					IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
						EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
					ELSE
						EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
				END
			END
			*/

			IF @getLogInfo <> 1 AND @addLogFiles <> 1 AND @expandTempDBSize <> 1 AND @optimizeLogFiles <> 1
			BEGIN
				IF @verbose=1 
					PRINT	'	Populate values into @filegroups temp table';
				
				INSERT @filegroups
				EXEC  ForEachDB_MyWay  ' 
			USE ?;
			SELECT db_name(), name, data_space_id, type_desc FROM [sys].[filegroups];
			 ' ;
				IF @verbose = 1
				BEGIN
					PRINT	'	SELECT * FROM @filegroups';
					SELECT 'SELECT * FROM @filegroups' AS RunningQuery, * FROM @filegroups;
				END
			END

			--	Begin code to find out complete data/log file usage details
			IF (@addDataFiles=1 OR @addLogFiles=1 OR @unrestrictFileGrowth=1 OR @getInfo=1 )
			BEGIN
				IF @verbose = 1
					PRINT	'	Populating data into @DBFiles.';

				INSERT @DBFiles
				EXEC ForEachDB_MyWay '
				USE ?;
				SELECT	DB_NAME() AS DbName,
						name AS FileName,
						data_space_id,
						physical_name,
						size/128.0 AS CurrentSizeMB,
						size/128.0 -CAST(FILEPROPERTY(name,''SpaceUsed'') AS INT)/128.0 AS FreeSpaceMB,
						CAST(FILEPROPERTY(name,''SpaceUsed'') AS INT)/128.0 AS [SpaceUsed],
						type_desc,
						growth,
						is_percent_growth,
						CASE WHEN size = 0 THEN 0 ELSE (((FILEPROPERTY(name,''SpaceUsed'') * 8.0) * 100) / (size * 8.0)) END as [% space used]
						--((CAST(FILEPROPERTY(name,''SpaceUsed'') AS INT)/128.0) * 100.0) / (size/128.0) AS [% space used]
				FROM sys.database_files;
				';

				IF @verbose = 1
				BEGIN
					PRINT	'	SELECT * FROM @DBFiles ORDER BY DbName, FileName';
					SELECT 'SELECT * FROM @DBFiles ORDER BY DbName, FileName' AS RunningQuery, * FROM @DBFiles ORDER BY DbName, FileName;
				END
			END

			IF ( @getInfo <> 1 AND @getLogInfo <> 1 AND @optimizeLogFiles <> 1) -- Don't execute common codes for @getInfo functionality
			BEGIN	-- Begin Block: Don't execute common codes for @getInfo functionality
				--	Get Database size details
				IF @verbose = 1
					PRINT	'	Populating data in @DatabasesBySize table';
				INSERT @DatabasesBySize
					SELECT	DBName, database_id, [Size (GB)] --, (CASE WHEN [Size (GB)] <= @smallDBSize THEN 'Small' ELSE 'Large' END) as Category
					FROM (	
							SELECT	db_name(database_id) as DBName, database_id, CONVERT(DECIMAL(20,2),((SUM(CONVERT(BIGINT,size))*8.0)/1024/1024)) AS [Size (GB)]
							FROM	master.sys.master_files as f
							GROUP BY db_name(database_id), database_id
						 ) AS d;
			
				SET @_mirroringPartner = (SELECT TOP 1 mirroring_partner_instance FROM sys.database_mirroring WHERE mirroring_state IS NOT NULL);

				IF @verbose=1 
					PRINT	'	Get All Databases with size information executed';
			
				--	Begin: Find Data/Log files on @oldVolume
				IF @verbose=1 
					PRINT	'	Starting Find Data/Log files on @oldVolume
					@_LogOrData = '+@_LogOrData;

				IF (@_LogOrData = 'Log') AND @expandTempDBSize <> 1
				BEGIN
					IF @verbose=1 
						PRINT	'	Begin Common Code: inside @_LogOrData = ''Log''';

					--	Find Log files on @oldVolume. [isLogExistingOn_NewVolume] column indicates if the same files exists on @newVolume.
					;WITH T_Files AS 
					(		
						--	Find Log files on @oldVolume
						SELECT	DB_NAME(database_id) as dbName, mf1.*, NULL as [fileGroup]
									-- Consider adding single file per filegroup for each database
								,[FileIDRankPerFileGroup] = row_number()over(partition by mf1.database_id order by mf1.file_id)
									-- Check if corresponding Data file for same FileGroup exists on @newVolume
								,[isExistingOn_NewVolume] = CASE WHEN NOT EXISTS (
																					SELECT	mf2.*, NULL as [fileGroup]
																					FROM	sys.master_files mf2
																					WHERE	mf2.type_desc = mf1.type_desc
																						AND	mf2.database_id = mf1.database_id
																						AND mf2.physical_name like (@newVolume+'%')
																				)
																THEN 0
																ELSE 1
																END
								,[isExisting_UnrestrictedGrowth_on_OtherVolume] = CASE WHEN NOT EXISTS (
																					SELECT	mf2.*, NULL as [fileGroup]
																					FROM	sys.master_files mf2
																					CROSS APPLY
																					(	SELECT	v2.Volume
																						FROM  (	SELECT MAX(LEN(v.Volume)) AS Max_Volume_Length FROM @mountPointVolumes as v WHERE mf2.physical_name LIKE (v.Volume+'%') ) as v1
																						INNER JOIN
																							  (	SELECT v.Volume FROM @mountPointVolumes as v WHERE mf2.physical_name LIKE (v.Volume+'%') ) as v2
																							ON	LEN(v2.Volume) = v1.Max_Volume_Length
																					) as v
																					WHERE	mf2.type_desc = mf1.type_desc
																						AND	mf2.database_id = mf1.database_id
																						AND mf2.growth <> 0
																						AND (	v.Volume IS NOT NULL
																							AND	v.Volume IN (select vi.Volume from @mountPointVolumes as vi WHERE vi.Volume <> @oldVolume AND [freespace(%)] >= 20.0)	  )
																				)
																THEN 0
																ELSE 1
																END
						FROM	sys.master_files mf1
						WHERE	mf1.type_desc = 'LOG'
							AND	mf1.physical_name LIKE (@oldVolume+'%')
					)
						INSERT #T_Files_Derived
						(	dbName, database_id, file_id, type_desc, data_space_id, name, physical_name, size, max_size, growth, is_percent_growth, fileGroup, 
							FileIDRankPerFileGroup, isExistingOn_NewVolume, isExisting_UnrestrictedGrowth_on_OtherVolume, [Size (GB)], _name, _physical_name, TotalSize_All_DataFiles_MB, TotalSize_All_LogFiles_MB, maxfileSize_oldVolumes_MB, TSQL_AddFile, TSQL_RestrictFileGrowth, TSQL_UnRestrictFileGrowth
						)
						SELECT	f.dbName, f.database_id, f.file_id, f.type_desc, f.data_space_id, f.name, f.physical_name, f.size, f.max_size, f.growth, f.is_percent_growth, f.fileGroup, 
								f.FileIDRankPerFileGroup, f.isExistingOn_NewVolume, f.isExisting_UnrestrictedGrowth_on_OtherVolume, d.[Size (GB)]
								,mf.[_name]
								--,[_physical_name] = @newVolume+[_name]+'.ldf'
								,[_physical_name] = @_newVolume+[_physicalName]+'.ldf'
								,u2.Sum_DataFilesSize_MB AS TotalSize_All_DataFiles_MB
								,u2.Sum_LogsFilesSize_MB AS TotalSize_All_LogFiles_MB
								,u.maxfileSize_oldVolumes_MB
								,[TSQL_AddFile] = CAST(NULL AS VARCHAR(2000))
								,[TSQL_RestrictFileGrowth] = CAST(NULL AS VARCHAR(2000))
								,[TSQL_UnRestrictFileGrowth] = CAST(NULL AS VARCHAR(2000))
						FROM	T_Files as f
						LEFT JOIN
								@DatabasesBySize	AS d
							ON	d.database_id = f.database_id
						LEFT JOIN
								(	select u.DbName, u.data_space_id
											,(CASE WHEN u.data_space_id = 0 THEN MAX(CurrentSizeMB) ELSE MAX(u.SpaceUsed) END) -- if log file then CurrentSizeMB else SpaceUsed
												 AS maxfileSize_oldVolumes_MB 
									from @DBFiles AS u 
									group by u.DbName, u.data_space_id
								) AS u
							ON	f.database_id = DB_ID(u.DbName)
							AND	f.data_space_id = u.data_space_id
						LEFT JOIN
								(	SELECT	DbName,	Sum_DataFilesSize_MB, Sum_LogsFilesSize_MB
									FROM  (	select u.DbName, (CASE WHEN u.[type_desc] = 'ROWS' THEN 'Sum_DataFilesSize_MB' ELSE 'Sum_LogsFilesSize_MB' END) AS [type_desc]
													,(CASE WHEN u.[type_desc] = 'LOG' THEN SUM(CurrentSizeMB) ELSE SUM(u.SpaceUsed) END) -- if log file then SUM(CurrentSizeMB) else SUM(SpaceUsed)
														 AS Sum_fileSize_ByType_MB 
											from @DBFiles AS u 
											group by u.DbName, u.[type_desc]
										  ) AS u
									PIVOT ( MAX(Sum_fileSize_ByType_MB) FOR [type_desc] in (Sum_DataFilesSize_MB, Sum_LogsFilesSize_MB) ) AS pvt
								) AS u2
							ON	f.database_id = DB_ID(u2.DbName)
						LEFT JOIN
							(	SELECT	database_id, DBName, type_desc, name, physical_name, _name = (CASE WHEN CHARINDEX(DBName,name) <> 0 THEN DBName ELSE '' END)+_Name_Without_DBName
										,_physicalName = (CASE WHEN CHARINDEX(DBName,Physical_Name_Without_Extension) <> 0 THEN DBName ELSE '' END)+[_PhysicalName_Without_DBName]
								FROM	(
											SELECT	database_id, DBName, type_desc, name, physical_name, Logical_FileNO_String, Logical_FileNO_Int, Logical_Name_Without_DBName, FileOrder, 
													[_Name_Without_DBName] = (CASE WHEN LEN( [_LogicalName_Without_DBName]) > 0 THEN [_LogicalName_Without_DBName] ELSE (CASE WHEN type_desc = 'ROWS' THEN '_Data01' ELSE '_Log01' END) END)
													,Physical_Name_Without_Extension
													,_PhysicalName_Without_DBName
											FROM	(
														SELECT	T_Files_01.*
																,FileOrder = ROW_NUMBER()OVER(PARTITION BY DBName, type_desc ORDER BY Logical_FileNO_Int DESC)
																,MaxFileNO = MAX(Logical_FileNO_Int)OVER(PARTITION BY DBName, type_desc)
																,[_LogicalName_Without_DBName] = CASE WHEN LEN(Logical_FileNO_String)<>0 THEN REPLACE(Logical_Name_Without_DBName,Logical_FileNO_String,(CASE WHEN LEN(Logical_FileNO_Int+1) = 1 THEN ('0'+CAST((Logical_FileNO_Int+1) AS VARCHAR(20))) ELSE CAST((Logical_FileNO_Int+1) AS VARCHAR(20)) END )) ELSE Logical_Name_Without_DBName + (CASE WHEN type_desc = 'LOG' THEN '01' ELSE '_data01' END) END
																,[_PhysicalName_Without_DBName] = CASE WHEN LEN(Physical_FileNO_String)<>0 THEN REPLACE(Physical_Name_Without_DBName,Physical_FileNO_String,(CASE WHEN LEN(Physical_FileNO_Int+1) = 1 THEN ('0'+CAST((Physical_FileNO_Int+1) AS VARCHAR(20))) ELSE CAST((Physical_FileNO_Int+1) AS VARCHAR(20)) END )) ELSE Physical_Name_Without_DBName + (CASE WHEN type_desc = 'LOG' THEN '01' ELSE '_data01' END) END
														FROM	(
																	SELECT mf.database_id, db_name(database_id) AS DBName, type_desc, name, mfi.physical_name
																			,mfi.Logical_FileNO_String
																			,mfi.Logical_FileNO_Int
																			,mfi.Logical_Name_Without_DBName
																			,mfi.Physical_Name_Without_Extension
																			,mfi.hasNameIssue
																			,Physical_FileNO_String = RIGHT(REPLACE(REPLACE(mfi.Physical_Name_Without_Extension,']',''),'[',''),PATINDEX('%[a-zA-Z_ ]%',REVERSE(REPLACE(REPLACE(mfi.Physical_Name_Without_Extension,']',''),'[','')))-1)
																			,Physical_FileNO_Int = CAST(RIGHT(REPLACE(REPLACE(mfi.Physical_Name_Without_Extension,']',''),'[',''),PATINDEX('%[a-zA-Z_ ]%',REVERSE(REPLACE(REPLACE(mfi.Physical_Name_Without_Extension,']',''),'[','')))-1) AS BIGINT)
																			,Physical_Name_Without_DBName = REPLACE ( mfi.Physical_Name_Without_Extension, db_name(mf.database_id), '')
																	FROM sys.master_files as mf
																	CROSS APPLY 
																		(	SELECT	physical_name = RIGHT(mfi.physical_name,CHARINDEX('\',REVERSE(mfi.physical_name),0)-1)
																					,Logical_FileNO_String = RIGHT(REPLACE(REPLACE(mfi.name,']',''),'[',''),PATINDEX('%[a-zA-Z_ ]%',REVERSE(REPLACE(REPLACE(mfi.name,']',''),'[','')))-1)
																					,Logical_FileNO_Int = CAST(RIGHT(REPLACE(REPLACE(mfi.name,']',''),'[',''),PATINDEX('%[a-zA-Z_ ]%',REVERSE(REPLACE(REPLACE(mfi.name,']',''),'[','')))-1) AS BIGINT)
																					,Logical_Name_Without_DBName = REPLACE ( mfi.name, db_name(mfi.database_id), '')
																					,Physical_Name_Without_Extension = LEFT(RIGHT(mfi.physical_name,CHARINDEX('\',REVERSE(mfi.physical_name),0)-1),CHARINDEX('.',RIGHT(mfi.physical_name,CHARINDEX('\',REVERSE(mfi.physical_name),0)-1))-1)
																					,hasNameIssue = CASE WHEN EXISTS ( SELECT * FROM sys.master_files as t WHERE t.name = mfi.name AND mfi.database_id <> t.database_id)
																								THEN 1
																								ELSE 0
																								END
																			FROM	sys.master_files as mfi 
																			WHERE	mfi.database_id = mf.database_id AND mfi.file_id = mf.file_id 
																		) as mfi
																	WHERE mf.type_desc = 'LOG'
																	AND	mf.database_id > 4
																) AS T_Files_01
													) AS T_Files_02
										) AS T_Files_03
								WHERE	FileOrder = 1
							)  AS mf
							ON	mf.database_id = f.database_id
							AND	mf.type_desc = f.type_desc;
				END
				--ELSE
				IF NOT (@_LogOrData = 'Log') AND @expandTempDBSize <> 1
				BEGIN
					IF @verbose=1 
						PRINT	'	Begin Common Code: inside else part of @_LogOrData = ''Log''';

					--	Find Data files on @oldVolume. [isExistingOn_NewVolume] column indicates if the same files exists on @newVolume.
					;WITH T_Files AS 
					(		
						--	Find Data files on @oldVolume
						SELECT	DB_NAME(database_id) as dbName, mf1.*, fg1.name as [fileGroup]
									-- Consider adding single file per filegroup for each database
								,[FileIDRankPerFileGroup] = row_number()over(partition by mf1.database_id, fg1.name order by mf1.file_id)
									-- Check if corresponding Data file for same FileGroup exists on @newVolume
								,[isExistingOn_NewVolume] = CASE WHEN NOT EXISTS (
																					SELECT	mf2.*, NULL as [fileGroup]
																					FROM	sys.master_files mf2
																					WHERE	mf2.type_desc = mf1.type_desc 
																						AND	mf2.database_id = mf1.database_id
																						AND mf2.data_space_id = mf1.data_space_id -- same filegroup
																						AND mf2.physical_name like (@newVolume+'%')
																				)
																THEN 0
																ELSE 1
																END
								,[isExisting_UnrestrictedGrowth_on_OtherVolume] = CASE WHEN EXISTS (
																					SELECT	mf2.*, NULL as [fileGroup]
																					FROM	sys.master_files mf2
																					OUTER APPLY
																						(	SELECT	v2.Volume
																							FROM  (	SELECT MAX(LEN(v.Volume)) AS Max_Volume_Length FROM @mountPointVolumes as v WHERE mf2.physical_name LIKE (v.Volume+'%') ) as v1
																							INNER JOIN
																								  (	SELECT v.Volume FROM @mountPointVolumes as v WHERE mf2.physical_name LIKE (v.Volume+'%') ) as v2
																								ON	LEN(v2.Volume) = v1.Max_Volume_Length
																						) as v
																					WHERE	mf2.type_desc = mf1.type_desc
																						AND	mf2.database_id = mf1.database_id
																						AND mf2.data_space_id = mf1.data_space_id -- same filegroup
																						AND mf2.growth <> 0
																						AND v.Volume IN (select Volume from @mountPointVolumes as vi WHERE vi.Volume <> @oldVolume AND [freespace(%)] >= 20.0)
																				)
																THEN 1
																ELSE 0
																END
						--FROM	sys.master_files mf1 inner join sys.filegroups fg1 on fg1.data_space_id = mf1.data_space_id
						FROM	sys.master_files mf1 left join @filegroups fg1 on fg1.data_space_id = mf1.data_space_id
							AND	fg1.DBName = DB_NAME(mf1.database_id)
						WHERE	mf1.type_desc = 'rows'
							AND	mf1.physical_name LIKE (@oldVolume+'%')
					)	--select 'Testing',* from T_Files;
					
						INSERT #T_Files_Derived
						(	dbName, database_id, file_id, type_desc, data_space_id, name, physical_name, size, max_size, growth, is_percent_growth, fileGroup, 
							FileIDRankPerFileGroup, isExistingOn_NewVolume, isExisting_UnrestrictedGrowth_on_OtherVolume, [Size (GB)], _name, _physical_name, TotalSize_All_DataFiles_MB, TotalSize_All_LogFiles_MB, maxfileSize_oldVolumes_MB, TSQL_AddFile, TSQL_RestrictFileGrowth, TSQL_UnRestrictFileGrowth
						)
					
						SELECT	f.dbName, f.database_id, f.file_id, f.type_desc, f.data_space_id, f.name, f.physical_name, f.size, f.max_size, f.growth, f.is_percent_growth, f.fileGroup, 
								f.FileIDRankPerFileGroup, f.isExistingOn_NewVolume, f.isExisting_UnrestrictedGrowth_on_OtherVolume, d.[Size (GB)]
								,[_name]
								--,[@_newVolume] = @_newVolume
								--,[_physicalName  ] = [_physicalName]
								,[_physical_name] = @_newVolume+[_physicalName]+'.ndf'
								--,[_physical_name] = @newVolume+[_name]+'.ndf'
								,u2.Sum_DataFilesSize_MB AS TotalSize_All_DataFiles_MB
								,u2.Sum_LogsFilesSize_MB AS TotalSize_All_LogFiles_MB
								,u.maxfileSize_oldVolumes_MB
								,[TSQL_AddFile] = CAST(NULL AS VARCHAR(2000))
								,[TSQL_RestrictFileGrowth] = CAST(NULL AS VARCHAR(2000))
								,[TSQL_UnRestrictFileGrowth] = CAST(NULL AS VARCHAR(2000))
						FROM	T_Files as f -- all data files on @oldVolume
						LEFT JOIN
								@DatabasesBySize	AS d
							ON	d.database_id = f.database_id
						LEFT JOIN
								(	select u.DbName, u.data_space_id
											,(CASE WHEN u.data_space_id = 0 THEN MAX(CurrentSizeMB) ELSE MAX(u.SpaceUsed) END) -- if log file then CurrentSizeMB else SpaceUsed
												 AS maxfileSize_oldVolumes_MB 
									from @DBFiles AS u 
									group by u.DbName, u.data_space_id
								) AS u
							ON	f.database_id = DB_ID(u.DbName)
							AND	f.data_space_id = u.data_space_id
						LEFT JOIN
								(	SELECT	DbName,	Sum_DataFilesSize_MB, Sum_LogsFilesSize_MB
									FROM  (	select u.DbName, (CASE WHEN u.[type_desc] = 'ROWS' THEN 'Sum_DataFilesSize_MB' ELSE 'Sum_LogsFilesSize_MB' END) AS [type_desc]
													,(CASE WHEN u.[type_desc] = 'LOG' THEN SUM(CurrentSizeMB) ELSE SUM(u.SpaceUsed) END) -- if log file then SUM(CurrentSizeMB) else SUM(SpaceUsed)
														 AS Sum_fileSize_ByType_MB 
											from @DBFiles AS u 
											group by u.DbName, u.[type_desc]
										  ) AS u
									PIVOT ( MAX(Sum_fileSize_ByType_MB) FOR [type_desc] in (Sum_DataFilesSize_MB, Sum_LogsFilesSize_MB) ) AS pvt
								) AS u2
							ON	f.database_id = DB_ID(u2.DbName)
						LEFT JOIN -- get new names per filegroup
							(	SELECT	database_id, DBName, type_desc, name, _name = (CASE WHEN CHARINDEX(DBName,name) <> 0 THEN DBName ELSE '' END)+_Name_Without_DBName
										,FileOrder ,data_space_id
										,_physicalName = (CASE WHEN CHARINDEX(DBName,Physical_Name_Without_Extension) <> 0 THEN DBName ELSE '' END)+[_PhysicalName_Without_DBName]
								FROM	(
											SELECT	database_id, DBName, type_desc, name, Logical_FileNO_String, Logical_FileNO_Int, Logical_Name_Without_DBName, FileOrder, data_space_id,
													[_PhysicalName_Without_DBName], [Physical_Name_Without_Extension],
													[_Name_Without_DBName] = (CASE WHEN LEN( [_LogicalName_Without_DBName]) > 0 THEN [_LogicalName_Without_DBName] ELSE (CASE WHEN type_desc = 'ROWS' THEN '_Data01' ELSE '_Log01' END) END)
											FROM	(

														SELECT	T_Files_01.*
																,FileOrder = ROW_NUMBER()OVER(PARTITION BY DBName, type_desc, data_space_id ORDER BY Logical_FileNO_Int DESC)
																,MaxFileNO = MAX(Logical_FileNO_Int)OVER(PARTITION BY DBName, type_desc)
																,[_LogicalName_Without_DBName] = CASE	WHEN LEN(Logical_FileNO_String)<>0 -- if more than 1 files already exist, then just increment no by 1
																								THEN REPLACE(Logical_Name_Without_DBName,Logical_FileNO_String,(CASE WHEN LEN(Logical_FileNO_Int+data_space_id) = 1 THEN ('0'+CAST((Logical_FileNO_Int+data_space_id) AS VARCHAR(20))) ELSE CAST((Logical_FileNO_Int+data_space_id) AS VARCHAR(20)) END )) 
																								ELSE Logical_Name_Without_DBName + (CASE WHEN type_desc = 'LOG' THEN '01' ELSE '_data01' END) END
																,[_PhysicalName_Without_DBName] = CASE	WHEN LEN(Physical_FileNO_String)<>0 -- if more than 1 files already exist, then just increment no by 1
																								THEN REPLACE(Physical_Name_Without_DBName,Physical_FileNO_String,(CASE WHEN LEN(Physical_FileNO_Int+data_space_id) = 1 THEN ('0'+CAST((Physical_FileNO_Int+data_space_id) AS VARCHAR(20))) ELSE CAST((Physical_FileNO_Int+data_space_id) AS VARCHAR(20)) END )) 
																								ELSE Physical_Name_Without_DBName + (CASE WHEN type_desc = 'LOG' THEN '01' ELSE '_data01' END) END
														FROM	(		
																	SELECT mf.database_id, db_name(database_id) AS DBName, type_desc, name, mfi.physical_name, data_space_id, growth
																			,mfi.Logical_FileNO_String
																			,mfi.Logical_FileNO_Int 
																			,mfi.Logical_Name_Without_DBName
																			,Physical_Name_Without_Extension
																			,mfi.hasNameIssue
																			,Physical_FileNO_String = RIGHT(REPLACE(REPLACE(mfi.Physical_Name_Without_Extension,']',''),'[',''),PATINDEX('%[a-zA-Z_ ]%',REVERSE(REPLACE(REPLACE(mfi.Physical_Name_Without_Extension,']',''),'[','')))-1)
																			,Physical_FileNO_Int = CAST(RIGHT(REPLACE(REPLACE(mfi.Physical_Name_Without_Extension,']',''),'[',''),PATINDEX('%[a-zA-Z_ ]%',REVERSE(REPLACE(REPLACE(mfi.Physical_Name_Without_Extension,']',''),'[','')))-1) AS BIGINT)
																			,Physical_Name_Without_DBName = REPLACE ( mfi.Physical_Name_Without_Extension, db_name(mf.database_id), '')
																	FROM sys.master_files as mf
																	CROSS APPLY 
																		(	SELECT	physical_name = RIGHT(mfi.physical_name,CHARINDEX('\',REVERSE(mfi.physical_name),0)-1)
																					,Logical_FileNO_String = RIGHT(REPLACE(REPLACE(mfi.name,']',''),'[',''),PATINDEX('%[a-zA-Z_ ]%',REVERSE(REPLACE(REPLACE(mfi.name,']',''),'[','')))-1)
																					,Logical_FileNO_Int = CAST(RIGHT(REPLACE(REPLACE(mfi.name,']',''),'[',''),PATINDEX('%[a-zA-Z_ ]%',REVERSE(REPLACE(REPLACE(mfi.name,']',''),'[','')))-1) AS BIGINT)
																					,Logical_Name_Without_DBName = REPLACE ( mfi.name, db_name(mfi.database_id), '')
																					,Physical_Name_Without_Extension = LEFT(RIGHT(mfi.physical_name,CHARINDEX('\',REVERSE(mfi.physical_name),0)-1),CHARINDEX('.',RIGHT(mfi.physical_name,CHARINDEX('\',REVERSE(mfi.physical_name),0)-1))-1)
																					,hasNameIssue = CASE WHEN EXISTS ( SELECT * FROM sys.master_files as t WHERE t.name = mfi.name AND mfi.database_id <> t.database_id)
																								THEN 1
																								ELSE 0
																								END
																			FROM	sys.master_files as mfi 
																			WHERE	mfi.database_id = mf.database_id AND mfi.file_id = mf.file_id 
																		) as mfi
																	WHERE mf.type_desc = 'ROWS'
																	AND mf.database_id > 4
																) AS T_Files_01

													) AS T_Files_02
										) AS T_Files_03
								WHERE	FileOrder = 1
							)  AS mf
							ON	mf.database_id = f.database_id
							AND	mf.type_desc = f.type_desc
							AND mf.data_space_id = f.data_space_id;
				END -- Ending of if else block 
			
				IF @verbose = 1 AND @expandTempDBSize <> 1
				BEGIN
					PRINT	'	Completed Data population in #T_Files_Derived';
					SELECT	Q.RunningQuery, d.*
					FROM  (	SELECT 'SELECT * FROM #T_Files_Derived ORDER BY dbName, file_id;' AS RunningQuery ) Q
					LEFT JOIN
							#T_Files_Derived AS d
					ON		1 = 1
					ORDER BY dbName, file_id;
				END

				/* By default, if another unrestricted file exists in any other volume, then don't create files for that db */
				IF @allowMultiVolumeUnrestrictedFiles = 1
				BEGIN
					IF	@_LogOrData = 'Log'
					BEGIN
						IF @verbose = 1
							PRINT	'	Updating #T_Files_Derived table for @allowMultiVolumeUnrestrictedFiles option.';
						UPDATE	fo
						SET		isExisting_UnrestrictedGrowth_on_OtherVolume = 0
						FROM	#T_Files_Derived AS fo
						WHERE	(	isExistingOn_NewVolume = 0 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 1) --if file not exists on @newVolume
							OR	(	isExistingOn_NewVolume = 1 -- if file exists on @newVolume but with 0 growth
								AND NOT EXISTS (select * from sys.master_files as fi where fi.database_id = fo.database_id and fi.data_space_id = fo.data_space_id and fi.growth <> 0 AND fi.physical_name LIKE (@newVolume+'%'))
								);
					END
				END

				------------------------------------------------------------------------------------------------
				--	Begin: Get All Databases that are not accessible being Offline, ReadOnly or in Restoring Mode
				INSERT @T_DatabasesNotAccessible
				SELECT	*
				FROM  (
						--	Database in 'Restoring' mode
						SELECT	d.database_id, DB_NAME(d.database_id) AS DBName
						FROM	sys.databases as d
						WHERE	d.state_desc = 'Restoring'
							AND	d.database_id NOT IN (SELECT m.database_id FROM sys.database_mirroring as m WHERE m.mirroring_role_desc IS NOT NULL)
							AND	d.database_id IN (select f.database_id from #T_Files_Derived as f)
						--
						UNION 
						--	Database that are 'Offline' or 'Read Only'
						SELECT	d.database_id, DB_NAME(d.database_id) AS DBName
						FROM	sys.databases as d
						WHERE	(CASE WHEN d.is_read_only = 1 THEN 'Read_Only' ELSE DATABASEPROPERTYEX(DB_NAME(d.database_id), 'Status') END) <> 'ONLINE'
					  ) AS A
				ORDER BY A.DBName;

				IF @verbose=1 
					PRINT	'	Begin Common Code: End of Find Data/Log files on @oldVolume';
				--	------------------------------------------------------------------------
				--	End: Find Data/Log files on @oldVolume
				--	============================================================================================

				IF @verbose=1 
					PRINT	'	Initializing values for @_mirrorDatabases and @_principalDatabases';
				IF	@_mirroringPartner IS NOT NULL
				BEGIN
					--	Find all databases that are part of Mirroring plan, their data files are +nt on @oldVolume and playing 'MIRROR' role.
					SELECT	@_mirrorDatabases = COALESCE(@_mirrorDatabases+', '+DB_NAME(database_id),DB_NAME(database_id))
					FROM	sys.database_mirroring m
					WHERE	m.mirroring_state IS NOT NULL
						AND	m.mirroring_role_desc = 'MIRROR'
						AND	m.database_id IN (select f.database_id from #T_Files_Derived as f);
					SET @_mirrorDatabaseCounts_Mirroring = (LEN(@_mirrorDatabases)-LEN(REPLACE(@_mirrorDatabases,',',''))+1);
		
					--	Find all databases that are part of Mirroring plan, their data files are +nt on @oldVolume and playing 'PRINCIPAL' role.
					SELECT	@_principalDatabases = COALESCE(@_principalDatabases+', '+DB_NAME(database_id),DB_NAME(database_id))
					FROM	sys.database_mirroring m
					WHERE	m.mirroring_state IS NOT NULL
						AND	m.mirroring_role_desc = 'PRINCIPAL'
						AND	m.database_id IN (select f.database_id from #T_Files_Derived as f where (@addDataFiles = 0 OR (@addDataFiles = 1 AND f.isExistingOn_NewVolume = 0)) OR (@restrictDataFileGrowth = 0 OR (@restrictDataFileGrowth = 1 AND growth <> 0 AND f.isExistingOn_NewVolume = 1)) OR  (@unrestrictFileGrowth = 0 OR (@unrestrictFileGrowth = 1 AND growth = 0)));	
					SET @_principalDatabaseCounts_Mirroring = (LEN(@_principalDatabases)-LEN(REPLACE(@_principalDatabases,',',''))+1);
				END

				IF @verbose=1 
					PRINT	'	Initializing values for @_databasesWithMultipleDataFiles';
				--	Find all databases having multiple files per filegroup on @oldVolume.
				SELECT	@_databasesWithMultipleDataFiles = COALESCE(@_databasesWithMultipleDataFiles+', '+DB_NAME(database_id),DB_NAME(database_id))
				FROM  (	SELECT DISTINCT database_id FROM #T_Files_Derived AS m WHERE FileIDRankPerFileGroup <> 1 ) as f;
				SET @_databasesWithMultipleDataFilesCounts = (LEN(@_databasesWithMultipleDataFilesCounts)-LEN(REPLACE(@_databasesWithMultipleDataFilesCounts,',',''))+1);

				IF @verbose=1 
					PRINT	'	Initializing values for @_nonAccessibleDatabases';
				SELECT	@_nonAccessibleDatabases = COALESCE(@_nonAccessibleDatabases+', '+DB_NAME(database_id),DB_NAME(database_id))
				FROM  @T_DatabasesNotAccessible;
				SET @_nonAccessibleDatabasesCounts = (LEN(@_nonAccessibleDatabases)-LEN(REPLACE(@_nonAccessibleDatabases,',',''))+1);


				IF @verbose=1 AND @_nonAccessibleDatabases IS NOT NULL
					PRINT	'	Below are few non-accessible databases:-
			'+@_nonAccessibleDatabases;

				IF @verbose=1
					PRINT	'	Create #T_Files_Final from #T_Files_Derived table';

				--	Create temp table #T_Files_Final with Data files of @oldVolume that can be successfully processed for @addDataFiles & @restrictDataFileGrowth operations.
				IF OBJECT_ID('tempdb..#T_Files_Final') IS NOT NULL
					DROP TABLE #T_Files_Final;
				
				IF @verbose=1 AND EXISTS (SELECT * FROM @filterDatabaseNames)
				BEGIN
					PRINT	'	Filtering #T_Files_Final for databases based on @DBs2Consider';
					SELECT	'SELECT * FROM @filterDatabaseNames' AS RunningQuery, *
					FROM	@filterDatabaseNames;
				END

				SELECT	*
				INTO	#T_Files_Final
				FROM	#T_Files_Derived AS f
				WHERE	f.database_id NOT IN (SELECT m.database_id FROM	sys.database_mirroring m WHERE m.mirroring_state IS NOT NULL AND m.mirroring_role_desc = 'MIRROR')
					AND	f.database_id NOT IN (	SELECT d.database_id FROM @T_DatabasesNotAccessible as d)
					AND (	NOT EXISTS (SELECT * FROM @filterDatabaseNames as d WHERE d.Category = 'IN')
						OR	f.database_id IN (SELECT DB_ID(d.DBName) FROM @filterDatabaseNames AS d WHERE d.Category = 'IN')
						)
					AND (	NOT EXISTS (SELECT * FROM @filterDatabaseNames as d WHERE d.Category = 'NOT IN')
						OR	f.database_id NOT IN (SELECT DB_ID(d._DBName) FROM @filterDatabaseNames AS d WHERE d.Category = 'NOT IN')
						);
		
				IF (@_LogOrData='Log')
				BEGIN
					IF @verbose=1 
						PRINT	'	Populate #T_Files_Final for Log Files';

					IF @verbose = 1
						PRINT	'	Updating value in [maxfileSize_oldVolumes_MB] column for Log files in #T_Files_Final';
					UPDATE	#T_Files_Final
					SET	maxfileSize_oldVolumes_MB = CASE WHEN (TotalSize_All_DataFiles_MB / 4) < maxfileSize_oldVolumes_MB -- Check if Max Log size > 1/4th of data file 
														 THEN maxfileSize_oldVolumes_MB -- keep log size
														 WHEN ((TotalSize_All_DataFiles_MB / 4) - TotalSize_All_LogFiles_MB) <= maxfileSize_oldVolumes_MB -- Check if 1/4th Data file - Total Log size is less than maxfileSize_oldVolumes_MB
														 THEN maxfileSize_oldVolumes_MB
														 ELSE ((TotalSize_All_DataFiles_MB / 4) - TotalSize_All_LogFiles_MB) 
														 END 
					WHERE maxfileSize_oldVolumes_MB < 16000;

					/*	Say, 4 Data files with sum(UsedSpace) = TotalSize_All_DataFiles_MB
							2 Log Files with sum(CurrentSize) = TotalSize_All_LogFiles_MB

							Max_Log_Size_MB		TotalSize_All_DataFiles_MB		TotalSize_All_LogFiles_MB	New_Max_Log_Size_Threadhold		Initial_Size		Growth
							6000				120000							10000						30000							8000MB				8000MB
							13000				120000							20000						13000							4000MB				1000MB
							10000				80000							20000						10000							4000MB				1000MB
							2000				120000							3000						27000							8000MB				8000MB
							2000				12000							2000						2000							4000MB				500MB
							6000				16000							7000						6000							4000MB				500MB
							20000				120000							22000						20000							8000MB				8000MB
							20000				300000							30000						20000							8000MB				8000MB
							500					4000							500							500								500MB				500MB
							120 gb				500 gb							128 gb						120 gb							8 gb				8gb
							512 mb				500 gb							1 gb						31 gb							8 gb				8gb


					*/
							

					IF @verbose = 1
						PRINT	'	Updating value in [_initialSize] column for Log files in #T_Files_Final';
					--	https://www.sqlskills.com/blogs/paul/important-change-vlf-creation-algorithm-sql-server-2014/
					UPDATE	#T_Files_Final
					SET		[_initialSize] =	CASE	WHEN	maxfileSize_oldVolumes_MB < 256 
														THEN	'256MB'
														WHEN	maxfileSize_oldVolumes_MB < 1000
														THEN	CAST(maxfileSize_oldVolumes_MB AS VARCHAR(20))+'MB'
														WHEN	maxfileSize_oldVolumes_MB = 8192
														THEN	'4000MB'
														WHEN	maxfileSize_oldVolumes_MB < 16000
														THEN	CAST(CAST( (maxfileSize_oldVolumes_MB/2) AS NUMERIC(20,0)) AS VARCHAR(20))+'MB'
														ELSE	'8000MB'
														END

					IF @verbose = 1
						PRINT	'	Updating value in [_autoGrowth] column for Log files in #T_Files_Final';
					UPDATE	#T_Files_Final
					SET		[_autoGrowth] =	CASE	WHEN	maxfileSize_oldVolumes_MB < 8000 
													THEN	'500MB'
													WHEN	maxfileSize_oldVolumes_MB < 16000
													THEN	'1000MB'
													ELSE	'8000MB'
													END

					UPDATE	#T_Files_Final
							SET		TSQL_AddFile = '
		'+(CASE WHEN @forceExecute = 0 THEN 'PRINT	''Adding new file '+QUOTENAME(_name)+' for database ['+dbName+']'';' ELSE '' END)+ '
	ALTER DATABASE ['+dbName+'] ADD LOG FILE ( NAME = N'+QUOTENAME(_name,'''')+', FILENAME = '+QUOTENAME(_physical_name,'''')+' , SIZE = '+[_initialSize]+' , FILEGROWTH = '+[_autoGrowth]+');'
									,TSQL_RestrictFileGrowth = '
		'+(CASE WHEN @forceExecute = 0 THEN 'PRINT	''Restricting growth for file '+QUOTENAME(name)+' of database ['+dbName+']'';' ELSE '' END)+ '
	ALTER DATABASE ['+dbName+'] MODIFY FILE ( NAME = '+QUOTENAME(name,'''')+', FILEGROWTH = 0);'
									,TSQL_UnRestrictFileGrowth = '
		'+(CASE WHEN @forceExecute = 0 THEN 'PRINT	''Removing restriction for file '+QUOTENAME(name)+' of database ['+dbName+']'';' ELSE '' END) + '
	ALTER DATABASE ['+dbName+'] MODIFY FILE ( NAME = '+QUOTENAME(name,'''')+', FILEGROWTH = '+[_autoGrowth]+')';

												END
				ELSE
				BEGIN
					
					IF @verbose = 1
						PRINT	'	Updating value in [_initialSize] column for Data files in #T_Files_Final';
					UPDATE	#T_Files_Final
					SET		[_initialSize] =	CASE	WHEN	[Size (GB)] < 2
														THEN	'256MB'
														WHEN	[Size (GB)] BETWEEN 2 AND 10
														THEN	'512MB'
														WHEN	[Size (GB)] > 10 AND [maxfileSize_oldVolumes_MB] < (50*1024) -- less than 50 gb
														THEN	'1024MB'
														WHEN	[maxfileSize_oldVolumes_MB] BETWEEN (50*1024) AND (200*1024) -- b/w 50 gb and 200 gb
														THEN	'10240MB' -- 10 GB
														WHEN	[maxfileSize_oldVolumes_MB] > (200*1024) -- greator than 200 gb
														THEN	'51200MB' -- 50 GB
														ELSE	NULL
														END

					IF @verbose = 1
						PRINT	'	Updating value in [_autoGrowth] column for Data files in #T_Files_Final';
					UPDATE	#T_Files_Final
					SET		[_autoGrowth] =	CASE	WHEN	[Size (GB)] < 2
														THEN	'256MB'
														WHEN	[Size (GB)] BETWEEN 2 AND 10
														THEN	'512MB'
														WHEN	[Size (GB)] > 10 AND [maxfileSize_oldVolumes_MB] < (50*1024) -- less than 50 gb
														THEN	'1024MB'
														WHEN	[maxfileSize_oldVolumes_MB] BETWEEN (50*1024) AND (200*1024) -- b/w 50 gb and 200 gb
														THEN	'2048MB'
														WHEN	[maxfileSize_oldVolumes_MB] > (200*1024) -- greator than 200 gb
														THEN	'5120MB'
														ELSE	NULL
														END

					UPDATE	#T_Files_Final
								SET	TSQL_AddFile = '
		'+(CASE WHEN @forceExecute = 0 THEN 'PRINT	''Adding new file '+QUOTENAME(_name)+' for database ['+dbName+']'';' ELSE '' END) + '
	ALTER DATABASE ['+dbName+'] ADD FILE ( NAME = '+QUOTENAME(_name,'''')+', FILENAME = '+QUOTENAME(_physical_name,'''')+' , SIZE = '+[_initialSize]+' , FILEGROWTH = '+[_autoGrowth]+') TO FILEGROUP '+QUOTENAME(fileGroup)+';'
									,TSQL_RestrictFileGrowth = '		
		'+(CASE WHEN @forceExecute = 0 THEN 'PRINT	''Restricting growth for file '+QUOTENAME(name)+' of database ['+dbName+']'';' ELSE '' END) + '
	ALTER DATABASE ['+dbName+'] MODIFY FILE ( NAME = '+QUOTENAME(name,'''')+', FILEGROWTH = 0);'
									,TSQL_UnRestrictFileGrowth = '		
		'+(CASE WHEN @forceExecute = 0 THEN 'PRINT	''Removing restriction for file '+QUOTENAME(name)+' of database ['+dbName+']'';' ELSE '' END) + '	
	ALTER DATABASE ['+dbName+'] MODIFY FILE ( NAME = '+QUOTENAME(name,'''')+', FILEGROWTH = '+[_autoGrowth]+');';
		
				END

				IF @verbose=1 AND @expandTempDBSize <> 1
				BEGIN
					PRINT	'	SELECT * FROM #T_Files_Final;';
					SELECT Q.RunningQuery , dt.*
					FROM (	SELECT 'SELECT * FROM #T_Files_Final;' AS RunningQuery) Q
					LEFT JOIN
							#T_Files_Final as dt
					ON 1 = 1;
				END
			
				IF @verbose=1 AND @expandTempDBSize <> 1
				BEGIN
					PRINT	'	Find the free space % on @oldVolume';
					PRINT	'	SELECT * FROM @mountPointVolumes AS v WHERE	v.Volume = @oldVolume;';
			
					SELECT RunningQuery, v.* 
					FROM (SELECT 'SELECT * FROM @mountPointVolumes AS v WHERE	v.Volume = @oldVolume' AS RunningQuery) Q
					LEFT JOIN @mountPointVolumes AS v 
					ON	1 = 1
					AND v.Volume = @oldVolume;
				END

				SELECT	@_freeSpace_OldVolume_GB = [freespace(GB)],
						@_totalSpace_OldVolume_GB = [capacity(GB)],
						@_freeSpace_OldVolume_Percent = [freespace(%)]
				FROM	@mountPointVolumes AS v 
				WHERE	v.Volume = @oldVolume;
			END	-- End Block: Don't execute common codes for @getInfo functionality

			IF @verbose=1 
				PRINT	'/*	******************** END: Common Code *****************************/

';
		END	-- End block of Common Code
		--	----------------------------------------------------------------------------
			--	End:	Common Code 
		--	============================================================================
	
		--	============================================================================
			--	Begin:	@getInfo = 1
		--	----------------------------------------------------------------------------	
		IF	@getInfo = 1
		BEGIN	-- Begin Block of @getInfo
			IF @verbose=1 
				PRINT	'
/*	******************** Begin:	@getInfo = 1 *****************************/';

			IF @verbose=1 
				PRINT	'	Creating temp table #FilesByFileGroup';

			IF OBJECT_ID('tempdb..#FilesByFileGroup') IS NOT NULL
				DROP TABLE #FilesByFileGroup;
			WITH T_FileGroup AS
			(	SELECT mf1.database_id, mf1.data_space_id, fg1.name as [FileGroup], CONVERT(DECIMAL(20,2),((SUM(CONVERT(BIGINT,size))*8.0)/1024/1024)) AS [TotalFilesSize(GB)]
				FROM sys.master_files AS mf1 LEFT JOIN @filegroups AS fg1 ON fg1.data_space_id = mf1.data_space_id AND fg1.DBName = DB_NAME(mf1.database_id)
				GROUP BY mf1.database_id, mf1.data_space_id, fg1.name
			)
			,T_Files_Filegroups AS
			(
				SELECT	mf.file_id, mf.database_id as [DB_ID], DB_NAME(mf.database_id) AS [DB_Name], fg.[TotalFilesSize(GB)], fg.[FileGroup]
						,growth 
						,(CASE WHEN growth = 0 THEN '0' WHEN is_percent_growth = 1 THEN CAST(growth AS VARCHAR(5))+'%' 
						ELSE CAST(CONVERT( DECIMAL(20,2),((growth*8.0)/1024.0)) AS VARCHAR(20))+' mb'
						END) AS [growth(MB)]
						,name as [FileName] 
						,v.Volume  as [Volume] 
				FROM	sys.master_files AS mf
				INNER JOIN
						T_FileGroup AS fg
					ON	mf.database_id = fg.database_id AND mf.data_space_id = fg.data_space_id
				OUTER APPLY
						(	SELECT	v2.Volume
							FROM  (	SELECT MAX(LEN(v.Volume)) AS Max_Volume_Length FROM @mountPointVolumes as v WHERE mf.physical_name LIKE (v.Volume+'%') ) as v1
							INNER JOIN
								  (	SELECT v.Volume FROM @mountPointVolumes as v WHERE mf.physical_name LIKE (v.Volume+'%') ) as v2
								ON	LEN(v2.Volume) = v1.Max_Volume_Length
						) as v
				WHERE	mf.type_desc = 'ROWS'
			)
			,T_Files_Usage AS
			(
				SELECT	DbName, [FileName], data_space_id, physical_name, CurrentSizeMB, FreeSpaceMB, SpaceUsed, type_desc, growth, is_percent_growth, [% space used]
						,size = CASE	WHEN CurrentSizeMB >= (1024.0 * 1024.0) -- size > 1 tb
										THEN CAST(CAST(CurrentSizeMB / (1024.0 * 1024.0) AS numeric(20,2)) AS VARCHAR(20))+' tb'
										WHEN CurrentSizeMB >= 1024 -- size < 1 tb but greater than 1024 mb
										THEN CAST(CAST(CurrentSizeMB / 1024 AS numeric(20,2)) AS VARCHAR(20))+ ' gb'
										ELSE CAST(CAST(CurrentSizeMB AS NUMERIC(20,2)) AS VARCHAR(20)) + ' mb'
										END
				FROM	@DBFiles AS f
			)
			,T_Volumes_Derived AS
			(
				SELECT	Volume
					   ,[capacity(MB)]
					   ,[freespace(MB)]
					   ,VolumeName
					   ,[capacity(GB)]
					   ,[freespace(GB)]
					   , [freespace(%)]
				FROM	@mountPointVolumes as v
				WHERE	EXISTS (SELECT 1 FROM T_Files_Filegroups AS fg WHERE fg.Volume = v.Volume)
			)
			,T_Files AS
			( 
				SELECT	DB_ID, DB_Name, [TotalFilesSize(GB)], [FileGroup], 
						--f.FileName+' (Growth by '+[growth(GB)]+')' AS FileSettings, 
						f.[FileName]+' (Size|% Used|AutoGrowth :: '+size+'|'+CAST([% space used] AS VARCHAR(50))+' %|'+[growth(MB)]+')' AS FileSettings, 
						v.VolumeName+'['+v.Volume+']'+' = '+CAST([freespace(GB)] AS VARCHAR(20))+'GB('+CAST([freespace(%)] AS VARCHAR(20))+'%) Free of '+CAST([capacity(GB)] AS VARCHAR(20))+' GB' as FileDrive
						,f.growth, f.[growth(MB)], f.[FileName], v.Volume, [capacity(MB)], [freespace(MB)], VolumeName, [capacity(GB)], [freespace(GB)], [freespace(%)]
						,ROW_NUMBER()OVER(PARTITION BY v.Volume, f.DB_Name, f.[FileGroup] ORDER BY f.[file_id])AS FileID
				FROM	T_Files_Filegroups AS f
				LEFT JOIN
						T_Files_Usage as u
					ON	u.DbName = f.[DB_Name]
					AND	u.[FileName] = f.[FileName]
				RIGHT OUTER JOIN
						T_Volumes_Derived AS v
					ON	v.Volume = f.[Volume]
			),T_Files_Derived AS
			(
				SELECT	DB_ID, DB_Name, CASE WHEN d.is_read_only = 1 THEN 'Read_Only' ELSE DATABASEPROPERTYEX(DB_Name, 'Status') END as DB_State, [TotalFilesSize(GB)], FileGroup, STUFF(
								(SELECT ', ' + f2.FileSettings
								 FROM T_Files as f2
								 WHERE f2.Volume = f.Volume AND f2.DB_Name = f.DB_Name AND f2.FileGroup = f.FileGroup
								 FOR XML PATH (''))
								  , 1, 1, ''
							) AS Files, FileDrive, growth, [growth(MB)], FileName, Volume, [capacity(MB)], [freespace(MB)], VolumeName, [capacity(GB)], [freespace(GB)], [freespace(%)], FileID
				FROM	T_Files as f LEFT OUTER JOIN sys.databases as d 
					ON	d.name = f.DB_Name
				WHERE	f.FileID = 1
			)
			SELECT	*
			INTO	#FilesByFileGroup
			FROM	T_Files_Derived;

			IF @verbose = 1
			BEGIN
				PRINT 'SELECT * FROM #FilesByFileGroup';
				SELECT 'SELECT * FROM #FilesByFileGroup' AS RunningQuery, * FROM #FilesByFileGroup;
			END

			IF @verbose = 1
			BEGIN
				SELECT  DISTINCT TOP 100 'SELECT DISTINCT TOP 100 FileDrive, LEFT(FileDrive,4) AS First4Char, CAST(SUBSTRING(Volume, PATINDEX(''%[0-9]%'', Volume), PATINDEX(''%[0-9][^0-9]%'', Volume + ''t'') - PATINDEX(''%[0-9]%'', 
							Volume) + 1) AS INT) AS Number, Volume
							,Vol_Order =	(CASE	WHEN	Volume LIKE ''%MSSQL%''
									THEN	1
									WHEN	Volume LIKE ''%TempDB%''
									THEN	2
									ELSE	3
									END)
					FROM #FilesByFileGroup 
					ORDER BY Vol_Order,First4Char,Number;'AS RunningQuery, FileDrive, LEFT(FileDrive,4) AS First4Char, CAST(SUBSTRING(Volume, PATINDEX('%[0-9]%', Volume), PATINDEX('%[0-9][^0-9]%', Volume + 't') - PATINDEX('%[0-9]%', 
							Volume) + 1) AS INT) AS Number, Volume
							,Vol_Order =	(CASE	WHEN	Volume LIKE '%MSSQL%'
									THEN	1
									WHEN	Volume LIKE '%TempDB%'
									THEN	2
									ELSE	3
									END)
					FROM #FilesByFileGroup 
					ORDER BY Vol_Order,First4Char,Number;
			END

			IF @verbose = 1
			BEGIN
				PRINT	'	Initiating value for @_commaSeparatedMountPointVolumes using COALESCE statement';
			END

			SELECT	@_commaSeparatedMountPointVolumes = COALESCE(@_commaSeparatedMountPointVolumes+', '+QUOTENAME(FileDrive), QUOTENAME(FileDrive))
			FROM (	SELECT DISTINCT TOP 100 FileDrive, LEFT(FileDrive,4) AS First4Char, CAST(SUBSTRING(Volume, PATINDEX('%[0-9]%', Volume), PATINDEX('%[0-9][^0-9]%', Volume + 't') - PATINDEX('%[0-9]%', Volume) + 1) AS INT) AS Number, Volume
							,Vol_Order =	(CASE	WHEN	Volume LIKE '%MSSQL%'
													THEN	1
													WHEN	Volume LIKE '%TempDB%'
													THEN	2
													ELSE	3
													END)
					FROM #FilesByFileGroup 
					ORDER BY Vol_Order,First4Char,Number
				) AS FD;

			IF @verbose = 1
			BEGIN
				PRINT	'	Value of @_commaSeparatedMountPointVolumes = ' + @_commaSeparatedMountPointVolumes;
			END

			--	Unfortunately table variables are out of scope of dynamic SQL, trying temp table method
			IF OBJECT_ID('tempdb..#filterDatabaseNames') IS NOT NULL
				DROP TABLE #filterDatabaseNames;
			SELECT * INTO #filterDatabaseNames FROM @filterDatabaseNames;

			SET @_sqlGetInfo = '
				SELECT	DB_ID, DB_Name, DB_State, [TotalFilesSize(GB)], FileGroup, '+@_commaSeparatedMountPointVolumes+'
				FROM  (
						SELECT	DB_ID, DB_Name, DB_State, [TotalFilesSize(GB)], FileGroup, Files, FileDrive
						FROM	#FilesByFileGroup
						WHERE	DB_Name IS NOT NULL
							AND	(	NOT EXISTS (SELECT * FROM #filterDatabaseNames) 
								OR	DB_Name IN (SELECT d.DBName FROM #filterDatabaseNames AS d)
								) 
					  ) up
				PIVOT	(MAX(Files) FOR FileDrive IN ('+@_commaSeparatedMountPointVolumes+')) AS pvt
				ORDER BY [DB_Name];';

			IF @verbose = 1
			BEGIN
				PRINT	'	Value of @_sqlText = ' + @_sqlGetInfo;
			END

			EXEC (@_sqlGetInfo)
			IF @verbose=1 
				PRINT	'/*	******************** End:	@getInfo = 1 *****************************/

';

		END	-- End Block of @getInfo
		
		--	----------------------------------------------------------------------------
			--	End:	@getInfo = 1
		--	============================================================================


		--	============================================================================
			--	Begin:	@volumeInfo = 1
		--	----------------------------------------------------------------------------
		IF	@volumeInfo = 1
		BEGIN
			IF @verbose=1 
				PRINT	'
/*	******************** Begin:	@volumeInfo = 1 *****************************/';

		SELECT	v.Volume, v.Label, v.[capacity(GB)], v.[freespace(GB)]
				,[UsedSpace(GB)] = v.[capacity(GB)]-v.[freespace(GB)]
				,v.[freespace(%)] 
				,[UsedSpace(%)] = 100-v.[freespace(%)]
		FROM	@mountPointVolumes AS v;

			IF @verbose=1 
				PRINT	'
/*	******************** Begin:	@volumeInfo = 1 *****************************/';
		END
		--	----------------------------------------------------------------------------
			--	End:	@volumeInfo = 1
		--	============================================================================
		

		--	============================================================================
			--	Begin:	@getVolumeSpaceConsumers = 1
		--	----------------------------------------------------------------------------
		getVolumeSpaceConsumers_GOTO_BOOKMARK:
		IF	@getVolumeSpaceConsumers = 1
		BEGIN
			IF @verbose=1 
				PRINT	'
/*	******************** Begin:	@getVolumeSpaceConsumers = 1 *****************************/';

			--	Begin: Get All the files from @oldVolume
			SET @_powershellCMD =  'powershell.exe -c "Get-ChildItem -Path '''+@oldVolume+''' -Recurse -Force -ErrorAction SilentlyContinue | Where-Object {$_.PSIsContainer -eq $false} | Select-Object   Name, @{l=''ParentPath'';e={$_.DirectoryName}}, @{l=''SizeBytes'';e={$_.Length}}, @{l=''Owner'';e={((Get-ACL $_.FullName).Owner)}}, CreationTime, LastAccessTime, LastWriteTime, @{l=''IsFolder'';e={if($_.PSIsContainer) {1} else {0}}} | foreach{ $_.Name + ''|'' + $_.ParentPath + ''|'' + $_.SizeBytes + ''|'' + $_.Owner + ''|'' + $_.CreationTime + ''|'' + $_.LastAccessTime + ''|'' + $_.LastWriteTime + ''|'' + $_.IsFolder }"';

			-- Clear previous output
			DELETE @output;

			IF @verbose = 1
			BEGIN
				PRINT	'	Executing xp_cmdshell command:-
		'+@_powershellCMD;
			END

			--inserting all files from @oldVolume in to temporary table
			INSERT @output
			EXEC xp_cmdshell @_powershellCMD;

			IF @verbose = 1
			BEGIN
				SELECT	'SELECT * FROM @output' AS RunningQuery, *
				FROM	@output as o
				ORDER BY ID;
			END

			--	If line items are truncated to new line from PowerShell output
			IF EXISTS (SELECT * FROM @output as o WHERE	line IS NOT NULL AND PipeCounts <> 7)
			BEGIN
				IF @verbose = 1
					PRINT	CHAR(13) + CHAR(10)+ '	Some lines found that were split due to size got larger than 255 characters.';

				DECLARE cursor_PowerShellMemoryConsumers CURSOR LOCAL SCROLL FOR
						SELECT ID, line, PipeCounts FROM @output WHERE line IS NOT NULL ORDER BY ID;

				OPEN cursor_PowerShellMemoryConsumers;

				FETCH NEXT FROM cursor_PowerShellMemoryConsumers INTO @_current_ID, @_current_line, @_current_PipeCounts;
				WHILE @@FETCH_STATUS = 0
				BEGIN  
					FETCH RELATIVE -1 FROM cursor_PowerShellMemoryConsumers INTO @_previous_ID, @_previous_line, @_previoust_PipeCounts;
					FETCH RELATIVE 2 FROM cursor_PowerShellMemoryConsumers INTO @_next_ID, @_next_line, @_next_PipeCounts;

					/*case 01:   if @_current_PipeCounts = 7 AND @_next_PipeCounts = 7
								 then [Valid]
								 if @_current_PipeCounts = 7 AND @_next_PipeCounts <> 7
								 {
									if @_current_PipeCounts + @_next_PipeCounts = 7
									then current row and next row are part of same line
									else next row is separater new line
								 }

					*/
					
					-- If next row is part of current row, then add next row in current row, and delete next row
					IF (@_current_PipeCounts + @_next_PipeCounts = 7)
					BEGIN
						/*
						IF @verbose = 1
						BEGIN
								PRINT	'--	--------------------------------------------
	Current ID - '+CAST(ISNULL(@_current_ID,0) as varchar(10)) +'		
	Line = '+@_current_line + '
	Next RowID = '+CAST(ISNULL(@_next_ID,0) as varchar(10))+ '
	Next Line = '+@_next_line;
						END
						*/
						UPDATE @output
						SET line = @_current_line + @_next_line
						WHERE ID = @_current_ID;
						
						DELETE @output
						WHERE CURRENT OF cursor_PowerShellMemoryConsumers;
						/*
						IF @verbose = 1
							PRINT	'	Combined lines of ID '+CAST(ISNULL(@_current_ID,0) as varchar(10))+' and '+CAST(ISNULL(@_next_ID,0) as varchar(10)) ;
						*/
						FETCH NEXT FROM cursor_PowerShellMemoryConsumers INTO @_current_ID, @_current_line, @_current_PipeCounts;
					END
					ELSE
					BEGIN
						SELECT  @_current_ID = @_next_ID, @_current_line = @_next_line, @_current_PipeCounts = @_next_PipeCounts;
					END
				END

				CLOSE cursor_PowerShellMemoryConsumers;  
				DEALLOCATE cursor_PowerShellMemoryConsumers;  

				IF @verbose = 1
				BEGIN
					SELECT	'Data after Deletion from @output table' AS RunningQuery, *
					FROM	@output
					ORDER BY ID;
				END
			END

			IF @forceExecute = 0
			BEGIN
				IF EXISTS (SELECT * FROM @output AS o WHERE line IS NOT NULL AND PipeCounts <> 7 )
				BEGIN
					PRINT	'	/*	Deleting few rows which are not as per our format.
	In order to find those rows, kindly add @verbose = 1 parameter and check table result for ''Files Data where PipeCounts <> 7'' */';
					
					DELETE @output
					WHERE line IS NULL
					OR	PipeCounts <> 7;

					IF @verbose = 1
					BEGIN
						SELECT	Q.RunningQuery, o.*
						FROM  (	SELECT 'Files Data where PipeCounts <> 7' AS RunningQuery ) Q
						LEFT OUTER JOIN
							  (SELECT * FROM @output AS o WHERE line IS NOT NULL AND PipeCounts <> 7 ) AS o
						ON		1 = 1;;
					END
				END
			END
			
			
			IF @verbose=1 
				PRINT	'	Extract Details for Files from PowerShell command output';
			;WITH t_RawData AS
			(
				SELECT	ID = 1, 
						line, 
						expression = left(line,CHARINDEX('|',line)-1), 
						searchExpression = SUBSTRING ( line , CHARINDEX('|',line)+1, LEN(line)+1 ), 
						delimitorPosition = CHARINDEX('|',SUBSTRING ( line , CHARINDEX('|',line)+1, LEN(line)+1 ))
				FROM	@output
				WHERE	line IS NOT NULL
				-- 
				UNION all
				--
				SELECT	ID = ID + 1, 
						line, 
						expression = CASE WHEN delimitorPosition = 0 THEN searchExpression ELSE left(searchExpression,delimitorPosition-1) END, 
						searchExpression = CASE WHEN delimitorPosition = 0 THEN NULL ELSE SUBSTRING(searchExpression,delimitorPosition+1,len(searchExpression)+1) END, 
						delimitorPosition = CASE WHEN delimitorPosition = 0 THEN -1 ELSE CHARINDEX('|',SUBSTRING(searchExpression,delimitorPosition+1,len(searchExpression)+1)) END
				FROM	t_RawData
				WHERE	delimitorPosition >= 0
			)
			,T_Files AS 
			(
				SELECT	line, Name, ParentPath, SizeBytes, Owner, CreationTime, LastAccessTime, LastWriteTime, IsFolder
				FROM (
						SELECT	line, --Name, ParentPath, SizeBytes, Owner, CreationTime, LastAccessTime, LastWriteTime, IsFolder
								[Column] =	CASE	ID
													WHEN 1
													THEN 'Name'
													WHEN 2
													THEN 'ParentPath'
													WHEN 3
													THEN 'SizeBytes'
													WHEN 4
													THEN 'Owner'
													WHEN 5
													THEN 'CreationTime'
													WHEN 6
													THEN 'LastAccessTime'
													WHEN 7
													THEN 'LastWriteTime'
													WHEN 8
													THEN 'IsFolder'
													ELSE NULL
													END,
								[Value] = expression
						FROM	t_RawData
						) as up
				PIVOT (MAX([Value]) FOR [Column] IN (Name, ParentPath, SizeBytes, Owner, CreationTime, LastAccessTime, LastWriteTime, IsFolder)) as pvt
				--ORDER BY LINE
			)
			--INSERT #VolumeFiles
			--	( Name, ParentPath, SizeBytes, Owner, CreationTime, LastAccessTime, LastWriteTime, IsFile )
			SELECT	Name, --[ParentPathID] = DENSE_RANK()OVER(ORDER BY ParentPath)
					ParentPath, SizeBytes, Owner, LTRIM(RTRIM(CreationTime)) AS CreationTime, 
					LTRIM(RTRIM(LastAccessTime)) AS LastAccessTime, LTRIM(RTRIM(LastWriteTime)) AS LastWriteTime
					,[IsFile] = 1
			FROM	T_Files v;

			IF @verbose=1
			BEGIN
				PRINT	'	Values populated for #VolumeFiles';
				PRINT	'	SELECT * FROM #VolumeFiles;'
				SELECT 'SELECT * FROM #VolumeFiles;' AS RunningQuery,* FROM #VolumeFiles;
			END

			--	Begin: Get All folders from @oldVolume
			SET @_powershellCMD =  'powershell.exe -c "Get-ChildItem -Path '''+@oldVolume+''' -Recurse -Force -ErrorAction SilentlyContinue | Where-Object {$_.PSIsContainer} | Select-Object   FullName, @{l=''Owner'';e={((Get-ACL $_.FullName).Owner)}}, CreationTime, LastAccessTime, LastWriteTime | foreach{ $_.FullName + ''|'' + $_.Owner + ''|'' + $_.CreationTime + ''|'' + $_.LastAccessTime + ''|'' + $_.LastWriteTime }"';

			-- Clear previous output
			DELETE @output;

			IF @verbose = 1
			BEGIN
				PRINT	'	Executing xp_cmdshell command:-
		'+@_powershellCMD;
			END

			--inserting all folders information from @oldVolume in to temporary table
			INSERT @output
			EXEC xp_cmdshell @_powershellCMD;

			IF @verbose = 1
			BEGIN
				PRINT	'	SELECT * FROM @output';
				SELECT 'Folders Data FROM @output table' AS RunningQuery,* FROM @output;
			END

			--	If line items are truncated to new line from PowerShell output
			IF EXISTS (SELECT * FROM @output as o WHERE	line IS NOT NULL AND PipeCounts <> 4)
			BEGIN
				IF @verbose = 1
					PRINT	CHAR(13) + CHAR(10)+ '	Some lines found that were split due to size got larger than 255 characters.';

				DECLARE cursor_PowerShellMemoryConsumers CURSOR LOCAL SCROLL FOR
						SELECT ID, line, PipeCounts FROM @output WHERE line IS NOT NULL ORDER BY ID;

				OPEN cursor_PowerShellMemoryConsumers;

				FETCH NEXT FROM cursor_PowerShellMemoryConsumers INTO @_current_ID, @_current_line, @_current_PipeCounts;
				WHILE @@FETCH_STATUS = 0
				BEGIN  
					FETCH RELATIVE -1 FROM cursor_PowerShellMemoryConsumers INTO @_previous_ID, @_previous_line, @_previoust_PipeCounts;
					FETCH RELATIVE 2 FROM cursor_PowerShellMemoryConsumers INTO @_next_ID, @_next_line, @_next_PipeCounts;

					/*case 01:   if @_current_PipeCounts = 4 AND @_next_PipeCounts = 4
								 then [Valid]
								 if @_current_PipeCounts = 4 AND @_next_PipeCounts <> 4
								 {
									if @_current_PipeCounts + @_next_PipeCounts = 4
									then current row and next row are part of same line
									else next row is separater new line
								 }

					*/
					
					-- If next row is part of current row, then add next row in current row, and delete next row
					IF (@_current_PipeCounts + @_next_PipeCounts = 4)
					BEGIN
						/*
						IF @verbose = 1
						BEGIN
								PRINT	'--	--------------------------------------------
	Current ID - '+CAST(ISNULL(@_current_ID,0) as varchar(10)) +'		
	Line = '+@_current_line + '
	Next RowID = '+CAST(ISNULL(@_next_ID,0) as varchar(10))+ '
	Next Line = '+@_next_line;
						END
						*/

						UPDATE @output
						SET line = @_current_line + @_next_line
						WHERE ID = @_current_ID;
						
						DELETE @output
						WHERE CURRENT OF cursor_PowerShellMemoryConsumers;
						/*
						IF @verbose = 1
							PRINT	'	Combined lines of ID '+CAST(ISNULL(@_current_ID,0) as varchar(10))+' and '+CAST(ISNULL(@_next_ID,0) as varchar(10)) ;
						*/
						FETCH NEXT FROM cursor_PowerShellMemoryConsumers INTO @_current_ID, @_current_line, @_current_PipeCounts;
					END
					ELSE
					BEGIN
						SELECT  @_current_ID = @_next_ID, @_current_line = @_next_line, @_current_PipeCounts = @_next_PipeCounts;
					END
				END

				CLOSE cursor_PowerShellMemoryConsumers;  
				DEALLOCATE cursor_PowerShellMemoryConsumers;  

				IF @verbose = 1
				BEGIN
					SELECT	'Folders Data after Deletion from @output table' AS RunningQuery, *
					FROM	@output
					ORDER BY ID;
				END
			END

			IF @forceExecute = 0
			BEGIN
				IF EXISTS (SELECT * FROM @output AS o WHERE line IS NOT NULL AND PipeCounts <> 7 )
				BEGIN
					PRINT	'	/*	Deleting few rows which are not as per our format.
	In order to find those rows, kindly add @verbose = 1 parameter and check table result for ''Folders Data where PipeCounts <> 4'' */';
					
					DELETE @output
					WHERE line IS NULL
					OR	PipeCounts <> 4;

					IF @verbose = 1
					BEGIN
						SELECT	Q.RunningQuery, o.*
						FROM  (	SELECT 'Folders Data where PipeCounts <> 4' AS RunningQuery ) Q
						LEFT OUTER JOIN
							  (SELECT * FROM @output AS o WHERE line IS NOT NULL AND PipeCounts <> 4 ) AS o
						ON		1 = 1;
					END
				END
			END

			IF @verbose=1 
				PRINT	'	Extract Details for Folders from PowerShell command output';
			;WITH t_RawData AS
			(
				SELECT	ID = 1, 
						line, 
						expression = left(line,CHARINDEX('|',line)-1), 
						searchExpression = SUBSTRING ( line , CHARINDEX('|',line)+1, LEN(line)+1 ), 
						delimitorPosition = CHARINDEX('|',SUBSTRING ( line , CHARINDEX('|',line)+1, LEN(line)+1 ))
				FROM	@output
				WHERE	line IS NOT NULL
				-- 
				UNION all
				--
				SELECT	ID = ID + 1, 
						line, 
						expression = CASE WHEN delimitorPosition = 0 THEN searchExpression ELSE left(searchExpression,delimitorPosition-1) END, 
						searchExpression = CASE WHEN delimitorPosition = 0 THEN NULL ELSE SUBSTRING(searchExpression,delimitorPosition+1,len(searchExpression)+1) END, 
						delimitorPosition = CASE WHEN delimitorPosition = 0 THEN -1 ELSE CHARINDEX('|',SUBSTRING(searchExpression,delimitorPosition+1,len(searchExpression)+1)) END
				FROM	t_RawData
				WHERE	delimitorPosition >= 0
			)
			,T_Folders AS 
			(
				SELECT	line, Name, Owner, CreationTime, LastAccessTime, LastWriteTime
				FROM (
						SELECT	line, --Name, Owner, CreationTime, LastAccessTime, LastWriteTime
								[Column] =	CASE	ID
													WHEN 1
													THEN 'Name'
													WHEN 2
													THEN 'Owner'
													WHEN 3
													THEN 'CreationTime'
													WHEN 4
													THEN 'LastAccessTime'
													WHEN 5
													THEN 'LastWriteTime'
													ELSE NULL
													END,
								[Value] = expression
						FROM	t_RawData
						) as up
				PIVOT (MAX([Value]) FOR [Column] IN (Name, Owner, CreationTime, LastAccessTime, LastWriteTime)) as pvt
			)
			INSERT #VolumeFolders
				( PathID, Name, SizeBytes, TotalChildItems, Owner, CreationTime, LastAccessTime, LastWriteTime, IsFolder )
			SELECT	[PathID] = d.PathID, 
					[Name] = LTRIM(RTRIM(d.Name)), 
					--[ParentPathID] = NULL, 
					[SizeBytes] = CASE WHEN d.PathID = 1 THEN v.[SizeBytes] ELSE fd.SizeBytes END, 
					[TotalChildItems] = CASE WHEN d.PathID = 1 THEN v.TotalChildItems ELSE fd.TotalChildItems END
					,LTRIM(RTRIM(d.Owner)) AS Owner
					,REPLACE(REPLACE(d.CreationTime, CHAR(13), ''), CHAR(10), '') AS CreationTime
					,REPLACE(REPLACE(d.LastAccessTime, CHAR(13), ''), CHAR(10), '') AS LastAccessTime
					,REPLACE(REPLACE(d.LastWriteTime, CHAR(13), ''), CHAR(10), '') AS LastWriteTime
					,IsFolder = 1
			FROM  (
					SELECT	PathID = 1,
							[Name] = REPLACE(@oldVolume,'\',''),
							[Owner] = NULL, 
							CreationTime = NULL, 
							LastAccessTime = NULL, 
							LastWriteTime = NULL
					--
					UNION ALL
					--
					SELECT	PathID = (DENSE_RANK() OVER (ORDER BY fldr.Name))+1, -- Leaving PathID = 1 for Base Drive like E:\
							Name, Owner, CreationTime, LastAccessTime, LastWriteTime					
					FROM	T_Folders as fldr
				  ) AS d -- as directory
			LEFT OUTER JOIN
				  (
					SELECT	--[PathID] = ParentPathID, 
							[Name] = ParentPath,
							[SizeBytes] = SUM(SizeBytes),
							[TotalChildItems] = COUNT(*),
							[Owner] = NULL, [CreationTime] = NULL, [LastAccessTime] = NULL, [LastWriteTime] = NULL, IsFolder = 1
					FROM	#VolumeFiles as vf
					GROUP BY ParentPath
				  ) AS fd -- as folder Details
				ON	fd.Name = d.Name
			FULL OUTER JOIN
				  (
					SELECT	[PathID] = 1, 
							--[Name] = REPLACE(@oldVolume,'\',''),
							[SizeBytes] = SUM(SizeBytes),
							[TotalChildItems] = COUNT(*)
					FROM	#VolumeFiles as vf
				  ) AS v -- as volume
				ON	v.PathID = d.PathID;

			IF @verbose = 1
				PRINT	'	Updating ParentPathID of #VolumeFolders.';
			-- Updating ParentPathID into #VolumeFolders table
			UPDATE	c
			SET	c.ParentPathID = p.PathID
			FROM	#VolumeFolders as c
			LEFT JOIN
					#VolumeFolders as p
				ON	p.Name = LEFT(c.[Name],LEN(c.[Name])-CHARINDEX('\',REVERSE(c.[Name])))
			WHERE	c.PathID <> 1;

			IF @verbose = 1
				PRINT	'	Updating ParentPathID of #VolumeFiles.';
			-- Updating ParentPathID into #VolumeFiles table
			UPDATE	c
			SET		c.ParentPathID = p.PathID
			FROM	#VolumeFiles as c
			LEFT JOIN
					#VolumeFolders as p
				ON	c.[ParentPath] = (CASE WHEN p.PathID = 1 THEN p.Name+'\' else p.Name END);
			
			IF @verbose = 1
				PRINT	'	Updating (\) blackslash issue for Root drive on #VolumeFolders.';
			UPDATE #VolumeFolders
			SET Name = (CASE WHEN CHARINDEX('\',[Name]) = 0 THEN [Name]+'\' ELSE [Name] END)
			WHERE	PathID = 1;

			IF @verbose = 1
				PRINT	'	Updating size of Folders'
			--DECLARE @pathID INT = 32;
			;WITH t_childfolders as
			(	SELECT f.PathID, f.PathID as ReferencePathID -- get top most path details
				FROM #VolumeFolders as f --where f.PathID = @pathID		-- select * from tempdb..VolumeFolders as f where f.ParentPathID = 32
				--
				UNION ALL
				--
				SELECT fd.PathID, b.ReferencePathID
				FROM t_childfolders as b
				INNER JOIN
					#VolumeFolders as fd -- get all folders directly under base path. Say 1 base path x 10 direct child folders
					ON fd.ParentPathID = b.PathID
			)
			UPDATE	o
			SET	SizeBytes = i.SizeBytes
			FROM	#VolumeFolders as o
			INNER JOIN
				  (	SELECT fd.ReferencePathID, SUM(SizeBytes) AS SizeBytes
					from #VolumeFiles as fl
					inner join t_childfolders as fd
					on fd.PathID = fl.ParentPathID
					GROUP BY fd.ReferencePathID
				  ) as i
			ON	o.PathID = i.ReferencePathID

			IF @verbose=1
			BEGIN
				PRINT	'	Values populated for #VolumeFolders';

				SELECT	Q.RunningQuery, o.*
				FROM  (	SELECT 'SELECT * FROM #VolumeFiles;' AS RunningQuery ) as Q
				LEFT JOIN
						#VolumeFiles AS o
					ON	1 = 1;

				PRINT	'	SELECT * FROM #VolumeFolders;'
				SELECT	Q.RunningQuery, o.*
				FROM  (	SELECT 'SELECT * FROM #VolumeFolders;' AS RunningQuery ) as Q
				LEFT JOIN
						#VolumeFolders AS o
					ON	1 = 1;
			END

			IF @verbose = 1
				PRINT	'	Showing result after Combining data of #VolumeFolders & #VolumeFiles.';

			IF @sortBySize = 0
			BEGIN
				SELECT	IsFolder = CASE WHEN IsFolder = 0 THEN '' ELSE CAST(IsFolder AS VARCHAR(2)) END,--ISNULL(NULLIF(IsFolder,0),''), 
						Name, --ParentPathID, 
						Size, TotalChildItems, Owner, CreationTime, LastAccessTime, LastWriteTime
						,SizeBytes ,[Path]
				FROM  (
						select	PathID, Name, ParentPathID, 
								SizeBytes, Size, TotalChildItems, Owner, CreationTime, LastAccessTime, LastWriteTime, IsFolder, Name as [Path]
						from	#VolumeFolders
						--
						UNION ALL
						--
						select	PathID = ParentPathID, 
								--Name = '|' + REPLICATE(' ',ParentPathID) + '|  '+Name, 
								Name = REPLICATE('|   ',LEN(ParentPath)-LEN(REPLACE(ParentPath,'\',''))+(CASE WHEN ParentPathID =1 THEN 0 ELSE 1 END))+Name,
								ParentPathID, --ParentPath, 
								SizeBytes, Size, TotalChildItems = NULL, Owner, CreationTime, LastAccessTime, LastWriteTime, IsFolder=0, ParentPath as [Path]
						from	#VolumeFiles
					  ) AS T
				ORDER BY PathID, IsFolder desc, Name;
			END
			ELSE
			BEGIN
				SELECT ParentPath, Name, Size, Owner, CreationTime, LastAccessTime, LastWriteTime 
				FROM #VolumeFiles
				ORDER BY SizeBytes DESC;
			END
			
			IF @verbose=1 
				PRINT	'
/*	******************** Begin:	@getVolumeSpaceConsumers = 1 *****************************/';
		END
		--	----------------------------------------------------------------------------
			--	End:	@getVolumeSpaceConsumers = 1
		--	============================================================================
		

		--	============================================================================
			--	Begin:	@getLogInfo = 1
		--	----------------------------------------------------------------------------
		IF	@getLogInfo = 1
		BEGIN
			IF @verbose=1 
				PRINT	'
/*	******************** Begin:	@getLogInfo = 1 *****************************/';

			IF @_productVersion LIKE '10.%' OR @_productVersion LIKE '9.%'
				ALTER TABLE #stage DROP COLUMN [RecoveryUnitId];

			INSERT @Databases -- Eliminate non-accessible DBs
			SELECT name FROM sys.databases d WHERE DATABASEPROPERTYEX(name, 'Status') = 'ONLINE';

			IF	@verbose = 1
			BEGIN
				PRINT	'	SELECT * FROM @Databases;';
				SELECT 'SELECT * FROM @Databases;' AS RunningQuery, * FROM @Databases;
			END
				
	
			SET	@_loopCounter = 1;
			SET	@_loopCounts = (SELECT COUNT(*) FROM @Databases);

			IF @verbose=1 
				PRINT	'	Start Loop, and find VLFs for each log file of every db';
			WHILE (@_loopCounter <= @_loopCounts)
			BEGIN
				--	Truncate temp table
				TRUNCATE TABLE #stage;
				SET @_dbName = NULL;
				SELECT @_dbName = DBName FROM @Databases WHERE ID = @_loopCounter ;
				SET @_loopSQLText = 'DBCC LOGINFO ('+QUOTENAME(@_dbName)+')
		WITH  NO_INFOMSGS;';
				
				INSERT #stage
				EXEC (@_loopSQLText);

				INSERT #LogInfoByFile
				SELECT	@_dbName AS DBName,
						file_id as FileId,
						COUNT(*) AS VLFCount
				FROM	#stage
				GROUP BY [file_id];

				SET @_loopCounter = @_loopCounter + 1;
			END
			
			IF	@verbose = 1
			BEGIN
				PRINT	'	Finished finding VLFs for each log file of every db
		SELECT * FROM #LogInfoByFile;';
				SELECT 'SELECT * FROM #LogInfoByFile;' AS RunningQuery, * FROM #LogInfoByFile;
			END

			IF	@verbose = 1
				PRINT	'	Creating table #LogFiles.';

			IF OBJECT_ID('tempdb..#LogFiles') IS NOT NULL
				DROP TABLE #LogFiles;
			;WITH T_Files_Size AS
			(
				SELECT mf.database_id, CONVERT(DECIMAL(20,2),((SUM(size)*8.0)/1024/1024)) AS [TotalFilesSize(GB)] FROM sys.master_files AS mf WHERE mf.type_desc = 'LOG' GROUP BY mf.database_id
			)
			,T_Files_Filegroups AS
			(
				SELECT	mf.database_id as [DB_ID], DB_NAME(mf.database_id) AS [DB_Name], CASE WHEN d.is_read_only = 1 THEN 'Read_Only' ELSE DATABASEPROPERTYEX(DB_NAME(mf.database_id), 'Status') END as DB_State
						,[TotalFilesSize(GB)]
						,(CASE	WHEN 	growth = 0 
								THEN 	'0' 
								WHEN 	is_percent_growth = 1 
								THEN 	CAST(growth AS VARCHAR(5))+'%' 
								ELSE 	CAST(CONVERT( DECIMAL(20,2),((mf.growth*8.0)/1024.0)) AS VARCHAR(20))+' mb'
						END) AS [growth(GB)]
						,(CASE	WHEN 	(size *8.0)/1024/1024/1024 >= 5.0 -- (page counts * 8) {KB}/1024 {MB}/1024 {GB}
								THEN 	CAST(CAST((size *8.0)/1024/1024/1024 AS numeric(20,2)) AS VARCHAR(20))+' tb'
								WHEN 	(size *8.0)/1024/1024 >= 1.0 -- (page counts * 8) {KB}/1024 {MB}/1024 {GB}
								THEN 	CAST(CAST((size *8.0)/1024/1024 AS numeric(20,2)) AS VARCHAR(20))+' gb'
								ELSE 	CAST(CAST((size *8.0)/1024 AS numeric(20,2)) AS VARCHAR(20))+' mb'
						END) AS [size(GB)]
						,mf.name as [FileName] 
						,v.Volume  as [Volume]
						,mf.* 
						,d.recovery_model_desc
				FROM	sys.master_files AS mf
				INNER JOIN
						sys.databases as d
				ON		d.database_id = mf.database_id
				LEFT JOIN
						T_Files_Size AS l
					ON	l.database_id = mf.database_id
				OUTER APPLY
						(	SELECT	v2.Volume
							FROM  (	SELECT MAX(LEN(v.Volume)) AS Max_Volume_Length FROM @mountPointVolumes as v WHERE mf.physical_name LIKE (v.Volume+'%') ) as v1
							INNER JOIN
								  (	SELECT v.Volume FROM @mountPointVolumes as v WHERE mf.physical_name LIKE (v.Volume+'%') ) as v2
								ON	LEN(v2.Volume) = v1.Max_Volume_Length
						) as v
				--OUTER APPLY
				--	(	SELECT v.Volume FROM @mountPointVolumes AS v WHERE mf.physical_name LIKE (v.Volume+'%')	) AS v
				WHERE	mf.type_desc = 'LOG'
			)
			,T_Volumes_Derived AS
			(
				SELECT	Volume
					   ,[capacity(MB)]
					   ,[freespace(MB)]
					   ,VolumeName
					   ,[capacity(GB)]
					   ,[freespace(GB)]
					   ,[freespace(%)]
				FROM	@mountPointVolumes as v
				WHERE	EXISTS (SELECT * FROM T_Files_Filegroups AS fg WHERE v.Volume = fg.[Volume])
					--OR	v.Volume LIKE '[A-Z]:\LOG[S][0-9]\'
					--OR	v.Volume LIKE '[A-Z]:\LOG[S][0-9][0-9]\'
			)
			,T_Files AS
			(
				SELECT	DB_ID, DB_Name, [TotalFilesSize(GB)], DB_State,
						f.FileName+' (VLF_Count|Size|AutoGrowth :: '+CAST(l.VLFCount AS VARCHAR(20))+'|'+[size(GB)]+'|'+[growth(GB)]+')' AS FileSettings, 
						v.VolumeName+QUOTENAME(v.Volume)+' = '+CAST([freespace(GB)] AS VARCHAR(20))+'GB('+CAST([freespace(%)] AS VARCHAR(20))+'%) Free of '+CAST([capacity(GB)] AS VARCHAR(20))+' GB' as FileDrive
						,growth, [growth(GB)], [FileName], l.VLFCount
						,v.Volume, [capacity(MB)], [freespace(MB)], VolumeName, [capacity(GB)], [freespace(GB)], [freespace(%)]
						,ROW_NUMBER()OVER(PARTITION BY v.Volume, f.DB_Name ORDER BY f.[file_id]) AS FileID
				FROM	T_Files_Filegroups AS f
				LEFT JOIN
						#LogInfoByFile AS l
					ON	l.DBName = DB_Name AND l.FileId = f.file_id
				RIGHT OUTER JOIN
						T_Volumes_Derived AS v
					ON	v.Volume = f.[Volume]
			)
			,T_Files_Derived AS
			(
				SELECT	DB_ID, DB_Name, DB_State, [TotalFilesSize(GB)], STUFF(
								(SELECT ', ' + f2.FileSettings
								 FROM T_Files as f2
								 WHERE f2.Volume = f.Volume AND f2.DB_Name = f.DB_Name
								 FOR XML PATH (''))
								  , 1, 1, ''
							) AS Files, FileDrive, growth, [growth(GB)], FileName, Volume, [capacity(MB)], [freespace(MB)], VolumeName, [capacity(GB)], [freespace(GB)], [freespace(%)], FileID
				FROM	T_Files as f
				WHERE	f.FileID = 1
			)
			SELECT	*
			INTO	#LogFiles
			FROM	T_Files_Derived;

			IF	@verbose = 1
			BEGIN
				PRINT	'	SELECT * FROM #LogFiles;';
				SELECT 'SELECT * FROM #LogFiles;' AS RunningQuery, * FROM #LogFiles ORDER BY DB_Name;
			END

			IF @verbose = 1
			BEGIN
				PRINT	'	Finding and arranging the log files names for Pivoting';
				SELECT DISTINCT TOP 100 'Finding and arranging the log files names for Pivoting' AS RunningQuery, FileDrive 
							,LEFT(FileDrive,4) AS First4Char, CAST(SUBSTRING(Volume, PATINDEX('%[0-9]%', Volume), PATINDEX('%[0-9][^0-9]%', Volume + 't') - PATINDEX('%[0-9]%', 
							Volume) + 1) AS INT) AS Number, Volume
							,Vol_Order =	(CASE	WHEN	Volume LIKE '%MSSQL%'
									THEN	1
									WHEN	Volume LIKE '%TempDB%'
									THEN	2
									ELSE	3
									END)
					FROM #LogFiles
					order by Vol_Order,First4Char,Number;
			END
	
			SELECT	@_commaSeparatedMountPointVolumes = COALESCE(@_commaSeparatedMountPointVolumes+', '+QUOTENAME(FileDrive), QUOTENAME(FileDrive)) --DISTINCT FileDrive
			FROM	(SELECT DISTINCT TOP 100 FileDrive 
							,LEFT(FileDrive,4) AS First4Char, CAST(SUBSTRING(Volume, PATINDEX('%[0-9]%', Volume), PATINDEX('%[0-9][^0-9]%', Volume + 't') - PATINDEX('%[0-9]%', 
							Volume) + 1) AS INT) AS Number, Volume
							,Vol_Order =	(CASE	WHEN	Volume LIKE '%MSSQL%'
									THEN	1
									WHEN	Volume LIKE '%TempDB%'
									THEN	2
									ELSE	3
									END)
					FROM #LogFiles
					order by Vol_Order,First4Char,Number
			) AS FD;
			
			IF @verbose = 1
				PRINT	'	@_commaSeparatedMountPointVolumes = '+@_commaSeparatedMountPointVolumes;

			--	Unfortunately table variables are out of scope of dynamic SQL, trying temp table method
			IF OBJECT_ID('tempdb..#filterDatabaseNames_Logs') IS NOT NULL
				DROP TABLE #filterDatabaseNames_Logs;
			SELECT * INTO #filterDatabaseNames_Logs FROM @filterDatabaseNames;

			SET @_sqlGetInfo = '
			SELECT	DB_ID, DB_Name, DB_State, [TotalFilesSize(GB)] as [TotalLogFilesSize(GB)], '+@_commaSeparatedMountPointVolumes+'
			FROM  (
					SELECT	DB_ID, DB_Name, DB_State, [TotalFilesSize(GB)], Files, FileDrive
					FROM	#LogFiles
					WHERE	NOT EXISTS (SELECT * FROM #filterDatabaseNames_Logs) 
						OR	DB_Name IN (SELECT d.DBName FROM #filterDatabaseNames_Logs AS d)
				  ) up
			PIVOT	(MAX(Files) FOR FileDrive IN ('+@_commaSeparatedMountPointVolumes+')) AS pvt
			WHERE	DB_Name IS NOT NULL
			ORDER BY [DB_Name];
			';

			EXEC (@_sqlGetInfo);
	
			IF @verbose=1 
				PRINT	'/*	******************** End:	@getLogInfo = 1 *****************************/
';
		END
		--	----------------------------------------------------------------------------
			--	End:	@getLogInfo = 1
		--	============================================================================

		--	============================================================================
		--	Begin:	@optimizeLogFiles = 1
		--	----------------------------------------------------------------------------
		IF	@optimizeLogFiles = 1
		BEGIN
			IF @verbose=1 
				PRINT	'
/*	******************** Begin:	@optimizeLogFiles = 1 *****************************/';
			
			IF CAST(LEFT(@_productVersion,CHARINDEX('.',@_productVersion)-1) AS INT) >= 12
			BEGIN
				PRINT 'You are running on SQL Server 2014 and later. So there is no need of Optimizing Log Files.
https://www.sqlskills.com/blogs/paul/important-change-vlf-creation-algorithm-sql-server-2014/
';
			END
			ELSE
			BEGIN -- Else Block for @_productVersion Check
				IF @_productVersion LIKE '10.%' OR @_productVersion LIKE '9.%'
					ALTER TABLE #stage DROP COLUMN [RecoveryUnitId];

				INSERT @Databases -- Eliminate non-accessible DBs
				SELECT name FROM sys.databases d 
				WHERE DATABASEPROPERTYEX(name, 'Status') = 'ONLINE'
				AND (	NOT EXISTS (SELECT * FROM @filterDatabaseNames as f WHERE f.Category = 'IN')
						OR	d.database_id IN (SELECT DB_ID(f.DBName) FROM @filterDatabaseNames AS f WHERE f.Category = 'IN')
					)
				AND (	NOT EXISTS (SELECT * FROM @filterDatabaseNames as f WHERE f.Category = 'NOT IN')
						OR	d.database_id NOT IN (SELECT DB_ID(f._DBName) FROM @filterDatabaseNames AS f WHERE f.Category = 'NOT IN')
					);

				IF	@verbose = 1
				BEGIN
					PRINT	'	SELECT * FROM @Databases;';
					SELECT 'SELECT * FROM @Databases;' AS RunningQuery, * FROM @Databases;
				END
				
	
				SET	@_loopCounter = 1;
				SET	@_loopCounts = (SELECT COUNT(*) FROM @Databases);

				IF @verbose=1 
					PRINT	'	Start Loop, and find VLFs for each log file of every db';
				WHILE (@_loopCounter <= @_loopCounts)
				BEGIN
					--	Truncate temp table
					TRUNCATE TABLE #stage;
					SET @_dbName = NULL;
					SELECT @_dbName = DBName FROM @Databases WHERE ID = @_loopCounter ;
					SET @_loopSQLText = 'DBCC LOGINFO ('+QUOTENAME(@_dbName)+')
			WITH  NO_INFOMSGS;';
				
					INSERT #stage
					EXEC (@_loopSQLText);

					INSERT #LogInfoByFile
					SELECT	@_dbName AS DBName,
							file_id as FileId,
							COUNT(*) AS VLFCount
					FROM	#stage
					GROUP BY [file_id];

					SET @_loopCounter = @_loopCounter + 1;
				END
			
				IF	@verbose = 1
				BEGIN
					PRINT	'	Finished finding VLFs for each log file of every db
			SELECT * FROM #LogInfoByFile;';
					SELECT 'SELECT * FROM #LogInfoByFile;' AS RunningQuery, * FROM #LogInfoByFile;
				END

				IF	@verbose = 1
					PRINT	'	Creating table #LogFiles.';

				IF OBJECT_ID('tempdb..#LogFilesVLF') IS NOT NULL
					DROP TABLE #LogFilesVLF;
				;WITH T_Files_Size AS
				(
					SELECT mf.database_id, CONVERT(DECIMAL(20,2),((SUM(size)*8.0)/1024/1024)) AS [TotalFilesSize(GB)] FROM sys.master_files AS mf WHERE mf.type_desc = 'LOG' GROUP BY mf.database_id
				)
				,T_Files_Filegroups AS
				(
					SELECT	mf.database_id as [DB_ID], DB_NAME(mf.database_id) AS [DB_Name], CASE WHEN d.is_read_only = 1 THEN 'Read_Only' ELSE DATABASEPROPERTYEX(DB_NAME(mf.database_id), 'Status') END as DB_State
							,[TotalFilesSize(GB)]
							,(CASE	WHEN 	growth = 0 
									THEN 	'0' 
									WHEN 	is_percent_growth = 1 
									THEN 	CAST(growth AS VARCHAR(5))+'%' 
									ELSE 	CAST(CONVERT( DECIMAL(20,2),((mf.growth*8.0)/1024.0)) AS VARCHAR(20))+' mb'
							END) AS [growth(GB)]
							,(CASE	WHEN 	(size *8.0)/1024/1024/1024 >= 5.0 -- (page counts * 8) {KB}/1024 {MB}/1024 {GB}
									THEN 	CAST(CAST((size *8.0)/1024/1024/1024 AS numeric(20,2)) AS VARCHAR(20))+' tb'
									WHEN 	(size *8.0)/1024/1024 >= 1.0 -- (page counts * 8) {KB}/1024 {MB}/1024 {GB}
									THEN 	CAST(CAST((size *8.0)/1024/1024 AS numeric(20,2)) AS VARCHAR(20))+' gb'
									ELSE 	CAST(CAST((size *8.0)/1024 AS numeric(20,2)) AS VARCHAR(20))+' mb'
							END) AS [size(GB)]
							,mf.name as [FileName] 
							,v.Volume  as [Volume]
							,mf.* 
							,d.recovery_model_desc
					FROM	sys.master_files AS mf
					INNER JOIN
							sys.databases as d
					ON		d.database_id = mf.database_id
					LEFT JOIN
							T_Files_Size AS l
						ON	l.database_id = mf.database_id
					OUTER APPLY
							(	SELECT	v2.Volume
								FROM  (	SELECT MAX(LEN(v.Volume)) AS Max_Volume_Length FROM @mountPointVolumes as v WHERE mf.physical_name LIKE (v.Volume+'%') ) as v1
								INNER JOIN
									  (	SELECT v.Volume FROM @mountPointVolumes as v WHERE mf.physical_name LIKE (v.Volume+'%') ) as v2
									ON	LEN(v2.Volume) = v1.Max_Volume_Length
							) as v
					--OUTER APPLY
					--	(	SELECT v.Volume FROM @mountPointVolumes AS v WHERE mf.physical_name LIKE (v.Volume+'%')	) AS v
					WHERE	mf.type_desc = 'LOG'
				)
				SELECT	*
						,_threshold_mb = (CASE WHEN ((size*8.0)/1024/1024) <= 1 THEN CAST(NULL AS INT) WHEN ((size*8.0)/1024/1024) <= 10 THEN 1024 WHEN ((size*8.0)/1024/1024) <= 65 THEN 66560 END)
						,_autogrowth_gb = (CASE WHEN ((size*8.0)/1024/1024) <= 10 THEN 1 WHEN ((size*8.0)/1024/1024) <= 65 THEN 2 ELSE 8 END)
				INTO	#LogFilesVLF
				FROM	T_Files_Filegroups as d
				WHERE	(	NOT EXISTS (SELECT * FROM @filterDatabaseNames as f WHERE f.Category = 'IN')
						OR	d.database_id IN (SELECT DB_ID(f.DBName) FROM @filterDatabaseNames AS f WHERE f.Category = 'IN')
						)
					AND (	NOT EXISTS (SELECT * FROM @filterDatabaseNames as f WHERE f.Category = 'NOT IN')
							OR	d.database_id NOT IN (SELECT DB_ID(f._DBName) FROM @filterDatabaseNames AS f WHERE f.Category = 'NOT IN')
						);

				IF	@verbose = 1
				BEGIN
					PRINT	'	SELECT * FROM #LogFilesVLF;';
					SELECT 'SELECT * FROM #LogFilesVLF;' AS RunningQuery, * FROM #LogFilesVLF ORDER BY DB_Name;
				END
			
				--	Process each log file using Cursor
				IF @verbose = 1
					PRINT 'Declaring Cursor to iterate through databases that require VLF Optimization';
				DECLARE @c_DBName VARCHAR(255)
						,@c_FileName VARCHAR(255)
						,@c_VLFCount INT
						,@c_Size_mb INT
						,@c_recovery_model_desc VARCHAR(20)
						,@c_threshold_mb INT
						,@c_new_autogrowth INT;

				DECLARE cursor_logFile CURSOR LOCAL STATIC FORWARD_ONLY READ_ONLY FOR
					SELECT	[DB_Name], FileName, vi.VLFCount, ((size*8)/1024) as Size_mb, recovery_model_desc, _autogrowth_gb, _threshold_mb
					FROM	#LogFilesVLF as f
					JOIN	#LogInfoByFile as vi
						ON	vi.DBName = f.[DB_Name]
						AND	vi.FileId = f.[file_id]
					WHERE f.[DB_Name] IN (SELECT i.DBName FROM #LogInfoByFile i GROUP BY i.DBName HAVING SUM(i.VLFCount) >= @vlfCountThreshold )
					AND vi.VLFCount >= ((((f.size*8.0)/1024)/8000)*16);

				OPEN cursor_logFile;
				FETCH NEXT FROM cursor_logFile INTO @c_DBName, @c_FileName, @c_VLFCount, @c_Size_mb, @c_recovery_model_desc, @c_new_autogrowth, @c_threshold_mb;

				WHILE @@FETCH_STATUS = 0
				BEGIN -- Cursor block
					SET @_loopSQLText_BackupLog = '';
					SET @_loopSQLText_DbccShrinkFile = '';
					SET @_loopSQLText_AlterDbModifySize = '';
					SET @_loopSQLText_AlterDbAutoGrowth = '';
					SET @_loopSQLText = '
/*	******************************************************************************************************
TSQL Code to remove high VLF Count for ['+@c_DBName+'] database. 
		Currently the log file '''+@c_FileName+''' has '+cast(@c_VLFCount as varchar(20))+' VLFs which is more than @vlfCountThreshold ('+cast(@vlfCountThreshold as varchar(20))+')
		Shrinking the log file '''+@c_FileName+''' to minimum possible size, and
		Trying to re-grow it to actual size of '+cast(@c_Size_mb as varchar(20))+' MB in chunks of '+cast(@c_new_autogrowth as varchar(10))+'GB
		--	https://dba.stackexchange.com/a/180150/98923
		--	https://sqlperformance.com/2013/02/system-configuration/transaction-log-configuration
*	******************************************************************************************************/
	';
				
					IF @c_recovery_model_desc <> 'SIMPLE' AND @generateLogBackup = 1
					BEGIN
						SET @_loopSQLText_BackupLog += '
-- Step 1: backup of the transaction log
USE [master];
BACKUP LOG ['+@c_DBName+'] TO  DISK = N''F:\SQLBackups\'+@c_DBName+'.trn'' WITH NOFORMAT, INIT, NAME = N''Transaction Log Backup during SpaceCapacityAutomation @OptimizeLogFiles'', SKIP, NOREWIND, NOUNLOAD, COMPRESSION, STATS = 3;
'				END
					ELSE
					BEGIN
						SET @_loopSQLText_BackupLog += '
-- Step 1: backup of the transaction log. 
	-- No action required as either database is in SIMPLE RECOVERY model
	-- Or, @generateLogBackup is set to 0

';
					END
				
					IF @verbose = 1
						PRINT 'Setting @_loopSQLText_DbccShrinkFile variable';
					SET @_loopSQLText_DbccShrinkFile = (CASE WHEN @forceExecute = 0 THEN '-- Step 2: Shrink the log file
' ELSE '' END) + (CASE WHEN @forceExecute = 0 THEN 'USE ['+@c_DBName+']; ' ELSE '' END)+ 'DBCC SHRINKFILE (N'''+@c_FileName+''' , 0, TRUNCATEONLY);'
				

					IF @forceExecute = 0
					BEGIN
						SET @_loopSQLText = @_loopSQLText + @_loopSQLText_BackupLog + @_loopSQLText_DbccShrinkFile; --+ @_loopSQLText_AlterDbModifySize + @_loopSQLText_AlterDbAutoGrowth;
						IF @verbose = 1
							PRINT '@_loopSQLText = ';
						PRINT @_loopSQLText+'
GO
';
					END
					ELSE
					BEGIN -- Execute SHRINKFILE
						BEGIN TRY
							IF @_loopSQLText_BackupLog IS NOT NULL AND LEN(@_loopSQLText_BackupLog) > 0
							BEGIN
								IF @verbose = 1
								BEGIN
									PRINT 'Proceeding to Execute Backup Transaction Log';
									PRINT '@_loopSQLText_BackupLog = '+CHAR(10)+CHAR(13)+@_loopSQLText_BackupLog;
								END

								EXEC (@_loopSQLText_BackupLog);
							END							

							IF @_loopSQLText_DbccShrinkFile IS NOT NULL AND LEN(@_loopSQLText_DbccShrinkFile) > 0
							BEGIN -- block for DbccShrinkFile
								DELETE FROM @T_DbccShrinkFile_Raw;
								DELETE FROM @T_DbccShrinkFile;
								TRUNCATE TABLE #T_DbccShrinkFile_LineSplit;

								IF @verbose = 1
								BEGIN
									PRINT 'Proceeding to perform Shrink Operation';
									PRINT @_loopSQLText_DbccShrinkFile;
								END								

								SET    @_sqlcmdCommand = 'sqlcmd -S "'+@@servername+'" -d "'+@c_DBName+'" -Q "'+@_loopSQLText_DbccShrinkFile+'"';
								IF @verbose = 1
									PRINT '@_sqlcmdCommand = '+CHAR(10)+CHAR(13)+@_sqlcmdCommand;

								INSERT @T_DbccShrinkFile_Raw
								EXEC   master..xp_cmdshell @_sqlcmdCommand;

								IF @verbose = 1
								BEGIN
									SELECT	Q.RunningQuery, o.*
									FROM  (	SELECT 'select * from @T_DbccShrinkFile_Raw WHERE ID <= 4' AS RunningQuery ) Q
									LEFT OUTER JOIN
										  (SELECT * FROM @T_DbccShrinkFile_Raw WHERE ID <= 4 ) AS o
									ON		1 = 1;
								END				

								/*
									Sometimes we may get error in 1st row of above table variable
									Cannot shrink log file 2 (EntryAggregation_log) because the logical log file located at the end of the file is in use.
								*/
								SELECT	@_DbccShrinkFile_RawText = output
								FROM	@T_DbccShrinkFile_Raw
								WHERE	ID = 2 + (SELECT i.ID FROM @T_DbccShrinkFile_Raw as i WHERE i.output like 'DbId%');

								Set @_DbccShrinkFile_RawText+='X'

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
								INSERT #T_DbccShrinkFile_LineSplit
								SELECT	IND=i, NUM=CAST( num  AS INT)
								--INTO	#T_DbccShrinkFile_LineSplit
								FROM	T_DbccShrinkFile_LineSplit
								WHERE	num like '%[0-9]%';

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

								IF @verbose = 1
								BEGIN
									
									SELECT	Q.RunningQuery, o.*
									FROM  (	SELECT 'select * from @T_DbccShrinkFile' AS RunningQuery ) Q
									LEFT OUTER JOIN
										  (SELECT * FROM @T_DbccShrinkFile) AS o
									ON		1 = 1;
								END

								--	https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-shrinkfile-transact-sql?view=sql-server-2017#troubleshooting
								SELECT @_spaceReleasedBySHRINKFILE_MB = ((f.size - o.CurrentSize)*8.0/1024)
								FROM #LogFilesVLF as f INNER JOIN @T_DbccShrinkFile AS o 
								ON o.DBID = f.[DB_ID] AND o.FileID = f.[file_id];

								SET @_errorMSG = 'Space released by DBCC SHRINKFILE operation on '''+@c_FileName+''' of database ['+@c_DBName+'] = '+CAST(@_spaceReleasedBySHRINKFILE_MB AS VARCHAR(20))+' MB ('+CAST(((@_spaceReleasedBySHRINKFILE_MB*100)/@c_Size_mb) AS VARCHAR(20))+'%) 
Incase more space release is expected, then kindly execute below query to troubleshoot:-
select d.name, d.recovery_model_desc, d.log_reuse_wait_desc from sys.databases d where d.name = '''+@c_DBName+''';
';
								IF @verbose = 1
									PRINT @_errorMSG;
							END -- block for DbccShrinkFile
						END TRY
						BEGIN CATCH
							IF @_errorOccurred = 0
								SET @_errorOccurred = 1;

							--	Make a FAIL entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Fail' AS Status
									,'@optimizeLogFiles - Remove High VLF' AS Category
									,@c_DBName AS  DBName
									,NULL AS [FileGroup]
									,@c_FileName AS [FileName]
									,ERROR_MESSAGE() AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END CATCH
					END -- Execute SHRINKFILE
					
					-- No SHRINKFILE operation completed successfully
					IF @_errorOccurred = 0
					BEGIN -- Block for Incrementing File Size & Setting AutoGrowth
						IF @verbose = 1
							PRINT 'Logic Block for Incrementing File Size & Setting AutoGrowth..';

						IF @verbose = 1
							PRINT 'Setting @_DbSizeAfterShrink_MB ';
						IF @forceExecute = 1
						BEGIN
							SELECT @_DbSizeAfterShrink_MB = ((CurrentSize*8.0)/1024) FROM @T_DbccShrinkFile;
						END
						ELSE
							SET @_DbSizeAfterShrink_MB = 0;

						IF @verbose = 1
							PRINT '@_DbSizeAfterShrink_MB = '+CAST(@_DbSizeAfterShrink_MB as varchar(20));
						
						IF @verbose = 1
							PRINT 'Check if log file size <= 1024';
						IF @c_Size_mb <= 1024 -- <= 1 gb
						BEGIN
							IF @verbose = 1
								PRINT 'Proceeding  for logic when log file size <= 1024';

							SET @_loopSQLText_AlterDbModifySize = '

-- Step 3: Grow the log file back to the desired size
	-- and the FILEGROWTH to '+cast(@c_new_autogrowth as varchar(10))+'gb
USE [master];
ALTER DATABASE ['+@c_DBName+'] MODIFY FILE (NAME = N'''+@c_FileName+''', SIZE = '+cast(@c_new_autogrowth as varchar(10))+'GB, FILEGROWTH = '+cast(@c_new_autogrowth as varchar(10))+'GB);
'
						END
						ELSE
						BEGIN
							IF @verbose = 1
								PRINT 'Proceeding  for logic when log file size > 1024 (larger log files)';
							
							SELECT @_loopCounter=1, @_loopCounts=CEILING(( (@c_Size_mb-@_DbSizeAfterShrink_MB)/1024.0)/(1.0*@c_new_autogrowth));
							IF @verbose = 1
								PRINT '@_loopCounts = '+CAST(@_loopCounts as varchar(20))+CHAR(10)+'@c_new_autogrowth = '+CAST(@c_new_autogrowth AS VARCHAR(20));

							SET @_loopSQLText_AlterDbModifySize = '
-- Step 3: Grow the log file back to the desired size ('+cast(@c_Size_mb as varchar(20))+') from size '+CAST(@_DbSizeAfterShrink_MB/1024 AS VARCHAR(20))+' GB, 
	-- and with FILEGROWTH of '+cast(@c_new_autogrowth as varchar(10))+'gb
	-- In case of manual execution, please commentout MODIFY statements where SIZE mentioned is less than CurrentSize from output of DBCC SHRINKFILE
USE [master];'
							WHILE @_loopCounter < @_loopCounts
							BEGIN	-- Begin Block of Loop
								SET @_loopSQLText_AlterDbModifySize += '
ALTER DATABASE ['+@c_DBName+'] MODIFY FILE (NAME = N'''+@c_FileName+''''+(CASE WHEN @_loopCounter <> @_loopCounts THEN ', SIZE = '+CAST(FLOOR(@_DbSizeAfterShrink_MB/1024)+(@c_new_autogrowth*@_loopCounter) AS VARCHAR(20))+'GB' ELSE '' END)+');'
								SET @_loopCounter = @_loopCounter + 1;
							END		-- End Block of Loop 

							SET @_loopSQLText_AlterDbAutoGrowth += (CASE WHEN @forceExecute = 0 THEN 'GO

' ELSE '' END) + 'USE [master];
ALTER DATABASE ['+@c_DBName+'] MODIFY FILE (NAME = N'''+@c_FileName+''' , FILEGROWTH = '+CAST(@c_new_autogrowth AS VARCHAR(20))+'GB);
'+(CASE WHEN @forceExecute = 0 THEN 'GO'+CHAR(10) ELSE '' END);
						END	
						
						IF @forceExecute = 1
						BEGIN
							IF @verbose = 1
							BEGIN
								IF @_loopCounts <> 0
								BEGIN
									PRINT '@_loopSQLText_AlterDbModifySize = '+CHAR(10)+CHAR(13);
									PRINT @_loopSQLText_AlterDbModifySize;
								END
								ELSE
									PRINT 'Log file could not be shrinked. So nothing to Grow back'
							END
							
							IF @verbose = 1
								PRINT 'Proceeding to re-grow the Log file ['+@c_FileName+']';
							EXEC(@_loopSQLText_AlterDbModifySize);

							IF @verbose = 1
							BEGIN
								PRINT '@_loopSQLText_AlterDbAutoGrowth = '+CHAR(10)+CHAR(13);
								PRINT @_loopSQLText_AlterDbAutoGrowth;
							END

							IF @verbose = 1
								PRINT 'Proceeding to set autogrowth for Log file ['+@c_FileName+']';
							EXEC(@_loopSQLText_AlterDbAutoGrowth);
						END
						ELSE
						BEGIN
							IF @verbose = 1
								PRINT '@_loopSQLText_AlterDbModifySize = '+CHAR(10)+CHAR(13);
							PRINT @_loopSQLText_AlterDbModifySize;

							IF @verbose = 1
								PRINT '@_loopSQLText_AlterDbAutoGrowth = '+CHAR(10)+CHAR(13);
							PRINT @_loopSQLText_AlterDbAutoGrowth;
						END	

						--	Make a SUCCESS entry for Sending message to end user
						INSERT @OutputMessages
							(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
						SELECT	'Success' AS Status
								,'@optimizeLogFiles - Remove High VLF' AS Category
								,@c_DBName AS  DBName
								,NULL AS [FileGroup]
								,@c_FileName AS [FileName]
								,'Log File successfully optimized according to ideal VLF counts.' AS MessageDetails
								,@_loopSQLText + @_loopSQLText_BackupLog + @_loopSQLText_DbccShrinkFile + @_loopSQLText_AlterDbModifySize AS TSQLCode;

					END -- Block for Incrementing File Size & Setting AutoGrowth

					FETCH NEXT FROM cursor_logFile INTO @c_DBName, @c_FileName, @c_VLFCount, @c_Size_mb, @c_recovery_model_desc, @c_new_autogrowth, @c_threshold_mb;
				END  -- Cursor block

				CLOSE cursor_logFile;
				DEALLOCATE cursor_logFile;
	
				IF @verbose=1 
					PRINT	'/*	******************** End:	@optimizeLogFiles = 1 *****************************/
';
			END -- Else Block for @_productVersion Check
		END
	--	----------------------------------------------------------------------------
		--	End:	@getLogInfo = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@help = 1
	--	----------------------------------------------------------------------------
	HELP_GOTO_BOOKMARK:
	IF	@help = 1
	BEGIN
		IF @verbose=1 
			PRINT	'
/*	******************** Begin:	@help = 1 *****************************/';

		-- VALUES constructor method does not work in SQL 2005. So using UNION ALL
		SELECT	[Parameter Name], [Data Type], [Default Value], [Parameter Description], [Supporting Parameters]
		FROM	(SELECT	'!~~~ Version ~~~~!' as [Parameter Name],'Information' as [Data Type],'3.7' as [Default Value],'Last Updated - 13/Mar/2019' as [Parameter Description], 'https://github.com/imajaydwivedi/Space-Capacity-Automation#space-capacity-automation' as [Supporting Parameters]
					--
				UNION ALL
					--
				SELECT	'@help' as [Parameter Name],'BIT' as [Data Type],'0' as [Default Value],'Displays this help message.' as [Parameter Description], '' as [Supporting Parameters]
					--
				UNION ALL
					--
				SELECT	'@getInfo','BIT','0','Displays distribution of Data Files across multiple data volumes. It presents file details like database name, its file groups, db status, logical name and autogrowth setting, and volume details like free space and total space.', '@DBs2Consider, @verbose' as [Supporting Parameters]
					--
				UNION ALL
					--
				SELECT	'@volumeInfo','BIT','0','Displays Total size, Used Space, Free Space and percentage for all Volumes/disk drives.', '@verbose' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@getLogInfo','BIT','0','Displays distribution of Log Files across multiple log volumes. It presents log file details like database name, db status, logical name, size, VLF counts and autogrowth setting, and volume details like free space and total space.', '@DBs2Consider, @verbose' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@addDataFiles','BIT','0','This generates TSQL code for adding data files on @newVolume for data files present on @oldVolume for each combination of database and filegroup.', '@newVolume, @oldVolume, @DBs2Consider, @forceExecute, @verbose' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@addLogFiles','BIT','0','This generates TSQL code for adding log files on @newVolume for log files present on @oldVolume for each database.', '@newVolume, @oldVolume, @DBs2Consider, @allowMultiVolumeUnrestrictedFiles, @forceExecute, @verbose' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@optimizeLogFiles','BIT','0','This generates TSQL code for removing High VLF counts from Log files.', '@vlfCountThreshold, @DBs2Consider, @forceExecute, @verbose' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@restrictDataFileGrowth','BIT','0','This generates TSQL code for restricting growth of Data files on @oldVolume.', '@oldVolume, @DBs2Consider, @forceExecute, @verbose' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@restrictLogFileGrowth','BIT','0','This generates TSQL code for restricting growth of Log files on @oldVolume.', '@oldVolume, @DBs2Consider, @forceExecute, @verbose' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@generateCapacityException','BIT','0','This generates TSQL code for adding capacity exception on MNA alerting database server for @oldVolume.', '@oldVolume' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@unrestrictFileGrowth','BIT','0','This generates TSQL code for removing the growth restrict for data/log files on @oldVolume.', '@oldVolume, @DBs2Consider, @forceExecute' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@removeCapacityException','BIT','0','This generates TSQL code for removing the added capacity exception on MNA alerting database server for @oldVolume.', '@oldVolume' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@UpdateMountPointSecurity','BIT','0','This prints directions on how to update access for sql service account on @newVolume.', '' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@restrictMountPointGrowth','BIT','0','This generates TSQL code for expanding/shrinking files upto @mountPointGrowthRestrictionPercent % of total volume capacity.', '@oldVolume, @mountPointGrowthRestrictionPercent, @DBs2Consider, @forceExecute, @verbose' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@expandTempDBSize','BIT','0','This generates TSQL code for expanding tempdb data files upto @tempDBMountPointPercent % of total tempdb volume capacity.', '@tempDBMountPointPercent, @tempDbMaxSizeThresholdInGB, @output4IdealScenario, @forceExecute, @verbose' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@getVolumeSpaceConsumers','BIT','0','This gives all files and folders with details like Owner, Size, Created Date, Updated By etc for @oldVolume.', '@oldVolume, @sortBySize' as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@newVolume','VARCHAR(50)',NULL,'Name of the new Volume where data/log files are to be added.', NULL as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@oldVolume','VARCHAR(50)',NULL,'Name of the old Volume where data/log files growth is to be restricted.', NULL as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@mountPointGrowthRestrictionPercent','TINYINT','79','Threshold value in percentage for restricting data/log files on @oldVolume. It will either increase initial size, or shrink the files based on current space occupied.', NULL as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@tempDBMountPointPercent','TINYINT','79','Threshold value in percentage for restricting tempdb data files on @oldVolume. This will be used with @expandTempDBSize parameter to re-size the tempdb files if space is added on volume.', NULL as [Supporting Parameters]
				--
				UNION ALL
				--
				SELECT	'@tempDbMaxSizeThresholdInGB','INT','16','Threshold value for total size of all data files of tempdb database.', NULL as [Supporting Parameters]
				--
				UNION ALL
				--
				SELECT	'@DBs2Consider','VARCHAR(1000)',NULL,'Comma (,) separated database names to filter the result set action', NULL as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@mountPointFreeSpaceThreshold_GB','INT','60','Threshold value of free space in GB on @oldVolume after which new data/log files to be on @newVolume.', NULL as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@verbose','BIT','0','Used for debugging procedure. It will display temp table results created in background for analyzing issues/logic.', NULL as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@testAllOptions','BIT','0','Used for debugging procedure. It will test all parameter options for procedure.', NULL as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@forceExecute','BIT','0','When set to 1, will execute the TSQL Code generated by main parameter options like @addDataFiles, @addLogFiles, @restrictDataFileGrowth, @restrictLogFileGrowth, @unrestrictFileGrowth, @restrictMountPointGrowth and @expandTempDBSize.', NULL as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@allowMultiVolumeUnrestrictedFiles','BIT','0','All creation of multiple data/log files with unrestricted growth on multiple volumes.', NULL as [Supporting Parameters]
				--
				UNION ALL
					--
				SELECT	'@output4IdealScenario','BIT','0','When set to 1, will generate TSQL code to add/remove data files based on the number Logical cores on server upto 8, and delete extra data files created on non-tempdb volumes.', NULL as [Supporting Parameters]
				--
				UNION ALL
				--
				SELECT	'@vlfCountThreshold','INT','500','Threshold value of VLF counts. Only Log files with value above this threshold will be considered for @optimizeLogFiles.', NULL as [Supporting Parameters]
				) AS Params; --([Parameter Name], [Data Type], [Default Value], [Parameter Description], [Supporting Parameters]);

		PRINT	'
	NAME
		[dbo].[usp_AnalyzeSpaceCapacity]

	SYNOPSIS
		Analyze the Data Volume mount points for free space, database files, growth restriction and capacity exception.

	SYNTAX
		EXEC [dbo].[usp_AnalyzeSpaceCapacity]	[ [@getInfo =] { 1 | 0 } ] [,@DBs2Consider = <comma separated database names>]
												|
												@getLogInfo = { 1 | 0 } [,@DBs2Consider = <comma separated database names>]
												|
												@volumeInfo = { 1 | 0 }
												|
												@help = { 1 | 0 }
												|
												@addDataFiles = { 1 | 0 } ,@newVolume = <drive_name>, @oldVolume = <drive_name> [,@DBs2Consider = <comma separated database names>] [,@forceExecute = 1] 
												|
												@addLogFiles = { 1 | 0 } ,@newVolume = <drive_name>, @oldVolume = <drive_name> [,@allowMultiVolumeUnrestrictedFiles = 1] [,@DBs2Consider = <comma separated database names>] [,@forceExecute = 1] 
												|
												@restrictDataFileGrowth = { 1 | 0 } ,@oldVolume = <drive_name> [,@DBs2Consider = <comma separated database names>] [,@forceExecute = 1]
												|
												@restrictLogFileGrowth = { 1 | 0 } ,@oldVolume = <drive_name> [,@DBs2Consider = <comma separated database names>] [,@forceExecute = 1]
												|
												@optimizeLogFiles = { 1 | 0 } [,@DBs2Consider = <comma separated database names>] [,@vlfCountThreshold = <int value>] [, @forceExecute = 1];
												|
												@generateCapacityException = { 1 | 0 }, @oldVolume = <drive_name>
												|
												@unrestrictFileGrowth = { 1 | 0 }, @oldVolume = <drive_name> [,@DBs2Consider = <comma separated database names>] [,@forceExecute = 1]
												|
												@removeCapacityException = { 1 | 0 }, @oldVolume = <drive_name>
												|
												@UpdateMountPointSecurity = { 1 | 0 }
												|
												@restrictMountPointGrowth = { 1 | 0}, @oldVolume = <drive_name> [,@mountPointGrowthRestrictionPercent = <value> ] [,@DBs2Consider = <comma separated database names>] [,@forceExecute = 1]
												|
												@expandTempDBSize = { 1 | 0} [,@tempDBMountPointPercent = <value> ] [,@tempDbMaxSizeThresholdInGB = <value> ] [,@output4IdealScenario = 1] [,@forceExecute = 1]
												|
												@getVolumeSpaceConsumers = { 1 | 0}, @oldVolume = <drive_name> [,@sortBySize = 1]
											  } [;]

		<drive_name> :: { ''E:\Data\'' | ''E:\Data01'' | ''E:\Data2'' | ... }
';
		PRINT '
		--------------------------------------- EXAMPLE 1 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity];
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] ,@DBs2Consider = ''unet, Test1Db, MirrorTestDB'';

		This procedure returns general information like Data volumes, data files on those data volumes, Free space on data volumes, Growth settings of dbs etc.

		--------------------------------------- EXAMPLE 2 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @getLogInfo = 1
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @getLogInfo = 1 ,@DBs2Consider = ''unet, Test1Db, MirrorTestDB''

		This procedure returns general information like Log volumes, Log files on those log volumes, Free space on log volumes, Growth settings of dbs etc.
	
		--------------------------------------- EXAMPLE 3 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @help = 1

		This returns help for procedure usp_AnalyzeSpaceCapacity along with definitions for each parameter.
'
		PRINT '
		--------------------------------------- EXAMPLE 4 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = ''E:\Data1\'' ,@oldVolume = ''E:\Data\'';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = ''E:\Data1\'' ,@oldVolume = ''E:\Data\'' ,@DBs2Consider = ''unet, Test1Db, MirrorTestDB'';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = ''E:\Data1\'' ,@oldVolume = ''E:\Data\'' ,@forceExecute = 1;

		This generates TSQL Code for add secondary data files on @newVolume for each file of @oldVolume per FileGroup.

		--------------------------------------- EXAMPLE 5 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictDataFileGrowth = 1 ,@oldVolume = ''E:\Data\'';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictDataFileGrowth = 1 ,@oldVolume = ''E:\Data\'' ,@DBs2Consider = ''unet, Test1Db, MirrorTestDB'';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictDataFileGrowth = 1 ,@oldVolume = ''E:\Data\'' ,@forceExecute = 1

		This generates TSQL Code to restrict growth of secondary data files on @oldVolume if corresponding Data files exists on @newVolume.

		--------------------------------------- EXAMPLE 6 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = ''E:\Logs1\'' ,@oldVolume = ''E:\Logs\''
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = ''E:\Logs1\'' ,@oldVolume = ''E:\Logs\'' ,@DBs2Consider = ''unet, Test1Db, MirrorTestDB'';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = ''E:\Logs1\'' ,@oldVolume = ''E:\Logs\'' ,@forceExecute = 1
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = ''E:\Logs1\'' ,@oldVolume = ''E:\Logs\'' ,@allowMultiVolumeUnrestrictedFiles = 1
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = ''E:\Logs1\'' ,@oldVolume = ''E:\Logs\'' ,@allowMultiVolumeUnrestrictedFiles = 1 ,@forceExecute = 1

		This generates TSQL Code for add log files on @newVolume for each database on @oldVolume.

		--------------------------------------- EXAMPLE 7 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictLogFileGrowth = 1 ,@oldVolume = ''E:\Logs\''
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictLogFileGrowth = 1 ,@oldVolume = ''E:\Logs\'' ,@DBs2Consider = ''unet, Test1Db, MirrorTestDB'';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictLogFileGrowth = 1 ,@oldVolume = ''E:\Logs\'',@forceExecute = 1

		This generates TSQL Code to restrict growth of log files on @oldVolume if corresponding log files exists on @newVolume.

		--------------------------------------- EXAMPLE 8 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @optimizeLogFiles = 1 ,@oldVolume = ''E:\Logs\''
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @optimizeLogFiles = 1 ,@DBs2Consider = ''unet, Test1Db, MirrorTestDB'', @vlfCountThreshold = 1000;
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @optimizeLogFiles = 1 ,@DBs2Consider = ''unet, Test1Db, MirrorTestDB'', @vlfCountThreshold = 1000, @forceExecute = 1;

		This generates TSQL code for removing High VLF counts from Log files.
	
		--------------------------------------- EXAMPLE 9 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @unrestrictFileGrowth = 1, @oldVolume = ''E:\Data\''
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @unrestrictFileGrowth = 1, @oldVolume = ''E:\Data\'' ,@DBs2Consider = ''unet, Test1Db, MirrorTestDB'';

		This generates TSQL Code for remove Data File growth Restriction for files on @oldVolume.
';
		PRINT '
		--------------------------------------- EXAMPLE 10 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @generateCapacityException = 1, @oldVolume = ''E:\Data\''

		This generates TSQL Code for adding Space Capacity Exception for @oldVolume.

		--------------------------------------- EXAMPLE 11 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @UpdateMountPointSecurity = 1

		This will generate Powershell command to provide Full Access on @newVolume for SQL Server service accounts.

		--------------------------------------- EXAMPLE 12 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = ''E:\Data\''
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = ''E:\Data\'', @mountPointGrowthRestrictionPercent = 95
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = ''E:\Data\'', @mountPointGrowthRestrictionPercent = 95, @DBs2Consider = ''CHSDB_Audit,CHSDBArchive''
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = ''E:\Logs2\''
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = ''E:\Logs2\'', @mountPointGrowthRestrictionPercent = 70

		This will generate TSQL Code to restrict all the files on @oldVolume such that total files size consumes upto 79% of the mount point volume.

		--------------------------------------- EXAMPLE 13 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @expandTempDBSize = 1
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @expandTempDBSize = 1, @output4IdealScenario = 1
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @expandTempDBSize = 1, @tempDBMountPointPercent = 89
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @expandTempDBSize = 1, @tempDbMaxSizeThresholdInGB

		This generates TSQL code for expanding tempdb data files upto @tempDBMountPointPercent % of total tempdb volume capacity.
		When @output4IdealScenario set to 1, will generate TSQL code to add/remove data files based on the number Logical cores on server upto 8, and delete extra data files created on non-tempdb volumes, and re-size TempdDB data files to occupy 89% of mount point volume.

		--------------------------------------- EXAMPLE 14 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @optimizeLogFiles = 1

		This generates TSQL code to re-size log files upto current size with objective to reduce high VLF Counts

		--------------------------------------- EXAMPLE 15 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @getVolumeSpaceConsumers = 1, @oldVolume = ''F:\''
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @getVolumeSpaceConsumers = 1, @oldVolume = ''F:\'' ,@sortBySize = 1

		This gives all files and folders including hidden items with details like Owner, Size, Created Date, Updated By etc for @oldVolume.
		When @sortBySize is set to 1, will show only files order by their size in descending order.

		--------------------------------------- EXAMPLE 16 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @volumeInfo = 1;

		Displays Total size, Used Space, Free Space and percentage for all Volumes/disk drives.
	';

		IF @verbose=1 
			PRINT	'/*	******************** End:	@help = 1 *****************************/
';
	END
	--	----------------------------------------------------------------------------
		--	End:	@help = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@addDataFiles = 1
	--	----------------------------------------------------------------------------
	IF	@addDataFiles = 1
	BEGIN
		IF @verbose=1 
			PRINT	'
/*	******************** Begin:	@addDataFiles = 1 *****************************/';
		IF (SELECT COUNT(*) FROM @mountPointVolumes as V WHERE V.Volume IN (@newVolume,@oldVolume))<>2
		BEGIN -- Begin block for Validation of Data volumes
			SET @_errorMSG = '@newVolume and @oldVolume parameter values mandatory with @addDataFiles = 1 parameter. Verify if valid values are supplied.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END -- End block for Validation of Data volumes
		ELSE
		BEGIN -- Begin Else portion for Validation of Data volumes
			IF @verbose=1 
			BEGIN
				PRINT	'	Validations completed successfully.';
				PRINT	'	Printing @_mirrorDatabases, @_nonAccessibleDatabases, @_principalDatabases, @_databasesWithMultipleDataFiles';
			END

			IF	@_mirrorDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_mirrorDatabaseCounts_Mirroring AS VARCHAR(5))+' database(s) '+(case when @_mirrorDatabaseCounts_Mirroring > 1 then 'are' else 'is' end) +' in role of ''Mirroring Partner''. So add secondary files on Partner server '''+@_mirroringPartner+''' for these dbs.
				'+@_mirrorDatabases+'
	*/';
				IF @forceExecute = 1
				BEGIN
					IF @_errorOccurred = 0
						SET @_errorOccurred = 1;
					
					/*	Not needed any more
					INSERT #ErrorMessages
					SELECT	'Mirror Server' AS ErrorCategory
							,NULL AS DBName 
							,NULL AS [FileName] 
							,@_errorMSG AS ErrorDetails 
							,NULL AS TSQLCode;
					*/

					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Fail' AS Status
							,'Mirror Server' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
				ELSE
					PRINT @_errorMSG;
			END
	
			IF	@_nonAccessibleDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_nonAccessibleDatabasesCounts AS VARCHAR(5))+' database(s) '+(case when @_nonAccessibleDatabasesCounts > 1 then 'are' else 'is' end) +' in non-accessible state. Either wait, or resolve the issue, and then create/restrict Data files.
				'+@_nonAccessibleDatabases+'
	*/';
				IF @forceExecute = 1
				BEGIN
					IF @_errorOccurred = 0
						SET @_errorOccurred = 1;
					
					/*	Not needed anymore
					INSERT #ErrorMessages
					SELECT	'Non-Accessible Databases' AS ErrorCategory
							,NULL AS DBName 
							,NULL AS [FileName] 
							,@_errorMSG AS ErrorDetails 
							,NULL AS TSQLCode;
					*/

					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Non-Accessible Databases' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
				ELSE
					PRINT @_errorMSG;
			END

			IF	@_principalDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_principalDatabaseCounts_Mirroring AS VARCHAR(5))+' database(s) '+(case when @_principalDatabaseCounts_Mirroring > 1 then 'are' else 'is' end) +' in role of ''Mirroring Principal''. So generating code to add secondary files for these dbs. Kindly make sure that Same Data Volumes exists on DR server '''+@_mirroringPartner+''' as well. Otherwise this shall fail.
				'+@_principalDatabases+'
	*/';
				IF @forceExecute = 0 -- Ignore this message if @forceExecute = 1 since this is information only message
					PRINT @_errorMSG;
				ELSE
				BEGIN
					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Principal Databases' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
			END
				

			IF	@_databasesWithMultipleDataFiles IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_databasesWithMultipleDataFilesCounts AS VARCHAR(5))+' database(s) exists that have multiple files per filegroup on @oldVolume '+QUOTENAME(@oldVolume,'''') + '. But, this script will add only single file per filegroup per database on @newVolume '+QUOTENAME(@newVolume,'''') + '.
				'+@_databasesWithMultipleDataFiles+'
	*/';
				IF @forceExecute = 0 -- Ignore this message if @forceExecute = 1 since this is information only message
					PRINT @_errorMSG;
				ELSE
				BEGIN
					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Multiple Files for FileGroup' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
			END

		
			IF @verbose = 1 
			BEGIN
					PRINT	'	Checking if new data files are to be added.';
					SELECT	'	SELECT	[Add New Files] = CASE WHEN isExistingOn_NewVolume = 0 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 THEN ''Yes'' ELSE ''No'' END
		,* 
		FROM #T_Files_Final;' AS RunningQuery, [Add New Files] = CASE WHEN isExistingOn_NewVolume = 0 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 THEN 'Yes' ELSE 'No' END
							,* 
					FROM #T_Files_Final;
			END

			--	Generate TSQL Code for adding data files when it does not exist
			IF EXISTS (SELECT * FROM #T_Files_Final WHERE isExistingOn_NewVolume = 0 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 )
			BEGIN	-- Begin block for tsql code generation
				IF @verbose = 1 
				BEGIN
					PRINT	'	Generating TSQL Code for adding data files when it does not exist';
					PRINT	'	Populate @T_Files_Final_Add';
				END

				DELETE @T_Files_Final_Add;
				INSERT @T_Files_Final_Add (TSQL_AddFile,DBName,[fileGroup],name,_name)
				SELECT TSQL_AddFile,DBName,[fileGroup],name,_name FROM #T_Files_Final as f WHERE isExistingOn_NewVolume = 0 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 AND [FileIDRankPerFileGroup] = 1 ORDER BY f.DBName;
				
				IF @verbose = 1 
				BEGIN
						PRINT	'	Checking the data of @T_Files_Final_Add table that is used for Adding new files in last step.';
						SELECT	'	SELECT * FROM @T_Files_Final_Add' AS RunningQuery, * 
						FROM	@T_Files_Final_Add;
				END

				--	Find if data files to be added for @_dbaMaintDatabase
				IF EXISTS (SELECT * FROM @mountPointVolumes AS v WHERE v.Volume = @oldVolume AND v.[freespace(GB)] >= @mountPointFreeSpaceThreshold_GB)
						AND EXISTS (SELECT * FROM sys.databases as d WHERE d.database_id = DB_ID(@_dbaMaintDatabase))
				BEGIN
					SET @_errorMSG = '/*	NOTE: Data file for @_dbaMaintDatabase database is not being created since @oldVolume '+QUOTENAME(@oldVolume,'''') + ' has more than '+CAST(@mountPointFreeSpaceThreshold_GB AS VARCHAR(10))+' gb of free space.	*/';
					IF @forceExecute = 0 -- Ignore this message if @forceExecute = 1 since this is information only message
						PRINT @_errorMSG;
					ELSE
					BEGIN
						--	Make a Information entry for Sending message to end user
						INSERT @OutputMessages
							(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
						SELECT	'Information' AS Status
								,'No action taken for ['+@_dbaMaintDatabase+']' AS Category
								,NULL AS  DBName
								,NULL AS [FileGroup]
								,NULL AS [FileName]
								,@_errorMSG AS MessageDetails
								,NULL AS TSQLCode;
					END

					DELETE FROM @T_Files_Final_Add
						WHERE DBName = @_dbaMaintDatabase;
					DELETE FROM #T_Files_Final
						WHERE DBName = @_dbaMaintDatabase;
				END

				IF @verbose = 1 
					PRINT	'	Initiating @_loopCounter and @_loopCounts';
				SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_Final_Add WHERE DBName NOT IN ('master','model','msdb','tempdb');
			
				IF @verbose=1 
					PRINT	'9.3) Inside Begin:	@addDataFiles = 1 - Starting to print Data File Addition Code in loop';
				WHILE @_loopCounter <= @_loopCounts
				BEGIN	-- Begin Block of Loop

					SELECT @_loopSQLText = '
--	Add File: '+CAST(@_loopCounter AS VARCHAR(5))+';'+TSQL_AddFile 
							,@_dbName = DBName ,@_name = name ,@_newName = _name
							,@_fileGroup = fileGroup
					FROM @T_Files_Final_Add as f WHERE f.ID = @_loopCounter;
					IF @_loopCounter = 1
						SET @_loopSQLText =	'USE [master];
--	=====================================================================================================
	--	TSQL Code to Add Secondary Data Files on @newVolume '+QUOTENAME(@newVolume) + ' that exists on @oldVolume '+QUOTENAME(@oldVolume) + ' per FileGroup.
	' + @_loopSQLText;

					IF @forceExecute = 1
					BEGIN
						BEGIN TRY
							EXEC (@_loopSQLText);

							--	Make a SUCCESS entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Success' AS Status
									,'Add Data File' AS Category
									,@_dbName AS  DBName
									,@_fileGroup AS [FileGroup]
									,@_name AS [FileName]
									,'Data Files Successfully Added for Combination of database and filegroup' AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END TRY
						BEGIN CATCH
							IF @_errorOccurred = 0
								SET @_errorOccurred = 1;
							
							/*	This is not needed anymore
							INSERT #ErrorMessages
							SELECT	'ALTER DATABASE Failed' AS ErrorCategory
									,@_dbName AS DBName 
									,@_name AS [FileName] 
									,ERROR_MESSAGE() AS ErrorDetails 
									,@_loopSQLText AS TSQLCode;
							*/

							--	Make a FAIL entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Fail' AS Status
									,'Add Data File' AS Category
									,@_dbName AS  DBName
									,@_fileGroup AS [FileGroup]
									,@_name AS [FileName]
									,ERROR_MESSAGE() AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END CATCH
					END
					ELSE
						PRINT @_loopSQLText;

					SET @_loopSQLText = '';
					SET @_loopCounter = @_loopCounter + 1;
				END		-- End Block of Loop
				IF @verbose=1 
					PRINT	'9.4) Inside Begin:	@addDataFiles = 1 - Loop Ended for print Data File Addition Code';
			END -- End block for tsql code generation
		
			IF @verbose = 1 
			BEGIN
					PRINT	'	Checking if need to un-restrict file growth if file already exists on @newVolume.';

					SELECT	'	SELECT	[Remove Growth Restriction] = CASE WHEN isExistingOn_NewVolume = 1 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 THEN ''Yes'' ELSE ''No'' END
		,* 
		FROM #T_Files_Final;' AS RunningQuery, [Remove Growth Restriction] = CASE WHEN isExistingOn_NewVolume = 1 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 THEN 'Yes' ELSE 'No' END
							,* 
					FROM #T_Files_Final;
			END

			--	Un-Restrict File Growth if file already exists on @newVolume
			IF EXISTS (SELECT * FROM #T_Files_Final WHERE isExistingOn_NewVolume = 1 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0)
			BEGIN	-- Begin block for Un-Restrict File Growth if file already exists on @newVolume
				IF @verbose=1 
					PRINT	'9.5) Inside Begin:	@addDataFiles = 1 - Begin block for Un-Restrict File Growth if file already exists on @newVolume';

				INSERT @T_Files_Final_AddUnrestrict (TSQL_AddFile,DBName,name,_name)
				SELECT	'
		'+(CASE WHEN @forceExecute = 0 THEN 'PRINT	''Modifying autogrowth setting for file '+QUOTENAME(name)+' of '+QUOTENAME(DB_NAME(mf.database_id))+' database'';' ELSE '' END) + '
ALTER DATABASE ['+DB_NAME(mf.database_id)+'] MODIFY FILE ( NAME = '+QUOTENAME(mf.name,'''')+', FILEGROWTH = '+s._autoGrowth+');'
						,DB_NAME(mf.database_id) AS dbName ,name, NULL as _name
				FROM	sys.master_files AS mf 
				INNER JOIN
						(	SELECT t.database_id, t.data_space_id, MAX(t._initialSize) AS _initialSize, MAX(t._autoGrowth) AS _autoGrowth FROM #T_Files_Final as t WHERE t.isExistingOn_NewVolume = 1 AND t.isExisting_UnrestrictedGrowth_on_OtherVolume = 0 GROUP BY t.database_id, t.data_space_id
						) AS s
				ON		s.database_id = mf.database_id
					AND	s.data_space_id = mf.data_space_id
				INNER JOIN
					(
						SELECT mf1.database_id, mf1.data_space_id, MAX(mf1.file_id) AS MAX_file_id 
						FROM sys.master_files AS mf1
						WHERE mf1.type_desc = 'ROWS' AND mf1.physical_name LIKE (@newVolume+'%')
						AND EXISTS (SELECT * FROM #T_Files_Final AS t -- Find files on @newVolume with restrict growth
										WHERE isExistingOn_NewVolume = 1 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 AND t.database_id = mf1.database_id AND t.data_space_id = mf1.data_space_id) 
						GROUP BY mf1.database_id, mf1.data_space_id
					) AS rf
				ON		rf.database_id = mf.database_id
					AND	rf.data_space_id = mf.data_space_id
					AND	rf.MAX_file_id = mf.file_id
				ORDER BY DB_NAME(mf.database_id); --pick the latest file in case multiple log files exists

				SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_Final_AddUnrestrict;
			
				WHILE @_loopCounter <= @_loopCounts
				BEGIN	-- Begin Block of Loop

					SELECT @_loopSQLText = '
--	Un-restrict Data File: '+CAST(@_loopCounter AS VARCHAR(5))+';'+TSQL_AddFile 
							,@_dbName = DBName ,@_name = name ,@_newName = _name
					FROM @T_Files_Final_AddUnrestrict as f WHERE f.ID = @_loopCounter;
					IF @_loopCounter = 1
						SET @_loopSQLText =	'

USE [master];
--	=====================================================================================================
	--	TSQL Code to Remove Data file Growth restriction on @newVolume '+QUOTENAME(@newVolume) + ' that exists on @oldVolume '+QUOTENAME(@oldVolume)+';
	'+ @_loopSQLText;

					IF @forceExecute = 1
					BEGIN
						BEGIN TRY
							EXEC @_loopSQLText;

							--	Make a SUCCESS entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Success' AS Status
									,'Add Data File - UnRestrict' AS Category
									,@_dbName AS  DBName
									,NULL AS [FileGroup]
									,@_name AS [FileName]
									,'Data File already existing on @newVolume has been set with un-restricted growth Successfully.' AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END TRY
						BEGIN CATCH
							IF @_errorOccurred = 0
								SET @_errorOccurred = 1;

							/*	This is not needed anymore
							INSERT #ErrorMessages
							SELECT	'ALTER DATABASE Failed' AS ErrorCategory
									,@_dbName AS DBName 
									,@_name AS [FileName] 
									,ERROR_MESSAGE() AS ErrorDetails 
									,@_loopSQLText AS TSQLCode;
							*/

							--	Make a FAIL entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Fail' AS Status
									,'Add Data File - UnRestrict' AS Category
									,@_dbName AS  DBName
									,NULL AS [FileGroup]
									,@_name AS [FileName]
									,ERROR_MESSAGE() AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END CATCH
					END
					ELSE
						PRINT @_loopSQLText;

					SET @_loopSQLText = '';
					SET @_loopCounter = @_loopCounter + 1;
				END		-- End Block of Loop
				IF @verbose=1 
					PRINT	'9.5) Inside Begin:	@addDataFiles = 1 - End block for Un-Restrict File Growth if file already exists on @newVolume';
			END -- End block for Un-Restrict File Growth if file already exists on @newVolume

			IF @verbose = 1 
			BEGIN
					PRINT	'	Checking if any action was taken for @addDataFiles.';

					SELECT	RunningQuery, F.*
					FROM  ( SELECT '/* Check if any action taken with @addDataFiles */
SELECT * FROM #T_Files_Final WHERE NOT (isExistingOn_NewVolume = 1 OR IsExisting_UnrestrictedGrowth_on_OtherVolume = 1)' AS RunningQuery ) AS Q
					LEFT JOIN #T_Files_Final AS F
					ON	1 = 1 AND NOT (isExistingOn_NewVolume = 1 OR isExisting_UnrestrictedGrowth_on_OtherVolume = 1);
			END

			IF NOT EXISTS (SELECT * FROM #T_Files_Final WHERE NOT (isExistingOn_NewVolume = 1 OR isExisting_UnrestrictedGrowth_on_OtherVolume = 1))
			BEGIN
				IF @forceExecute = 0
					PRINT	'	/*	~~~~ No secondary Data files to add on @newVolume '+QUOTENAME(@newVolume)+' with respect to @oldVolume '+QUOTENAME(@oldVolume) + '. ~~~~ */';
				ELSE
				BEGIN
					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Add Data File' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,'No secondary Data files to add on @newVolume '+QUOTENAME(@newVolume)+' with respect to @oldVolume '+QUOTENAME(@oldVolume) AS MessageDetails
							,NULL AS TSQLCode
				END
			END			
			
			IF	@verbose = 1
				PRINT	'/*	******************** End:	@addDataFiles = 1 *****************************/
';
		END	-- End Else portion for Validation of Data volumes

	END -- End block of @addDataFiles = 1
	--	----------------------------------------------------------------------------
		--	End:	@addDataFiles = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@addLogFiles = 1
	--	----------------------------------------------------------------------------
	IF	@addLogFiles = 1
	BEGIN
		IF @verbose = 1
			PRINT	'
/*	******************** Begin:	@addLogFiles = 1 *****************************/';

		IF (SELECT COUNT(*) FROM @mountPointVolumes as V WHERE V.Volume IN (@newVolume,@oldVolume))<>2
		BEGIN -- Begin block for Validation of Data volumes
			SET @_errorMSG = '@newVolume and @oldVolume parameter values mandatory with @addLogFiles = 1 parameter. Verify if valid values are supplied.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END -- End block for Validation of Data volumes
		ELSE
		BEGIN -- Begin Else portion for Validation of Data volumes
			IF @verbose = 1
			BEGIN
				PRINT	'	Validation of @newVolume and @oldVolume completed successfullly.';
				PRINT	'	Printing messages related to @_mirrorDatabases, @_nonAccessibleDatabases and @_principalDatabases';
			END

			IF	@_mirrorDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_mirrorDatabaseCounts_Mirroring AS VARCHAR(5))+' database(s) '+(case when @_mirrorDatabaseCounts_Mirroring > 1 then 'are' else 'is' end) +' in role of ''Mirroring Partner''. So add secondary files on Partner server '''+@_mirroringPartner+''' for these dbs.
				'+@_mirrorDatabases+'
	*/';
				IF @forceExecute = 1
				BEGIN
					IF @_errorOccurred = 0
						SET @_errorOccurred = 1;

					/*	Not needed any more
					INSERT #ErrorMessages
					SELECT	'Mirror Server' AS ErrorCategory
							,NULL AS DBName 
							,NULL AS [FileName] 
							,@_errorMSG AS ErrorDetails 
							,NULL AS TSQLCode;
					*/

					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Fail' AS Status
							,'Mirror Server' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
				ELSE
					PRINT @_errorMSG;
			END
	
			IF	@_nonAccessibleDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_nonAccessibleDatabasesCounts AS VARCHAR(5))+' database(s) '+(case when @_nonAccessibleDatabasesCounts > 1 then 'are' else 'is' end) +' in non-accessible state. Either wait, or resolve the issue, and then create/restrict Data files.
				'+@_nonAccessibleDatabases+'
	*/';
				IF @forceExecute = 1
				BEGIN
					IF @_errorOccurred = 0
						SET @_errorOccurred = 1;

					/*	Not needed anymore
					INSERT #ErrorMessages
					SELECT	'Non-Accessible Databases' AS ErrorCategory
							,NULL AS DBName 
							,NULL AS [FileName] 
							,@_errorMSG AS ErrorDetails 
							,NULL AS TSQLCode;
					*/

					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Non-Accessible Databases' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
				ELSE
					PRINT @_errorMSG;
			END

			IF	@_principalDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_principalDatabaseCounts_Mirroring AS VARCHAR(5))+' database(s) '+(case when @_principalDatabaseCounts_Mirroring > 1 then 'are' else 'is' end) +' in role of ''Mirroring Principal''. So generating code to add files for these dbs. Kindly make sure that Same Data Volumes exists on DR server '''+@_mirroringPartner+''' as well. Otherwise this shall fail.
				'+@_principalDatabases+'
	*/';
				IF @forceExecute = 0 
					PRINT @_errorMSG;
				ELSE
				BEGIN
					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Principal Databases' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
			END

			--	Check if there are multiple log files on @oldVolume
			IF	@_databasesWithMultipleDataFiles IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_databasesWithMultipleDataFilesCounts AS VARCHAR(5))+' database(s) exists that have multiple log files on @oldVolume '+QUOTENAME(@oldVolume,'''') + '. But, this script will add only single log file per database on @newVolume '+QUOTENAME(@newVolume,'''') + '.
				'+@_databasesWithMultipleDataFiles+'
	*/';
				IF @forceExecute = 0 -- Ignore this message if @forceExecute = 1 since this is information only message
					PRINT @_errorMSG;
				ELSE
				BEGIN
					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Multiple Log Files for Database' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
			END

			IF @verbose = 1
			BEGIN
				PRINT	'	Validate and Generate TSQL Code for adding log files when it does not exist';
				SELECT	RunningQuery, [Add New Log Files] = CASE WHEN isExistingOn_NewVolume = 0 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 THEN 'Yes' ELSE 'No' END, f.*
				FROM  (	SELECT ('SELECT [Add New Log Files] = CASE WHEN isExistingOn_NewVolume = 0 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 THEN ''Yes'' ELSE ''No'' END
		,* 
		FROM #T_Files_Final;') AS RunningQuery ) Query
				LEFT JOIN
						#T_Files_Final AS f
					ON	1 = 1;
			END

			--	Generate TSQL Code for adding log files when it does not exist
			IF EXISTS (SELECT * FROM #T_Files_Final WHERE isExistingOn_NewVolume = 0 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0)
			BEGIN	-- Begin block for tsql code generation
				IF @verbose = 1
				BEGIN
					PRINT	'	Declaring variables and inserting data into @T_LogFiles_Final_Add';
				END
			
				DELETE @T_LogFiles_Final_Add;
				INSERT @T_LogFiles_Final_Add (TSQL_AddFile,DBName,name,_name)
				SELECT TSQL_AddFile,DBName,name,_name FROM #T_Files_Final as f WHERE isExistingOn_NewVolume = 0 AND [FileIDRankPerFileGroup] = 1 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 ORDER BY f.dbName;

				--	Find if log files to be added for @_dbaMaintDatabase & [tempdb]
				IF EXISTS (SELECT * FROM @mountPointVolumes AS v WHERE v.Volume = @oldVolume AND v.[freespace(GB)] >= @mountPointFreeSpaceThreshold_GB)
					AND EXISTS (SELECT * FROM sys.databases as d WHERE d.database_id = DB_ID(@_dbaMaintDatabase))
				BEGIN
					SET @_errorMSG = '/*	NOTE: Log file for @_dbaMaintDatabase and [tempdb] databases is not being created since @oldVolume '+QUOTENAME(@oldVolume,'''') + ' has more than '+cast(@mountPointFreeSpaceThreshold_GB as varchar(20))+' gb of free space.	*/';
					IF @forceExecute = 0 -- Ignore this message if @forceExecute = 1 since this is information only message
						PRINT @_errorMSG;
					ELSE
					BEGIN
						--	Make an Information entry for Sending message to end user
						INSERT @OutputMessages
							(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
						SELECT	'Information' AS Status
								,'No action taken for ['+@_dbaMaintDatabase+'] and [tempdb]' AS Category
								,NULL AS  DBName
								,NULL AS [FileGroup]
								,NULL AS [FileName]
								,@_errorMSG AS MessageDetails
								,NULL AS TSQLCode;
					END

					DELETE FROM @T_LogFiles_Final_Add
						WHERE DBName IN (@_dbaMaintDatabase,'tempdb');
				END

				IF @verbose = 1
				BEGIN
					PRINT	'	SELECT * FROM @T_LogFiles_Final_Add';
					SELECT 'SELECT * FROM @T_LogFiles_Final_Add' AS RunningQuery, * FROM @T_LogFiles_Final_Add
					PRINT	'	Preparing loop variables @_loopCounter and @_loopCounts';
				END

				SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_LogFiles_Final_Add;
			
				WHILE @_loopCounter <= @_loopCounts
				BEGIN	-- Begin Block of Loop

					SELECT @_loopSQLText = '
	--	Add File: '+CAST(@_loopCounter AS VARCHAR(5))+';'+TSQL_AddFile
							,@_dbName = DBName ,@_name = name ,@_newName = _name
					FROM @T_LogFiles_Final_Add as f WHERE f.ID = @_loopCounter;

					IF @_loopCounter = 1
						SET @_loopSQLText =	'USE [master];
--	=====================================================================================================
	--	TSQL Code to Add Log Files on @newVolume '+QUOTENAME(@newVolume) + ' that exists on @oldVolume '+QUOTENAME(@oldVolume) + '.
' + @_loopSQLText;

					IF @forceExecute = 1
					BEGIN
						BEGIN TRY
							EXEC (@_loopSQLText);

							--	Make a SUCCESS entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Success' AS Status
									,'Add Log File' AS Category
									,@_dbName AS  DBName
									,NULL AS [FileGroup]
									,@_name AS [FileName]
									,'Log File Successfully Added for database.' AS MessageDetails
									,@_loopSQLText AS TSQLCode;

						END TRY
						BEGIN CATCH
							IF @_errorOccurred = 0
								SET @_errorOccurred = 1;

							/*	This is not needed anymore
							INSERT #ErrorMessages
							SELECT	'ALTER DATABASE Failed' AS ErrorCategory
									,@_dbName AS DBName 
									,@_name AS [FileName] 
									,ERROR_MESSAGE() AS ErrorDetails 
									,@_loopSQLText AS TSQLCode;
							*/

							--	Make a FAIL entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Fail' AS Status
									,'Add Log File' AS Category
									,@_dbName AS  DBName
									,NULL AS [FileGroup]
									,@_name AS [FileName]
									,ERROR_MESSAGE() AS MessageDetails
									,@_loopSQLText AS TSQLCode;

						END CATCH
					END
					ELSE
						PRINT @_loopSQLText;

					SET @_loopSQLText = '';
					SET @_loopCounter = @_loopCounter + 1;
				END		-- End Block of Loop
			END -- End block for tsql code generation
		
			IF @verbose = 1
			BEGIN
				PRINT	'	Validate and Generate TSQL Code to Un-Restrict File Growth if file already exists on @newVolume';

				SELECT	RunningQuery, [Remove Growth Restriction] = CASE WHEN isExistingOn_NewVolume = 1 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 THEN 'Yes' ELSE 'No' END, f.*
				FROM  (	SELECT 'SELECT [Remove Growth Restriction] = CASE WHEN isExistingOn_NewVolume = 1 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 THEN ''Yes'' ELSE ''No'' END
				,* 
		FROM #T_Files_Final' AS RunningQuery) AS Query
				LEFT JOIN
						#T_Files_Final AS f
					ON	1 = 1;
			END

			--	Un-Restrict File Growth if file already exists on @newVolume
			IF EXISTS (SELECT * FROM #T_Files_Final WHERE isExistingOn_NewVolume = 1 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0)
			BEGIN	-- Begin block for Un-Restrict File Growth if file already exists on @newVolume

				INSERT @T_Files_Final_AddUnrestrictLogFiles (TSQL_AddFile,DBName,name,_name)
				SELECT	'
		'+(CASE WHEN @forceExecute = 0 THEN 'PRINT	''Modifying autogrowth setting for file '+QUOTENAME(name)+' of '+QUOTENAME(DB_NAME(mf.database_id))+' database'';' ELSE '' END) + '
ALTER DATABASE ['+DB_NAME(mf.database_id)+'] MODIFY FILE ( NAME = '+QUOTENAME(mf.name,'''')+', FILEGROWTH = '+s._autoGrowth+');',
						DB_NAME(mf.database_id) as dbName ,name, NULL as _name
				FROM	sys.master_files AS mf 
				INNER JOIN
						(	SELECT t.database_id, t.data_space_id, MAX(t._initialSize) AS _initialSize, MAX(t._autoGrowth) AS _autoGrowth FROM #T_Files_Final as t 
								WHERE t.isExistingOn_NewVolume = 1 AND t.isExisting_UnrestrictedGrowth_on_OtherVolume = 0
								GROUP BY t.database_id, t.data_space_id
						) AS s
				ON		s.database_id = mf.database_id
					AND	s.data_space_id = mf.data_space_id
				WHERE	mf.type_desc = 'LOG'
				AND		mf.physical_name LIKE (@newVolume+'%')
				AND		EXISTS (SELECT * FROM #T_Files_Final AS t -- Find files on @newVolume with restrict growth
								WHERE isExistingOn_NewVolume = 1 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0 AND t.database_id = mf.database_id)
				AND		mf.file_id IN (SELECT MAX(file_id) FROM sys.master_files AS mf1 WHERE mf1.type_desc = 'LOG' AND mf1.physical_name LIKE (@newVolume+'%') GROUP BY mf1.database_id)
				ORDER BY DB_NAME(mf.database_id); --pick the latest file in case multiple log files exists

				SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_Final_AddUnrestrictLogFiles;
			
				WHILE @_loopCounter <= @_loopCounts
				BEGIN	-- Begin Block of Loop

					SELECT @_loopSQLText = '
--	Un-restrict Log File: '+CAST(@_loopCounter AS VARCHAR(5))+';'+TSQL_AddFile 
							,@_dbName = DBName ,@_name = name ,@_newName = _name
					FROM @T_Files_Final_AddUnrestrictLogFiles as f WHERE f.ID = @_loopCounter;
					IF @_loopCounter = 1
						SET @_loopSQLText =	'

USE [master];
--	=====================================================================================================
	--	TSQL Code to Remove Log file Growth restriction on @newVolume '+QUOTENAME(@newVolume) + ' that exists on @oldVolume '+QUOTENAME(@oldVolume)+'
	'+ @_loopSQLText;

					IF @forceExecute = 1
					BEGIN
						BEGIN TRY
							EXEC @_loopSQLText;

							--	Make a SUCCESS entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Success' AS Status
									,'Add Log File - UnRestrict' AS Category
									,@_dbName AS  DBName
									,NULL AS [FileGroup]
									,@_name AS [FileName]
									,'Log File already existing on @newVolume has been set with un-restricted growth Successfully.' AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END TRY
						BEGIN CATCH
							IF @_errorOccurred = 0
								SET @_errorOccurred = 1;

							/*	This is not needed anymore
							INSERT #ErrorMessages
							SELECT	'ALTER DATABASE Failed' AS ErrorCategory
									,@_dbName AS DBName 
									,@_name AS [FileName] 
									,ERROR_MESSAGE() AS ErrorDetails 
									,@_loopSQLText AS TSQLCode;
							*/

							--	Make a FAIL entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Fail' AS Status
									,'Add Log File - UnRestrict' AS Category
									,@_dbName AS  DBName
									,NULL AS [FileGroup]
									,@_name AS [FileName]
									,ERROR_MESSAGE() AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END CATCH
					END
					ELSE
						PRINT @_loopSQLText;

					SET @_loopSQLText = '';
					SET @_loopCounter = @_loopCounter + 1;
				END		-- End Block of Loop

			END -- End block for Un-Restrict File Growth if file already exists on @newVolume
		
			IF @verbose = 1
			BEGIN
				PRINT	'	Checking if any action was taken for @addLogFiles.';
				
				SELECT	RunningQuery, F.*
					FROM  ( SELECT '/* Check if any action taken with @addLogFiles */
SELECT * FROM #T_Files_Final WHERE NOT (isExistingOn_NewVolume = 1 OR IsExisting_UnrestrictedGrowth_on_OtherVolume = 1)' AS RunningQuery ) Q
					LEFT JOIN #T_Files_Final AS F
					ON	1 = 1 AND NOT (isExistingOn_NewVolume = 1 OR isExisting_UnrestrictedGrowth_on_OtherVolume = 1);
			END

			IF NOT EXISTS (SELECT * FROM #T_Files_Final WHERE NOT (isExistingOn_NewVolume = 1 OR isExisting_UnrestrictedGrowth_on_OtherVolume = 1))
			BEGIN
				IF @forceExecute = 0
					PRINT	'	/*	~~~~ No Log files to add on @newVolume '+QUOTENAME(@newVolume)+' with respect to @oldVolume '+QUOTENAME(@oldVolume) + '. ~~~~ */'; 
				ELSE
				BEGIN
					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Add Log File' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,'No Log files to add on @newVolume '+QUOTENAME(@newVolume)+' with respect to @oldVolume '+QUOTENAME(@oldVolume) AS MessageDetails
							,NULL AS TSQLCode;
				END
			END

			IF	@verbose = 1
				PRINT	'/*	******************** End:	@addLogFiles = 1 *****************************/
';
		END	-- End Else portion for Validation of Data volumes
	END -- End block of @addLogFiles = 1
	--	----------------------------------------------------------------------------
		--	End:	@addLogFiles = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@restrictDataFileGrowth = 1
	--	----------------------------------------------------------------------------
	IF	@restrictDataFileGrowth = 1
	BEGIN
		IF @verbose = 1
		BEGIN
			PRINT	'
/*	******************** Begin:	@restrictDataFileGrowth = 1 *****************************/';
		END

		IF (SELECT COUNT(*) FROM @mountPointVolumes as V WHERE V.Volume IN (@oldVolume))<>1
		BEGIN -- Begin block for Validation of Data volumes
			SET @_errorMSG = '@oldVolume parameter value is must with @restrictDataFileGrowth = 1 parameter. Verify if valid values are supplied.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END

		IF EXISTS(SELECT 1 FROM @mountPointVolumes as V WHERE V.Volume = @oldVolume AND [freespace(%)] > (100-@mountPointGrowthRestrictionPercent)) -- default 21%
		BEGIN -- Begin block for Validation of Data volumes
			SET @_errorMSG = '@oldVolume '+QUOTENAME(@oldVolume)+' has free space more than '+CAST((100-@mountPointGrowthRestrictionPercent) AS VARCHAR(20))+' percent. So, skipping the data file restriction. If required, re-run the procedure with lower value for @mountPointGrowthRestrictionPercent parameter.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END
		ELSE
		BEGIN -- Begin Else portion for Validation of Data volumes
			IF @verbose = 1
			BEGIN
				PRINT	'	Validation of @oldVolume completed successfully.';
				PRINT	'	Printing values for @_mirrorDatabases, @_nonAccessibleDatabases, @_principalDatabases';
			END

			IF	@_mirrorDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_mirrorDatabaseCounts_Mirroring AS VARCHAR(5))+' database(s) '+(case when @_mirrorDatabaseCounts_Mirroring > 1 then 'are' else 'is' end) +' in role of ''Mirroring Partner''. So restrict growth on Partner server '''+@_mirroringPartner+''' for these dbs.
				'+@_mirrorDatabases+'
	*/';
				IF @forceExecute = 1
				BEGIN
					IF @_errorOccurred = 0
						SET @_errorOccurred = 1;

					/*	Not needed any more
					INSERT #ErrorMessages
					SELECT	'Mirror Server' AS ErrorCategory
							,NULL AS DBName 
							,NULL AS [FileName] 
							,@_errorMSG AS ErrorDetails 
							,NULL AS TSQLCode;
					*/

					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Fail' AS Status
							,'Mirror Server' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
				ELSE
					PRINT @_errorMSG;
			END
				
	
			IF	@_nonAccessibleDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_nonAccessibleDatabasesCounts AS VARCHAR(5))+' database(s) '+(case when @_nonAccessibleDatabasesCounts > 1 then 'are' else 'is' end) +' in non-accessible state. Either wait, or resolve the issue, and then create/restrict Data files.
				'+@_nonAccessibleDatabases+'
	*/';
				IF @forceExecute = 1
				BEGIN
					IF @_errorOccurred = 0
						SET @_errorOccurred = 1;

					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Non-Accessible Databases' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
				ELSE
					PRINT @_errorMSG;
			END

			IF	@_principalDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_principalDatabaseCounts_Mirroring AS VARCHAR(5))+' database(s) '+(case when @_principalDatabaseCounts_Mirroring > 1 then 'are' else 'is' end) +' in role of ''Mirroring Principal''. So generating code to restrict growth of secondary files for these dbs.
				'+@_principalDatabases+'
	*/';
				IF @forceExecute = 0 -- Ignore this message if @forceExecute = 1 since this is information only message
					PRINT @_errorMSG;
				ELSE
				BEGIN
					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Principal Databases' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
			END

			IF @verbose = 1
				PRINT	'	Find all databases for which Secondary Data files are yet to be added on @newVolume';
		
			--	Find all databases for which Secondary Data files are yet to be added on @newVolume.
			SELECT	@_nonAddedDataFilesDatabases = COALESCE(@_nonAddedDataFilesDatabases+', '+DB_NAME(database_id),DB_NAME(database_id))
			FROM	(SELECT DISTINCT database_id FROM #T_Files_Final WHERE [isExisting_UnrestrictedGrowth_on_OtherVolume] = 0) as d;
			SET @_nonAddedDataFilesDatabasesCounts = (LEN(@_nonAddedDataFilesDatabases)-LEN(REPLACE(@_nonAddedDataFilesDatabases,',',''))+1);
			
			IF	@_nonAddedDataFilesDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: New Data files for following '+CAST(@_nonAddedDataFilesDatabasesCounts AS VARCHAR(5))+' database(s) are yet to be added on other volumes with free space greater than 20%. So skipping these database for growth restriction.
				'+@_nonAddedDataFilesDatabases+'
	*/';
				IF @forceExecute = 0
					PRINT @_errorMSG;
				ELSE
				BEGIN
					IF @_errorOccurred = 0
						SET @_errorOccurred = 1;

					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Fail' AS Status
							,'Restrict Data File - New Files Not Created' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
			END

			IF @verbose = 1
			BEGIN
				PRINT	'	@_nonAddedDataFilesDatabases = ' + ISNULL(@_nonAddedDataFilesDatabases,'');
			END

			--	Generate TSQL Code for restricting data files growth
			IF EXISTS (SELECT * FROM #T_Files_Final WHERE [isExisting_UnrestrictedGrowth_on_OtherVolume] = 1 AND growth <> 0)
			BEGIN	-- Begin block for tsql code generation
				IF @verbose = 1
					PRINT	'		Begin block for tsql code generation';

				DELETE @T_Files_Final_Restrict;
				INSERT @T_Files_Final_Restrict (TSQL_RestrictFileGrowth,DBName,name,_name)
				SELECT [TSQL_RestrictFileGrowth],dbName,name,_name FROM #T_Files_Final as f WHERE [isExisting_UnrestrictedGrowth_on_OtherVolume] = 1 AND growth <> 0 ORDER BY f.dbName;

				SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_Final_Restrict;
			
				WHILE @_loopCounter <= @_loopCounts
				BEGIN
					SELECT @_loopSQLText = '
--	Restrict Growth of File: '+CAST(ID AS VARCHAR(5))+';'+[TSQL_RestrictFileGrowth] 
					,@_dbName = DBName ,@_name = name ,@_newName = _name
					FROM @T_Files_Final_Restrict as f WHERE f.ID = @_loopCounter;
					IF @_loopCounter = 1
						SET @_loopSQLText =	'USE [master];
--	=====================================================================================================
	--	TSQL Code to Restrict Data Files growth on @oldVolume '+QUOTENAME(@oldVolume) + ' for which Data file already exists on other Data volumes.
' + @_loopSQLText;

					IF @forceExecute = 1
					BEGIN
						BEGIN TRY
							EXEC (@_loopSQLText);

							--	Make a SUCCESS entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Success' AS Status
									,'Restrict Data File' AS Category
									,@_dbName AS  DBName
									,NULL AS [FileGroup]
									,@_name AS [FileName]
									,'Data File Successfully restricted from growing.' AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END TRY
						BEGIN CATCH
							IF @_errorOccurred = 0
								SET @_errorOccurred = 1;
								
							--	Make a FAIL entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Fail' AS Status
									,'Restrict Data File' AS Category
									,@_dbName AS  DBName
									,NULL AS [FileGroup]
									,@_name AS [FileName]
									,ERROR_MESSAGE() AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END CATCH
					END
					ELSE
						PRINT @_loopSQLText;

					SET @_loopSQLText = '';
					SET @_loopCounter = @_loopCounter + 1;
				END
			END -- End block for tsql code generation
			ELSE
			BEGIN
				IF @forceExecute = 0
					PRINT	'	/*	~~~~ No Data files to restrict on @oldVolume '+QUOTENAME(@oldVolume)+'.';
				ELSE
				BEGIN
					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Restrict Data File Growth' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,'No Data Files eligible for restricting autogrowth on @oldVolume '+QUOTENAME(@oldVolume) AS MessageDetails
							,NULL AS TSQLCode
				END
			END
		END	-- End Else portion for Validation of Data volumes

		IF @verbose = 1
			PRINT	'/*	******************** End:	@restrictDataFileGrowth = 1 *****************************/
';
	END -- End block of @restrictDataFileGrowth = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@restrictLogFileGrowth = 1
	--	----------------------------------------------------------------------------
	IF	@restrictLogFileGrowth = 1
	BEGIN
		IF @verbose = 1
			PRINT	'
/*	******************** Begin:	@restrictLogFileGrowth = 1 *****************************/';
	
		IF (SELECT COUNT(*) FROM @mountPointVolumes as V WHERE V.Volume IN (@oldVolume))<>1
		BEGIN -- Begin block for Validation of Log volumes
			SET @_errorMSG = '@oldVolume parameter value is must with @restrictLogFileGrowth = 1 parameter. Verify if valid values are supplied.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END
		ELSE
		BEGIN -- Begin Else portion for Validation of Log volumes
			IF @verbose = 1
			BEGIN
				PRINT	'	Validation of @newVolume and @oldVolume completed successfullly.';
				PRINT	'	Printing messages related to @_mirrorDatabases, @_nonAccessibleDatabases and @_principalDatabases';
			END

			IF	@_mirrorDatabases IS NOT NULL
				PRINT	'		/*	NOTE: Following '+CAST(@_mirrorDatabaseCounts_Mirroring AS VARCHAR(5))+' database(s) '+(case when @_mirrorDatabaseCounts_Mirroring > 1 then 'are' else 'is' end) +' in role of ''Mirroring Partner''. So restrict growth on Partner server '''+@_mirroringPartner+''' for these dbs.
					'+@_mirrorDatabases+'
		*/';
	
			IF	@_nonAccessibleDatabases IS NOT NULL
			PRINT	'		/*	NOTE: Following '+CAST(@_nonAccessibleDatabasesCounts AS VARCHAR(5))+' database(s) '+(case when @_nonAccessibleDatabasesCounts > 1 then 'are' else 'is' end) +' in non-accessible state. Either wait, or resolve the issue, and then create/restrict Log files.
					'+@_nonAccessibleDatabases+'
		*/';

			IF	@_principalDatabases IS NOT NULL
				PRINT	'		/*	NOTE: Following '+CAST(@_principalDatabaseCounts_Mirroring AS VARCHAR(5))+' database(s) '+(case when @_principalDatabaseCounts_Mirroring > 1 then 'are' else 'is' end) +' in role of ''Mirroring Principal''. So generating code to restrict growth of files for these dbs.
					'+@_principalDatabases+'
		*/';
		
			IF @verbose = 1
				PRINT	'	Finding all databases for which log files are yet to be added on @newVolume';

			--	Find all databases for which log files are yet to be added on @newVolume.
			SELECT	@_nonAddedLogFilesDatabases = COALESCE(@_nonAddedLogFilesDatabases+', '+DB_NAME(database_id),DB_NAME(database_id))
			FROM	(SELECT DISTINCT database_id FROM #T_Files_Final WHERE [isExisting_UnrestrictedGrowth_on_OtherVolume] = 0) as d;
			SET @_nonAddedLogFilesDatabasesCounts = (LEN(@_nonAddedLogFilesDatabases)-LEN(REPLACE(@_nonAddedLogFilesDatabases,',',''))+1);
			IF	@_nonAddedLogFilesDatabases IS NOT NULL
				PRINT	'		/*	NOTE: New Log files for following '+CAST(@_nonAddedLogFilesDatabasesCounts AS VARCHAR(5))+' database(s) are yet to be added. So skipping these database for growth restriction.
					'+@_nonAddedLogFilesDatabases+'
		*/';

			IF @verbose = 1
				PRINT	'	Validate and Generate TSQL Code for to restrict log files growth';

			--	Generate TSQL Code for restricting log files growth
			IF EXISTS (SELECT * FROM #T_Files_Final WHERE [isExisting_UnrestrictedGrowth_on_OtherVolume] = 1 AND growth <> 0)
			BEGIN	-- Begin block for tsql code generation
				IF @verbose = 1
					PRINT	'	Declaring variables and inserting data into @T_Files_Final_Restrict';

				DELETE @T_Files_Final_Restrict;
				INSERT @T_Files_Final_Restrict (TSQL_RestrictFileGrowth,DBName,name,_name)
				SELECT [TSQL_RestrictFileGrowth],dbName,name,_name  FROM #T_Files_Final as f WHERE [isExisting_UnrestrictedGrowth_on_OtherVolume] = 1 AND growth <> 0 ORDER BY f.dbName;

				IF @verbose = 1
					PRINT	'	Preparing loop variables @_loopCounter and @_loopCounts';

				SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_Final_Restrict;
			
				WHILE @_loopCounter <= @_loopCounts
				BEGIN
					SELECT @_loopSQLText = '
	--	Restrict Growth of File: '+CAST(ID AS VARCHAR(5))+';'+[TSQL_RestrictFileGrowth] FROM @T_Files_Final_Restrict as f WHERE f.ID = @_loopCounter;
					IF @_loopCounter = 1
						SET @_loopSQLText =	'USE [master];
--	=====================================================================================================
	--	TSQL Code to Restrict Log Files growth on @oldVolume '+QUOTENAME(@oldVolume) + ' for which Log file already exists on other Log volumes.
' + @_loopSQLText;

					PRINT @_loopSQLText;

					SET @_loopSQLText = '';
					SET @_loopCounter = @_loopCounter + 1;
				END
			END -- End block for tsql code generation
			ELSE
				PRINT	'	--	No Log files to restrict growth for @oldVolume '+QUOTENAME(@oldVolume)+'.';
		END	-- End Else portion for Validation of Log volumes

		IF @verbose = 1
			PRINT	'/*	******************** End:	@restrictLogFileGrowth = 1 *****************************/
';
	END -- End block of @restrictLogFileGrowth = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@unrestrictFileGrowth = 1
	--	----------------------------------------------------------------------------
	IF	@unrestrictFileGrowth = 1
	BEGIN
		IF @verbose = 1
			PRINT	'
/*	******************** Begin:	@unrestrictFileGrowth = 1 *****************************/';

		IF (SELECT COUNT(*) FROM @mountPointVolumes as V WHERE V.Volume IN (@oldVolume))<>1
		BEGIN -- Begin block for Validation of Data volumes
			SET @_errorMSG = '@oldVolume parameter value is mandatory with @unrestrictFileGrowth = 1 parameter. Verify if valid values are supplied.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END -- End block for Validation of Data volumes
		ELSE
		BEGIN -- Begin Else portion for Validation of Data volumes
			IF	@_mirrorDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_mirrorDatabaseCounts_Mirroring AS VARCHAR(5))+' database(s) '+(case when @_mirrorDatabaseCounts_Mirroring > 1 then 'are' else 'is' end) +' in role of ''Mirroring Partner''. So unrestrict data files growth on Partner server '''+@_mirroringPartner+''' for these dbs.
				'+@_mirrorDatabases+'
	*/';
				IF @forceExecute = 1
				BEGIN
					IF @_errorOccurred = 0
						SET @_errorOccurred = 1;

					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Fail' AS Status
							,'Mirror Server' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
				ELSE
					PRINT @_errorMSG;
			END
	
			IF	@_nonAccessibleDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_nonAccessibleDatabasesCounts AS VARCHAR(5))+' database(s) '+(case when @_nonAccessibleDatabasesCounts > 1 then 'are' else 'is' end) +' in non-accessible state. Either wait, or resolve the issue, and then unrestrict Data files.
				'+@_nonAccessibleDatabases+'
	*/';
				IF @forceExecute = 1
				BEGIN
					IF @_errorOccurred = 0
						SET @_errorOccurred = 1;

					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Non-Accessible Databases' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
				ELSE
					PRINT @_errorMSG;
			END

			IF	@_principalDatabases IS NOT NULL
			BEGIN
				SET @_errorMSG = '/*	NOTE: Following '+CAST(@_principalDatabaseCounts_Mirroring AS VARCHAR(5))+' database(s) '+(case when @_principalDatabaseCounts_Mirroring > 1 then 'are' else 'is' end) +' in role of ''Mirroring Principal''. So generating code to un-restrict growth of secondary files for these dbs.
				'+@_principalDatabases+'
	*/';
				IF @forceExecute = 0 -- Ignore this message if @forceExecute = 1 since this is information only message
					PRINT @_errorMSG;
				ELSE
				BEGIN
					--	Make a FAIL entry for Sending message to end user
					INSERT @OutputMessages
						(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
					SELECT	'Information' AS Status
							,'Principal Databases' AS Category
							,NULL AS  DBName
							,NULL AS [FileGroup]
							,NULL AS [FileName]
							,@_errorMSG AS MessageDetails
							,NULL AS TSQLCode;
				END
			END

			--	Generate TSQL Code for un-restricting data file growth
			IF EXISTS (SELECT * FROM #T_Files_Final WHERE growth = 0)
			BEGIN	-- Begin block for tsql code generation
				IF @verbose = 1
					PRINT	'		Begin block for tsql code generation';

				DECLARE @T_Files_Final_UnRestrictFiles TABLE (ID INT IDENTITY(1,1), TSQL_UnRestrictFileGrowth VARCHAR(2000));
				INSERT @T_Files_Final_UnRestrictFiles
				SELECT TSQL_UnRestrictFileGrowth FROM #T_Files_Final as f WHERE growth = 0;

				SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_Final_UnRestrictFiles;
			
				WHILE @_loopCounter <= @_loopCounts
				BEGIN	-- Begin Block of Loop
					SELECT @_loopSQLText = '
	--	Un-restrict Growth of File: '+CAST(ID AS VARCHAR(5))+';'+TSQL_UnRestrictFileGrowth FROM @T_Files_Final_UnRestrictFiles as f WHERE f.ID = @_loopCounter;
					IF @_loopCounter = 1
						SET @_loopSQLText =	'USE [master];
--	=====================================================================================================
	--	TSQL Code to Remove Restriction of Auto Growth for files on @oldVolume '+QUOTENAME(@oldVolume) + '.
' + @_loopSQLText;

					IF @forceExecute = 1
					BEGIN
						BEGIN TRY
							EXEC (@_loopSQLText);

							--	Make a SUCCESS entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Success' AS Status
									,'Un-Restrict File' AS Category
									,@_dbName AS  DBName
									,NULL AS [FileGroup]
									,@_name AS [FileName]
									,'Autogrowth restriction removed for file' AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END TRY
						BEGIN CATCH
							IF @_errorOccurred = 0
								SET @_errorOccurred = 1;
								
							--	Make a FAIL entry for Sending message to end user
							INSERT @OutputMessages
								(Status, Category, DBName, FileGroup, FileName, MessageDetails, TSQLCode)
							SELECT	'Fail' AS Status
									,'Un-Restrict File' AS Category
									,@_dbName AS  DBName
									,NULL AS [FileGroup]
									,@_name AS [FileName]
									,ERROR_MESSAGE() AS MessageDetails
									,@_loopSQLText AS TSQLCode;
						END CATCH
					END
					ELSE
						PRINT @_loopSQLText;

					SET @_loopSQLText = '';
					SET @_loopCounter = @_loopCounter + 1;
				END		-- End Block of Loop
			END -- End block for tsql code generation
			ELSE
				PRINT	'/*	------------------------------------------------------------------------------------------------
		No files exists on @oldVolume '+QUOTENAME(@oldVolume) + ' with Auto growth restriction.
	------------------------------------------------------------------------------------------------
	*/';
		END	-- End Else portion for Validation of Data volumes

		IF @verbose = 1
			PRINT	'/*	******************** End:	@unrestrictFileGrowth = 1 *****************************/
';
	END -- End block of @unrestrictFileGrowth = 1
	--	----------------------------------------------------------------------------
		--	End:	@unrestrictFileGrowth = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@generateCapacityException = 1
	--	----------------------------------------------------------------------------
	IF	@generateCapacityException = 1
	BEGIN
		IF @verbose = 1
			PRINT	'
/*	******************** Begin: @generateCapacityException = 1 *****************************/';

		IF (SELECT COUNT(*) FROM @mountPointVolumes as V WHERE V.Volume = @oldVolume) <> 1
		BEGIN -- Begin block for Validation of Data volumes
			SET @_errorMSG = '@oldVolume parameter value is mandatory with @generateCapacityException = 1 parameter. Verify if valid values are supplied.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END -- End block for Validation of Data volumes
		--
		IF EXISTS (SELECT * FROM @mountPointVolumes as V WHERE V.Volume = @oldVolume AND [freespace(%)] > 20.0 )
		BEGIN -- if % free space on @oldVolume is more than 20%, then Add an entry in #ErrorMessages table but Continue with this code generation.
			SET @_errorMSG = '@oldVolume still has free space more than 20%. So, it is not recommended to add Capacity Exception in MNA table right now.
Kindly use @restrictMountPointGrowth functionality to increase the space utilization of files.';
			
			BEGIN
				IF @_errorOccurred = 0
					SET @_errorOccurred = 1;

				INSERT #ErrorMessages
				SELECT	'Under utilized Space Capacity' AS ErrorCategory
						,NULL AS DBName 
						,NULL AS [FileName] 
						,@_errorMSG AS ErrorDetails 
						,NULL AS TSQLCode;
			END
			
		END -- End block for Validation of Data volumes
		--
		IF EXISTS (SELECT f.dbName, f.data_space_id FROM #T_Files_Final AS f WHERE f.dbName NOT IN (@_dbaMaintDatabase,'tempdb') AND f.growth <> 0 GROUP BY f.dbName, f.data_space_id)
		BEGIN	--	Check if all the files are set to 0 auto growth
			SET @_errorMSG = 'Kindly restrict the data/log files on @oldVolume before using @generateCapacityException option.';
			IF (select CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),charindex('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT)) >= 12
				EXEC sp_executesql N'THROW 50000,@_errorMSG,1',N'@_errorMSG VARCHAR(200)', @_errorMSG;
			ELSE
				EXEC sp_executesql N'RAISERROR (@_errorMSG, 16, 1)', N'@_errorMSG VARCHAR(200)', @_errorMSG;
		END
		--
		ELSE
		BEGIN -- Begin Else portion for Validation of Data volumes
			PRINT	'	--	NOTE:	'+CAST(@_freeSpace_OldVolume_GB AS VARCHAR(20))+'gb('+CAST(@_freeSpace_OldVolume_Percent AS VARCHAR(20))+'%) of '+CAST(@_totalSpace_OldVolume_GB AS VARCHAR(20))+'gb is available on @oldVolume '+QUOTENAME(@oldVolume,'''')+'.';
			PRINT	'
	--	Add Space Capacity Exception for '+QUOTENAME(@oldVolume,'''')+'
		--	Execute Below code on MNA server <DBMNAServer>
	';
			--	Find FQN
			DECLARE @Domain varchar(100), @key varchar(100);
			SET @key = 'SYSTEM\ControlSet001\Services\Tcpip\Parameters\';
			EXEC master..xp_regread @rootkey='HKEY_LOCAL_MACHINE', @key=@key,@value_name='Domain',@value=@Domain OUTPUT;

			IF @verbose = 1
			BEGIN
				PRINT	'	SELECT * FROM @mountPointVolumes v WHERE v.Volume = @oldVolume;';
				SELECT 'SELECT * FROM @mountPointVolumes v WHERE v.Volume = @oldVolume;' AS RunningQuery, * FROM @mountPointVolumes v WHERE v.Volume = @oldVolume;
			END

			;WITH T_Thresholds AS
			(
				--	Removing below code since sys.dm_os_volume_stats DMV is not available before SQL 2008.
				SELECT	Volume, 
						[capacity (gb)] = [capacity(GB)],
						[pWarningThreshold%] = CEILING(100-[freespace(%)]), -- find used %
						[pWarningThreshold (gb)] = FLOOR([capacity(GB)] - [freespace(GB)]), -- find used space
						[pCriticalThreshold%] = CEILING(100-[freespace(%)])+2, -- set critical to used % + 2.
						[pCriticalThreshold (gb)] = FLOOR(((CEILING(100-[freespace(%)])+2)*[capacity(GB)])/100)
						--,s.*--,f.ID
				FROM  @mountPointVolumes v WHERE v.Volume = @oldVolume

			)
			,T_Exception AS
			(
				SELECT	*
						,[pReason] = 'Data '+LEFT(f.Volume,LEN(f.Volume)-1)+' Unrestricted Cap:'+CAST([capacity (gb)] AS VARCHAR(20))+'gbs  Warn:'+CAST([pWarningThreshold%] AS VARCHAR(20))+'% '+CAST([pWarningThreshold (gb)] AS VARCHAR(20))+'gbs  Crit:'+CAST([pCriticalThreshold%] AS VARCHAR(20))+'% '+CAST([pCriticalThreshold (gb)] AS VARCHAR(20))+'gbs'
				FROM T_Thresholds as f
			)
				SELECT	@_capacityExceptionSQLText = '
	IF NOT EXISTS (SELECT * FROM MNA.MA.EXCEPTION e WHERE e.eventName = ''Capacity Constrained'' AND e.serverName LIKE '''+@@SERVERNAME+'%'' AND volumeName = '''+LEFT(e.Volume,LEN(e.Volume)-1)+''')
	BEGIN
		IF NOT EXISTS (SELECT * FROM MNA.ma.VolumeUseType AS v WHERE v.serverName LIKE '''+@@SERVERNAME+'%'' AND v.volume = '''+LEFT(e.Volume,LEN(e.Volume)-1)+''')
			PRINT	''--	Data Volume is not present on MNA.ma.VolumeUseType table.''
		ELSE
		BEGIN
			DECLARE	@DateOfException SMALLDATETIME = GETDATE();

			--	Space Capacity Exception
			EXEC MNA.ma.SpaceCapacity_AddException	
					@pServerName	= '''+@@servername+'.'+@Domain+''',
					@pVolumeName		= '''+LEFT(e.Volume,LEN(e.Volume)-1)+''',
					@pWarningThreshold	= '+CAST([pWarningThreshold%] AS VARCHAR(20))+',
					@pCriticalThreshold = '+CAST([pCriticalThreshold%] AS VARCHAR(20))+',
					@pStartDTS			= @DateOfException,
					@pEndDTS			= NULL,
					@pReason			= '''+[pReason]+''';
		END
	END
	'
				FROM	T_Exception AS e;

				PRINT	@_capacityExceptionSQLText;
		END	-- End Else portion for Validation of Data volumes

		IF @verbose = 1
			PRINT	'/*	******************** End: @generateCapacityException = 1 *****************************/
';
	END -- End block of @generateCapacityException = 1
	--	----------------------------------------------------------------------------
		--	End:	@generateCapacityException = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@UpdateMountPointSecurity = 1
	--	----------------------------------------------------------------------------
	IF	@UpdateMountPointSecurity = 1
	BEGIN
			PRINT	'/*	Import <<SQLDBATools>> powershell module, and then use <<Update-MountPointSecurity>> command after that.

	Import-Module "\\DBATools\SQLDBATools.psm1"
	Update-MountPointSecurity -ServerName '+QUOTENAME(@@SERVERNAME,'"')+ '
	Update-TSMFolderPermissions -ServerName '+QUOTENAME(@@SERVERNAME,'"')+ '
	Update-SQLBackupFolderPermissions -ServerName '+QUOTENAME(@@SERVERNAME,'"')+ '
	*/';

	END -- End block of @UpdateMountPointSecurity = 1
	--	----------------------------------------------------------------------------
		--	End:	@UpdateMountPointSecurity = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@restrictMountPointGrowth = 1
	--	----------------------------------------------------------------------------
	IF	@restrictMountPointGrowth = 1
	BEGIN
		IF @verbose = 1
			PRINT	'Begin:	@restrictMountPointGrowth = 1';

		IF EXISTS (SELECT * FROM sys.master_files as mf WHERE mf.physical_name LIKE (@oldVolume + '%') AND mf.growth <> 0  
															AND DB_NAME(mf.database_id)IN (SELECT f.DBName FROM @filterDatabaseNames AS f))
		BEGIN
			PRINT	'Kindly restrict the growth of files +nt in @oldVolume = '+QUOTENAME(@oldVolume,'''')+'. Then, proceed for this step.';
		END
		--ELSE
		BEGIN	-- Begin block : Real Logic for restricting mount point volume

			IF @verbose = 1
				PRINT	'	Assigning values for @_Total_Files_Size_MB, @_Space_That_Can_Be_Freed_MB, @_Total_Files_SpaceUsed_MB and @_SpaceToBeFreed_MB.';

			SELECT	@_Space_That_Can_Be_Freed_MB = SUM(f.FreeSpaceMB)
			FROM	@DBFiles AS f
			WHERE	f.physical_name LIKE (@oldVolume + '%')
				AND	f.DbName IN (SELECT d.DBName FROM @filterDatabaseNames AS d);

			--	Since some DBs can be in offline state, Total File Size can not be calculated using @DBFiles.
			--	@_SpaceToBeFreed_MB will be -ve is @mountPointGrowthRestrictionPercent > [% Full]. Otherwise +ve if space has to be released from files using Shrink operation.
			SELECT	@_SpaceToBeFreed_MB = (((100-@mountPointGrowthRestrictionPercent)*v.[capacity(MB)])/100) - [freespace(MB)]
					,@_Total_Files_Size_MB = ([capacity(MB)] - [freespace(MB)])
					,@_Total_Files_SpaceUsed_MB = ([capacity(MB)] - [freespace(MB)]) - @_Space_That_Can_Be_Freed_MB
			FROM	@mountPointVolumes v
			WHERE	v.Volume = @oldVolume;

			IF @verbose = 1
				PRINT	'	Values for @_Total_Files_Size_MB = '+CAST(@_Total_Files_Size_MB AS VARCHAR)+' 
			@_Space_That_Can_Be_Freed_MB = '+CAST(@_Space_That_Can_Be_Freed_MB AS VARCHAR)+'
			@_Total_Files_SpaceUsed_MB = '+CAST(@_Total_Files_SpaceUsed_MB AS VARCHAR)+' 
			@_SpaceToBeFreed_MB = '+CAST(@_SpaceToBeFreed_MB AS VARCHAR);

			IF @verbose = 1
				PRINT '		Creating temp table #DBFiles_By_Weightage.';

			--	Create table with Weightage of files
			IF OBJECT_ID('tempdb..#DBFiles_By_Weightage') IS NOT NULL
				DROP TABLE #DBFiles_By_Weightage;
			WITH T_DBFiles_By_Weightage AS 
			(
				SELECT	*
						,[Weightage] = [% space used] + [SpaceRatio_b/w_All]
				FROM  (
						SELECT	*
								,[SpaceRatio_b/w_All] = CAST( (SpaceUsed * 100.0) / @_Total_Files_SpaceUsed_MB AS DECIMAL(18,2))
						FROM	@DBFiles AS f 
						WHERE	f.physical_name LIKE (@oldVolume + '%')
							AND	f.DbName IN (SELECT d.DBName FROM @filterDatabaseNames AS d)
					  ) AS f1
			)
			,T_DBFiles_Total_Weightage_Sum AS
			(
				SELECT Weightage_Sum = SUM([Weightage]) FROM T_DBFiles_By_Weightage
			)
			SELECT	*
					,Weightage_Ratio = CAST([Weightage] / Weightage_Sum AS DECIMAL(18,2))
			INTO	#DBFiles_By_Weightage
			FROM	T_DBFiles_By_Weightage,T_DBFiles_Total_Weightage_Sum;

			IF @verbose = 1
			BEGIN
				PRINT	'Printing Data of @DBFiles';
				SELECT	*
				FROM	#DBFiles_By_Weightage
				ORDER BY [Weightage] DESC;

				PRINT	'Printing Data of @mountPointVolumes';
				SELECT	*
				FROM	@mountPointVolumes v
				WHERE	v.Volume = @oldVolume;
			END

			IF EXISTS (SELECT * FROM @mountPointVolumes WHERE Volume = @oldVolume AND [freespace(%)] > (100-@mountPointGrowthRestrictionPercent))
			BEGIN
				IF @verbose = 1
					PRINT '	Increase size of files +nt on @oldVolume';

				--	Find space that has to be added to Data/Log files
				SELECT	@_Space_To_Add_to_Files_MB = (([freespace(%)]-(100.0-@mountPointGrowthRestrictionPercent))*[capacity(MB)])/100
				FROM	@mountPointVolumes 
				WHERE	Volume = @oldVolume

				IF @verbose = 1
					PRINT '	space that has to be added to Data/Log files: '+cast(@_Space_To_Add_to_Files_MB as varchar);
			
				PRINT	'--	Add space in files on volume '+QUOTENAME(@oldVolume,'''')+ ' to '+CAST(@mountPointGrowthRestrictionPercent AS VARCHAR(10))+'% of mount point capacity.

	';

				--	Truncate table
				DELETE FROM @T_Files_restrictMountPointGrowth;

				IF @verbose = 1
				BEGIN
					PRINT	'	Printing data of #DBFiles_By_Weightage';
						SELECT 'SELECT * FROM #DBFiles_By_Weightage;' AS RunningQuery, * FROM #DBFiles_By_Weightage;
					PRINT	'	Printing data of below CTE
		WITH T_FileSpace_01 AS
		(
			SELECT	*
					,RowID = ROW_NUMBER()OVER(ORDER BY Weightage DESC)
					,SpaceToAddOnFile = Weightage_Ratio * @_Space_To_Add_to_Files_MB
			FROM	#DBFiles_By_Weightage AS f
			WHERE	Weightage_Ratio <> 0.0
		)
		,T_FileSpace_Final AS
		(
			SELECT	DbName, FileName, physical_name, CurrentSizeMB, FreeSpaceMB, SpaceUsed, type_desc, growth, is_percent_growth, [% space used], [SpaceRatio_b/w_All], Weightage, Weightage_Sum, Weightage_Ratio, RowID --, SpaceToAddOnFile
					,SpaceToAddOnFile =		CASE	WHEN s.RowID = (SELECT MAX(s1.RowID) FROM T_FileSpace_01 AS s1)
													THEN @_Space_To_Add_to_Files_MB - (SELECT SUM(s1.SpaceToAddOnFile) FROM T_FileSpace_01 AS s1 WHERE s1.RowID < s.RowID)
													ELSE SpaceToAddOnFile
											END
			FROM	T_FileSpace_01 AS s
		)
		SELECT * FROM T_FileSpace_Final';
					WITH T_FileSpace_01 AS
					(
						SELECT	*
								,RowID = ROW_NUMBER()OVER(ORDER BY Weightage DESC)
								,SpaceToAddOnFile = Weightage_Ratio * @_Space_To_Add_to_Files_MB
						FROM	#DBFiles_By_Weightage AS f
						WHERE	Weightage_Ratio <> 0.0
					)
					,T_FileSpace_Final AS
					(
						SELECT	DbName, FileName, physical_name, CurrentSizeMB, FreeSpaceMB, SpaceUsed, type_desc, growth, is_percent_growth, [% space used], [SpaceRatio_b/w_All], Weightage, Weightage_Sum, Weightage_Ratio, RowID --, SpaceToAddOnFile
								,SpaceToAddOnFile =		CASE	WHEN s.RowID = (SELECT MAX(s1.RowID) FROM T_FileSpace_01 AS s1)
																THEN @_Space_To_Add_to_Files_MB - ISNULL((SELECT SUM(s1.SpaceToAddOnFile) FROM T_FileSpace_01 AS s1 WHERE s1.RowID < s.RowID),0)
																ELSE SpaceToAddOnFile
														END
						FROM	T_FileSpace_01 AS s
					)
					SELECT * FROM T_FileSpace_Final;
				END;

				--	Prepare code
				WITH T_FileSpace_01 AS
				(
					SELECT	*
							,RowID = ROW_NUMBER()OVER(ORDER BY Weightage DESC)
							,SpaceToAddOnFile = Weightage_Ratio * @_Space_To_Add_to_Files_MB
					FROM	#DBFiles_By_Weightage AS f
					WHERE	Weightage_Ratio <> 0.0
				)
				,T_FileSpace_Final AS
				(
					SELECT	DbName, FileName, physical_name, CurrentSizeMB, FreeSpaceMB, SpaceUsed, type_desc, growth, is_percent_growth, [% space used], [SpaceRatio_b/w_All], Weightage, Weightage_Sum, Weightage_Ratio, RowID --, SpaceToAddOnFile
							,SpaceToAddOnFile =		CASE	WHEN s.RowID = (SELECT MAX(s1.RowID) FROM T_FileSpace_01 AS s1)
															THEN @_Space_To_Add_to_Files_MB - ISNULL((SELECT SUM(s1.SpaceToAddOnFile) FROM T_FileSpace_01 AS s1 WHERE s1.RowID < s.RowID),0)
															ELSE SpaceToAddOnFile
													END
					FROM	T_FileSpace_01 AS s
				)
					INSERT @T_Files_restrictMountPointGrowth (TSQL_restrictMountPointGrowth)
					SELECT	--*,
							TSQL_ShrinkFile = '		PRINT	''Adding additional space for file '+QUOTENAME([FileName])+' of database '+QUOTENAME(DbName)+'.'';
ALTER DATABASE ['+DbName+'] MODIFY FILE ( NAME = N'''+[FileName]+''', SIZE = '+CAST(CAST(CurrentSizeMB+SpaceToAddOnFile AS BIGINT) AS VARCHAR(20))+'MB);
		'
					FROM	T_FileSpace_Final AS s;

				IF @verbose = 1
				BEGIN
					PRINT	'	Printing data of @T_Files_restrictMountPointGrowth';
					SELECT 'SELECT * FROM @T_Files_restrictMountPointGrowth;' AS RunningQuery, * FROM @T_Files_restrictMountPointGrowth;
				END
			
				SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_restrictMountPointGrowth;
				WHILE @_loopCounter <= @_loopCounts
				BEGIN	-- Begin Block of Loop
					SELECT @_loopSQLText = '	--	Add Space into File: '+CAST(ID AS VARCHAR(5))+';'+TSQL_restrictMountPointGrowth FROM @T_Files_restrictMountPointGrowth as f WHERE f.ID = @_loopCounter;
					IF @_loopCounter = 1
						SET @_loopSQLText =	'USE [master];
	--	=====================================================================================================
	--	TSQL Code to Shrink file.
		' + @_loopSQLText;

					PRINT @_loopSQLText;

					SET @_loopSQLText = '';
					SET @_loopCounter = @_loopCounter + 1;
				END		-- End Block of Loop

			END
			ELSE
			BEGIN
				IF @verbose = 1
					PRINT '	Shrink files +nt on @oldVolume such that required space is returned to Drive';

				PRINT	'--	Generate Code for shrinking files on volume '+QUOTENAME(@oldVolume,'''')+ ' to '+CAST(@mountPointGrowthRestrictionPercent AS VARCHAR(10))+'% of mount point capacity.';

				;WITH T_FileSpace_01 AS
				(
					SELECT	*
							,RowID = ROW_NUMBER()OVER(ORDER BY FreeSpaceMB DESC)
					FROM	@DBFiles AS f
					WHERE	f.physical_name LIKE (@oldVolume + '%')
						AND	f.DbName IN (SELECT d.DBName FROM @filterDatabaseNames AS d)
				)
				,T_FileSpace_Final AS
				(
					SELECT	*
							,SpaceFreedOnFile = (s.FreeSpaceMB-512)
							,Total_SpaceFreedTillNow = (SELECT SUM(s1.FreeSpaceMB-512) FROM T_FileSpace_01 as s1 WHERE s1.RowID <= s.RowID)
					FROM	T_FileSpace_01 AS s
				)
					INSERT @T_Files_restrictMountPointGrowth (TSQL_restrictMountPointGrowth)
					SELECT	--*,
							TSQL_ShrinkFile = '
		USE ['+DbName+'];
		DBCC SHRINKFILE (N'''+[FileName]+''' , '+ (CASE WHEN s.Total_SpaceFreedTillNow <= @_SpaceToBeFreed_MB THEN cast(convert(numeric,(SpaceUsed+512) ) as varchar(50)) ELSE (cast(convert(numeric,(SpaceUsed+512+(Total_SpaceFreedTillNow-@_SpaceToBeFreed_MB)) ) as varchar(50))) END)   +');
			PRINT	''Shrinking file '+QUOTENAME([FileName])+ ' for database '+QUOTENAME(DbName)+'.'';
		--	Space freed on file '+QUOTENAME([FileName])+ ' for database '+QUOTENAME(DbName)+' = '+cast(SpaceFreedOnFile as varchar(50))+' MB
		'
					FROM	T_FileSpace_Final AS s
					WHERE	s.Total_SpaceFreedTillNow <= @_SpaceToBeFreed_MB
						OR	(s.Total_SpaceFreedTillNow - @_SpaceToBeFreed_MB < SpaceFreedOnFile);
			
				SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_restrictMountPointGrowth;
				WHILE @_loopCounter <= @_loopCounts
				BEGIN	-- Begin Block of Loop
					SELECT @_loopSQLText = '
		--	Shrink File: '+CAST(ID AS VARCHAR(5))+';'+TSQL_restrictMountPointGrowth FROM @T_Files_restrictMountPointGrowth as f WHERE f.ID = @_loopCounter;
					IF @_loopCounter = 1
						SET @_loopSQLText =	'USE [master];
		--	=====================================================================================================
		--	TSQL Code to Shrink file.
			' + @_loopSQLText;

					PRINT @_loopSQLText;

					SET @_loopSQLText = '';
					SET @_loopCounter = @_loopCounter + 1;
				END		-- End Block of Loop
			END
			
			--END
		END	-- End block : Real Logic for restricting mount point volume

		IF @verbose = 1
			PRINT	'End - @restrictMountPointGrowth = 1';
	END -- End block of @restrictMountPointGrowth = 1
	--	----------------------------------------------------------------------------
		--	End:	@restrictMountPointGrowth = 1
	--	============================================================================


	--	============================================================================
		--	Begin:	@expandTempDBSize = 1
	--	----------------------------------------------------------------------------
	IF	@expandTempDBSize = 1
	BEGIN
		IF @verbose = 1
			PRINT	'
/*	******************** Begin:	@expandTempDBSize = 1 *****************************/';

		IF @verbose = 1
			PRINT	'	Populate data into @tempDBFiles';

		IF (SELECT SERVERPROPERTY ('IsHadrEnabled')) = 1
		BEGIN
			SET @_loopSQLText = 'The server is part of AlwaysOn. Kindly run this procedure on other replicas as well.';

			IF @forceExecute = 1
			BEGIN
				IF @_errorOccurred = 0
					SET @_errorOccurred = 1;

				INSERT #ErrorMessages
				SELECT	'Need Extra Efforts' AS ErrorCategory
						,NULL AS DBName 
						,NULL AS [FileName] 
						,@_loopSQLText AS ErrorDetails 
						,NULL AS TSQLCode;
			END
			ELSE
				PRINT '/********* '+ @_loopSQLText+'			*/';
		END

		SET @_logicalCores = (select cpu_count from sys.dm_os_sys_info);
		IF @verbose = 1
			PRINT	'	Logical CPU = '+CAST(@_logicalCores AS VARCHAR(10));
			
		--	Get TempDb data files
		INSERT @tempDBFiles
			([DBName], FileId, [LogicalName], [physical_name], [FileSize_MB], [Volume], [VolumeName], [VolumeSize_MB])
		SELECT	DB_NAME(mf.database_id) as DBName, mf.file_id, mf.name as LogicalName, mf.physical_name, ((mf.size*8.0)/1024) as FileSize_MB, 
				s.Volume, s.VolumeName, s.[capacity(MB)] as VolumeSize_MB
		FROM	sys.master_files as mf
		CROSS APPLY
		(	SELECT	v2.*
			FROM  (	SELECT MAX(LEN(v.Volume)) AS Max_Volume_Length FROM @mountPointVolumes as v WHERE mf.physical_name LIKE (v.Volume+'%') ) as v1
			INNER JOIN
				  (	SELECT v.* FROM @mountPointVolumes as v WHERE mf.physical_name LIKE (v.Volume+'%') ) as v2
				ON	LEN(v2.Volume) = v1.Max_Volume_Length
		) as s
		WHERE	mf.database_id = DB_ID('tempdb')
			AND	mf.type_desc = 'ROWS'
		ORDER BY mf.[file_id] ASC;

		IF @verbose = 1
			PRINT	'	Updating @tempDBFiles.[isToBeDeleted] when Files are more then @_logicalCores';
		UPDATE @tempDBFiles
		SET isToBeDeleted = 1
		WHERE cast(fileNo as int)-cast(@_logicalCores as int) > 0;

		IF @verbose = 1
		BEGIN
			SELECT	RunningQuery, tf.*
			FROM  (SELECT 'SELECT * FROM @tempDBFiles' AS RunningQuery) AS Qry
			LEFT JOIN
				@tempDBFiles as tf
			ON	1 = 1;
		END

		-- Get Integer value for [tempdb] data files. For example, value would be 8 from file name [tempdev8].
		SET @_maxFileNO = (SELECT MAX( CAST(RIGHT(REPLACE(REPLACE(LogicalName,']',''),'[',''),PATINDEX('%[a-zA-Z_ ]%',REVERSE(REPLACE(REPLACE(LogicalName,']',''),'[','')))-1) AS BIGINT)) FROM @tempDBFiles);

		--	Get count of Valid tempdb data files
		SET @_fileCounts = (SELECT COUNT(*) FROM @tempDBFiles as f WHERE [isToBeDeleted] = 0);
		IF @verbose = 1
			PRINT	'	Valid files (@_fileCounts) = '+CAST(@_fileCounts AS VARCHAR(10));

		IF @_fileCounts <> (CASE WHEN @_logicalCores >= 8 THEN 8 ELSE @_logicalCores END)
			SET @_counts_of_Files_To_Be_Created = (CASE WHEN @_logicalCores >= 8 THEN 8 ELSE @_logicalCores END) - @_fileCounts;
		
		IF @verbose = 1
		BEGIN
			IF @_logicalCores > 8
				PRINT	'	Logical CPU are more than 8. Still creating tempdb files upto 8 only.';
			PRINT	'	Extra Tempdb data files to be created (@_counts_of_Files_To_Be_Created) = '+CAST(@_counts_of_Files_To_Be_Created AS VARCHAR(10));
		END

		IF @verbose = 1
			PRINT	'	Dropping and creating temp table #tempDBFiles';
			 
		/*	This table will contain tempdb files upto 8 */
		IF OBJECT_ID('tempdb..#tempDBFiles') IS NOT NULL
			DROP TABLE #tempDBFiles
		SELECT	O.*				
				,TSQL_AddFile = CASE WHEN isToBeCreated = 1 THEN '
	ALTER DATABASE [tempdb] ADD FILE ( NAME = N'''+LogicalName+''', FILENAME = N'''+physical_name+''' , SIZE = '+CAST(CAST(FileSize_MB AS NUMERIC(10,0)) AS VARCHAR(10))+'MB , FILEGROWTH = 0);' ELSE NULL END				
				,[TSQL_EmptyFile] = CASE WHEN isToBeDeleted = 1 THEN '
	DBCC SHRINKFILE (N'''+LogicalName+''' , EMPTYFILE);' ELSE NULL END
				,[TSQL_RemoveFile] = CASE WHEN isToBeDeleted = 1 THEN '
	ALTER DATABASE [tempdb]  REMOVE FILE ['+LogicalName+'];' ELSE NULL END
		INTO	#tempDBFiles
		FROM  (
				SELECT	COALESCE(tf.DBName, df.DBName) as DBName,
						COALESCE(tf.LogicalName,'tempdev'+ cast( (@_maxFileNO+df.FileNo_Add) as varchar(3) )) as LogicalName,
						COALESCE(tf.physical_name,LEFT(df.physical_name,LEN(df.physical_name)-CHARINDEX('\',REVERSE(df.physical_name))+1) +'tempdb'+ cast( (@_maxFileNO+df.FileNo_Add) as varchar(3) ) + '.ndf') as physical_name,
						COALESCE(tf.FileSize_MB,200) AS FileSize_MB,
						COALESCE(tf.Volume,df.Volume) AS Volume, 
						COALESCE(tf.VolumeName,df.VolumeName) AS VolumeName, 
						COALESCE(tf.VolumeSize_MB, df.VolumeSize_MB) AS VolumeSize_MB,
						isToBeDeleted = COALESCE(tf.isToBeDeleted, 0),
						isExtraFile = CASE WHEN tf.fileNo-(CASE WHEN @_logicalCores >= 8 THEN 8 ELSE @_logicalCores END) > 0 THEN 1 ELSE 0 END,
						isToBeCreated = CASE WHEN tf.isToBeDeleted IS NULL THEN 1 ELSE 0 END
				FROM	@tempDBFiles as tf
				FULL OUTER JOIN
					(	SELECT DBName, LogicalName, physical_name, FileSize_MB, Volume, VolumeName, VolumeSize_MB, isToBeDeleted, FileNo_Add
						--FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8)) AS FileIterator_Table (FileNo_Add) 
						FROM (SELECT 1  AS FileNo_Add
							UNION ALL
							SELECT 2
							UNION ALL
							SELECT 3 
							UNION ALL
							SELECT 4
							UNION ALL
							SELECT 5
							UNION ALL
							SELECT 6
							UNION ALL
							SELECT 7
							UNION ALL
							SELECT 8) AS FileIterator_Table
						CROSS JOIN
						(SELECT TOP 1 * FROM @tempDBFiles WHERE [isToBeDeleted] = 0 ORDER BY FileId DESC) AS t
						WHERE	FileIterator_Table.FileNo_Add <= @_counts_of_Files_To_Be_Created
						AND @output4IdealScenario = 1
					) AS df
				ON		1 = 2
				) AS O;

		IF @verbose = 1
		BEGIN
			SELECT	'SELECT * FROM #tempDBFiles' AS RunningQuery, *
			FROM	#tempDBFiles
		END

		--	If some invalid file exists, then remove that file
		IF EXISTS (SELECT * FROM #tempDBFiles WHERE isToBeDeleted = 1) AND @output4IdealScenario = 1
		BEGIN
			DELETE @T_Files_Remove;
			INSERT @T_Files_Remove ( TSQL_EmptyFile, TSQL_RemoveFile, name, Volume )
			SELECT TSQL_EmptyFile,TSQL_RemoveFile,LogicalName,Volume FROM #tempDBFiles as f WHERE isToBeDeleted = 1 OR isExtraFile = 1 ORDER BY f.LogicalName DESC;

			IF @verbose = 1 
				PRINT	'	Initiating @_loopCounter and @_loopCounts';
			SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_Remove;

			IF @verbose=1 
					PRINT	'	Starting Loop to remove tempdb files which are either on non-tempdb volumes, or extra files (more than CPU count)';
			WHILE @_loopCounter <= @_loopCounts
			BEGIN	-- Begin Block of Loop

				SELECT @_loopSQLText = '
--	Empty File: '+CAST(@_loopCounter AS VARCHAR(5))+TSQL_EmptyFile+'

--	Remove File: '+CAST(@_loopCounter AS VARCHAR(5))+TSQL_RemoveFile
						,@_dbName = 'tempdb' ,@_name = name
				FROM @T_Files_Remove as f WHERE f.ID = @_loopCounter;

				IF @_loopCounter = 1
					SET @_loopSQLText =	'USE [tempdb];
--	=====================================================================================================
--	TSQL Code to Remove data files which are either on non-tempdb volumes, or extra files (more than CPU count).
' + @_loopSQLText;

				IF @forceExecute = 1
				BEGIN
					BEGIN TRY
						EXEC (@_loopSQLText);
					END TRY
					BEGIN CATCH
						IF @_errorOccurred = 0
							SET @_errorOccurred = 1;

						INSERT #ErrorMessages
						SELECT	'Remove TempDB File Failed' AS ErrorCategory
								,@_dbName AS DBName 
								,@_name AS [FileName] 
								,ERROR_MESSAGE() AS ErrorDetails 
								,@_loopSQLText AS TSQLCode;
					END CATCH
				END
				ELSE
					PRINT @_loopSQLText;

				SET @_loopSQLText = '';
				SET @_loopCounter = @_loopCounter + 1;
			END		-- End Block of Loop
		END

		--	If number of tempdb files is less than CPUs
		IF EXISTS (SELECT * FROM #tempDBFiles WHERE isToBeCreated = 1) AND @output4IdealScenario = 1
		BEGIN
			DELETE @T_Files_Final_Add;
			INSERT @T_Files_Final_Add (TSQL_AddFile,DBName,name,_name)
			SELECT TSQL_AddFile,DBName,LogicalName,'' FROM #tempDBFiles as f WHERE isToBeCreated = 1 ORDER BY f.LogicalName;

			IF @verbose = 1 
				PRINT	'	Initiating @_loopCounter and @_loopCounts';
			SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_Final_Add;

			IF @verbose=1 
					PRINT	'	Starting Loop to add tempdb files as per number of logical Cores';
			WHILE @_loopCounter <= @_loopCounts
			BEGIN	-- Begin Block of Loop

				SELECT @_loopSQLText = '
--	Add File: '+CAST(@_loopCounter AS VARCHAR(5))+';	'+TSQL_AddFile 
						,@_dbName = DBName ,@_name = name ,@_newName = _name
				FROM @T_Files_Final_Add as f WHERE f.ID = @_loopCounter;

				IF @_loopCounter = 1
					SET @_loopSQLText =	'

USE [master];
--	=====================================================================================================
--	TSQL Code to Add Secondary Data Files on tempdb database as per no of logical CPUs upto 8.
' + @_loopSQLText;

				IF @forceExecute = 1
				BEGIN
					BEGIN TRY
						EXEC (@_loopSQLText);
					END TRY
					BEGIN CATCH
						IF @_errorOccurred = 0
							SET @_errorOccurred = 1;

						INSERT #ErrorMessages
						SELECT	'Remove TempDB File Failed' AS ErrorCategory
								,@_dbName AS DBName 
								,@_name AS [FileName] 
								,ERROR_MESSAGE() AS ErrorDetails 
								,@_loopSQLText AS TSQLCode;
					END CATCH
				END
				ELSE
					PRINT @_loopSQLText;

				SET @_loopSQLText = '';
				SET @_loopCounter = @_loopCounter + 1;
			END		-- End Block of Loop
		END
		
		IF @verbose = 1
		BEGIN
			IF @tempDbMaxSizeThresholdInGB IS NOT NULL
				PRINT	'	Implementing logic to resize tempdb data files upto @tempDbMaxSizeThresholdInGB = '+CAST(@tempDbMaxSizeThresholdInGB AS VARCHAR(5))+' GB. ';
			ELSE
				PRINT	'	Implementing logic to resize tempdb data files upto @tempDBMountPointPercent = '+CAST(@tempDBMountPointPercent AS VARCHAR(5))+' %. ';
		END

		;WITH T_Files_01 AS
		(
			--	Find all the data files with details to be re-sized
			SELECT	DBName, LogicalName, physical_name, FileSize_MB, Volume, VolumeSize_MB
			FROM	#tempDBFiles as f
			WHERE	(@output4IdealScenario = 1 AND f.isToBeDeleted = 0 AND isExtraFile = 0)
				OR	 @output4IdealScenario = 0
		)
		,T_Volume_Details_01 AS
		(
			--	Find tempdb volume details
			SELECT	Volume, MAX(VolumeSize_MB) AS VolumeSize_MB, COUNT(*) AS FileCount
			FROM	T_Files_01
			GROUP BY Volume
		)
		,T_Volume_Details_02 AS
		(
			SELECT	Volume, VolumeSize_MB, FileCount
					/* CapacityThresholdSize_MB = Total Size to be given to Data/Log files of [tempdb] database
							if 	@tempDbMaxSizeThresholdInGB is set, then use it
							else (@tempDBMountPointPercent - space of Tempdb log files if log files is in same as data volume)
					*/
					,CapacityThresholdSize_MB = CASE WHEN @tempDbMaxSizeThresholdInGB IS NOT NULL
													THEN @tempDbMaxSizeThresholdInGB * 1024
													ELSE (	(@tempDBMountPointPercent * VolumeSize_MB)/100.00) - ISNULL( (SELECT ((SUM(size) * 8.00)/1024) as size_MB 
																															FROM sys.master_files AS mf 
																															WHERE mf.database_id = DB_ID('tempdb') AND mf.type_desc = 'LOG'
																															 AND mf.physical_name LIKE (v.Volume + '%') ),0)
													END
					--,CapacityThresholdSizePerFile_MB = CAST((((@tempDBMountPointPercent) * VolumeSize_MB)/100.0)/FileCount AS NUMERIC(20,2))
			FROM	T_Volume_Details_01 AS v
		)
		,T_Volume_Details AS
		(
			SELECT	Volume, VolumeSize_MB, FileCount, CapacityThresholdSize_MB
					,CapacityThresholdSizePerFile_MB = CAST((CapacityThresholdSize_MB)/FileCount AS NUMERIC(20,0))
			FROM	T_Volume_Details_02
		)
		INSERT @T_Files_ReSizeTempDB (TSQL_ResizeTempDB_Files)
		SELECT	'
	ALTER DATABASE [tempdb] MODIFY FILE ( NAME = N'''+f.LogicalName+''', SIZE = '+CAST(CapacityThresholdSizePerFile_MB AS VARCHAR(20))+'MB );
'
		FROM	T_Files_01 as f
		INNER JOIN
				T_Volume_Details AS v
			ON	v.Volume = f.Volume;

		SELECT @_loopCounter=MIN(ID), @_loopCounts=MAX(ID) FROM	@T_Files_ReSizeTempDB;
		WHILE @_loopCounter <= @_loopCounts
		BEGIN	-- Begin Block of Loop
			SELECT @_loopSQLText = '
--	Resize File: '+CAST(ID AS VARCHAR(5))+';'+TSQL_ResizeTempDB_Files FROM @T_Files_ReSizeTempDB as f WHERE f.ID = @_loopCounter;
			IF @_loopCounter = 1
				SET @_loopSQLText =	'

USE [master];
--	=====================================================================================================
--	TSQL Code to reset Initial Size for TempDB files.
' + @_loopSQLText;

			PRINT @_loopSQLText;

			SET @_loopSQLText = '';
			SET @_loopCounter = @_loopCounter + 1;
		END		-- End Block of Loop

		IF @verbose = 1
			PRINT	'/*	******************** End:	@expandTempDBSize = 1 *****************************/
';
	END -- End block of @expandTempDBSize = 1
	--	----------------------------------------------------------------------------
		--	End:	@expandTempDBSize = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@optimizeLogFiles = 1
	--	----------------------------------------------------------------------------
	IF	@optimizeLogFiles = 1
	BEGIN
		IF @verbose = 1
			PRINT	'
/*	******************** Begin:	@optimizeLogFiles = 1 *****************************/';

		

		IF @verbose = 1
			PRINT	'/*	******************** End:	@optimizeLogFiles = 1 *****************************/
';
	END -- End block of @optimizeLogFiles = 1
	--	----------------------------------------------------------------------------
		--	End:	@optimizeLogFiles = 1
	--	============================================================================

	--	============================================================================
		--	Begin:	@testAllOptions = 1
	--	----------------------------------------------------------------------------
	IF	@testAllOptions = 1
	BEGIN
		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] 
	*/';
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @help = 1
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @help = 1
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @getLogInfo = 1
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @getLogInfo = 1
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = ''E:\Data1\'' ,@oldVolume = ''E:\Data\''
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = 'E:\Data1\' ,@oldVolume = 'E:\Data\'
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictDataFileGrowth = 1 ,@oldVolume = ''E:\Data\''
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictDataFileGrowth = 1 ,@oldVolume = 'E:\Data\'
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = ''E:\Logs1\'' ,@oldVolume = ''E:\Logs\''
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = 'E:\Logs1\' ,@oldVolume = 'E:\Logs\'
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictLogFileGrowth = 1 ,@oldVolume = ''E:\Logs\''
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictLogFileGrowth = 1 ,@oldVolume = 'E:\Logs\'
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @unrestrictFileGrowth = 1, @oldVolume = ''E:\Data\''
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @unrestrictFileGrowth = 1, @oldVolume = 'E:\Data\'
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @generateCapacityException = 1, @oldVolume = ''E:\Data\''
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @generateCapacityException = 1, @oldVolume = 'E:\Data\'
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @UpdateMountPointSecurity = 1
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @UpdateMountPointSecurity = 1
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = ''E:\Data\'', @mountPointGrowthRestrictionPercent = 95
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = 'E:\Data\', @mountPointGrowthRestrictionPercent = 95
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = ''E:\Data\'', @mountPointGrowthRestrictionPercent = 70
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = 'E:\Data\', @mountPointGrowthRestrictionPercent = 70
			WAITFOR DELAY '00:01';

		PRINT	'/*	Executing 
					EXEC [dbo].[usp_AnalyzeSpaceCapacity] @expandTempDBSize = 1, @tempDBMountPointPercent = 89
	*/';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @expandTempDBSize = 1, @tempDBMountPointPercent = 89
			WAITFOR DELAY '00:01';

	END -- End block of @testAllOptions = 1
	--	----------------------------------------------------------------------------
		--	End:	@testAllOptions = 1
	--	============================================================================
	--	Sample Error
	--SELECT 1/0 as [Divide By Zero];

		IF @verbose = 1
			PRINT	'	This is END TRY point for outermost Try/Catch Block	';

	END TRY	-- Try Catch for executable blocks that may throw error
	BEGIN CATCH
		IF @verbose=1
			PRINT	'*** Inside OuterMost Catch Block ***';
	
		-- If we are inside Catch block, that means something went wrong.
		IF @_errorOccurred = 0
			SET @_errorOccurred = 1;

		/*	This is no longer needed
		--	If some select/update tsql statement failed, it will be called compilation error
		INSERT #ErrorMessages
			SELECT	CASE WHEN PATINDEX('%has free space more than%',ERROR_MESSAGE()) > 0
						THEN 'Reconsider Space Threshold'
						WHEN PATINDEX('%Kindly restrict the data/log files on @oldVolume before using @generateCapacityException option.%',ERROR_MESSAGE()) > 0
						THEN 'Files Growth Yet to be Restricted'
						WHEN ERROR_MESSAGE() = 'Volume configuration is not per standard. Kindly perform the activity manually.'
						THEN 'Not Supported'
						WHEN ERROR_MESSAGE() = 'Backup job is running. So kindly create/restrict files later.'
						THEN 'Backup Job is running.'
						WHEN CHARINDEX('@',ERROR_MESSAGE()) > 0
						THEN 'Improper Parameter'
						ELSE 'Compilation Error'
						END 
								AS ErrorCategory
					,NULL AS DBName 
					,NULL AS [FileName] 
					,ERROR_MESSAGE() AS ErrorDetails 
					,NULL AS TSQLCode;
		*/

		--	Make a FAIL entry for Sending message to end user
		INSERT @OutputMessages
			([Status], Category, DBName, [FileGroup], [FileName], MessageDetails, TSQLCode)
		SELECT	'Error' AS [Status],
				CASE WHEN PATINDEX('%has free space more than%',ERROR_MESSAGE()) > 0
					THEN 'Reconsider Space Threshold'
					WHEN PATINDEX('%Kindly restrict the data/log files on @oldVolume before using @generateCapacityException option.%',ERROR_MESSAGE()) > 0
					THEN 'Files Growth Yet to be Restricted'
					WHEN ERROR_MESSAGE() = 'Volume configuration is not per standard. Kindly perform the activity manually.'
						 OR ERROR_MESSAGE() LIKE '%PowerShell is not found on this server.'
					THEN 'Not Supported'
					WHEN ERROR_MESSAGE() = 'Backup job is running. So kindly create/restrict files later.'
					THEN 'Backup Job is running.'
					WHEN CHARINDEX('@',ERROR_MESSAGE()) > 0
					THEN 'Improper Parameter'
					ELSE 'Compilation Error'
					END 
						AS ErrorCategory
				,NULL AS DBName 
				,NULL AS [FileGroup]
				,NULL AS [FileName] 
				,ERROR_MESSAGE() AS ErrorDetails 
				,NULL AS TSQLCode;

	END CATCH

	--	set the cmdshell setting to initial state
	IF (@handleXPCmdShell = 1 AND @_configurationValue_CmdShell = 0)
	BEGIN
		--	enable cmdshell if it is otherwise
		IF @_configurationValue_CmdShell = 0
		BEGIN
			-- To allow advanced options to be changed.  
			EXEC sp_configure 'show advanced options', 1;  
			-- To update the currently configured value for advanced options.  
			RECONFIGURE; 
			-- To enable the feature.  
			EXEC sp_configure 'xp_cmdshell', 0;  
			-- To update the currently configured value for this feature.  
			RECONFIGURE;
		END
	END

	-- Return data from @OutputMessages table which contains Error Messages, Action Taken, and its result (pass/fail)
		-- Show this output only when @forceExecute = 1 or @_errorOccurred = 1
	IF ( @forceExecute = 1 
							AND (	@addDataFiles = 1 
								OR	@addLogFiles = 1	
								OR	@restrictDataFileGrowth = 1
								OR	@restrictLogFileGrowth = 1
								OR	@unrestrictFileGrowth = 1
								OR	@removeCapacityException = 1
								OR	@restrictMountPointGrowth = 1
								OR	@expandTempDBSize = 1
							)	 
		) OR (@_errorOccurred = 1)
	BEGIN
		IF @verbose=1
			PRINT	'Returing @OutputMessages table data';
		SELECT * FROM @OutputMessages;
	END

	RETURN @_errorOccurred; -- 1 = Error, 0 = Success

END -- End Procedure