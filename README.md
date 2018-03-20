# Space-Capacity-Automation
![](images/general_functionalities.JPG)

### General Info
This automation has been designed to eliminate manual efforts on Space Capacity ESC tickets where DBA has to add new data or log files on new volume, and restrict data or log files on old volume. Apart from this, this procedure can be used for variety of tasks related to capacity management. 

For example, say, on server dbTest1774, a new data volume <b>E:\Data1\ </b> has been added. So, DBA has to add new data files on <b>@newVolume</b> (E:\Data1\) and restrict data files on <b>@oldVolume</b> (E:\Data\). This can be accomplished by below methods:-<br><br>
<i>EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = 'E:\Data1\' ,@oldVolume = 'E:\Data\';</i><br><br>
This generates TSQL code for adding data files on <b>@newVolume</b> for data files present on <b>@oldVolume</b> for each combination of database and filegroup.<br><br>
In case, we don’t want TSQL code generation, rather wish to execute it right away, we can execute procedure with @forceExecute parameter.<br><br>
<i>EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = 'E:\Data1\' ,@oldVolume = 'E:\Data\' ,@forceExecute = 1;</i>

Similarly the procedure [dbo].[usp_AnalyzeSpaceCapacity] can be used for multiple activities related to space capacity.

### Find Help (@help)
The parameter provides directions on how to use this procedure. It also presents 12 examples in it.
 
<i>exec dbo.[usp_AnalyzeSpaceCapacity] @help = 1</i>

Also, below are the parameters for procedure with default values:-<br>
![](images/@help_TableResult.JPG)

For best result, always take out help from procedure using @help parameter. Below are few examples in Messages tab:-


	NAME
		[dbo].[usp_AnalyzeSpaceCapacity]

	SYNOPSIS
		Analyze the Data Volume mount points for free space, database files, growth restriction and capacity exception.

	SYNTAX
		EXEC [dbo].[usp_AnalyzeSpaceCapacity	[ [@getInfo =] { 1 | 0 } ] [,@DBs2Consider = <comma separated database names>]
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
							@getVolumeSpaceConsumers = { 1 | 0}, @oldVolume = <drive_name>
							[;]

		<drive_name> :: { 'E:\Data\' | 'E:\Data01' | 'E:\Data2' | ... }

		--------------------------------------- EXAMPLE 1 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity];
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] ,@DBs2Consider = 'unet, Test1Db, MirrorTestDB';

		This procedure returns general information like Data volumes, data files on those data volumes, Free space on data volumes, Growth settings of dbs etc.

		--------------------------------------- EXAMPLE 2 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @getLogInfo = 1
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @getLogInfo = 1 ,@DBs2Consider = 'unet, Test1Db, MirrorTestDB'

		This procedure returns general information like Log volumes, Log files on those log volumes, Free space on log volumes, Growth settings of dbs etc.
	
		--------------------------------------- EXAMPLE 3 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @help = 1

		This returns help for procedure usp_AnalyzeSpaceCapacity along with definitions for each parameter.

		--------------------------------------- EXAMPLE 4 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = 'E:\Data1\' ,@oldVolume = 'E:\Data\';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = 'E:\Data1\' ,@oldVolume = 'E:\Data\' ,@DBs2Consider = 'unet, Test1Db, MirrorTestDB';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addDataFiles = 1 ,@newVolume = 'E:\Data1\' ,@oldVolume = 'E:\Data\' ,@forceExecute = 1;

		This generates TSQL Code for add secondary data files on @newVolume for each file of @oldVolume per FileGroup.

		--------------------------------------- EXAMPLE 5 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictDataFileGrowth = 1 ,@oldVolume = 'E:\Data\';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictDataFileGrowth = 1 ,@oldVolume = 'E:\Data\' ,@DBs2Consider = 'unet, Test1Db, MirrorTestDB';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictDataFileGrowth = 1 ,@oldVolume = 'E:\Data\' ,@forceExecute = 1

		This generates TSQL Code to restrict growth of secondary data files on @oldVolume if corresponding Data files exists on @newVolume.

		--------------------------------------- EXAMPLE 6 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = 'E:\Logs1\' ,@oldVolume = 'E:\Logs\'
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = 'E:\Logs1\' ,@oldVolume = 'E:\Logs\' ,@DBs2Consider = 'unet, Test1Db, MirrorTestDB';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = 'E:\Logs1\' ,@oldVolume = 'E:\Logs\' ,@forceExecute = 1
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = 'E:\Logs1\' ,@oldVolume = 'E:\Logs\' ,@allowMultiVolumeUnrestrictedFiles = 1
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @addLogFiles = 1 ,@newVolume = 'E:\Logs1\' ,@oldVolume = 'E:\Logs\' ,@allowMultiVolumeUnrestrictedFiles = 1 ,@forceExecute = 1

		This generates TSQL Code for add log files on @newVolume for each database on @oldVolume.

		--------------------------------------- EXAMPLE 7 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictLogFileGrowth = 1 ,@oldVolume = 'E:\Logs\'
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictLogFileGrowth = 1 ,@oldVolume = 'E:\Logs\' ,@DBs2Consider = 'unet, Test1Db, MirrorTestDB';
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictLogFileGrowth = 1 ,@oldVolume = 'E:\Logs\',@forceExecute = 1

		This generates TSQL Code to restrict growth of log files on @oldVolume if corresponding log files exists on @newVolume.
	
		--------------------------------------- EXAMPLE 8 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @unrestrictFileGrowth = 1, @oldVolume = 'E:\Data\'
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @unrestrictFileGrowth = 1, @oldVolume = 'E:\Data\' ,@DBs2Consider = 'unet, Test1Db, MirrorTestDB';

		This generates TSQL Code for remove Data File growth Restriction for files on @oldVolume.

		--------------------------------------- EXAMPLE 9 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @generateCapacityException = 1, @oldVolume = 'E:\Data\'

		This generates TSQL Code for adding Space Capacity Exception for @oldVolume.

		--------------------------------------- EXAMPLE 10 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @UpdateMountPointSecurity = 1

		This will generate Powershell command to provide Full Access on @newVolume for SQL Server service accounts.

		--------------------------------------- EXAMPLE 11 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = 'E:\Data\'
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = 'E:\Data\', @mountPointGrowthRestrictionPercent = 95
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = 'E:\Data\', @mountPointGrowthRestrictionPercent = 95, @DBs2Consider = 'CHSDB_Audit,CHSDBArchive'
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = 'E:\Logs2\'
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @restrictMountPointGrowth = 1, @oldVolume = 'E:\Logs2\', @mountPointGrowthRestrictionPercent = 70

		This will generate TSQL Code to restrict all the files on @oldVolume such that total files size consumes upto 79% of the mount point volume.

		--------------------------------------- EXAMPLE 12 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @expandTempDBSize = 1
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @expandTempDBSize = 1, @output4IdealScenario = 1
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @expandTempDBSize = 1, @tempDBMountPointPercent = 89
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @expandTempDBSize = 1, @tempDbMaxSizeThresholdInGB

		This generates TSQL code for expanding tempdb data files upto @tempDBMountPointPercent % of total tempdb volume capacity.
		When @output4IdealScenario set to 1, will generate TSQL code to add/remove data files based on the number Logical cores on server upto 8, and delete extra data files created on non-tempdb volumes, and re-size TempdDB data files to occupy 89% of mount point volume.

		--------------------------------------- EXAMPLE 13 ----------------------------------------------
		EXEC [dbo].[usp_AnalyzeSpaceCapacity] @optimizeLogFiles = 1

		This generates TSQL code to re-size log files upto current size with objective to reduce high VLF Counts
	


