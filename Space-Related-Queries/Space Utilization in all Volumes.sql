DECLARE @output TABLE (line varchar(255));
DECLARE @_powershellCMD VARCHAR(400);

IF OBJECT_ID('tempdb..#mountPointVolumes') IS NOT NULL
	DROP TABLE #mountPointVolumes;
CREATE TABLE #mountPointVolumes 
	( Volume VARCHAR(200), [capacity(MB)] DECIMAL(20,2), [freespace(MB)] DECIMAL(20,2) 
	,VolumeName VARCHAR(50), [capacity(GB)]  DECIMAL(20,2), [freespace(GB)]  DECIMAL(20,2), [freespace(%)]  DECIMAL(20,2) 
	);

--	Begin: Get Data & Log Mount Point Volumes
SET @_powershellCMD =  'powershell.exe -c "Get-WmiObject -ComputerName ' + QUOTENAME(@@servername,'''') + ' -Class Win32_Volume -Filter ''DriveType = 3'' | select name,capacity,freespace | foreach{$_.name+''|''+$_.capacity/1048576+''%''+$_.freespace/1048576+''*''}"';

-- Clear previous output
DELETE @output WHERE 1 = 1;

--inserting disk name, total space and free space value in to temporary table
INSERT @output
EXEC xp_cmdshell @_powershellCMD;

;WITH T_Volumes AS
(
	SELECT	RTRIM(LTRIM(SUBSTRING(line,1,CHARINDEX('|',line) -1))) as Volume
			,ROUND(CAST(RTRIM(LTRIM(SUBSTRING(line,CHARINDEX('|',line)+1,
			(CHARINDEX('%',line) -1)-CHARINDEX('|',line)) )) as Float),0) as 'capacity(MB)'
			,ROUND(CAST(RTRIM(LTRIM(SUBSTRING(line,CHARINDEX('%',line)+1,
			(CHARINDEX('*',line) -1)-CHARINDEX('%',line)) )) as Float),0) as 'freespace(MB)'
	FROM	@output
	WHERE line like '[A-Z][:]%'
)
INSERT INTO #mountPointVolumes
(Volume, [capacity(MB)], [freespace(MB)] ,VolumeName, [capacity(GB)], [freespace(GB)], [freespace(%)])
SELECT	Volume
		,[capacity(MB)]
		,[freespace(MB)]
		,REVERSE(SUBSTRING(REVERSE(v.Volume),2,CHARINDEX('\',REVERSE(v.Volume),2)-2)) as VolumeName
		,CAST(([capacity(MB)]/1024.0) AS DECIMAL(20,2)) AS [capacity(GB)]
		,CAST(([freespace(MB)]/1024.0) AS DECIMAL(20,2)) AS [freespace(GB)]
		,CAST(([freespace(MB)]*100.0)/[capacity(MB)] AS DECIMAL(20,2)) AS [freespace(%)]
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
	OR	v.Volume LIKE '[A-Z]:\mssql\'
	OR	v.Volume LIKE '[A-Z]:\mssql[0-9]\'
	OR	v.Volume LIKE '[A-Z]:\mssql[0-9][0-9]\'
	OR	v.Volume LIKE '[A-Z]:\tsmsql\'
	OR	v.Volume LIKE '[A-Z]:\tsmsql[0-9]\'
	OR	v.Volume LIKE '[A-Z]:\tsmsql[0-9][0-9]\'
	;

SELECT * FROM #mountPointVolumes V ORDER BY v.VolumeName;