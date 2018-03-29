--	tul1cipmbdb2 
use tempdb
go

--EXEC tempdb..[usp_AnalyzeSpaceCapacity] @volumeInfo = 1
--EXEC tempdb..[usp_AnalyzeSpaceCapacity] @getVolumeSpaceConsumers = 1, @oldVolume = 'F:\' --,@verbose = 1
--go

--DECLARE @pathID INT = 32;
;WITH t_childfolders as
(	SELECT f.PathID, f.PathID as ReferencePathID -- get top most path details
	FROM tempdb..VolumeFolders as f --where f.PathID = @pathID		-- select * from tempdb..VolumeFolders as f where f.ParentPathID = 32
	--
	UNION ALL
	--
	SELECT fd.PathID, b.ReferencePathID
	FROM t_childfolders as b
	INNER JOIN
		tempdb..VolumeFolders as fd -- get all folders directly under base path. Say 1 base path x 10 direct child folders
		ON fd.ParentPathID = b.PathID
)
--SELECT * FROM t_childfolders AS c order by c.PathID
--SELECT * FROM tempdb..VolumeFolders as f where f.PathID IN (SELECT c.PathID FROM t_childfolders AS c)
SELECT fd.ReferencePathID, SUM(SizeBytes) AS SizeBytes
		,[Size] = (CASE	WHEN	SUM(SizeBytes)/1024.0/1024/1024 > 1.0 
						THEN	CAST(CAST(SUM(SizeBytes)/1024.0/1024/1024 AS DECIMAL(20,2)) AS VARCHAR(21)) + ' gb'
						WHEN	SUM(SizeBytes)/1024.0/1024 > 1.0 
						THEN	CAST(CAST(SUM(SizeBytes)/1024.0/1024 AS DECIMAL(20,2)) AS VARCHAR(21)) + ' mb'
						WHEN	SUM(SizeBytes)/1024.0 > 1.0 
						THEN	CAST(CAST(SUM(SizeBytes)/1024.0 AS DECIMAL(20,2)) AS VARCHAR(21)) + ' kb'
						ELSE	CAST(CAST(SUM(SizeBytes) AS DECIMAL(20,2)) AS VARCHAR(21)) + ' bytes'
						END)
from tempdb..VolumeFiles as fl
inner join t_childfolders as fd
on fd.PathID = fl.ParentPathID
GROUP BY fd.ReferencePathID
--ORDER BY ReferencePathID;
go

SELECT * FROM tempdb..VolumeFolders 
SELECT * FROM tempdb..VolumeFiles 

xp_dirtree 'F:\mssqldata\backup\TUL1CIPMBDB2\VaraLogix\FULL',1,1

restore headeronly from disk = 'F:\mssqldata\backup\TUL1CIPMBDB2\VaraLogix\FULL\TUL1CIPMBDB2_VaraLogix_FULL_20180328_000001.bak'
restore headeronly from disk = 'F:\mssqldata\backup\TUL1CIPMBDB2\VaraLogix\FULL\TUL1CIPMBDB2_VaraLogix_FULL_20180329_000001.bak'