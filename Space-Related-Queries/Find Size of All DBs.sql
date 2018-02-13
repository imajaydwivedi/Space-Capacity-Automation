/*	Created By:		Ajay Dwivedi
	Purpose:		Get Space Utilization of All DB Files.
					This considers even non-accessible DBs
*/

SELECT DB_NAME(mf.database_id) AS DbName,
					name AS FileName
					,max_size = case	when max_size = -1 then '-1'
										when ((max_size * 8.0) / 1024 / 1024) >= 1 then cast(cast(((max_size * 8.0) / 1024 / 1024) as numeric(20,2)) as varchar(40)) + ' gb'
										when ((max_size * 8.0) / 1024) >= 1 then cast(((max_size * 8.0) / 1024) as varchar(40)) + ' mb'
										else cast((max_size * 8.0) as varchar(40)) + ' kb'
										end
					,growth as [growth_Pages],
					physical_name,
					CAST(size/128.0 AS NUMERIC(20,2)) AS CurrentSizeMB,
					CurrentSize = case	when size = 0 then '0'
										when (size * 8 / 1024 / 1024) >= 1 then cast((size * 8 / 1024 / 1024) as varchar(20)) + ' gb'
										when (size * 8 / 1024) >= 1 then cast((size * 8 / 1024) as varchar(20)) + ' mb'
										else cast((size * 8) as varchar(20)) + ' kb'
										end,
					type_desc,
					is_percent_growth
FROM sys.master_files as mf
ORDER BY mf.size DESC