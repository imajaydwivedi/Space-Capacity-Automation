ln 2700 for --	Un-Restrict File Growth if file already exists on @newVolume

line 1100
SELECT * FROM #T_Files_Final WHERE isExistingOn_NewVolume = 1 AND isExisting_UnrestrictedGrowth_on_OtherVolume = 0)

,[isExisting_UnrestrictedGrowth_on_OtherVolume] = CASE WHEN EXISTS (
																					SELECT	mf2.*, NULL as [fileGroup]
																					FROM	sys.master_files mf2
																					WHERE	mf2.type_desc = mf1.type_desc
																						AND	mf2.database_id = mf1.database_id
																						AND mf2.data_space_id = mf1.data_space_id -- same filegroup
																						AND mf2.growth <> 0
																						AND LEFT(mf2.physical_name, CHARINDEX('\',mf2.physical_name,4)) IN (select Volume from @mountPointVolumes V WHERE V.Volume <> @oldVolume AND [freespace(%)] >= 20.0)
																				)
																THEN 1
																ELSE 0
																END


/*	Use code code to Get Volume by mf.physical_name	*/
OUTER APPLY
						(	SELECT	v2.Volume
							FROM  (	SELECT MAX(LEN(v.Volume)) AS Max_Volume_Length FROM @mountPointVolumes as v WHERE mf.physical_name LIKE (v.Volume+'%') ) as v1
							INNER JOIN
								  (	SELECT v.Volume FROM @mountPointVolumes as v WHERE mf.physical_name LIKE (v.Volume+'%') ) as v2
								ON	LEN(v2.Volume) = v1.Max_Volume_Length
						) as v

