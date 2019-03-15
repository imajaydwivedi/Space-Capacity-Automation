USE DBA;
--EXEC DBA..[usp_AnalyzeSpaceCapacity] @help = 1

EXEC DBA..[usp_AnalyzeSpaceCapacity] @optimizeLogFiles = 1 ,@vlfCountThreshold = 100 
									--,@verbose = 1 
									,@DBs2Consider = 'AMGMusicAuthority,AMGMusicMore,AMGMusic,EntryAggregation,VestaMusicProcessing' 
									--,@forceExecute = 1
									