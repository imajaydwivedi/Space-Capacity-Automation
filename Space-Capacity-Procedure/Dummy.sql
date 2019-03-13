USE DBA;
--EXEC DBA..[usp_AnalyzeSpaceCapacity] @help = 1
--EXEC DBA..[usp_AnalyzeSpaceCapacity] @getLogInfo = 1,@verbose = 1
EXEC DBA..[usp_AnalyzeSpaceCapacity] @optimizeLogFiles = 1 ,@vlfCountThreshold = 100 
									--,@verbose = 1 
									,@DBs2Consider = 'VestaMusicProcessing' 
									,@forceExecute = 1

/*
MODIFY FILE failed for file "prism_log". At least one property per file must be specified.


  /* ******************************************************************************************************  TSQL Code to remove high VLF Count for [Prism] database.     Currently the log file 'prism_log' has 1261 VLFs which is more than @vlfCountThreshold (100), and also above the ideal VLF counts 32 (16 VLFs for every 8000 MB Log File Size)    Shrinking the log file 'prism_log' to minimum possible size, and    Trying to re-grow it to actual size of 16162 MB in chunks of 8000 mb    -- https://dba.stackexchange.com/a/180150/98923    -- https://sqlperformance.com/2013/02/system-configuration/transaction-log-configuration  * ******************************************************************************************************/   
*/