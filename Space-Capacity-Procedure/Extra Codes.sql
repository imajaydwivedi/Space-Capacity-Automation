--EXEC [dbo].[usp_AnalyzeSpaceCapacity]	@addDataFiles = 1 
--										,@newVolume = 'E:\Data\' 
--										,@oldVolume = 'E:\MSSQL\' 
--										,@forceExecute = 1

-- To allow advanced options to be changed.  
EXEC sp_configure 'show advanced options', 1;  
GO  
-- To update the currently configured value for advanced options.  
RECONFIGURE;  
GO  
-- To enable the feature.  
EXEC sp_configure 'xp_cmdshell', 1;  
GO  
-- To update the currently configured value for this feature.  
RECONFIGURE;  
GO

@handleXPCmdShell
@_configurationValue_CmdShell

select value from sys.configurations as c
	where c.name = 'xp_cmdshell'