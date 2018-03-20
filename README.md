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




