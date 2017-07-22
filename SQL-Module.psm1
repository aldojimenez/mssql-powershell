Function Get-MyModule
{
  Param([string]$name)
  if(-not(Get-Module -name $name))
  {
    if(Get-Module -ListAvailable |
    Where-Object { $_.name -eq $name })
    {
      Import-Module -Name $name
      $true
    } #end if module available then import
    else
    {
      $assemblylist =
        "Microsoft.SqlServer.Management.Common",
        "Microsoft.SqlServer.Smo",
        "Microsoft.SqlServer.Dmf ",
        "Microsoft.SqlServer.Instapi ",
        "Microsoft.SqlServer.SqlWmiManagement ",
        "Microsoft.SqlServer.ConnectionInfo ",
        "Microsoft.SqlServer.SmoExtended ",
        "Microsoft.SqlServer.SqlTDiagM ",
        "Microsoft.SqlServer.SString ",
        "Microsoft.SqlServer.Management.RegisteredServers ",
        "Microsoft.SqlServer.Management.Sdk.Sfc ",
        "Microsoft.SqlServer.SqlEnum ",
        "Microsoft.SqlServer.RegSvrEnum ",
        "Microsoft.SqlServer.WmiEnum ",
        "Microsoft.SqlServer.ServiceBrokerEnum ",
        "Microsoft.SqlServer.ConnectionInfoExtended ",
        "Microsoft.SqlServer.Management.Collector ",
        "Microsoft.SqlServer.Management.CollectorEnum",
        "Microsoft.SqlServer.Management.Dac",
        "Microsoft.SqlServer.Management.DacEnum",
        "Microsoft.SqlServer.Management.Utility"

      foreach ($asm in $assemblylist)
      {
          $asm = [Reflection.Assembly]::LoadWithPartialName($asm)
      }
      $false
      } #module not available
  } # end if not module
  else { $true } #module already loaded
} #end function get-MyModule

$isSqplsLoaded = Get-MyModule sqlps

Function Check-ServerInstance
{
    param(
        [parameter(Mandatory=$true)]
        [AllowNull()]
        [AllowEmptyString()]
        [string]$ServerInstance)

    if($ServerInstance)
    {
        if($ServerInstance.Contains("\"))
        {
            Write-Host "$ServerInstance contains Backslash"
            return $ServerInstance
        }
        else
        {
            $ServerInstance += "\Default"
        }
    }
    else
    {
        $ServerInstance = "$env:computername\Default"
    }
    return $ServerInstance
}

function Get-Database
{
[CmdletBinding()]
  param (
      [parameter(Mandatory=$false, ValueFromPipelineByPropertyName=$true)]
      [string]$ServerInstance = $env:computername,

      [parameter(Mandatory=$false, ValueFromPipelineByPropertyName=$true)]
      [string]$Database
      )
    Write-Debug "Creating server object"
    $srv = New-Object 'Microsoft.SqlServer.Management.Smo.Server' $Server

    Write-Debug "Getting Databases"

    if($Database)
    {
        Write-Debug "Database filter specified"
        $databases = $srv.Databases | Select @{Label="Database";Expression={($_.Name)}}, Collation, @{Label="ServerName";Expression={($_.Parent.ComputerNamePhysicalNetBIOS)}}, Status, Size, PrimaryFilePath, Owner, LogFiles | Where {$_.Database -Like $Database}
    }
    else
    {
        $databases = $srv.Databases | Select @{Label="Database";Expression={($_.Name)}}, Collation, @{Label="ServerName";Expression={($_.Parent.ComputerNamePhysicalNetBIOS)}}, Status, Size, PrimaryFilePath, Owner, LogFiles
    }

    foreach ($db in $databases)
    {
      $database = New-Object –TypeNamePSObject
      $database | Add-Member –MemberTypeNoteProperty –Name Database –Value $db.Database
      $database | Add-Member –MemberTypeNoteProperty –Name Collation –Value $db.Collation
      $database | Add-Member –MemberTypeNoteProperty –Name ServerName –Value $db.ServerName
      $database | Add-Member –MemberTypeNoteProperty –Name Status –Value $db.Status
      $database | Add-Member –MemberTypeNoteProperty –Name Size –Value $db.Size
      $database | Add-Member –MemberTypeNoteProperty –Name PrimaryFilePath –Value $db.PrimaryFilePath
      $database | Add-Member –MemberTypeNoteProperty –Name Owner –Value $db.Owner
      $database | Add-Member –MemberTypeNoteProperty –Name LogFiles –Value $db.LogFiles

      Write-Output $database
    }
}

Function Backup-Database-SMO
{
  [CmdletBinding()]
  param (
      [parameter(Mandatory=$false)]
      [string]$ServerInstance = $env:computername,

      [parameter(Mandatory=$true)]
      [string]$BackupFile,

      [parameter(Mandatory=$false, ValueFromPipeline=$true)]
      [string]$Database,

      [parameter(Mandatory=$false)]
      [switch]$CopyOnly=$false,

      [parameter(Mandatory=$false)]
      [switch]$WhatIf
    )

      $dbname = $Database
      $server.ConnectionContext.StatementTimeout = 0
      $backup = New-Object "Microsoft.SqlServer.Management.Smo.Backup"
      $backup.Action = "Database"
      $backup.CopyOnly = $CopyOnly
      $device = New-Object "Microsoft.SqlServer.Management.Smo.BackupDeviceItem"
      $device.DeviceType = "File"
      $device.Name = $backupfile
      $backup.Devices.Add($device)
      $backup.Database = $dbname

      $percent = [Microsoft.SqlServer.Management.Smo.PercentCompleteEventHandler] {
        Write-Progress -id 1 -activity "Backing up database $dbname to $backupfile" -percentcomplete $_.Percent -status ([System.String]::Format("Progress: {0} %", $_.Percent))
      }
      $backup.add_PercentComplete($percent)
      $backup.add_Complete($complete)

      Write-Progress -id 1 -activity "Backing up database $dbname to $backupfile" -percentcomplete 0 -status ([System.String]::Format("Progress: {0} %", 0))
      Write-Output "Backing up $dbname"

      try
      {
        $backup.SqlBackup($server)
        Write-Progress -id 1 -activity "Backing up database $dbname to $backupfile" -status "Complete" -Completed
        Write-Output "Backup succeeded"
        return $true
      }
      catch
      {
        Write-Progress -id 1 -activity "Backup" -status "Failed" -completed
        Write-Host $_
        return $false
      }
}

Function Backup-Databases
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$false)]
        [string]$ServerInstance = $env:computername,

        [parameter(Mandatory=$true)]
        [string]$BackupDirectory,

        [parameter(Mandatory=$false, ValueFromPipelineByPropertyName=$true)]
        [string[]]$Database,

        [parameter(Mandatory=$false)]
        [string[]]$Exclude,

        [parameter(Mandatory=$false)]
        [switch]$CopyOnly,

        [parameter(Mandatory=$false)]
        [switch]$WhatIf
    )

    $Serverpath = Check-ServerInstance($ServerInstance)

    if($Database -and $Exclude)
    {
        Write-Warning "Cannot specify both Database and Exclude parameters"
    }
    else
    {

        if(!(Test-Path $BackupDirectory))
        {
            Write-Output "$BackupDirectory path does not exist"
        }
        else
        {
            # TODO I believe this is not used so commented out to double check
            #Write-Output "sqlserver:\sql\$Serverpath\Databases\"
            #Set-Location "sqlserver:\sql\$Serverpath\Databases\"

            $databases = ls

            foreach ($db in $databases)
            {
                # set database name
                $dbname = $db.Name
                if($Database)
                {
                    if($Database.Contains($dbname))
                    {
                        Write-Output "Including Databse: $dbname"
                    }
                    else
                    {
                        Write-Output "$Database not in include list - ignoring"
                        continue
                    }
                }
                else
                {
                    if($Exclude)
                    {
                        if($Exclude.Contains($dbname))
                        {
                            Write-Output "Excluding Database: $dbname"
                            continue
                        }
                    }
                }

                cd $BackupDirectory
                

                # location of backup subfolder
                $dbpath = "$BackupDirectory\$dbname"

                # check if folder already exists, if not then create it
                if (!(Test-Path $dbpath))
                {
                    New-Item -ItemType Directory -Path $dbpath
                }
                else
                {
                    Write-Output "$dbpath already exists. Not creating it."
                }

                # set date format for naming
                $dt = get-date -format yyyyMMddHHmmss

                # set backup file location and name
                $backupfile = "$dbpath\$($dbname)_db_$($dt).bak"

                # check whether to use new cmdlet or SMO
                if($isSqplsLoaded)
                {
                  #Backup the database
                  Backup-SqlDatabase -ServerInstance $ServerInstance -Database $dbname -BackupFile $backupfile -CopyOnly -WhatIf
                }
                else
                {
                  Backup-Database-SMO
                }
            }
        }
    }
}

Function Restore-SqlDatabase-SMO
		{
			[CmdletBinding()]
			param (
				[string]$ServerInstance,
				[string]$Database,
				[string[]]$Backupfile,
        [object]$newdbfileloc,
        [object]$newlogfileloc,
        [switch]$StandBy=$false,
        [switch]$WhatIf
			)

      # Create sql server object
      $server = New-Object ("Microsoft.SqlServer.Management.Smo.Server") $ServerInstance

      $dbname = $database

      # create restore object
			$servername = $server.name
			$server.ConnectionContext.StatementTimeout = 0
			$restore = New-Object Microsoft.SqlServer.Management.Smo.Restore

      if($Backupfile -is [system.array])
      {
        Write-Host "Backupfile is an array, the length is " + $Backupfile.length
        if($Backupfile.length -gt 1)
        {
          Write-Warning "Restore failed for databse [$dbname]: Cannot accept multiple files for SMO Restore"
          return $false;
        }
      }

      if($WhatIf)
      {
        Write-Output "WhatIf: Restoring $dbname to $servername - NoRecovery: $StandBy"
        return $true
      }

      # add relocate files
			$null = $restore.RelocateFiles.Add($newdbfileloc)
      $null = $restore.RelocateFiles.Add($newlogfileloc)

			Write-Output "Restoring $dbname to $servername"

			try
			{

				$percent = [Microsoft.SqlServer.Management.Smo.PercentCompleteEventHandler] {
					Write-Progress -id 1 -activity "Restoring $dbname to $servername" -percentcomplete $_.Percent -status ([System.String]::Format("Progress: {0} %", $_.Percent))
				}
                #$server.KillAllProcesses($dbname)

                Write-Host "Backupfile is: $backupfile"

				$restore.add_PercentComplete($percent)
				$restore.PercentCompleteNotification = 1
				$restore.add_Complete($complete)
				$restore.ReplaceDatabase = $true
				$restore.Database = $dbname
				$restore.Action = "Database"
				$restore.NoRecovery = $StandBy
                #$restore.FileNumber = 1
				$device = New-Object -TypeName Microsoft.SqlServer.Management.Smo.BackupDeviceItem
				$device.name = $backupfile
				$device.devicetype = "File"
				$restore.Devices.Add($device)

				Write-Progress -id 1 -activity "Restoring $dbname to $servername" -percentcomplete 0 -status ([System.String]::Format("Progress: {0} %", 0))
				$restore.sqlrestore($server)
				Write-Progress -id 1 -activity "Restoring $dbname to $servername" -status "Complete" -Completed


				return $true
			}
			catch
			{
				Write-Warning "Restore failed: $($_.Exception.InnerException.Message)"
				Write-Host $_
				return $false
			}
		}

 Function Restore-Database
 {
    param
    (
        [parameter(Mandatory=$false)]
        [string]$ServerInstance = $env:computername,

        [parameter(Mandatory=$true)]
        [string]$Database,

        [parameter(Mandatory=$true)]
        [string[]]$BackupFile,

        [parameter(Mandatory=$true)]
        [string]$DataFileLocation,

        [parameter(Mandatory=$true)]
        [string]$LogFileLocation,

        [parameter(Mandatory=$false)]
        [switch]$StandBy,

        [parameter(Mandatory=$false)]
        [switch]$WhatIf
    )

    Write-Output "PS Bound Parameters for Restore-Database are:"
    Write-Output $psBoundParameters

    $Serverpath = Check-ServerInstance($ServerInstance)

    if($DataFileLocation -and $LogFileLocation)
    {
        $newlogfileloc = new-object('Microsoft.SqlServer.Management.Smo.RelocateFile') (($database + '_log'), ($LogFileLocation + '\' + $database +'.ldf'))
        $newdbfileloc = new-object('Microsoft.SqlServer.Management.Smo.RelocateFile') ($database , ($DataFileLocation + '\' + $database +'.mdf'))

        Write-Host "CREATE: Relocate File information: "
        Write-Host $newdbfileloc
        Write-Host $newlogfileloc
        Write-Host ""

        $Serverpath = "sqlserver:\sql\$Serverpath\Databases\"
        Write-Host $Serverpath
        #Set-Location "sqlserver:\sql\$Serverpath\Databases\"

        if($isSqplsLoaded)
        {
          Restore-SqlDatabase -Database $dbname -BackupFile $dbfile -NoRecovery -RelocateFile @( $newdbfileloc, $newlogfileloc) -Path $Serverpath -StandbyFile "$LogFileLocation\$Database.trn" -WhatIf
        }
        else
        {
          Write-Output "SQLPS is not loaded, will execute via SMO"
          $RestoreArgs = @{}
          $RestoreArgs.add("ServerInstance", $ServerInstance)
          $RestoreArgs.add("newlogfileloc",$newlogfileloc)
          $RestoreArgs.add("newdbfileloc",$newdbfileloc)
          $RestoreArgs.add("Database", $dbname)
          $RestoreArgs.add("WhatIf", $WhatIf)
          $RestoreArgs.add("StandBy", $StandBy)

          Restore-SqlDatabase-SMO @RestoreArgs -BackupFile $BackupFile
        }
    }
    else
    {
        Write-Output "Need to specify both Log and Data file locations"
    }
 }

 Function Restore-DatabaseBackups
 {
    param
    (
        [parameter(Mandatory=$false)]
        [string]$ServerInstance = $env:computername,

        [parameter(Mandatory=$true)]
        [string]$BackupDirectory,

        [parameter(Mandatory=$true)]
        [string]$LogFileLocation,

        [parameter(Mandatory=$true)]
        [string]$DataFileLocation,

        [parameter(Mandatory=$false)]
        [string[]]$Include,

        [parameter(Mandatory=$false)]
        [string[]]$Exclude,

        [parameter(Mandatory=$false)]
        [switch]$StandBy,

        [parameter(Mandatory=$false)]
        [switch]$WhatIf
    )

    Write-Output "SQLPS is loaded: $isSqplsLoaded"

    # Add server instance to bound parameters array
    if(!$PSBoundParameters.ContainsKey('ServerInstance'))
    {
      $PSBoundParameters.ServerInstance = $ServerInstance
    }

    Write-Output "PS Bound Parameters for Restore-DatabaseBackups are:"
    Write-Output $psBoundParameters
    Write-Output ""

    $Serverpath = Check-ServerInstance($ServerInstance)

    Write-Host "Server instance is $ServerInstance"


    if($Database -and $Exclude)
    {
        Write-Output "Cannot specify both Database and Exclude parameters"
    }
    else
    {
        cd c:\

        if(!(Test-Path $BackupDirectory))
        {
            Write-Output "$BackupDirectory path does not exist"
        }
        else
        {
            $backups = ls $BackupDirectory

            foreach ($backup in $backups)
            {
                $dbname= $backup.Name;

                Write-Host "Database name is: $dbname"

                if($Include)
                {
                    if($Include.Contains($dbname))
                    {
                        Write-Output "Including Databse: $dbname"
                        # $dbfile = ls $backup.FullName -filter '*.bak';
                    }
                    else
                    {
                        Write-Output "$Database not in include list - ignoring"
                        continue
                    }
                }
                else
                {
                    if($Exclude)
                    {
                        if($Exclude.Contains($dbname))
                        {
                            Write-Output "Excluding Database: $dbname"
                            continue
                        }
                    }
                }
                $dbfile = ls $backup.FullName -filter '*.bak' | Foreach-Object {$_.Fullname}

                if($dbfile -is [System.array])
                {
                    Write-Host "Found multiple backup files, treating it as one set"
                    foreach ($file in $dbfile)
                    {
                      Write-Host $file
                    }
                }
                else
                {
                    Write-Host "DBFile is a single file"
                }
                Write-Host "DBFile currently is: $dbfile"

                # use splatting to pass parameters
                $RestoreArgs = @{}
                $RestoreArgs.add("LogFileLocation",$LogFileLocation)
                $RestoreArgs.add("DataFileLocation",$DataFileLocation)
                $RestoreArgs.add("Database", $dbname)
                $RestoreArgs.add("BackupFile", $dbfile)
                $RestoreArgs.add("WhatIf", $WhatIf)
                $RestoreArgs.add("StandBy", $StandBy)

                # Call the restore database function
                Restore-Database @RestoreArgs
            }
        }
    }
 }

Function Configure-SecondaryLogShipping
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$false)]
        [AllowNull()]
        [AllowEmptyString()]
        [string]$ServerInstance,

        [parameter(Mandatory=$true)]
        [string]$LogParentDir,

        [parameter(Mandatory=$false, ValueFromPipeline=$true)]
        [string[]]$Database,

        [parameter(Mandatory=$false)]
        [string[]]$Exclude,

        [parameter(Mandatory=$false)]
        [int]$RetentionPeriod = 1440,

        [parameter(Mandatory=$false)]
        [switch]$JobsEnabled,

        [parameter(Mandatory=$false)]
        [switch]$WhatIf
    )

    $Serverpath = Check-ServerInstance($ServerInstance)
    if(!$ServerInstance)
    {
        $ServerInstance = $env:computername
    }

    if($Database -and $Exclude)
    {
        Write-Output "Cannot specify both Database and Exclude parameters"
    }
    else
    {

        if(!(Test-Path $LogParentDir))
        {
            Write-Output "$LogParentDir path does not exist"
        }
        else
        {
            Write-Output "Server Path is: sqlserver:\sql\$Serverpath\Databases\"
            Set-Location "sqlserver:\sql\$Serverpath\Databases\"

            $databases = ls

            foreach ($db in $databases)
            {
                if($Database)
                {
                    $dbname = $db.Name

                    if($Database.Contains($dbname))
                    {
                        Write-Output "Including Databse: $dbname"
                    }
                    else
                    {
                        Write-Output "$dbname not in include list - ignoring"
                        continue
                    }
                }
                else
                {
                    $dbname = $db.Name

                    if($Exclude)
                    {
                        if($Exclude.Contains($dbname))
                        {
                            Write-Output "Excluding Database: $dbname"
                            continue
                        }
                    }
                }

                cd $LogParentDir

                $dbpath = "$LogParentDir\$dbname"
                if (!(Test-Path $dbpath))
                {
                    Write-Output "ERROR - $dbpath directory does not exist! Ignoring $dbname"
                    continue
                }
                else
                {
                    Write-Output "$dbpath exists, continuing"
                }

                $exec_secondary_database = @"
DECLARE @LS_Secondary__CopyJobId AS uniqueidentifier
DECLARE @LS_Secondary__RestoreJobId AS uniqueidentifier
DECLARE @LS_Secondary__SecondaryId  AS uniqueidentifier
DECLARE @LS_Secondary__ScheduleId  AS uniqueidentifier

EXEC master.dbo.sp_add_log_shipping_secondary_primary
@primary_server = N'RIV10DATA'
,@primary_database = N'$dbname'
,@backup_source_directory = N'E:\Temp\Attenda\'
,@backup_destination_directory = N'$LogParentDir\$dbname'
,@copy_job_name = N'LS_Copy_$dbname'
,@restore_job_name = N'LS_Restore_$dbname'
,@file_retention_period = $RetentionPeriod
,@copy_job_id = @LS_Secondary__CopyJobId OUTPUT
,@restore_job_id = @LS_Secondary__RestoreJobId OUTPUT
,@secondary_id = @LS_Secondary__SecondaryId OUTPUT ;
GO

exec master.dbo.sp_add_log_shipping_secondary_database
 @secondary_database = '$dbname',
 @primary_server = 'RIV10DATA',
 @primary_database = '$dbname',
 @restore_delay = 0,
 @restore_all = 1,
 @restore_mode = 1,
 @disconnect_users = 1,
 @restore_threshold = 120,
 @threshold_alert_enabled = 1
GO

EXEC msdb.dbo.sp_update_jobstep
    @job_name = N'LS_Restore_$dbname',
    @step_id = 1,
	@output_file_name = N'$dbpath\Restore_$dbname.log',
	@flags = 2
GO
"@

    $exec_enable_job = @"
EXEC msdb.dbo.sp_update_job
    @job_name = N'LS_Restore_$dbname',
    @enabled = 1
GO
"@

    $exec_schedule = @"
EXEC msdb.dbo.sp_add_schedule @schedule_name=N'Restore_$dbname',
		@enabled=1,
		@freq_type=4,
		@freq_interval=1,
		@freq_subday_type=4,
		@freq_subday_interval=15,
		@freq_relative_interval=0,
		@freq_recurrence_factor=1,
		@active_start_date=20160713,
		@active_end_date=99991231,
		@active_start_time=0,
		@active_end_time=235959
GO
"@

                $exec_attach_schedule = @"
EXEC msdb.dbo.sp_attach_schedule
@job_name = N'LS_Restore_$dbname',
@schedule_name = N'Restore_$dbname'
GO
"@

                if($WhatIf)
                {
                    Write-Output "Secondary Database Configuration is:"
                    Write-Output $exec_secondary_database

                    Write-Output ""
                    Write-Output "Create schedule script"
                    Write-Output $exec_schedule

                    Write-Output ""
                    Write-Output "Attach schedule script"
                    Write-Output $exec_attach_schedule

                    if($JobsEnabled)
                    {
                        Write-Output ""
                        Write-Output "Enabling job"
                        Write-Output $exec_enable_job
                    }
                }
                else
                {
                    Write-Output ""
                    Write-Output "Executing secondary log shipping configuration"
                    Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $exec_secondary_database

                    if($JobsEnabled)
                    {
                        Write-Output ""
                        Write-Output "Enabling job"
                        Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $exec_enable_job
                    }

                    Write-Output ""
                    Write-Output "Creating Schedule for Jobs"
                    Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $exec_schedule

                    Write-Output ""
                    Write-Output "Attaching schedule to jobs"
                    Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $exec_attach_schedule
                }
            }
        }
    }
}

Function Get-SQLAgentJobs
{
    param (
        [parameter(Mandatory=$false)]
        [AllowNull()]
        [AllowEmptyString()]
        [string]$ServerInstance,

        [parameter(Mandatory=$true)]
        [string]$JobName,

        [parameter(Mandatory=$false)]
        [switch]$WhatIf
        )

    if(!$ServerInstance)
    {
        $ServerInstance = $env:computername
    }

    $exec_get_jobs_query = "select name from msdb.dbo.sysjobs where name LIKE '$JobName'"

    Write-Host "Job Query"
    Write-Host $exec_get_jobs_query

    $jobs = Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $exec_get_jobs_query

    foreach ($job in $jobs)
    {
        $jobname = $job.Name
        Write-Host "Found job: $jobname"
    }

    if($jobs)
    {
        return $jobs
    }
    else
    {
        return $null
    }

}

Function Update-SQLAgentJobs
{
    param (
        [parameter(Mandatory=$false)]
        [AllowNull()]
        [AllowEmptyString()]
        [string]$ServerInstance,

        [parameter(Mandatory=$true)]
        [string]$JobName,

        [parameter(Mandatory=$true)]
        [boolean]$Enable,

        [parameter(Mandatory=$false)]
        [switch]$WhatIf
        )

    if(!$ServerInstance)
    {
        $ServerInstance = $env:computername
    }
    $enabled = 0
    $enable_message = ""

    if($Enable)
    {
        $enable_message = "Enabling"
        $enabled = 1
    }
    else
    {
        $enable_message = "Disabling"
        $enabled = 0
    }

    $jobs = Get-SQLAgentJobs -ServerInstance $ServerInstance -JobName $JobName

    foreach($job in $jobs)
    {
        $this_jobname = $job.Name
        $exec_enable_job = @"
EXEC msdb.dbo.sp_update_job
    @job_name = N'$this_jobname',
    @enabled = $enabled
GO
"@

        if($WhatIf)
        {
            Write-Output ""
            Write-Output "$enable_message the $this_jobname SQL Job"
            Write-Output $exec_enable_job
        }
        else
        {
            Write-Output ""
            Write-Output "$enable_message the $this_jobname SQL Job"
            Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $exec_enable_job
        }
    }


}

Function Configure-PrimaryLogShipping
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$false)]
        [AllowNull()]
        [AllowEmptyString()]
        [string]$ServerInstance,

        [parameter(Mandatory=$true)]
        [string]$LogParentDir,

        [parameter(Mandatory=$false, ValueFromPipeline=$true)]
        [string[]]$Database,

        [parameter(Mandatory=$false)]
        [string[]]$Exclude,

        [parameter(Mandatory=$false)]
        [int]$RetentionPeriod = 1440,

        [parameter(Mandatory=$false)]
        [switch]$JobsEnabled,

        [parameter(Mandatory=$false)]
        [switch]$WhatIf
    )

    $Serverpath = Check-ServerInstance($ServerInstance)
    if(!$ServerInstance)
    {
        $ServerInstance = $env:computername
    }

    if($Database -and $Exclude)
    {
        Write-Output "Cannot specify both Database and Exclude parameters"
    }
    else
    {

        if(!(Test-Path $LogParentDir))
        {
            Write-Output "$LogParentDir path does not exist"
        }
        else
        {
            Write-Output "Server Path is: sqlserver:\sql\$Serverpath\Databases\"
            Set-Location "sqlserver:\sql\$Serverpath\Databases\"

            $databases = ls

            foreach ($db in $databases)
            {
                if($Database)
                {
                    $dbname = $db.Name

                    if($Database.Contains($dbname))
                    {
                        Write-Output "Including Databse: $dbname"
                    }
                    else
                    {
                        Write-Output "$dbname not in include list - ignoring"
                        continue
                    }
                }
                else
                {
                    $dbname = $db.Name

                    if($Exclude)
                    {
                        if($Exclude.Contains($dbname))
                        {
                            Write-Output "Excluding Database: $dbname"
                            continue
                        }
                    }
                }

                cd $LogParentDir

                $dbpath = "$LogParentDir\$dbname"
                if(!(Test-Path $dbpath))
                {
                    if(!$WhatIf)
                    {
                        Write-Output "$dbpath does not exist - Creating it"
                        New-Item -ItemType Directory -Path $dbpath
                    }
                    else
                    {
                        Write-Output "$dbpath does not exist - Would create it here"
                    }
                }
                else
                {
                    Write-Output "$dbpath already exists. Not creating it."
                }


                $exec_primary_database = @"
DECLARE @LS_BackupJobId AS uniqueidentifier ;
DECLARE @LS_PrimaryId AS uniqueidentifier ;

EXEC master.dbo.sp_add_log_shipping_primary_database
@database = N'$dbname'
,@backup_directory = N'$dbpath'
,@backup_share = N'$dbpath'
,@backup_job_name = N'LS_Copy_$dbname'
,@backup_retention_period = $RetentionPeriod
,@backup_threshold = 120
,@history_retention_period = $RetentionPeriod
,@backup_job_id = @LS_BackupJobId OUTPUT
,@primary_id = @LS_PrimaryId OUTPUT
,@overwrite = 1
,@backup_compression = 0;
GO

EXEC msdb.dbo.sp_update_jobstep
    @job_name = N'LS_Copy_$dbname',
    @step_id = 1,
	@output_file_name = N'$dbpath\Copy_$dbname.log',
	@flags = 2
GO
"@

    $exec_enable_job = @"
EXEC msdb.dbo.sp_update_job
    @job_name = N'LS_Copy_$dbname',
    @enabled = 1
GO
"@

    $exec_schedule = @"
EXEC msdb.dbo.sp_add_schedule @schedule_name=N'Copy_$dbname',
		@enabled=1,
		@freq_type=4,
		@freq_interval=1,
		@freq_subday_type=4,
		@freq_subday_interval=15,
		@freq_relative_interval=0,
		@freq_recurrence_factor=1,
		@active_start_date=20160713,
		@active_end_date=99991231,
		@active_start_time=0,
		@active_end_time=235959
GO
"@

                $exec_attach_schedule = @"
EXEC msdb.dbo.sp_attach_schedule
@job_name = N'LS_Copy_$dbname',
@schedule_name = N'Copy_$dbname'
GO
"@

                if($WhatIf)
                {
                    Write-Output "Primary Database Configuration is:"
                    Write-Output $exec_primary_database

                    Write-Output ""
                    Write-Output "Create schedule script"
                    Write-Output $exec_schedule

                    Write-Output ""
                    Write-Output "Attach schedule script"
                    Write-Output $exec_attach_schedule

                    if($JobsEnabled)
                    {
                        Write-Output ""
                        Write-Output "Enabling job"
                        Write-Output $exec_enable_job
                    }
                }
                else
                {
                    Write-Output ""
                    Write-Output "Executing primary log shipping configuration"
                    Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $exec_primary_database

                    if($JobsEnabled)
                    {
                        Write-Output ""
                        Write-Output "Enabling job"
                        Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $exec_enable_job
                    }

                    Write-Output ""
                    Write-Output "Creating Schedule for Jobs"
                    Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $exec_schedule

                    Write-Output ""
                    Write-Output "Attaching schedule to jobs"
                    Invoke-Sqlcmd -ServerInstance $ServerInstance -Query $exec_attach_schedule
                }
            }
        }
    }
}

Function Set-DatabaseToRecovery
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$false)]
        [AllowNull()]
        [AllowEmptyString()]
        [string]$ServerInstance,

        [parameter(Mandatory=$false, ValueFromPipeline=$true)]
        [string[]]$Database,

        [parameter(Mandatory=$false)]
        [string[]]$Exclude,

        [parameter(Mandatory=$false)]
        [switch]$WhatIf
    )

    $Serverpath = Check-ServerInstance($ServerInstance)
    if(!$ServerInstance)
    {
        $ServerInstance = $env:computername
    }

    if($Database -and $Exclude)
    {
        Write-Output "Cannot specify both Database and Exclude parameters"
    }
    else
    {
        Write-Output "Server Path is: sqlserver:\sql\$Serverpath\Databases\"
        Set-Location "sqlserver:\sql\$Serverpath\Databases\"

        $databases = ls

        foreach ($db in $databases)
        {
            if($Database)
            {
                $dbname = $db.Name

                if($Database.Contains($dbname))
                {
                    Write-Output "Including Databse: $dbname"
                }
                else
                {
                    Write-Output "$dbname not in include list - ignoring"
                    continue
                }
            }
            else
            {
                $dbname = $db.Name

                if($Exclude)
                {
                    if($Exclude.Contains($dbname))
                    {
                        Write-Output "Excluding Database: $dbname"
                        continue
                    }
                }
            }


            $exec_recovery = @"
USE [MASTER];
GO

RESTORE DATABASE $dbname WITH RECOVERY;
GO
"@

            if(!$WhatIf)
            {
                Write-Output "\nSetting $dbname to read/write mode"
                Invoke-SQLCMD -ServerInstance $ServerInstance -Query $exec_restore
            }
            else
            {
                Write-Output "Setting $dbname to read/write mode"
                Write-Output $exec_recovery
            }
        }
    }
}

Function Remove-AGDatabases
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$false)]
        [AllowNull()]
        [AllowEmptyString()]
        [string]$ServerInstance,

        [parameter(Mandatory=$false)]
        [AllowNull()]
        [AllowEmptyString()]
        [string[]]$SecondaryInstance,

        [parameter(Mandatory=$true)]
        [string]$AvailabilityGroup,

        [parameter(Mandatory=$false, ValueFromPipeline=$true)]
        [string[]]$Database,

        [parameter(Mandatory=$false)]
        [string[]]$Exclude,

        [parameter(Mandatory=$false)]
        [switch]$WhatIf
    )

    $Serverpath = Check-ServerInstance($ServerInstance)
    if(!$ServerInstance)
    {
        $ServerInstance = $env:computername
    }

    if($Database -and $Exclude)
    {
        Write-Output "Cannot specify both Database and Exclude parameters"
    }
    else
    {
        $AGpath = "sqlserver:\sql\$Serverpath\AvailabilityGroups\$AvailabilityGroup\AvailabilityDatabases"
        Write-Output "Availability Group path is: $AGPath"
        Set-Location $AGpath

        $databases = ls

        foreach ($db in $databases)
        {
            $dbname = $db.Name
            if($Database)
            {
                if($Database.Contains($dbname))
                {
                    Write-Output "Including Databse: $dbname"
                }
                else
                {
                    Write-Output "$dbname not in include list - ignoring"
                    continue
                }
            }
            else
            {
                if($Exclude)
                {
                    if($Exclude.Contains($dbname))
                    {
                        Write-Output "Excluding Database: $dbname"
                        continue
                    }
                }
            }

            $dbpath = "$AGpath\$dbname"


            if(!$WhatIf)
            {
                Write-Output ""
                Write-Output "Currently removing $dbname from the Availability Group"
                Remove-SqlAvailabilityDatabase -Path $dbpath
            }
            else
            {
                Write-Output ""
                Write-Output "WhatIf - Would remove $dbname from the Availability Group"
                Write-Output "Database path is: $dbpath"
                Remove-SqlAvailabilityDatabase -Path $dbpath -WhatIf
            }

            $instances = @()
            $instances += $ServerInstance

            if($SecondaryInstance)
            {
                $instances += $SecondaryInstance
            }

            Write-Output "Instances are: $instances"

            foreach($instance in $instances)
            {
                Write-Output "Removing $dbname from $instance"
                if(!$WhatIf)
                {
                    Invoke-SQLcmd -ServerInstance $ServerInstance -Query ("EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'" + $dbname + "'")
                    Invoke-SQLcmd -ServerInstance $ServerInstance -Query ("ALTER DATABASE [" + $dbname + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
                    Invoke-SQLcmd -ServerInstance $ServerInstance -Query ("DROP DATABASE [" + $dbname + "]")
                    Write-Output "Removal of $dbname completed"
                }
            }
        }
    }
}

Function Remove-SecondaryLogShipping
{
    [CmdletBinding()]
    param (
        [parameter(Mandatory=$false)]
        [AllowNull()]
        [AllowEmptyString()]
        [string]$ServerInstance,

        [parameter(Mandatory=$false, ValueFromPipeline=$true)]
        [string[]]$Database,

        [parameter(Mandatory=$false)]
        [string[]]$Exclude,

        [parameter(Mandatory=$false)]
        [switch]$WhatIf
    )

    $Serverpath = Check-ServerInstance($ServerInstance)
    if(!$ServerInstance)
    {
        $ServerInstance = $env:computername
    }

    if($Database -and $Exclude)
    {
        Write-Output "Cannot specify both Database and Exclude parameters"
    }
    else
    {
        Write-Output "Server Path is: sqlserver:\sql\$Serverpath\Databases\"
        Set-Location "sqlserver:\sql\$Serverpath\Databases\"

        $databases = ls

        foreach ($db in $databases)
        {
            if($Database)
            {
                $dbname = $db.Name

                if($Database.Contains($dbname))
                {
                    Write-Output "Including Databse: $dbname"
                }
                else
                {
                    Write-Output "$dbname not in include list - ignoring"
                    continue
                }
            }
            else
            {
                $dbname = $db.Name

                if($Exclude)
                {
                    if($Exclude.Contains($dbname))
                    {
                        Write-Output "Excluding Database: $dbname"
                        continue
                    }
                }
            }


            $exec_delete_secondary_database = @"
USE [MASTER];
GO

exec sp_delete_log_shipping_secondary_database @secondary_database = '$dbname';
GO
"@

            if(!$WhatIf)
            {
                Write-Host ""
                Write-Host "Removing secondary log shipping config for $dbname database"
                Invoke-SQLCMD -ServerInstance $ServerInstance -Query $exec_delete_secondary_database
            }
            else
            {
                Write-Host ""
                Write-Host "Removing secondary log shipping config for $dbname database"
                Write-Host $exec_delete_secondary_database
            }
        }
    }
}
