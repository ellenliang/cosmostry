
Set-StrictMode -Version 2 
$ErrorActionPreference = "Stop"

Import-Module ./CodePackage

# User controlled parameters
$include_datetime_in_zipname = $true
#$remove_staging_folder_when_finished = $true
$remove_staging_folder_when_finished = $true

#Script parameters
$mydocs = [Environment]::GetFolderPath("MyDocuments")
$staging_prefix = "tmp_"
$zipfile_location = $mydocs 
$datestring = Get-Date -format yyyy-MM-dd

$scriptpath = $MyInvocation.MyCommand.Path
$script_dir = Split-Path $scriptpath


# Calculate the root of the path we want to zip up
$src_root_path = resolve-path (join-path $script_dir "../..")

# Calculate basename which is used for the staging folder and the zip file
$basename = split-path -leaf $src_root_path 
if ($include_datetime_in_zipname)
{
    $basename = $basename + "-(" + $datestring  + ")"
}

# Create Staging Folder for ZIP packaging
$staging_folder = join-path $mydocs ($staging_prefix  + $basename) 
Write-host temp fldr: $staging_folder
Remove-FolderIfExists -Folder $staging_folder -Description "Staging Folder" -Verbose
New-Item $staging_folder -ItemType directory
Write-Host Creating Staging Folder $staging_folder
Copy-CodeFolder -SourceFolder $src_root_path -OutputFolder $staging_folder 
Write-Host Unbinding Folder $staging_folder
UnbindVSSourceControl -Folder $staging_folder -Verbose


# CREATE ZIP
Write-Host Creating ZIP 
$zipfile = join-path $zipfile_location ( $basename +".zip" )
Remove-FileIfExists -Filename $zipfile -Description "ZIPfile" -Verbose
$includebasedir = $false
ZIPFolder -Folder $staging_folder -ZipFile $zipfile -IncludeFolderInZip $includebasedir -Verbose

# CLEAN UP
if ($remove_staging_folder_when_finished) 
{
	Remove-FolderIfExists -Folder $staging_folder -Description "Staging Folder" -Verbose
}

