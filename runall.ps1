Param ( [string]$Path, [string]$Like="*") 

Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"

$scriptpath = $MyInvocation.MyCommand.Path

if ($Path -eq "")
{
    $Path = Split-Path $scriptpath
}
else
{
    $Path = Resolve-Path $Path
}





$files = Get-ChildItem -Path $Path -Filter *.script
if ($Like -ne "*")
{
    $files = $files | Where-Object { $_.BaseName -like $Like }
}

Write-Host $files.Count scripts will be run

$headerline = "--------------------------------------------------------------------------------"
$header_fg_color = "Green"

$failed_scripts = New-Object “System.Collections.Generic.List[string]”

foreach ($file in $files)
{
    Write-Host $headerline -BackgroundColor Black -ForegroundColor $header_fg_color
    Write-Host $file.FullName -BackgroundColor Black -ForegroundColor $header_fg_color
    Write-Host $headerline -BackgroundColor Black -ForegroundColor $header_fg_color

    $runscript = join-path $scriptpath "..\run.ps1";

    &$runscript -Script $file.FullName

    if ($LASTEXITCODE -ne 0)
    {
        Write-Host Last Exit Code $LASTEXITCODE -BackgroundColor Black -ForegroundColor Red
        $failed_scripts.Add($file.FullName)
    }
}


if ($failed_scripts.Count>0)
{
     Write-Host Failed to Execute $failed_scripts.Count -BackgroundColor Black -ForegroundColor Red
    foreach ($fs in $failed_scripts)
    {
        Write-Host $fs -BackgroundColor Black -ForegroundColor Red
    }
}
