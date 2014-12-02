Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"

$azcopy = "C:\Program Files (x86)\Microsoft SDKs\Azure\AzCopy\azcopy.exe"
$cosmosfolder = "vc://cosmos08/sandbox/my/WikipediaPageCounts"
$dest_path = "d:\"
$container = "https://sparkstore.blob.core.windows.net/ms-hadoop-blueprint"
$storagekey = "FUtzpJAesOSneIHLHegyGQ3Z+xJLJ/k5cBj+E/l/WfDdnIkbiWGCbRBjxreMDRW/Hd4acubnViiPpfOeXR2ROw=="

#$streams = Get-CosmosStream $cosmosfolder –Recurse

# Notes
# How to use Azcopy http://azure.microsoft.com/en-us/documentation/articles/storage-use-azcopy/
# The journal file will be here: %LocalAppData%\Microsoft\Azure\AzCopy

Resolve-Path $azcopy

$dblquote = "`""
$storagekey = $dblquote + $storagekey + $dblquote

foreach ($stream in $streams)
{
    Write-Host
    $basename = split-path $stream.StreamName –leaf
    $destfile = Join-Path $dest_path $basename

    if (Test-Path $destfile)
    {
        Write-Host file already exists. Not Downloading
    }
    else
    {
        Write-Host Downloading 
        Write-Host FROM $stream.StreamName
        Write-Host   TO $destfile
        Export-CosmosStreamToFile $stream $destfile
    }

    Write-Host File $basename
    Write-Host Uploading to Container
    & $azcopy $dest_path $container /destkey:$storagekey $basename
    if (Test-Path $dest_path)
    {
        Remove-Item $destfile
    }
}
 
