Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"

Import-Module Cosmos

$cluster = "cosmos08"
$vcname = "sandbox"
$vc = "vc://" + $cluster + "/" + $vcname
$p2 = "/shares/searchIndexDiscoverySelection.Prod/URLRepositoryV2Snapshots/StaticRank-Prod-Bn1/scratch1/"
$p = $vc + $p2 

Write-Host Retrieving all the directories from $p
$dirs = Get-CosmosStream $p

Write-Host Filtering out all the empty directories
$dirs = $dirs | ? { $_.IsDirectory -eq $true } | ? { $_.Length -ne 0 }

$minsize = 5 * 1024 * 1024 * 1024 * 1024 
$dirs = $dirs | Where-Object { $_.Length -gt $minsize }
Write-Host Filtering out all directories containing containing the work Backup
$dirs = $dirs | Where-Object { !($_.StreamName.Contains( "backup" ) -or $_.StreamName.Contains( "Backup" )) }


$dirs



$b = $false


$strings = $dirs | % { $_.StreamName } | Split-Path -Leaf 
$ints = @($strings | % { $_ -as [int] } | Sort-Object -Descending)



#$ints

$script = $null

foreach ($int in $ints)
{
	Write-Host $int

	$p3 = $p2 + $int
	$p4 = $p3 + "/snapshot/url.ss"

	Write-Host Path to Most Recent URL.SS on $cluster $vcname
	$p4
	$fullpath = "vc://" + $cluster + "/" + $vcname + $p4 

	$streampath = $p4 
	Write-Host 1111111 Test-CosmosStream $fullpath
	if (!(Test-CosmosStream $fullpath))
	{
		continue
	}
	$si = Get-CosmosStream $fullpath

	$tb_size = $si.Length/1000.0/1000.0/1000.0/1000.0
	$tib_size = $si.Length/1024.0/1024.0/1024.0/1024.0
	Write-Host TB Length: $tb_size
	Write-Host TiB Length: $tib_size



	$script = @"

// --------------------------------------------------------------------------------
e =  SSTREAM @"$streampath";

g = 
 SELECT TOP 10000 * 
 FROM e 
 WHERE Domain == @"h-----com.microsoft." AND HostSuffix == @"www.";

OUTPUT TO CONSOLE;
// --------------------------------------------------------------------------------

"@



	Write-Host $script

	$mydocs =  [environment]::getfolderpath("mydocuments")
	Write-Host $mydocs
	$output_file = join-path $mydocs "QueryURlRep.script"
	$script | out-file $output_file 
	break
}

if ($script -eq $null)
{
	Write-Host Could not find a url.ss
}




