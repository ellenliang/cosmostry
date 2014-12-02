Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"

Import-Module CosmosPS

$orig_path = Get-Location


#if ((Test-CosmosCredential) -ne $true)
#{
#    Write-Error "Cosmos Credentials are not set or are incorrect"
#}

function makepath( $path )
{
    if (Test-CosmosFolder $path)
    {
        # do nothing
    }
    else
    {
        New-CosmosFolder $path
    }
}


$path0 = "vc://cosmos08/sandbox/my/ScopeTutorial"
$path1 = $path0 + "/SampleInputs"

makepath( $path0 )
makepath( $path1 )


$scriptfilename = $MyInvocation.MyCommand.Path
$scriptpath = Split-Path $scriptfilename

$input_files_path = join-path $scriptpath "..\my\ScopeTutorial\SampleInputs"

Import-CosmosStreamFromFile $input_files_path $path1 -Recurse -Overwrite

