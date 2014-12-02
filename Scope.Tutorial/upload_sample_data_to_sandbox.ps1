Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"

Import-Module Cosmos

function makepath( $path )
{
    if (!(Test-CosmosFolder $path))
    {
        New-CosmosFolder $path
    }
}

$commonpath = "my/Scope.Tutorial"

# Create the folders on the VC
$path0 = "vc://cosmos08/sandbox/my/Scope.Tutorial"
$path1 = $path0 + "/Inputs"

makepath( $path0 )
makepath( $path1 )

# Locate the input files relative to this script
$scriptfilename = $MyInvocation.MyCommand.Path
$scriptpath = Split-Path $scriptfilename
$input_files_path = join-path $scriptpath ( $commonpath + "\Inputs" )
$input_files_path = Resolve-Path $input_files_path

Import-CosmosStreamFromFile $input_files_path $path1 -Recurse -Overwrite

