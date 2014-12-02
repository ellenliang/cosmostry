Param ( [string]$Script="./Scope.script" ) 

Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"

$ScopeSDKFolder = Resolve-Path "\ScopeSDK"
$ScopeEXE = join-path $ScopeSDKFolder "scope.exe"
$Script = resolve-path $Script
$ScriptPath = Split-Path $Script
$OutputPath = $ScriptPath


Write-Host Paths
Write-Host Script $Script
Write-Host ScriptPath $ScriptPath
Write-Host OutputPath $OutputPath

$ps_script_path = Split-Path $MyInvocation.MyCommand.Path
$local_my_path = resolve-path (join-path $ps_script_path "my/CosmosSamples")

Import-Module Cosmos
Import-CosmosStreamFromFile $local_my_path vc://cosmos08/sandbox/my/CosmosSamples -Recurse

