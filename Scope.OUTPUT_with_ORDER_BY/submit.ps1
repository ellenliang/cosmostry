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

&$ScopeEXE submit -i $Script -vc vc://cosmos08/sandbox
#-OUTPUT_PATH $OutputPath -workingroot $env:TEMP



