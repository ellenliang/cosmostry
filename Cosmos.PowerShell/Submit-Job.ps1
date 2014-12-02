$script_file = Join-Path $env:TEMP "Demo.script"

$script = @"

a = VIEW @"/my/Colors.view";

OUTPUT a TO SSTREAM @"/my/Colors_output.ss";

"@

$script | out-file $script_file 
$vc = "vc://cosmos08/sandbox"

Import-Module Cosmos
$job = Submit-CosmosScopeJob -ScriptPath $script_file -VC $vc

