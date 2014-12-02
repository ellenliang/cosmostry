$script_file = Join-Path $env:TEMP "Demo.script"

$script = @"


a = VIEW @@view@@;

OUTPUT a TO SSTREAM @"/my/Colors_output.ss";

"@

$script | out-file $script_file 
$vc = "vc://cosmos08/sandbox"

Import-Module Cosmos

$params = @{ view="/my/Colors.view" }
$job = Submit-CosmosScopeJob -ScriptPath $script_file -VC $vc -Parameters $params


