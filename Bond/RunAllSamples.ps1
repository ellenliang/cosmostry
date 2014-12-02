Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"


$script_dir = split-path $MyInvocation.MyCommand.Path -Parent

$scripts = dir $script_dir -Filter *.script | Sort-Object -Property Name

$scope_exe = join-path $script_dir "CosmosSDK\Scope.exe"
$geometry_dll = join-path $script_dir "Geometry\bin\debug\Geometry.dll"


function write-header
{
    Write-Host "----------------------------------------" -ForegroundColor Magenta
}




write-header
Write-Host Running all Cosmos and Bond Samples -ForegroundColor Magenta
write-header

Write-Host

Write-Host

write-header
Write-Host Checking Paths of Binaries -ForegroundColor Magenta
write-header

Resolve-Path $scope_exe
Resolve-Path $geometry_dll




write-header
Write-Host Checking Bond Dependencies
write-header

function show_bond_dependencies( $dll )
{
    $dllname = Split-Path $dll -Leaf
    $asm = [System.Reflection.Assembly]::LoadFile($dll)
    $ref_asms = $asm.GetReferencedAssemblies()

    foreach ($ref_asm in $ref_asms)
    {
        if ($ref_asm.Name.StartsWith("Microsoft.Bond"))
        {
            Write-Host $dllname Depends on $ref_asm.Name $ref_asm.Version -ForegroundColor Magenta
        }
    }
}

show_bond_dependencies $geometry_dll
show_bond_dependencies $scope_exe

foreach ($script in $scripts) 
{
    write-header
    Write-Host $script -ForegroundColor Magenta
    write-header

    $scriptname = Split-Path $script -Leaf

    &$scope_exe run -i $scriptname -OUTPUT_PATH "$script_dir"
}


write-header
Write-Host Done! -ForegroundColor Magenta
write-header
