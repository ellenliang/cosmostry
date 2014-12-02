$script_path = split-path $MyInvocation.MyCommand.Path -Parent

$bondfile = Join-Path $script_path "Geometry.bond"

$bond_exe = Join-Path $script_path "..\BondBinaries\bondc.exe"

&$bond_exe /Bond-CS /O:$script_path $bondfile

$proxies_cs = Join-Path $script_path "Geometry_proxies.cs"
$services_cs = Join-Path $script_path "Geometry_services.cs"
$types_cs = Join-Path $script_path "Geometry_types.cs"

