Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"

Import-Module Cosmos

$srcpath = Get-CosmosUri -fullpath "http://cosmos08.osdinfra.net/cosmos/sandbox/my/Demo"
$dstpath = Get-CosmosUri -fullpath "http://cosmos08.osdinfra.net/cosmos/sandbox/my/Demo3"


$srcuri =  $srcpath.ToHttpUri()

$src_streams = Get-CosmosStream $srcuri -Recurse -RecurseItemLimit 10000
foreach ($src_stream in $src_streams)
{
    Write-Host $src_stream.StreamName

    $str_src_uri = Get-CosmosUri -fullpath $src_stream.StreamName
    $trailer = $str_src_uri.Path.Substring($srcpath.Path.Length, $str_src_uri.Path.Length - $srcpath.Path.Length)
    while ( $trailer.StartsWith("/" ))
    {
        $trailer = $trailer.Substring(1)
    }
    
    $dest_uri = $dstpath.Combine( $trailer )

    $dest_stream_name = $dest_uri.ToHttpUri()

    $ext = [System.IO.Path]::GetExtension( $trailer ) 
    $temp_name = join-path $env:TEMP ([System.IO.Path]::GetFileName( $trailer ) )

    Export-CosmosStreamToFile $src_stream.StreamName $temp_name -Overwrite
    Import-CosmosStreamFromFile $temp_name $dest_stream_name -Overwrite -Verbose
    Remove-Item $temp_name
} 
