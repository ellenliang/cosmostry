Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"

$scopesdk_folder = "D:\ScopeSDK"
$nuget_exe = "D:\NuGet.CommandLine.2.8.2\nuget.exe"


function Assert-FolderExists( $path )
{

    if (!(Test-Path $path ))
    {
        Write-Host Path $path does not exist
        exit
    }
    else
    {
        Write-Host $path exists
    }
}

function Assert-FileExists( $path )
{

    if (!(Test-Path $path ))
    {
        Write-Host Path $path does not exist
        exit
    }
    else
    {
        Write-Host $path exists
    }
}

function Create-Folder( $path )
{
   if (!(Test-Path $path ))
   {
        New-Item $path -ItemType Directory | Out-Null
   }
}


Assert-FolderExists $scopesdk_folder
Assert-FolderExists $nuget_exe


$scopesdk_nuget_id = "Microsoft.Cosmos.ScopeSDK"
$scopesdk_nuget_ver = "1.0.0"
$scopesdk_nuget_authors = "Foo"
$scopesdk_nuget_owners = "bar"
$scopesdk_nuget_license = "http://www.microsoft.com/web/webpi/eula/nuget_release_eula.htm"
$scopesdk_nuget_license = "http://nuget.codeplex.com/"
$scopesdk_nuget_description = "Whatevs"
$scopesdk_nuget_copyright = "CR"

$scopesdk_nuspec = @"
<?xml version="1.0"?>
<package xmlns="http://schemas.microsoft.com/packaging/2011/08/nuspec.xsd">
  <metadata>
    <id>$scopesdk_nuget_id</id>
    <version>$scopesdk_nuget_ver</version>
    <authors>$scopesdk_nuget_authors</authors>
    <owners>$scopesdk_nuget_owners</owners>
    <licenseUrl>$scopesdk_nuget_license</licenseUrl>
    <projectUrl>$scopesdk_nuget_license</projectUrl>
    <requireLicenseAcceptance>false</requireLicenseAcceptance>
    <description>$scopesdk_nuget_description</description>
    <copyright>$scopesdk_nuget_copyright</copyright>
  </metadata>
</package>
"@

$scopesdk_targets =@"
<Project
 ToolsVersion="4.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003"> 

     <ItemGroup> 
      <MyPackageSourceFiles Include="d:\scopesdk\*.*"/> 
    </ItemGroup> 

  <Target Name="AfterBuild"> 
    <Copy SourceFiles="@(MyPackageSourceFiles)" DestinationFolder="`$(OutputPath)" > 
    </Copy> 
  </Target> 
</Project>
"@

$scopesdk_targets =@"
<Project
 ToolsVersion="4.0" xmlns="http://schemas.microsoft.com/developer/msbuild/2003"> 

     <ItemGroup> 
      <MyPackageSourceFiles Include="`$(MSBuildProjectDirectory)\..\Packages\Microsoft.Cosmos.ScopeSDK.1.0.0\binaries\*.*"/> 
    </ItemGroup> 

  <Target Name="AfterBuild"> 
    <Copy SourceFiles="@(MyPackageSourceFiles)" DestinationFolder="`$(OutputPath)" > 
    </Copy> 
  </Target> 
</Project>
"@



$pkg_folder = New-Object –TypeName PSObject
$pkg_folder | Add-Member –MemberType NoteProperty –Name Path –Value "D:\ScopeSDK_PackagingFolderxxx"
$pkg_folder | Add-Member –MemberType NoteProperty –Name LibFolder –Value (Join-Path $pkg_folder.Path "lib")
$pkg_folder | Add-Member –MemberType NoteProperty –Name ContentFolder –Value $(Join-Path $pkg_folder.Path "content")
$pkg_folder | Add-Member –MemberType NoteProperty –Name BinariesFolder –Value (Join-Path $pkg_folder.Path "binaries")
$pkg_folder | Add-Member –MemberType NoteProperty –Name BuildFolder –Value (Join-Path $pkg_folder.Path "build")
$pkg_folder | Add-Member –MemberType NoteProperty –Name ToolsFolder –Value (Join-Path $pkg_folder.Path "tools")

$pkg_folder

if (Test-Path $pkg_folder.Path)
{
    Write-Host $pkg_folder.Path exists
    Write-Host Cleaning $pkg_folder.Path
    Get-ChildItem -Path $pkg_folder.Path -Include *.* -Recurse | foreach { $_.Delete()}
}

if (!(Test-Path $pkg_folder.Path))
{
    Write-Host $pkg_folder.Path
    Create-Folder $pkg_folder.Path
}

Create-Folder $pkg_folder.BinariesFolder
Create-Folder $pkg_folder.BuildFolder
Create-Folder $pkg_folder.ToolsFolder
Create-Folder $pkg_folder.BinariesFolder
Create-Folder $pkg_folder.ContentFolder
Create-Folder $pkg_folder.LibFolder



$scopesdk_nuspec_filename = Join-Path $pkg_folder.Path ($scopesdk_nuget_id + ".nuspec")

if (Test-Path $scopesdk_nuspec_filename)
{
    Remove-Item $scopesdk_nuspec_filename
}

$scopesdk_nuspec | Out-File $scopesdk_nuspec_filename


$scopesdk_targets_filename = Join-Path $pkg_folder.BuildFolder ($scopesdk_nuget_id + ".targets")

if (Test-Path $scopesdk_targets_filename)
{
    Remove-Item $scopesdk_targets_filename
}

$scopesdk_targets  | Out-File $scopesdk_targets_filename

$bin_paths = @( "\*.lib", "\cqo.dll", "\*roslyn*.dll", "\scoperuntime.*", "\*.config")
foreach ($path in $bin_paths)
{
    $srcpath = Join-Path $scopesdk_folder $path
    Write-Host Copying $srcpath to binaries
    Copy-Item -Path $srcpath -Destination $pkg_folder.BinariesFolder
}

$lib_paths = @( "\*.lib", "\cqo.dll", "\*roslyn*.dll", "\scoperuntime.*", "\*.config")
foreach ($path in $lib_paths)
{
    $srcpath = Join-Path $scopesdk_folder $path
    Write-Host Copying $srcpath to lib
    Copy-Item -Path $srcpath -Destination $pkg_folder.LibFolder
}
