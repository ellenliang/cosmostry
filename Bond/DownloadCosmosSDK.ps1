Import-Module CosmosPS

$script_dir = split-path $MyInvocation.MyCommand.Path -Parent

$sdkfolder = Join-Path $script_dir "CosmosSDK"

$scope_exe = Join-Path $sdkfolder "scope.exe"

if (!(Test-Path $sdkfolder))
{
    mkdir $sdkfolder
}

Get-childitem -Path $sdkfolder -recurse -Force | remove-item -Recurse -Force

if (!(Test-Path $scope_exe))
{
    Export-CosmosSDKToFolder -Folder $sdkfolder
}






 