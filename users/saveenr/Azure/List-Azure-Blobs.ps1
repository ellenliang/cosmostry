Set-StrictMode -Version 2
$ErrorActionPreference = "Stop"

Import-Module "C:\Program Files (x86)\Microsoft SDKs\Azure\PowerShell\ServiceManagement\Azure\Azure.psd1"

$storageaccount_name = "sparkstore"
$storageaccount_url = "https://" + $storageaccount_name + ".blob.core.windows.net"
$container =  "ms-hadoop-blueprint"
$storageaccount_key = "FUtzpJAesOSneIHLHegyGQ3Z+xJLJ/k5cBj+E/l/WfDdnIkbiWGCbRBjxreMDRW/Hd4acubnViiPpfOeXR2ROw=="


$context = New-AzureStorageContext -StorageAccountName $storageaccount_name -StorageAccountKey $storageaccount_key -Protocol https
Get-AzureStorageContainer -Context $context

#List the files
Get-AzureStorageBlob -Container $container -Context $context

