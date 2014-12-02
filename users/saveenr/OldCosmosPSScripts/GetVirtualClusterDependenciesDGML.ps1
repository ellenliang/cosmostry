[CmdletBinding()]
Param ([string]$VCName, [string]$pathToVC, [string]$UserName)

import-module CosmosPS

$cred=(Get-Credential $UserName)
New-PSDrive X -PSProvider Cosmos -Root $pathToVC -Credential $cred
X:
$vc = Get-CosmosVirtualCluster -VirtualCluster $VCName
$vc_shares = $vc.GetShares();

#create the data table                
$data_table = New-Object System.Data.DataTable "$vc.Name"

#now set up the columns and types
$from_vc = New-Object system.Data.DataColumn FromVC,([string])
$to_vc = New-Object system.Data.DataColumn ToVC,([string])
$to_folder = New-Object system.Data.DataColumn ToFolder,([string])
$to_folder_size = New-Object system.Data.DataColumn ToFolderSize,([string])
$perms = New-Object system.Data.DataColumn Permissions,([string])

$data_table.columns.add($from_vc)
$data_table.columns.add($to_vc)
$data_table.columns.add($to_folder)
$data_table.columns.add($to_folder_size)
$data_table.columns.add($perms)

#Now we are ready to add data to the pipeline
$vc_shares | ForEach-Object {
                            foreach($folder in $_.GetSharedFolders()) 
                            {
                                $row=$data_table.NewRow();
                                $row.FromVC = $vc.Name;
                                $row.ToVC = $_.SourceVirtualCluster;
                                $row.ToFolder = $folder.StreamName;
                                $row.ToFolderSize = "{0:F3}" -f($folder.CommittedLength/1gb);
                                $row.Permissions = $folder.Permissions;
                                $data_table.Rows.Add($row);
                            } 
                       }
                       
#Now that the data_table is populated, invoke Saveen's pretty picture generators
$data_table | out-gridview