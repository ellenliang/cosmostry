#------------------------------------------------------------------------------
# This script generates a DGML graph representation of which customers have data
# on which volumes in a cluster. 
# 
# Change the $accesslevel parameter to get an understanding of the nature of the 
# access (R or W or All) that each customer has to specific volumes
# 
#------------------------------------------------------------------------------

#import-module CosmosPS

function GenerateGraph($cluster, $custinfo, $accesslevel)
{

    $allvcs = Get-CosmosVirtualCluster
    $d = New-DGMLDocument

    foreach($vc in $allvcs)
    {
        $cur_custinfo = $custinfo | ? { $_.CustomerID -eq $vc.CustomerId }
        foreach($mp in $vc.GetMountPoints())
        {    
            if (($accesslevel -eq "all") -or $mp.Permissions.Contains($accesslevel))
            {
                $team = $cur_custinfo.BusinessUnit + "_" + $cur_custinfo.Group
                $vol = $mp.Volume
                $d.AddLink($cur_custinfo.BusinessUnit, $team);
                #$d.AddLink($cur_custinfo.BusinessUnit, $vol);
                $d.AddLink($team, $vol);
            }
        }
    }

    $d.Save("c:\temp\" + $cluster + "-" + $accesslevel +".dgml")
}

$custinfo = Import-Csv .\ClusterConfiguration\custinfo.csv

New-PSDrive X -PSProvider Cosmos -Root vc://cosmos04/cosmosadmin
X:
GenerateGraph -cluster cosmos04 -custinfo $custinfo -accesslevel "all"

New-PSDrive Y -PSProvider Cosmos -Root vc://cosmos05/cosmosadmin
Y:
GenerateGraph -cluster cosmos05 -custinfo $custinfo -accesslevel "all"

New-PSDrive Z -PSProvider Cosmos -Root vc://cosmos06/cosmosadmin
Z:
GenerateGraph -cluster cosmos06 -custinfo $custinfo -accesslevel "all"



