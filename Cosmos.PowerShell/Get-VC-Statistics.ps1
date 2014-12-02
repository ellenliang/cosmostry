Set-StrictMode -Version 2 
$ErrorActionPreference = "Stop"

Import-Module Cosmos


$clusters = Get-CosmosCluster
#$vcs = $clusters | Get-CosmosVirtualCluster

Write-Host "########################################"
Write-Host Num Clusters total
Write-Host "########################################"
$vcs | Measure-Object


Write-Host "########################################"
Write-Host Num VCs by Cluster
Write-Host "########################################"
$vcs | Group-Object Cluster


Write-Host "########################################"
Write-Host Total Tokens 
Write-Host "########################################"
$vcs | Measure-Object TokenAllocation -Sum

Write-Host "########################################"
Write-Host Tokens by Cluster
Write-Host "########################################"
$vcgroups = $vcs | Group-Object Cluster 
foreach ($vcgroup in $vcgroups)
{
    Write-Host $vcgroup.Name
    $vcs | ? { $_.Cluster -eq $vcgroup.Name } | Measure-Object TokenAllocation -Sum

}


Write-Host "########################################"
Write-Host Top 10 VCs by Token Allocation
Write-Host "########################################"
$vcs | Sort-Object TokenAllocation -Descending | Select-Object -Property Name,Description,TokenAllocation -First 10 