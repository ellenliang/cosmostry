Import-Module Cosmos

$enddate = Get-Date
$enddate = $enddate.AddDays(-2)
$startdate = $enddate.AddDays(-62)
$query = @"
SlapiPageView =

    VIEW "/shares/searchDM/distrib/released/SLAPI/SearchLogPageView.view"
    PARAMS
    (
        Start = @"@@DATE@@",
        End = @"@@DATE@@",
        UseSample = false,
        Dataset = @"Bing.com"
    );

rs0 =
    SELECT     SessionId,
               Market,
               Vertical,
               Query_RawQuery,
               Request_Browser,
               Request_Referrer,
               Request_Platform,
               Session_Duration,
               User_Locale,
               User_SafeSearchSetting,
       User_ReverseIpInfo.CountryIso  AS CountryIso, 
       User_ReverseIpInfo.State AS State,
       User_ReverseIpInfo.City AS City,
       User_ReverseIpInfo.Region AS Region,
       User_ReverseIpInfo.PostalCode AS PostalCode,  
       User_ReverseIpInfo.Lat AS Lat,
       User_ReverseIpInfo.Long AS Long,
       User_ReverseIpInfo.TimeZone AS TimeZone

    WHERE 
	(Query_RawQuery!=null) AND
	(Query_RawQuery!="") AND
        (Query_IsAdult == false) AND 
        (Vertical=="web") AND 
        (Market == "en-US")
    FROM SlapiPageView;

OUTPUT rs0
    TO @"@@OUTPUTFILENAME@@"
    USING DefaultTextOutputter();

#CS

public static class MyHelpers
{
    public static bool ContainsCaseInsensitive(string source, string target)
    {
        return source.IndexOf(target, StringComparison.OrdinalIgnoreCase)>=0;
    }

    public static bool ContainsAnyCaseInsensitive(string source, params string [] targets)
    {
        foreach (string target in targets)
        {
            if (ContainsCaseInsensitive(source,target))
            {
                return true;
            }
        }
        return false;
    }
}
#ENDCS
"@


foreach ($n in 0..-31)
{
    Write-Host $n
    $curdate = $enddate.AddDays($n)
    $curdate_s = Get-Date $curdate -Format "yyyy-MM-dd"
    Write-Host $curdate_s
    $outputfilename = "/local/bigdatasearches/" + $curdate_s + "-bing_big_data_queries.tsv"
    $new_query = $query
    $new_query = $new_query.Replace("@@DATE@@",$curdate_s)
    $new_query = $new_query.Replace("@@OUTPUTFILENAME@@",$outputfilename)
    Write-Host $new_query
    $script = "d:\testbing.script"
    $new_query | Out-File $script 
    $name = "bing_big_data_" + $curdate_s
    $name = $name.Replace( "-", "_" )
    $j = Submit-CosmosScopeJob $script -VC "vc://cosmos08/sandbox" -Tokens 201 -FriendlyName $name 
    $j2 = Wait-CosmosJob $j -TimeOut 1000000
}





