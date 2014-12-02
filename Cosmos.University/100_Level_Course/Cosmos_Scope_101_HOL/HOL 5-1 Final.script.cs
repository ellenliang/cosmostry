using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class Helpers
{
    public static int GetFirstClickedResultPosition(string results, string clicks)
    {
        //first check if there were clicks
        if (clicks == "NULL")
        {
            return 0;
        }

        int firstSemicolon = clicks.IndexOf(';');
        string firstClick = null;

        //check if there was only one click
        if (firstSemicolon == -1)
        {
            firstClick = clicks;
        }
        else
        {
            firstClick = clicks.Substring(0, firstSemicolon);
        }

        //you can make it efficient, or make it simple. this is simple.
        string[] splitResults = results.Split(';');
        for (int i = 0; i < splitResults.Length; i++)
        {
            if (splitResults[i] == firstClick)
                return i + 1;
        }

        //somehow the click wasn't in the results
        return 0;
    }

    public static float NonEnUsMarketProbability(IEnumerable<string> markets)
    {
        const string enus = "en-us";
        int nonenuscount = 0;
        int totalcount = 0;

        foreach (string market in markets)
        {
            if (market != "en-us")
            {
                nonenuscount++;
            }
            totalcount++;
        }

        return (float)nonenuscount / totalcount * 100.0f;
    }
}


public class Url
{
    public string FullText { get; private set; }
    
    public string Domain
    {
        get
        {
            //D-Cards are just treated here as their own domains, so first check if that's what we have
            //and return it back.
            if (FullText.StartsWith("d-card:"))
            {
                return FullText;
            }

            //let's get everything up to the first slash
            int indexOfSlash = FullText.IndexOf('/');
            string first = FullText.Substring(0, indexOfSlash >= 0 ? indexOfSlash : FullText.Length);

            //now we need to separate on "." and come up with domain and TLD
            string[] parts = first.Split('.');
            if (parts.Length == 1)
            {
                return parts[0];
            }
            return parts[parts.Length - 2] + "." + parts[parts.Length - 1];
        }
    }

    public Url(string text)
    {
        FullText = text;
    }
}

public class PageViewActivityData
{
    public List<Url> Results { get; private set; }
    public List<Url> Clicks { get; private set; }

    public PageViewActivityData(string results, string clicks)
    {
        if (results == null || clicks == null)
        {
            throw new ArgumentException("Arguments 'results' and 'clicks' must be non-negative");
        }

        Results = new List<Url>();
        foreach (string result in results.Split(';'))
        {
            if (result != "NULL")
            {
                Results.Add(new Url(result));
            }
        }

        Clicks = new List<Url>();
        foreach (string click in clicks.Split(';'))
        {
            if (click != "NULL")
            {
                Clicks.Add(new Url(click));
            }
        }

    }

    public int GetResultPositionofClick(int clickNumber)
    {
        if (clickNumber > Clicks.Count)
        {
            return 0;
        }
        string click = Clicks[clickNumber - 1].FullText;

        for (int i = 0; i < Clicks.Count; i++)
        {
            if (Results[i].FullText == click)
            {
                return i + 1;
            }
        }

        return 0;
    }
}