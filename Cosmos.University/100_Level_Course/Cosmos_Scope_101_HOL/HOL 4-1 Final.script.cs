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
                return i+1;
        }

        //somehow the click wasn't in the results
        return 0;
    }
}



public class PageViewActivityData
{
    public List<string> Results { get; private set; }
    public List<string> Clicks { get; private set; }

    public PageViewActivityData(string results, string clicks)
    {
        if (results == null || clicks == null)
        {
            throw new ArgumentException("Arguments 'results' and 'clicks' must be non-negative");
        }

        Results = new List<string>();
        foreach (string result in results.Split(';'))
        {
            if (result != "NULL")
            {
                Results.Add(result);
            }
        }

        Clicks = new List<string>();
        foreach (string click in clicks.Split(';'))
        {
            if (click != "NULL")
            {
                Clicks.Add(click);
            }
        }
        
    }

    public int GetResultPositionofClick(int clickNumber)
    {
        if (clickNumber > Clicks.Count)
        {
            return 0;
        }

        string click = Clicks[clickNumber - 1];

        for (int i = 0; i < Clicks.Count; i++)
        {
            if (Results[i] == click)
            {
                return i + 1;
            }
        }

        return 0;
    }
}