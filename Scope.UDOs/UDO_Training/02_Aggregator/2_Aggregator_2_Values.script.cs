using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class MySum2_IntegerString : Aggregate2<int, string, string>
{
    int sum = 0;
    string myMarket = "";

    public override void Initialize()
    {
        sum = 0;
    }

    public override void Add(int y, string market)
    {
        sum += y;
        myMarket = market;
    }

    public override string Finalize()
    {
        return myMarket + " " + sum.ToString();
    }
}
