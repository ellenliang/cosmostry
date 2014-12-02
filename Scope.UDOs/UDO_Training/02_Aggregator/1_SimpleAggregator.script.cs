using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
public class MySum_Integer : Aggregate1<int, int>
{
    int sum = 0;

    public override void Initialize()
    {
        sum = 0;
    }

    public override void Add(int y)
    {
        sum += y;
    }

    public override int Finalize()
    {
        return sum;
    }
}