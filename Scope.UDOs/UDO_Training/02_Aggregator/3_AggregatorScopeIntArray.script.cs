using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class myHelper
{
    public static string IntArrayToString(Microsoft.SCOPE.Types.ScopeArray<int> input)
    {
        var sb = new System.Text.StringBuilder();
        for (int i = 0; i < input.Count; i++)
        {
            if (i > 0)
            {
                sb.Append(",");
            }
            sb.Append(input[i]);
        }

        return sb.ToString();
    }
}

public class Accumulate_Microsoft_SCOPE_Types_ScopeArray_1__System_Int32__mscorlib__Version_4_0_0_0__Culture_neutral__PublicKeyToken_b77a5c561934e089__
 : Aggregate1<Microsoft.SCOPE.Types.ScopeArray<int>, Microsoft.SCOPE.Types.ScopeArray<int>>
{
    int[] acc;

    public override void Initialize()
    {
    }

    public override void Add(Microsoft.SCOPE.Types.ScopeArray<int> y)
    {
        if (this.acc == null)
        {
            this.acc = new int[y.Count];
        }
        for (int i = 0; i < y.Count; i++)
        {
            this.acc[i] += y[i];
        }
    }

    public override Microsoft.SCOPE.Types.ScopeArray<int> Finalize()
    {
        var arr = new Microsoft.SCOPE.Types.ScopeArray<int>(this.acc);
        return arr;
    }
}