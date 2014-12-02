using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class MySum3_Microsoft_SCOPE_Types_ScopeArray_1__System_Int32__mscorlib__Version_4_0_0_0__Culture_neutral__PublicKeyToken_b77a5c561934e089__ : Aggregate1<Microsoft.SCOPE.Types.ScopeArray<int>, int>
{
    int sum;
    public override void Initialize()
    {
        sum = 0;
    }

    public override void Add(Microsoft.SCOPE.Types.ScopeArray<int> valueArray)
    {
        for (int i = 0; i < valueArray.Count; i++)
        {
            sum += valueArray[i];
        }
    }

    public override int Finalize()
    {
        return sum;
    }
}