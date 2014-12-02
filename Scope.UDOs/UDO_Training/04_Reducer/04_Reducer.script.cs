using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class MyTaxReducer : Reducer
{
    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        return new Schema("State:string,TotalTax:int");
    }

    public override IEnumerable<Row> Reduce(RowSet input, Row output, string[] args)
    {
        int total = 0;
        string state = "";
        foreach (Row row in input.Rows)
        {
            state = (string)row[0].Value;
            total += (int) row[1].Value;
        }
        output[0].Set(state);
        output[1].Set(total);
        yield return output;
    }
}