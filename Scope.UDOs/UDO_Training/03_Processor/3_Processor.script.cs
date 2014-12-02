using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;


public class CopyProcessor : Processor
{
    public override Schema Produces(string[] requested_columns, string[] args, Schema input_schema)
    {
        var output_schema = input_schema.CloneWithSource();
        var newcol = new ColumnInfo("Market2", typeof(string));
        output_schema.Add(newcol);
        return output_schema;
    }
    public override IEnumerable<Row> Process(RowSet input_rowset, Row output_row, string[] args)
    {
        foreach (Row input_row in input_rowset.Rows)
        {
            input_row.CopyTo(output_row);
            string market = input_row[0].String;
            output_row[1].Set("foo " + market);
            yield return output_row;
        }
    }
}
