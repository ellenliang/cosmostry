using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class MyCrossJoinCombiner:Combiner
{
    public override Schema Produces(
                        string[] requestedColumns, 
                        string[] args, Schema leftSchema, 
                        string leftTable, 
                        Schema rightSchema, 
                        string rightTable)
    {
        var tokens = new List<string>();
        foreach(var col in leftSchema.Columns)
        {
            string prefix = rightSchema.Contains(col.Name) ? leftTable : "";
            tokens.Add(string.Format("{0}{1}:{2}", prefix, col.Name, col.CLRType));
        }
        foreach (var col in rightSchema.Columns)
        {
            string prefix = leftSchema.Contains(col.Name) ? rightTable : "";
            tokens.Add(string.Format("{0}{1}:{2}", prefix, col.Name, col.CLRType));
        }
        var schemastring = string.Join(",", tokens);
        return new Schema(schemastring);
    }

    public override IEnumerable<Row> Combine(RowSet left, RowSet right, Row outputRow, string[] args)
    {
        var _rowList = new RowList();
        _rowList.Load(right); //load the right RowSet into memory
        foreach(Row leftRow in left.Rows)
        {
            leftRow.CopyTo(outputRow); //copy the data from the leftRow to the output
            //Copy the data from the rightRow to the output
            foreach(Row rightRow in _rowList.Rows)
            {
                for(int i = 0; i <rightRow.Count; ++i)
                {
                    rightRow[i].CopyTo(outputRow[i + leftRow.Count]);
                }
                yield return outputRow;
            }
        }
    }
}