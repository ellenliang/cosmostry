using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class MyTsvExtractor : Extractor
{
    private bool _capitalizeStrings = false;
    public override Schema Produces(string[] requested_columns, string[] args)
    {
        return new Schema(requested_columns);
    }
    public MyTsvExtractor(bool capitalizeStrings = false)
    {
        _capitalizeStrings = capitalizeStrings;
    }
    public override IEnumerable<Row> Extract(StreamReader reader, Row output_row, string[] args)
    {
        char delimiter = '\t';
        string line;
        while ((line = reader.ReadLine()) != null)
        {
            var tokens = line.Split(delimiter);
            for (int i = 0; i < tokens.Length; ++i)
            {
                output_row[i].UnsafeSet(tokens[i]);
                if(output_row[i].Type == ColumnDataType.String)
                {
                    output_row[i].UnsafeSet(output_row[i].Value.ToString().ToUpper());
                }
            }
            yield return output_row;
        }
    }
}


public class MyTsvOutputter : Outputter
{
    public override void Output(RowSet input, StreamWriter writer, string[] args)
    {
        foreach (Row row in input.Rows)
        {
            int c = 0;
            for (int i = 0; i < row.Count; i++)
            {
                if (c > 0)
                {
                    writer.Write('\t');
                }
                row[i].Serialize(writer);
                c++;
            }
            writer.WriteLine();
            writer.Flush();
        }
    }
}
