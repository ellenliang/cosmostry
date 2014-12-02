using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class MyTsvExtractor : Extractor
{
    public override Schema Produces(string[] requested_columns, string[] args)
    {
        return new Schema(requested_columns);
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