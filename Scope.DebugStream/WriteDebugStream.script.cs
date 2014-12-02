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

        int debug_count = 0;

        while ((line = reader.ReadLine()) != null)
        {
            var tokens = line.Split(delimiter);
            for (int i = 0; i < tokens.Length; ++i)
            {
                output_row[i].UnsafeSet(tokens[i]);
            }

            string debug_msg = string.Format("Line {0}: {1}", debug_count, line.Trim());
            ScopeRuntime.Diagnostics.DebugStream.WriteLine(debug_msg);

            debug_count++;

            yield return output_row;
        }
    }
}
