using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using System.Linq;

/// <summary>
/// 
/// </summary>
public class MyProcessor : Processor
{
    private static int Market_ColIndex ;
    private static int Language_ColIndex;

    private string source_col;
    private string dest_col;

    public MyProcessor( string source, string dest)
    {
        this.source_col = source;
        this.dest_col = dest;

        if (source == dest)
        {
            throw new System.ArgumentException("Source and Dest columns must have different names");
        }
    }


    public override Schema Produces(string[] columns, string[] args, Schema input)
    {
        // Create a copy of the schema and add a new column at the end
        var output_schema = input.Clone();
        var newcol = new ColumnInfo(this.dest_col, typeof(string));
        output_schema.Add(newcol);
        return output_schema;
    }

    public override IEnumerable<Row> Process(RowSet input, Row output, string[] args)
    {
        // cache the locations of the two columns being used
        Market_ColIndex = output.Schema.IndexOf(this.source_col);
        Language_ColIndex = output.Schema.IndexOf(this.dest_col);

        string langmap_text = System.IO.File.ReadAllText("langmap.txt").Trim();

        var lines = langmap_text.Split('\n');
        lines = lines.Select(s => s.Trim()).ToArray();

        var market_to_lang = new Dictionary<string, string>();
        foreach (string line in lines)
        {
            var tokens = line.Split('=');
            string market = tokens[0];
            string language = tokens[1];

            market_to_lang[market] = language;
        }

        foreach (Row row in input.Rows)
        {
            row.CopyTo(output);
            string market = row[Market_ColIndex].String;
            if (market_to_lang.ContainsKey(market))
            {
                string lang = market_to_lang[market];
                output[Language_ColIndex].Set(lang);                
            }
            else
            {
                output[Language_ColIndex].Set("Unknown");                                
            }
            yield return output;
        }
    }
}
