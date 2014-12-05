using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public static class MyHelper
{
    public static int ParseDuration(string s)
    {
        string [] tokens = s.Split(new char[] {':'});
        int hours_in_sec = int.Parse(tokens[0])*60*60;
        int minutes_in_sec = int.Parse(tokens[1]) * 60;
        int seconds = int.Parse(tokens[2]);
        int total = hours_in_sec + minutes_in_sec + seconds;
        return total;
    }

    public static double SecondsToHours(int seconds)
    {
        double hours = seconds/(60.0*60.0);
        return hours;
    }

    public static string FOO(string s)
    {
        return s.ToUpper();
    }

    public static string [] Split( string s, char c)
    {
        return s.Split(new char[] {c});
    }

    public static string Join(char c, IEnumerable<string> tokens)
    {
        var sb = new System.Text.StringBuilder();
        int n = 0;
        foreach (var token in tokens)
        {
            if (n>0)
            {
                sb.Append(c);
            }
            sb.Append(token);
            n++;
        }
        return sb.ToString();
    }

}

public class UrlList
{
    public List<string> Items;
    private static char [] sepchars = new char[] {','};

    public UrlList(string s)
    {
        this.Items = new List<string>(s.Split(sepchars));
    }

    public static UrlList Create(string s)
    {
        return new UrlList(s);
    }

    public string Serialize()
    {
        return string.Join(";", this.Items);
    }
}


class MySum_Double : Aggregate1<double, double>
{
    double sum = 0;

    public override void Initialize()
    {
        sum = 0;
    }

    public override void Add(double y)
    {
        sum += y;
    }

    public override double Finalize()
    {
        return sum;
    }
}


class MyAgg2_IntegerDouble : Aggregate2<int, double, string>
{
    
    System.Text.StringBuilder sb;
    public override void Initialize()
    {
        sb = new System.Text.StringBuilder();
    }

    public override void Add(int count, double value)
    {
        for (int i=0; i<count; i++)
        {
            sb.Append(value.ToString());
        }
    }

    public override string Finalize()
    {
        return sb.ToString();
    }
}


public class MyTSVExtractor : Extractor
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

public class MyCSVOutputter: Outputter
{
    public override void Output(RowSet input, StreamWriter writer, string[] args)
    {
        foreach (Row row in input.Rows)
        {
            int c = 0;
            for (int i=0;i<row.Count;i++)
            {
                if (c>0)
                {
                    writer.Write(",");                    
                }
                row[i].Serialize(writer);
                c++;
            }
            writer.WriteLine();
            writer.Flush();
        }
    }
}

public class MyCombiner: Combiner
{
    private RowList _rowList = new RowList();

    public override Schema Produces(string[] requestedColumns, string[] args, Schema leftSchema, string leftTable, Schema rightSchema, string rightTable)
    {
        var tokens = new List<string>();
        foreach (var col in leftSchema.Columns)
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
        _rowList.Load(right); //  Load the right RowSet into memory
        foreach (Row leftRow in left.Rows)
        {
            leftRow.CopyTo(outputRow); // Copy the data from the leftRow to the output

            //  Copy the data from the leftRow to the output
            foreach (Row rightRow in _rowList.Rows)
            {
                for (int i = 0; i < rightRow.Count; ++i)
                {
                    rightRow[i].CopyTo(outputRow[i + leftRow.Count]);
                }
                yield return outputRow;
            }
        }
    }
}

