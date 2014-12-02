using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;

public class MyVersion
{
    private Version _version;
    public int CompareTo(MyVersion versionIn)
    {
        return this._version.CompareTo(versionIn._version);
    }
    public MyVersion(string vString)
    {
        _version = new Version(vString);
    }
    public void Serialize(StreamWriter writer)
    {
        writer.Write(_version.ToString());
    }
}

public class VersionExtractor : Extractor
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
            output_row[0].UnsafeSet(tokens[0]);
            output_row[1].UnsafeSet(new MyVersion(tokens[1]));
            output_row[2].UnsafeSet(tokens[2]);
            yield return output_row;
        }
    }
}
public class VersionOutputter : Outputter
{
    public override void Output(RowSet input, StreamWriter writer, string[] args)
    {
        foreach (Row row in input.Rows)
        {
            ColumnData col0 = row[0];
            col0.Serialize(writer);
            writer.Write('\t');
            ColumnData col1 = row[1];
            ((MyVersion)col1.Value).Serialize(writer);
            writer.Write('\t');
            ColumnData col2 = row[2];
            col2.Serialize(writer);
            writer.WriteLine();
            writer.Flush();
        }
    }
}
