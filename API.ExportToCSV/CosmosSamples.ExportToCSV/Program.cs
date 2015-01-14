﻿using System;

namespace CosmosSamples.ExportToCSV
{
    class Program
    {
        static void Main(string[] args)
        {
            var exporter = new Exporter();
            string stream = "https://cosmos08.osdinfra.net/cosmos/sandbox/my/katyperry.ss";
            string filename = @"d:\output.txt";

            exporter.Export(stream,filename);

        }
    }

    public class Exporter
    {
        public string Separator = ",";
        public string Terminator = "\r\n";
        public bool ExcludeHeaders;
        public int Top = -1;
        public string[] Columns;

        public Exporter()
        {
           
        }

        public void Export(string stream, string filename)
        {
            var settings = new Microsoft.Cosmos.ExportClient.ScopeExportSettings();
            settings.Path = stream;

            var exportClient = new Microsoft.Cosmos.ExportClient.ExportClient(settings);
            
            // Top
            if (this.Top > 0)
            {
                settings.Top = this.Top;
            }


            // Columns
            settings.ColumnFilters = this.Columns;


            // Partitions
            // if PartitionIndices is set to null, or not set, default to get all partitions
            settings.PartitionIndices = exportClient.GetAllPartitionIndices(null).Result;

            try
            {
                var task = exportClient.Export(null, new System.Threading.CancellationToken());
                var readTask = task.ContinueWith((prevTask) =>
                {
                    using (Microsoft.Cosmos.ExportClient.IExportDataReader dataReader = prevTask.Result.DataReader)
                    {
                        OutputToFile(dataReader, filename, this.Separator, this.Terminator);
                    }
                });
                readTask.Wait();
            }
            catch (System.AggregateException ae)
            {
                System.Console.WriteLine(ae.ToString());
            }

        }


        private void OutputToFile(System.Data.IDataReader reader, string fileName, string separator, string terminator)
        {
            using (var s = System.IO.File.Create(fileName))
            {
                using (var writer = new System.IO.StreamWriter(s))
                {

                    var schematable = reader.GetSchemaTable();

                    var schemacol_colname = schematable.Columns["ColumnName"];
                    var schemacol_ordinal = schematable.Columns["ColumnOrdinal"];
                    var schemacol_datatypename = schematable.Columns["DataTypeName"];
                    var schemacol_providertype = schematable.Columns["ProviderType"];
                    var schemacol_allowdbnull = schematable.Columns["AllowDBNull"];


                    var column_names = new string[schematable.Rows.Count];
                    if (!this.ExcludeHeaders)
                    {
                        for (int i = 0; i < schematable.Rows.Count; i++)
                        {
                            var row = schematable.Rows[i];
                            string colname = (string)row[schemacol_colname.Ordinal];
                            string datatypename = (string)row[schemacol_datatypename.Ordinal];
                            System.Type providertype = (System.Type)row[schemacol_providertype.Ordinal];

                            column_names[i] = colname;

                            if (i > 0)
                            {
                                Console.Write(this.Separator);
                            }
                            Console.Write("{0}", colname);
                        }
                        Console.Write(this.Terminator);                        
                    }


                    int n = 0;
                    object[] values = new object[reader.FieldCount];
                    while (reader.Read())
                    {
                        int num_fields = reader.GetValues(values);
                        for (int i = 0; i < num_fields; i++)
                        {
                            if (i > 0)
                            {
                                Console.Write(this.Separator);
                            }
                            Console.Write(values[i]);
                        }
                        Console.Write(this.Terminator);
                        n++;
                    }
                }
            }
        }


    }
}

