using System;
using System.Collections.Generic;

namespace CosmosSamples.Export.GetStreamSchema
{


    class Program
    {
        static void Main(string[] args)
        {
            var exporter = new SchemaExplorer();
            string stream = "http://cosmos08.osdinfra.net:88/cosmos/sandbox/users/saveenr/Nullable.ss";
            
            var schema = exporter.ShowSchema(stream);

            foreach (var col in schema.Columns)
            {
                Console.WriteLine("{0} {1} {2}", col.Name, col.DataTypeName, col.ProviderType);
            }

        }
    }

    public class SchemaExplorer
    {
        public SchemaExplorer()
        {

        }

        public Schema ShowSchema(string stream)
        {
            var settings = new Microsoft.Cosmos.ExportClient.ScopeExportSettings();
            settings.Path = stream;

            var exportClient = new Microsoft.Cosmos.ExportClient.ExportClient(settings);

            settings.Top = 1;
            settings.PartitionIndices = exportClient.GetAllPartitionIndices(null).Result;

            var schema = new Schema();

            try
            {
                var task = exportClient.Export(null, new System.Threading.CancellationToken());
                var readTask = task.ContinueWith((prevTask) =>
                {
                    using (Microsoft.Cosmos.ExportClient.IExportDataReader reader = prevTask.Result.DataReader)
                    {
                        schema.GetSchemaFromIDataReader(reader);
                    }
                });
                readTask.Wait();
            }
            catch (System.AggregateException ae)
            {
                System.Console.WriteLine(ae.ToString());
            }

            return schema;
        }
    }

    public class Schema
    {
        public List<Column> Columns;


        public void GetSchemaFromIDataReader(System.Data.IDataReader reader)
        {
            var schematable = reader.GetSchemaTable();

            if (schematable == null)
            {
                throw new System.Exception("schematable is null");
            }

            var schemacol_colname = schematable.Columns["ColumnName"];
            var schemacol_ordinal = schematable.Columns["ColumnOrdinal"];
            var schemacol_datatypename = schematable.Columns["DataTypeName"];
            var schemacol_providertype = schematable.Columns["ProviderType"];
            var schemacol_allowdbnull = schematable.Columns["AllowDBNull"];


            this.Columns = new List<Column>(schematable.Rows.Count);

            for (int i = 0; i < schematable.Rows.Count; i++)
            {
                var row = schematable.Rows[i];
                string colname = (string)row[schemacol_colname.Ordinal];
                string datatypename = (string)row[schemacol_datatypename.Ordinal];
                System.Type providertype = (System.Type)row[schemacol_providertype.Ordinal];

                var col = new Column();
                col.Name = colname;
                col.DataTypeName = datatypename;
                col.ProviderType = providertype;

                this.Columns.Add(col);
            }
        }
    }

    public class Column
    {
        public string Name;
        public string DataTypeName;
        public System.Type ProviderType;
    }
}
