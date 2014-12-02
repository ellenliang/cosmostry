using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosSamples.Export.GetRowCount
{

    class Program
    {
        static void Main(string[] args)
        {
            string stream = "http://cosmos08.osdinfra.net:88/cosmos/sandbox/users/saveenr/Nullable.ss";
            
            var settings = new Microsoft.Cosmos.ExportClient.ScopeExportSettings();
            settings.Path = stream;

            var exportClient = new Microsoft.Cosmos.ExportClient.ExportClient(settings);

            settings.Path = stream;
            long rowCount = exportClient.GetRowCount(null).Result;

        }
    }
}
