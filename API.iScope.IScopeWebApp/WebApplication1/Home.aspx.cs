using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace WebApplication1
{
    public partial class Home : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {

        }

        protected void Button1_Click(object sender, EventArgs e)
        {
            string script_text = @"
wiki = SSTREAM @""/shares/cosmosAdmin/iScopeCosting/WikipediaWordCount.ss"";

rs = SELECT TOP 10 * 
FROM wiki
WHERE keyword == ""bing"";

OUTPUT rs TO CONSOLE;
";

            var compiler = new Microsoft.Cosmos.Client.ScopeCompiler();
            compiler.Target = Microsoft.Cosmos.Client.CompilerTarget.Interactive;
            compiler.Settings.Vc = new Microsoft.Cosmos.Client.CompilerSettings.VcSettings();
            compiler.Settings.Vc.Path = "http://cosmos08.osdinfra.net/cosmos/sandbox/";
            compiler.Settings.Execution = new Microsoft.Cosmos.Client.CompilerSettings.ExecutionSettings();
            compiler.Settings.Execution.RuntimeVersion = "iscope_default";

            var script = new Microsoft.Cosmos.Client.ScopeScript();
            script.Contents = script_text;
            script.Name = "Demo iScope";

            var job = compiler.Execute(script);
            using (var reader = job.Result.Console)
            {
                var schema = reader.GetSchemaTable();

                var schemacol_colname = schema.Columns["ColumnName"];
                var schemacol_ordinal = schema.Columns["ColumnOrdinal"];
                var schemacol_datatypename = schema.Columns["DataTypeName"];
                var schemacol_providertype = schema.Columns["ProviderType"];
                var schemacol_allowdbnull = schema.Columns["AllowDBNull"];


                var sb = new StringBuilder();

                var column_names = new string[schema.Rows.Count];
                for (int i = 0; i < schema.Rows.Count; i++)
                {
                    var row = schema.Rows[i];
                    string colname = (string)row[schemacol_colname.Ordinal];
                    string datatypename = (string)row[schemacol_datatypename.Ordinal];
                    System.Type providertype = (System.Type)row[schemacol_providertype.Ordinal];

                    if (i > 0)
                    {
                        sb.Append(",");
                    }
                    sb.AppendFormat("{0}", colname, providertype.Name);
                }
                sb.AppendLine();


                int n = 0;
                object[] values = new object[reader.FieldCount];
                while (reader.Read())
                {
                    int num_fields = reader.GetValues(values);
                    for (int i = 0; i < num_fields; i++)
                    {
                        if (i > 0)
                        {
                            sb.Append(",");

                        }
                        sb.Append(values[i]);
                    }
                    sb.AppendLine();
                    n++;
                }
                this.TextBox1.Text = sb.ToString();
            }
            // finished with reader


        }

    }
}