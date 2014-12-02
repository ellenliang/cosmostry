using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using ScopeRuntime;

namespace RowsetLib
{
    public class UnionAllSparse : ScopeRuntime.FunctionResolver
    {
        public override string Resolve(string name, IEnumerable<Parameter> parameters, string[] args)
        {
            var resolver_params = new ResolverParameters(parameters);
            var rowset_params = resolver_params.Rowsets.Values.ToList();

            if (rowset_params.Count < 1)
            {
                throw new System.ArgumentException("Must have at least one rowset");
            }

            var coldic = new Dictionary<string, ColumnDataType>();
            var collist = new List<ColumnInfo>();
            foreach (var rowset in rowset_params)
            {
                foreach (var col in rowset.Schema.Columns)
                {
                    if (coldic.ContainsKey(col.Name))
                    {
                        // should check that types match
                    }
                    else
                    {
                        coldic[col.Name] = col.Type;
                        collist.Add(col);
                    }
                }
            }


            var output_schema = new ScopeRuntime.Schema();
            foreach (var col in coldic)
            {
                output_schema.Add( new ColumnInfo(col.Key,col.Value));
            }

            // Generate the funtion body
            var sb = new StringBuilder();
            sb.AppendFormat("FUNC {0}\r\n", name);
            sb.AppendFormat(" RETURN ROWSET({0})\r\n", output_schema);
            sb.AppendFormat(" PARAMS( \r\n");
            for (int i = 0; i < rowset_params.Count; i++)
            {
                if (i > 0)
                {
                    sb.AppendFormat(" , \r\n");
                }

                var input_rowset = rowset_params[i];
                sb.AppendFormat("      {0} ROWSET( {1} ) \r\n", input_rowset.Name, input_rowset.Schema);                
            }
            sb.AppendFormat("  )\r\n");
            sb.AppendFormat("BEGIN\r\n");

            for (int i = 0; i < rowset_params.Count; i++)
            {
                var input_rowset = rowset_params[i];
                string schema = "*";

                sb.AppendFormat("\r\n");

                sb.AppendFormat(" rs{0} = ", i );
                sb.AppendFormat(" SELECT ");

                for (int j = 0; j < collist.Count; j++)
                {
                    if (j > 0)
                    {
                        sb.AppendFormat(" , " );                    
                    }
                    var xcol = collist[j];
                    if (input_rowset.Schema.Contains(xcol.Name))
                    {
                        sb.AppendFormat("  {0}  ", xcol.Name);                        
                    }
                    else
                    {
                        sb.AppendFormat("  default({0}) AS  {1} ", xcol.ColumnCLRType.Name, xcol.Name);                        
                    }
                }
                sb.AppendFormat(" FROM {0};\r\n", input_rowset.Name);
            }


            sb.AppendFormat("\r\n");

            sb.AppendFormat(" rs_all= \r\n");
            for (int i = 0; i < rowset_params.Count; i++)
            {
                if (i > 0)
                {
                    sb.AppendFormat("              UNION ALL \r\n");
                }

                var input_rowset = rowset_params[i];
                sb.AppendFormat("      SELECT * FROM rs{0} \r\n", i);
            }
            sb.AppendFormat("      ;\r\n");

            sb.AppendFormat("END FUNC\n");

            string function_body = sb.ToString();

                        System.IO.File.WriteAllText("D:\\xxx.txt",function_body);

            return function_body;
        }
    }
}