using System.Collections.Generic;
using System.Linq;
using System.Text;
using ScopeRuntime;

namespace RowsetLib
{
    public abstract class ColumnFilter : ScopeRuntime.FunctionResolver
    {
        public static string FilterColumns(string name, IEnumerable<Parameter> parameters, TextUtil.FilterAction filter_action)
        {
            string target_cols_param_name = "columns";
            string input_rowset_param_name = "input";

            var resolver_params = new ResolverParameters(parameters);

            // Make sure the parameters we need were provided
            resolver_params.AssertRowSetParameterExists(input_rowset_param_name);
            resolver_params.AssertScalarParameterExists(target_cols_param_name);

            // Get the parameter values
            var input_rowset = resolver_params.GetRowSetParameter(input_rowset_param_name);
            var target_cols = resolver_params.GetParameterAsStringArray(target_cols_param_name);

            // Create the output schema
            var output_cols = TextUtil.FilterObjectsByNames(input_rowset.Schema.Columns, target_cols,
                col => col.Name, false, filter_action).ToList();

            // The output has to contain at least 1 column
            if (output_cols.Count < 1)
            {
                string msg = string.Format("The output rowset must contain at least one column");
                throw new System.ArgumentException(msg);
            }

            var output_schema = new ScopeRuntime.Schema();
            output_schema.AddRange(output_cols);

            // Generate the funtion body
            string select_clause = Helper.GetCommaSeparatedColumnNames(output_cols);
            var sb = new StringBuilder();
            sb.AppendFormat("FUNC {0}\n", name);
            sb.AppendFormat(" RETURN ROWSET({0})\n", output_schema);
            sb.AppendFormat(" PARAMS({0} ROWSET( {1} ) , {2} string DEFAULT = \"\")\n", input_rowset.Name, input_rowset.Schema,
                target_cols_param_name);
            sb.AppendFormat("BEGIN\n");
            sb.AppendFormat(" rs1 = SELECT {0} FROM {1};\n", select_clause, input_rowset.Name);
            sb.AppendFormat("END FUNC\n");

            string function_body = sb.ToString();
            return function_body;
        }
    }
}
