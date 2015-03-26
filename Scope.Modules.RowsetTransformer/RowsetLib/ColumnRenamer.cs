using System.Collections.Generic;
using System.Text;
using ScopeRuntime;

namespace RowsetLib
{
    public abstract class ColumnRenamer: ScopeRuntime.FunctionResolver
    {
        public string CreateRenameFunctionBody(string name, RowSetParameter input_rowset, Dictionary<string, string> renames_dic,
            string target_cols_param_name)
        {
            // Create the output schema
            var sb_select_columns = new System.Text.StringBuilder();
            var output_cols = new List<ScopeRuntime.ColumnInfo>();

            var output_schema = GetOutputSchema(input_rowset, renames_dic, sb_select_columns, output_cols);

            // Generate the funtion body
            var sb = new StringBuilder();
            sb.AppendFormat("FUNC {0}\n", name);
            sb.AppendFormat(" RETURN ROWSET({0})\n", output_schema);
            sb.AppendFormat(" PARAMS({0} ROWSET( {1} ) , {2} string DEFAULT = \"\")\n", input_rowset.Name, input_rowset.Schema,
                target_cols_param_name);
            sb.AppendFormat("BEGIN\n");
            sb.AppendFormat(" rs1 = SELECT {0} FROM {1};\n", sb_select_columns, input_rowset.Name);
            sb.AppendFormat("END FUNC\n");

            string function_body = sb.ToString();
            return function_body;
        }

        public string CreateReplacerFunctionBody(string name, RowSetParameter input_rowset, Dictionary<string, string> renames_dic,
            string target_cols_param_name, string replacements_param_name)
        {
            // Create the output schema
            var sb_select_columns = new System.Text.StringBuilder();
            var output_cols = new List<ScopeRuntime.ColumnInfo>();

            var output_schema = GetOutputSchema(input_rowset, renames_dic, sb_select_columns, output_cols);

            // Generate the funtion body
            var sb = new StringBuilder();
            sb.AppendFormat("FUNC {0}\n", name);
            sb.AppendFormat(" RETURN ROWSET({0})\n", output_schema);
            sb.AppendFormat(" PARAMS({0} ROWSET( {1} ) , {2} string DEFAULT = \"\", {3} string DEFAULT = \"\")\n", input_rowset.Name, input_rowset.Schema,
                target_cols_param_name, replacements_param_name);
            sb.AppendFormat("BEGIN\n");
            sb.AppendFormat(" rs1 = SELECT {0} FROM {1};\n", sb_select_columns, input_rowset.Name);
            sb.AppendFormat("END FUNC\n");

            string function_body = sb.ToString();
            return function_body;
        }

        private static Schema GetOutputSchema(RowSetParameter input_rowset, Dictionary<string, string> renames_dic,
            StringBuilder sb_select_columns, List<ColumnInfo> output_cols)
        {
            int count = 0;
            foreach (var input_col in input_rowset.Schema.Columns)
            {
                if (count > 0)
                {
                    sb_select_columns.Append(" , ");
                }

                string old_name = input_col.Name;

                if (renames_dic.ContainsKey(old_name))
                {
                    string new_name = renames_dic[old_name];
                    output_cols.Add(new ScopeRuntime.ColumnInfo(new_name, input_col.ColumnCLRType));
                    sb_select_columns.AppendFormat(" {0} AS {1} ", old_name, new_name);
                }
                else
                {
                    output_cols.Add(new ScopeRuntime.ColumnInfo(old_name, input_col.ColumnCLRType));
                    sb_select_columns.AppendFormat(" {0} ", old_name);
                }

                count++;
            }

            var output_schema = new ScopeRuntime.Schema();
            output_schema.AddRange(output_cols);
            return output_schema;
        }
    }
}
