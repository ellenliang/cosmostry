using System.Collections.Generic;
using System.Linq;
using ScopeRuntime;

namespace RowsetLib
{
    public class Rename : ColumnRenamer
    {
        public override string Resolve(string name, IEnumerable<Parameter> parameters, string[] args)
        {
            string target_cols_param_name = "columns";
            string input_rowset_param_name = "input";

            var resolver_params = new ResolverParameters(parameters);

            // Make sure the parameters we need were provided
            resolver_params.AssertRowSetParameterExists(input_rowset_param_name);
            resolver_params.AssertScalarParameterExists(target_cols_param_name);

            // Get the parameter values
            var input_rowset = resolver_params.GetRowSetParameter(input_rowset_param_name);
            var renames = resolver_params.GetParameterAsStringArray(target_cols_param_name);

            var pairs = NamePair.Parse(renames);
            var renames_dic = new Dictionary<string, string>(renames.Length);
            foreach (var pair in pairs)
            {
                renames_dic[pair.First] = pair.Second;
            }

            return CreateRenameFunctionBody(name, input_rowset, renames_dic, target_cols_param_name);
        }
    }

    public class Replace : ColumnRenamer
    {
        public override string Resolve(string name, IEnumerable<Parameter> parameters, string[] args)
        {
            string target_cols_param_name = "columns";
            string replace_param_name = "replace";
            string input_rowset_param_name = "input";

            var resolver_params = new ResolverParameters(parameters);

            // Make sure the parameters we need were provided
            resolver_params.AssertRowSetParameterExists(input_rowset_param_name);
            resolver_params.AssertScalarParameterExists(target_cols_param_name);
            resolver_params.AssertScalarParameterExists(replace_param_name);

            // Get the parameter values
            var input_rowset = resolver_params.GetRowSetParameter(input_rowset_param_name);
            var target_cols = resolver_params.GetParameterAsStringArray(target_cols_param_name);
            var renames = resolver_params.GetParameterAsStringArray(replace_param_name);

            target_cols = GetColumnsMatchingTheseNames(input_rowset, target_cols).Select(i=>i.Name).ToArray();

            var pairs = NamePair.Parse(renames);

            var rename_dic = new Dictionary<string, string>();
            foreach (var input_col in input_rowset.Schema.Columns)
            {
                string old_name = input_col.Name;
                string new_name = input_col.Name;
                rename_dic[old_name] = new_name;
            }


            foreach (var original_name in target_cols)
            {
                string new_name = rename_dic[original_name];

                foreach (var replace_pair in pairs)
                {
                    string pattern = replace_pair.First;
                    string replacement = replace_pair.Second;

                    new_name = new_name.Replace(pattern, replacement);
                }

                rename_dic[original_name] = new_name;
            }

            return CreateReplacerFunctionBody(name, input_rowset, rename_dic, target_cols_param_name, replace_param_name);
        }

        public static IEnumerable<ColumnInfo> GetColumnsMatchingTheseNames(RowSetParameter rowset, string[] columns)
        {
            return TextUtil.FilterObjectsByNames(rowset.Schema.Columns, columns, col => col.Name, false, TextUtil.FilterAction.Include );
        }
    }
}