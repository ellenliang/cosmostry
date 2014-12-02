using System.Collections.Generic;
using System.Linq;
using ScopeRuntime;

namespace RowsetLib
{
    public class ResolverParameters
    {
        public Dictionary<string, ScopeRuntime.Resolver.RowSetParameter> Rowsets;
        public Dictionary<string, ScopeRuntime.Resolver.ScalarParameter> Scalars;

        public ResolverParameters(IEnumerable<ScopeRuntime.Resolver.Parameter> parameters)
        {
            var parameters_list = parameters as IList<Resolver.Parameter> ?? parameters.ToList();
            this.Rowsets = parameters_list.OfType<ScopeRuntime.Resolver.RowSetParameter>().ToDictionary(p => p.Name);
            this.Scalars = parameters_list.OfType<ScopeRuntime.Resolver.ScalarParameter>().ToDictionary(p => p.Name);            
        }

        public string[] GetParameterAsStringArray(string name)
        {
            string param_value = this.GetParameterValueAsString(name);
            var target_cols = split_string(param_value);
            return target_cols;
        }

        public string GetParameterValueAsString(string name)
        {
            var param = this.Scalars[name];

            if (param.Type != typeof(string))
            {
                string msg = string.Format("the value of the parameter must be a string");
                throw new System.ArgumentException(msg);
            }
            return (string)param.Value;
        }

        private static string[] split_string(string name)
        {
            var tokens = name.Trim().Split(',', ';');
            tokens = tokens.Select(s => s.Trim()).ToArray();
            return tokens;
        }

        public void AssertScalarParameterExists(string name)
        {
            if (!this.Scalars.ContainsKey(name))
            {
                string msg = string.Format("Must include the {0} scalar parameter", name);
                throw new System.ArgumentException(msg);
            }
        }

        public void AssertRowSetParameterExists(string name)
        {
            if (!this.Rowsets.ContainsKey(name))
            {
                string msg = string.Format("Must include the {0} rowset parameter", name);
                throw new System.ArgumentException(msg);
            }
        }

        public ScopeRuntime.Resolver.RowSetParameter GetRowSetParameter(string input_rowset_param_name)
        {
            var input_rowset = this.Rowsets[input_rowset_param_name];
            return input_rowset;
        }
    }
}