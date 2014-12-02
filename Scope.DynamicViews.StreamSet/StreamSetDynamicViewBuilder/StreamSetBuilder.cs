using System;
using System.Collections.Generic;
using System.Linq;

namespace DynamicViewUtils
{
    public class StreamSetBuilder
    {
        public bool VirtualColumns { get; set; }
        public IList<Expansion> Expansions { get; private set; }
        public string Schema { get; private set; }
        public string Template { get; private set; }
        public IDictionary<string, object> Parameters { get; private set; }

        public StreamSetBuilder(IDictionary<string, object> parameters)
        {
            this.Parameters = parameters;
        }

        public string GetViewDefinition()
        {
            this.Template  = (string)this.Parameters["Template"];
            this.Schema = (string)this.Parameters["Schema"];

            if (this.Parameters.ContainsKey("VirtualColumns"))
            {
                this.VirtualColumns = ((bool) this.Parameters["VirtualColumns"]);
            }
            else
            {
                this.VirtualColumns = false;
            }

            this.Expansions = new List<Expansion>();

            
            foreach (string key in this.Parameters.Keys)
            {
                if (key == "Template" || key == "Schema" || key == "VirtualColumns")
                {
                    continue;
                }
                string exp_name = key;
                string exp_value = (string) this.Parameters[exp_name];

                if (exp_value.StartsWith(DateExpansion.Prefix))
                {
                    var exp = new DateExpansion(exp_name, exp_value);
                    this.Expansions.Add(exp);                                                
                }
                else if (exp_value.StartsWith(IntExpansion.Prefix))
                {
                    var exp = new IntExpansion(exp_name, exp_value);
                    this.Expansions.Add(exp);                        
                }
                else if (exp_value.StartsWith(EnumExpansion.Prefix))
                {
                    // enum expansion
                    var exp = new EnumExpansion(exp_name, exp_value);
                    this.Expansions.Add(exp);
                }
                else
                {
                    throw new ArgumentException();
                }
            }
            
            var expanded_streams = this.Expand();
            var rowsets = Enumerable.Range(0, expanded_streams.Count).Select(i => string.Format("extracted_rowset_{0}", i)).ToList();
            var sb = new System.Text.StringBuilder();

            string extract_schema = Schema;
            string view_schema = extract_schema;

            // If Expansions need to be inthe output rowset then the VIEW schema needs
            // to be modified. Expansions 
            if (VirtualColumns)
            {
                foreach (var exp in this.Expansions)
                {
                    view_schema += string.Format(", {0}:{1}", exp.Name, exp.VirtualColumnType);                    
                }
            }

            string view_params = string.Format(@"Schema string , Template string, VirtualColumns bool DEFAULT = false, {0} ", string.Join(" , ", this.Expansions.Select(e=>string.Format("{0} {1}",e.Name, e.ViewParameterType) ) ) );

            sb.AppendFormat(@"
        CREATE VIEW DynamicViewStreamSet 
            SCHEMA ({0}) 
            PARAMS ({1})
        AS BEGIN ", view_schema, view_params);

            // Generate RowSets for each stream

            for (int i = 0; i < expanded_streams.Count; i++)
            {
                string rowset_name = rowsets[i];
                if (this.VirtualColumns)
                {
                    var exp_stream = expanded_streams[i];
                    sb.AppendFormat(@"{0}_before_vc = EXTRACT {1} FROM ""{2}"" USING DefaultTextExtractor();", rowset_name, extract_schema, expanded_streams[i].Stream);
                    sb.AppendFormat(@"{0} = SELECT *", rowset_name);
                    foreach (var exp in this.Expansions)
                    {
                        var exp_value = exp_stream.ExpansionValues[exp.Name];
                        sb.AppendFormat(@" , {0} ", exp.GetVirtualColumn(exp_value));
                    }
                    sb.AppendFormat(@"FROM {0}_before_vc;", rowset_name);
                }
                else
                {
                    sb.AppendFormat(@"{0} = EXTRACT {1} FROM ""{2}"" USING DefaultTextExtractor();", rowset_name, extract_schema, expanded_streams[i].Stream);
                }
            }

            sb.Append("\r\n");
            sb.Append("\r\n");
            sb.AppendFormat(@"data = {0};", string.Join(" UNION ALL ", rowsets.Select(r => string.Format("SELECT * FROM {0}", r)).ToArray()));

            sb.Append(@"END;");

            return sb.ToString();
        }

        private List<ExpandedStream> Expand()
        {
            var streams = new List<ExpandedStream>();
            var expansion_values = new Dictionary<string,ExpandedValue>();
            this._Expand(0,this.Template,streams,expansion_values);
            return streams;
        }
        private void _Expand(int depth, string t, List<ExpandedStream> items, Dictionary<string, ExpandedValue> exp_values)
        {
            var exp = this.Expansions[depth];
            foreach (var exp_value in exp.Items)
            {
                exp_values[exp.Name] = exp_value;
                var stream = exp.Replace(t, exp_value);

                if (depth < (this.Expansions.Count-1))
                {
                    this._Expand(depth+1,stream,items,exp_values);   
                }

                if (depth == this.Expansions.Count - 1)
                {
                    var item = new ExpandedStream();
                    item.Stream = stream;
                    item.ExpansionValues = new Dictionary<string,ExpandedValue>();
                    foreach (var kv in exp_values)
                    {
                        item.ExpansionValues[kv.Key] = kv.Value;
                    }
                    items.Add(item);                    
                }
            }
        }
    }

    public class ExpandedStream
    {
        public string Stream;
        public Dictionary<string, ExpandedValue> ExpansionValues;
    }
}
